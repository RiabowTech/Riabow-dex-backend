//! TTL cache with request coalescing.
//!
//! When multiple callers request the same key concurrently, only **one** fetch
//! is executed; all others await the result via a `tokio::sync::watch` channel.

use dashmap::DashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// A single cache entry with its insertion time.
struct CacheEntry<T> {
    value: Arc<T>,
    inserted_at: Instant,
}

/// Generic coalescing cache.
///
/// * `T` must be `Clone + Send + Sync + 'static`.
/// * TTL controls freshness.  `get_stale` ignores TTL (used for circuit-breaker fallback).
pub struct CoalescingCache<T: Clone + Send + Sync + 'static> {
    entries: DashMap<String, CacheEntry<T>>,
    in_flight: DashMap<String, watch::Receiver<Option<Arc<T>>>>,
    ttl: Duration,
}

impl<T: Clone + Send + Sync + 'static> CoalescingCache<T> {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: DashMap::new(),
            in_flight: DashMap::new(),
            ttl,
        }
    }

    /// Return cached value if still within TTL.
    fn get_fresh(&self, key: &str) -> Option<Arc<T>> {
        let entry = self.entries.get(key)?;
        if entry.inserted_at.elapsed() < self.ttl {
            Some(Arc::clone(&entry.value))
        } else {
            None
        }
    }

    /// Return cached value regardless of TTL (for stale-while-revalidate).
    pub fn get_stale(&self, key: &str) -> Option<Arc<T>> {
        self.entries.get(key).map(|e| Arc::clone(&e.value))
    }

    /// Get the value for `key`, fetching it with `fetcher` if not cached.
    ///
    /// Concurrent calls for the same key will coalesce: only one `fetcher`
    /// invocation happens; the rest await the result.
    pub async fn get_or_fetch<F, Fut, E>(
        &self,
        key: &str,
        fetcher: F,
    ) -> Result<Arc<T>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>> + Send,
        E: Send + 'static,
    {
        // 1. Fast path — cache hit
        if let Some(val) = self.get_fresh(key) {
            return Ok(val);
        }

        // 2. Check if another task is already fetching this key
        if let Some(rx_ref) = self.in_flight.get(key) {
            let mut rx = rx_ref.clone();
            drop(rx_ref); // release DashMap read lock
            // Wait for the in-flight request to complete
            let _ = rx.changed().await;
            let val = { rx.borrow().clone() };
            if let Some(val) = val {
                return Ok(val);
            }
            // The in-flight fetch failed; fall through and try ourselves
        }

        // 3. We are the leader — create a watch channel
        let (tx, rx) = watch::channel(None);
        self.in_flight.insert(key.to_string(), rx);

        // 4. Execute the fetch
        let result = fetcher().await;

        // 5. Publish result and clean up
        match result {
            Ok(value) => {
                let arc = Arc::new(value);
                self.entries.insert(
                    key.to_string(),
                    CacheEntry {
                        value: Arc::clone(&arc),
                        inserted_at: Instant::now(),
                    },
                );
                let _ = tx.send(Some(Arc::clone(&arc)));
                self.in_flight.remove(key);
                Ok(arc)
            }
            Err(e) => {
                let _ = tx.send(None); // signal failure
                self.in_flight.remove(key);
                Err(e)
            }
        }
    }
}
