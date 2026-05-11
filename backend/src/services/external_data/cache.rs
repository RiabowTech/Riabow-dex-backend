use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use crate::api::handlers::market::TickerResponse;

/// Cache entry with expiration
#[derive(Debug, Clone)]
struct CachedData {
    data: HashMap<String, TickerResponse>,
    timestamp: Instant,
}

/// External market data cache with TTL
/// Prevents excessive API calls to Hyperliquid
#[derive(Debug, Clone)]
pub struct ExternalMarketCache {
    cache: Arc<RwLock<Option<CachedData>>>,
    ttl: Duration,
}

impl ExternalMarketCache {
    /// Create a new cache with specified TTL
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            cache: Arc::new(RwLock::new(None)),
            ttl: Duration::from_secs(ttl_secs),
        }
    }
    
    /// Create with default 60 second TTL
    #[allow(dead_code)]
    pub fn default() -> Self {
        Self::new(60)
    }
    
    /// Get cached data if it's still valid
    pub async fn get(&self) -> Option<HashMap<String, TickerResponse>> {
        let cache = self.cache.read().await;
        
        if let Some(cached) = cache.as_ref() {
            if cached.timestamp.elapsed() < self.ttl {
                return Some(cached.data.clone());
            }
        }
        
        None
    }
    
    /// Set/update cached data
    pub async fn set(&self, data: HashMap<String, TickerResponse>) {
        let mut cache = self.cache.write().await;
        *cache = Some(CachedData {
            data,
            timestamp: Instant::now(),
        });
    }
    
    /// Clear the cache
    #[allow(dead_code)]
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        *cache = None;
    }
    
    /// Check if cache has valid data
    #[allow(dead_code)]
    pub async fn is_valid(&self) -> bool {
        let cache = self.cache.read().await;
        
        if let Some(cached) = cache.as_ref() {
            cached.timestamp.elapsed() < self.ttl
        } else {
            false
        }
    }
}
