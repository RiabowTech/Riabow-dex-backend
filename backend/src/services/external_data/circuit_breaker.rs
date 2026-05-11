//! Lock-free circuit breaker (all atomic, zero contention).
//!
//! State machine: `Closed → (N failures) → Open → (timeout) → HalfOpen → (success) → Closed`

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Closed,
    Open,
    HalfOpen,
}

pub struct CircuitBreaker {
    consecutive_failures: AtomicU32,
    last_failure_epoch_ms: AtomicU64,
    failure_threshold: u32,
    recovery_timeout_ms: u64,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, recovery_timeout_ms: u64) -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            last_failure_epoch_ms: AtomicU64::new(0),
            failure_threshold,
            recovery_timeout_ms,
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    pub fn state(&self) -> State {
        let failures = self.consecutive_failures.load(Relaxed);
        if failures < self.failure_threshold {
            return State::Closed;
        }
        let last = self.last_failure_epoch_ms.load(Relaxed);
        if Self::now_ms().saturating_sub(last) >= self.recovery_timeout_ms {
            State::HalfOpen
        } else {
            State::Open
        }
    }

    /// Returns `true` if the request should be allowed through.
    pub fn allow_request(&self) -> bool {
        match self.state() {
            State::Closed => true,
            State::HalfOpen => true,  // let one probe through
            State::Open => false,
        }
    }

    /// Record a successful call — resets the failure counter.
    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Relaxed);
    }

    /// Record a failed call — increments failures and stamps the time.
    pub fn record_failure(&self) {
        self.consecutive_failures.fetch_add(1, Relaxed);
        self.last_failure_epoch_ms.store(Self::now_ms(), Relaxed);
    }
}
