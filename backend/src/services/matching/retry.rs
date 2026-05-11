//! Retry mechanism for database operations
//!
//! Provides exponential backoff retry logic for transient failures.
//! Used primarily for trade persistence to handle temporary database issues.

use std::fmt::Display;
use std::future::Future;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, warn};

use crate::constants::retry as retry_constants;

/// Configuration for retry behavior
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Initial delay in milliseconds before first retry
    pub initial_delay_ms: u64,
    /// Maximum delay in milliseconds (caps exponential growth)
    pub max_delay_ms: u64,
    /// Multiplier for exponential backoff (typically 2.0)
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: retry_constants::MAX_ATTEMPTS,
            initial_delay_ms: retry_constants::INITIAL_DELAY_MS,
            max_delay_ms: retry_constants::MAX_DELAY_MS,
            backoff_multiplier: retry_constants::BACKOFF_MULTIPLIER,
        }
    }
}

impl RetryConfig {
    /// Create a new retry config with custom values
    #[allow(dead_code)]
    pub fn new(
        max_attempts: u32,
        initial_delay_ms: u64,
        max_delay_ms: u64,
        backoff_multiplier: f64,
    ) -> Self {
        Self {
            max_attempts,
            initial_delay_ms,
            max_delay_ms,
            backoff_multiplier,
        }
    }

    /// Create a config for critical operations (more retries, longer delays)
    #[allow(dead_code)]
    pub fn critical() -> Self {
        Self {
            max_attempts: 5,
            initial_delay_ms: 200,
            max_delay_ms: 10000,
            backoff_multiplier: 2.0,
        }
    }

    /// Create a config for quick operations (fewer retries, shorter delays)
    #[allow(dead_code)]
    pub fn quick() -> Self {
        Self {
            max_attempts: 2,
            initial_delay_ms: 50,
            max_delay_ms: 1000,
            backoff_multiplier: 2.0,
        }
    }
}

/// Result of a retry operation
#[allow(dead_code)]
#[derive(Debug)]
pub struct RetryResult<T, E> {
    /// The final result (success or last error)
    pub result: Result<T, E>,
    /// Number of attempts made
    pub attempts: u32,
    /// Total time spent in retries (milliseconds)
    pub total_delay_ms: u64,
}

/// Execute an async operation with retry logic
///
/// # Arguments
/// * `config` - Retry configuration
/// * `operation_name` - Name for logging purposes
/// * `operation` - The async operation to retry
///
/// # Returns
/// The result of the operation, or the last error after all retries exhausted
///
/// # Example
/// ```ignore
/// let result = with_retry(
///     &RetryConfig::default(),
///     "persist_trade",
///     || async { persist_trade(&pool, &trade).await }
/// ).await;
/// ```
pub async fn with_retry<F, Fut, T, E>(
    config: &RetryConfig,
    operation_name: &str,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Display,
{
    let mut attempt = 0;
    let mut delay = config.initial_delay_ms;

    loop {
        attempt += 1;

        match operation().await {
            Ok(result) => {
                if attempt > 1 {
                    debug!(
                        "{} succeeded on attempt {} after retries",
                        operation_name, attempt
                    );
                }
                return Ok(result);
            }
            Err(error) => {
                if attempt >= config.max_attempts {
                    error!(
                        "❌ {} failed after {} attempts: {}",
                        operation_name, attempt, error
                    );
                    return Err(error);
                }

                warn!(
                    "⚠️ {} attempt {}/{} failed: {}. Retrying in {}ms...",
                    operation_name, attempt, config.max_attempts, error, delay
                );

                sleep(Duration::from_millis(delay)).await;

                // Exponential backoff with cap
                delay = ((delay as f64) * config.backoff_multiplier) as u64;
                delay = delay.min(config.max_delay_ms);
            }
        }
    }
}

/// Execute an async operation with retry logic and return detailed result
///
/// Similar to `with_retry` but returns additional metadata about the retry process.
#[allow(dead_code)]
pub async fn with_retry_detailed<F, Fut, T, E>(
    config: &RetryConfig,
    operation_name: &str,
    mut operation: F,
) -> RetryResult<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Display,
{
    let mut attempt = 0;
    let mut delay = config.initial_delay_ms;
    let mut total_delay_ms = 0u64;

    loop {
        attempt += 1;

        match operation().await {
            Ok(result) => {
                if attempt > 1 {
                    debug!(
                        "{} succeeded on attempt {} after {}ms total delay",
                        operation_name, attempt, total_delay_ms
                    );
                }
                return RetryResult {
                    result: Ok(result),
                    attempts: attempt,
                    total_delay_ms,
                };
            }
            Err(error) => {
                if attempt >= config.max_attempts {
                    error!(
                        "❌ {} failed after {} attempts ({}ms total delay): {}",
                        operation_name, attempt, total_delay_ms, error
                    );
                    return RetryResult {
                        result: Err(error),
                        attempts: attempt,
                        total_delay_ms,
                    };
                }

                warn!(
                    "⚠️ {} attempt {}/{} failed: {}. Retrying in {}ms...",
                    operation_name, attempt, config.max_attempts, error, delay
                );

                sleep(Duration::from_millis(delay)).await;
                total_delay_ms += delay;

                // Exponential backoff with cap
                delay = ((delay as f64) * config.backoff_multiplier) as u64;
                delay = delay.min(config.max_delay_ms);
            }
        }
    }
}

/// Check if an error is retryable
///
/// Some errors (like constraint violations) should not be retried,
/// while others (like connection timeouts) should be.
#[allow(dead_code)]
pub fn is_retryable_sqlx_error(error: &sqlx::Error) -> bool {
    match error {
        // Connection errors are retryable
        sqlx::Error::Io(_) => true,
        sqlx::Error::PoolTimedOut => true,
        sqlx::Error::PoolClosed => false, // Pool closed = shutdown, don't retry

        // Database errors
        sqlx::Error::Database(db_err) => {
            // PostgreSQL error codes that are retryable
            if let Some(code) = db_err.code() {
                match code.as_ref() {
                    // Connection/network issues
                    "08000" | "08003" | "08006" | "08001" | "08004" | "08007" | "08P01" => true,
                    // Serialization failure (can retry)
                    "40001" => true,
                    // Deadlock detected (can retry)
                    "40P01" => true,
                    // Lock not available (can retry)
                    "55P03" => true,
                    // Statement timeout (might succeed on retry)
                    "57014" => true,
                    // Admin shutdown (don't retry)
                    "57P01" | "57P02" | "57P03" => false,
                    // Constraint violations (don't retry - will always fail)
                    "23000" | "23001" | "23502" | "23503" | "23505" | "23514" => false,
                    // Syntax errors (don't retry)
                    "42000" | "42601" | "42602" | "42P01" => false,
                    // Default: don't retry unknown errors
                    _ => false,
                }
            } else {
                false
            }
        }

        // Other errors - generally don't retry
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_retry_succeeds_first_try() {
        let config = RetryConfig::default();
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let result: Result<i32, String> = with_retry(&config, "test_op", || {
            let count = Arc::clone(&call_count_clone);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_succeeds_after_failures() {
        let config = RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 10,
            max_delay_ms: 100,
            backoff_multiplier: 2.0,
        };

        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let result: Result<i32, String> = with_retry(&config, "test_op", || {
            let count = Arc::clone(&call_count_clone);
            async move {
                let current = count.fetch_add(1, Ordering::SeqCst);
                if current < 2 {
                    Err("temporary failure".to_string())
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let config = RetryConfig {
            max_attempts: 2,
            initial_delay_ms: 10,
            max_delay_ms: 100,
            backoff_multiplier: 2.0,
        };

        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let result: Result<i32, String> = with_retry(&config, "test_op", || {
            let count = Arc::clone(&call_count_clone);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<i32, String>("permanent failure".to_string())
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_retry_detailed() {
        let config = RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 10,
            max_delay_ms: 100,
            backoff_multiplier: 2.0,
        };

        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let retry_result = with_retry_detailed(&config, "test_op", || {
            let count = Arc::clone(&call_count_clone);
            async move {
                let current = count.fetch_add(1, Ordering::SeqCst);
                if current < 1 {
                    Err::<i32, String>("temp".to_string())
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert!(retry_result.result.is_ok());
        assert_eq!(retry_result.attempts, 2);
        assert!(retry_result.total_delay_ms >= 10);
    }
}
