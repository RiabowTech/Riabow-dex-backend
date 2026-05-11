//! Safe arithmetic operations to prevent division by zero
//!
//! Provides macros and utilities for safe division that log errors
//! and prevent panics from division by zero.


/// Safe division for Decimal types with error logging
///
/// Returns ZERO if divisor is zero, logs an error with context
///
/// # Example
/// ```ignore
/// let result = safe_div!(numerator, denominator, "calculating price percentage");
/// ```
#[macro_export]
macro_rules! safe_div {
    ($num:expr, $den:expr, $context:expr) => {{
        let numerator = $num;
        let denominator = $den;
        if denominator == Decimal::ZERO || denominator.is_zero() {
            tracing::error!(
                "Division by zero prevented: {} (numerator: {}, denominator: {})",
                $context,
                numerator,
                denominator
            );
            Decimal::ZERO
        } else {
            numerator / denominator
        }
    }};
}

/// Safe division for Decimal types with custom default value
///
/// Returns the specified default value if divisor is zero
///
/// # Example
/// ```ignore
/// let result = safe_div_or!(numerator, denominator, Decimal::ONE, "calculating ratio");
/// ```
#[macro_export]
macro_rules! safe_div_or {
    ($num:expr, $den:expr, $default:expr, $context:expr) => {{
        let numerator = $num;
        let denominator = $den;
        if denominator == Decimal::ZERO || denominator.is_zero() {
            tracing::warn!(
                "Division by zero, using default {}: {} (numerator: {}, denominator: {})",
                $default,
                $context,
                numerator,
                denominator
            );
            $default
        } else {
            numerator / denominator
        }
    }};
}

/// Safe division for primitive types (f64, i32, etc.)
///
/// Returns 0.0 if divisor is zero
#[macro_export]
macro_rules! safe_div_f64 {
    ($num:expr, $den:expr, $context:expr) => {{
        let numerator = $num;
        let denominator = $den;
        if denominator == 0.0 || denominator.abs() < f64::EPSILON {
            tracing::error!(
                "Division by zero prevented (f64): {} (numerator: {}, denominator: {})",
                $context,
                numerator,
                denominator
            );
            0.0
        } else {
            numerator / denominator
        }
    }};
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    #[test]
    fn test_safe_div_normal() {
        let result = safe_div!(dec!(10), dec!(2), "test");
        assert_eq!(result, dec!(5));
    }

    #[test]
    fn test_safe_div_by_zero() {
        let result = safe_div!(dec!(10), Decimal::ZERO, "test");
        assert_eq!(result, Decimal::ZERO);
    }

    #[test]
    fn test_safe_div_or_normal() {
        let result = safe_div_or!(dec!(10), dec!(2), dec!(99), "test");
        assert_eq!(result, dec!(5));
    }

    #[test]
    fn test_safe_div_or_by_zero() {
        let result = safe_div_or!(dec!(10), Decimal::ZERO, dec!(99), "test");
        assert_eq!(result, dec!(99));
    }
}
