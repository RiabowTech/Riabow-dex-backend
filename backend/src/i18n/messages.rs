//! Error codes for API responses
//!
//! Centralized error code definitions for consistent API responses.

/// Error code constants
pub mod codes {
    // Auth errors (1xxx)
    pub const AUTH_FAILED: &str = "AUTH_FAILED";
    pub const TIMESTAMP_EXPIRED: &str = "TIMESTAMP_EXPIRED";
    pub const USER_NOT_FOUND: &str = "USER_NOT_FOUND";
    pub const SIGNATURE_INVALID: &str = "SIGNATURE_INVALID";
    pub const SIGNATURE_FORMAT_INVALID: &str = "INVALID_SIGNATURE_FORMAT";
    pub const JWT_GENERATION_FAILED: &str = "JWT_GENERATION_FAILED";
    pub const UNAUTHORIZED: &str = "UNAUTHORIZED";
    pub const API_KEY_INVALID: &str = "API_KEY_INVALID";

    // Database errors (2xxx)
    pub const DATABASE_ERROR: &str = "DATABASE_ERROR";

    // Validation errors (3xxx)
    pub const VALIDATION_FAILED: &str = "VALIDATION_FAILED";
    pub const INVALID_ADDRESS: &str = "INVALID_ADDRESS";
    pub const INVALID_AMOUNT: &str = "INVALID_AMOUNT";
    pub const INVALID_SYMBOL: &str = "INVALID_SYMBOL";

    // Order errors (4xxx)
    pub const ORDER_NOT_FOUND: &str = "ORDER_NOT_FOUND";
    pub const ORDER_ALREADY_FILLED: &str = "ORDER_ALREADY_FILLED";
    pub const ORDER_CANCELLED: &str = "ORDER_CANCELLED";
    pub const INSUFFICIENT_BALANCE: &str = "INSUFFICIENT_BALANCE";
    pub const INSUFFICIENT_MARGIN: &str = "INSUFFICIENT_MARGIN";

    // Position errors (5xxx)
    pub const POSITION_NOT_FOUND: &str = "POSITION_NOT_FOUND";
    pub const POSITION_TOO_SMALL: &str = "POSITION_TOO_SMALL";
    pub const LEVERAGE_TOO_HIGH: &str = "LEVERAGE_TOO_HIGH";

    // Withdraw errors (6xxx)
    pub const WITHDRAW_FAILED: &str = "WITHDRAW_FAILED";
    pub const WITHDRAW_PENDING: &str = "WITHDRAW_PENDING";

    // Earn errors (7xxx)
    pub const EARN_PRODUCT_NOT_FOUND: &str = "EARN_PRODUCT_NOT_FOUND";
    pub const EARN_SUBSCRIPTION_FAILED: &str = "EARN_SUBSCRIPTION_FAILED";
    pub const EARN_CLAIM_FAILED: &str = "EARN_CLAIM_FAILED";
    pub const EARN_PRODUCT_NOT_ACTIVE: &str = "EARN_PRODUCT_NOT_ACTIVE";
    pub const EARN_QUOTA_EXCEEDED: &str = "EARN_QUOTA_EXCEEDED";

    // General errors (9xxx)
    pub const INTERNAL_ERROR: &str = "INTERNAL_ERROR";
    pub const NOT_FOUND: &str = "NOT_FOUND";
    pub const BAD_REQUEST: &str = "BAD_REQUEST";
    pub const RATE_LIMIT_EXCEEDED: &str = "RATE_LIMIT_EXCEEDED";
    pub const SERVICE_UNAVAILABLE: &str = "SERVICE_UNAVAILABLE";
}
