//! Internationalization (i18n) module for response messages
//!
//! Centralized management of all user-facing messages.
//! Default language: English (en)
//! Supported languages: en, zh

use std::collections::HashMap;
use std::sync::OnceLock;

#[allow(dead_code)]
pub mod messages;

/// Global message store (initialized once on first access)
static MESSAGES: OnceLock<MessageStore> = OnceLock::new();

fn get_messages() -> &'static MessageStore {
    MESSAGES.get_or_init(MessageStore::new)
}

/// Supported languages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Language {
    En, // English (default)
    Zh, // Chinese
}

impl Default for Language {
    fn default() -> Self {
        Language::En
    }
}

impl From<&str> for Language {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "zh" | "zh-cn" | "zh-tw" | "chinese" => Language::Zh,
            _ => Language::En,
        }
    }
}

/// Message keys for all user-facing messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageKey {
    // Auth errors
    AuthFailed,
    AuthTimestampExpired,
    AuthUserNotFound,
    AuthSignatureInvalid,
    AuthSignatureFormatInvalid,
    AuthJwtGenerationFailed,
    AuthUnauthorized,
    AuthApiKeyInvalid,

    // Database errors
    DatabaseError,

    // Validation errors
    ValidationFailed,
    InvalidAddress,
    InvalidAmount,
    InvalidSymbol,

    // Order errors
    OrderNotFound,
    OrderAlreadyFilled,
    OrderCancelled,
    InsufficientBalance,
    InsufficientMargin,

    // Position errors
    PositionNotFound,
    PositionTooSmall,
    LeverageTooHigh,

    // Withdraw errors
    WithdrawFailed,
    WithdrawPending,

    // Earn errors
    EarnProductNotFound,
    EarnSubscriptionFailed,
    EarnClaimFailed,
    EarnProductNotActive,
    EarnQuotaExceeded,

    // General errors
    InternalError,
    NotFound,
    BadRequest,
    RateLimitExceeded,
    ServiceUnavailable,
}

/// Message store holding all translations
pub struct MessageStore {
    messages: HashMap<(Language, MessageKey), &'static str>,
}

impl MessageStore {
    fn new() -> Self {
        let mut messages = HashMap::new();

        // =========================================================================
        // English messages (default)
        // =========================================================================

        // Auth
        messages.insert((Language::En, MessageKey::AuthFailed), "Authentication failed");
        messages.insert((Language::En, MessageKey::AuthTimestampExpired), "Timestamp expired");
        messages.insert((Language::En, MessageKey::AuthUserNotFound), "User not found, please get nonce first");
        messages.insert((Language::En, MessageKey::AuthSignatureInvalid), "Signature verification failed");
        messages.insert((Language::En, MessageKey::AuthSignatureFormatInvalid), "Invalid signature format");
        messages.insert((Language::En, MessageKey::AuthJwtGenerationFailed), "Failed to generate token");
        messages.insert((Language::En, MessageKey::AuthUnauthorized), "Unauthorized");
        messages.insert((Language::En, MessageKey::AuthApiKeyInvalid), "Invalid API key");

        // Database
        messages.insert((Language::En, MessageKey::DatabaseError), "Database error");

        // Validation
        messages.insert((Language::En, MessageKey::ValidationFailed), "Validation failed");
        messages.insert((Language::En, MessageKey::InvalidAddress), "Invalid address");
        messages.insert((Language::En, MessageKey::InvalidAmount), "Invalid amount");
        messages.insert((Language::En, MessageKey::InvalidSymbol), "Invalid trading pair");

        // Order
        messages.insert((Language::En, MessageKey::OrderNotFound), "Order not found");
        messages.insert((Language::En, MessageKey::OrderAlreadyFilled), "Order already filled");
        messages.insert((Language::En, MessageKey::OrderCancelled), "Order cancelled");
        messages.insert((Language::En, MessageKey::InsufficientBalance), "Insufficient balance");
        messages.insert((Language::En, MessageKey::InsufficientMargin), "Insufficient margin");

        // Position
        messages.insert((Language::En, MessageKey::PositionNotFound), "Position not found");
        messages.insert((Language::En, MessageKey::PositionTooSmall), "Position size too small");
        messages.insert((Language::En, MessageKey::LeverageTooHigh), "Leverage exceeds maximum");

        // Withdraw
        messages.insert((Language::En, MessageKey::WithdrawFailed), "Withdrawal failed");
        messages.insert((Language::En, MessageKey::WithdrawPending), "Withdrawal pending");

        // Earn
        messages.insert((Language::En, MessageKey::EarnProductNotFound), "Product not found");
        messages.insert((Language::En, MessageKey::EarnSubscriptionFailed), "Subscription failed");
        messages.insert((Language::En, MessageKey::EarnClaimFailed), "Claim failed");
        messages.insert((Language::En, MessageKey::EarnProductNotActive), "Product not active");
        messages.insert((Language::En, MessageKey::EarnQuotaExceeded), "Quota exceeded");

        // General
        messages.insert((Language::En, MessageKey::InternalError), "Internal server error");
        messages.insert((Language::En, MessageKey::NotFound), "Not found");
        messages.insert((Language::En, MessageKey::BadRequest), "Bad request");
        messages.insert((Language::En, MessageKey::RateLimitExceeded), "Rate limit exceeded");
        messages.insert((Language::En, MessageKey::ServiceUnavailable), "Service unavailable");

        // =========================================================================
        // Chinese messages
        // =========================================================================

        // Auth
        messages.insert((Language::Zh, MessageKey::AuthFailed), "认证失败");
        messages.insert((Language::Zh, MessageKey::AuthTimestampExpired), "时间戳已过期");
        messages.insert((Language::Zh, MessageKey::AuthUserNotFound), "用户不存在，请先获取nonce");
        messages.insert((Language::Zh, MessageKey::AuthSignatureInvalid), "签名验证失败");
        messages.insert((Language::Zh, MessageKey::AuthSignatureFormatInvalid), "签名格式无效");
        messages.insert((Language::Zh, MessageKey::AuthJwtGenerationFailed), "Token生成失败");
        messages.insert((Language::Zh, MessageKey::AuthUnauthorized), "未授权");
        messages.insert((Language::Zh, MessageKey::AuthApiKeyInvalid), "API Key无效");

        // Database
        messages.insert((Language::Zh, MessageKey::DatabaseError), "数据库错误");

        // Validation
        messages.insert((Language::Zh, MessageKey::ValidationFailed), "验证失败");
        messages.insert((Language::Zh, MessageKey::InvalidAddress), "地址无效");
        messages.insert((Language::Zh, MessageKey::InvalidAmount), "金额无效");
        messages.insert((Language::Zh, MessageKey::InvalidSymbol), "交易对无效");

        // Order
        messages.insert((Language::Zh, MessageKey::OrderNotFound), "订单不存在");
        messages.insert((Language::Zh, MessageKey::OrderAlreadyFilled), "订单已成交");
        messages.insert((Language::Zh, MessageKey::OrderCancelled), "订单已取消");
        messages.insert((Language::Zh, MessageKey::InsufficientBalance), "余额不足");
        messages.insert((Language::Zh, MessageKey::InsufficientMargin), "保证金不足");

        // Position
        messages.insert((Language::Zh, MessageKey::PositionNotFound), "仓位不存在");
        messages.insert((Language::Zh, MessageKey::PositionTooSmall), "仓位太小");
        messages.insert((Language::Zh, MessageKey::LeverageTooHigh), "杠杆超过最大值");

        // Withdraw
        messages.insert((Language::Zh, MessageKey::WithdrawFailed), "提款失败");
        messages.insert((Language::Zh, MessageKey::WithdrawPending), "提款处理中");

        // Earn
        messages.insert((Language::Zh, MessageKey::EarnProductNotFound), "产品不存在");
        messages.insert((Language::Zh, MessageKey::EarnSubscriptionFailed), "申购失败");
        messages.insert((Language::Zh, MessageKey::EarnClaimFailed), "领取失败");
        messages.insert((Language::Zh, MessageKey::EarnProductNotActive), "产品未激活");
        messages.insert((Language::Zh, MessageKey::EarnQuotaExceeded), "超出额度");

        // General
        messages.insert((Language::Zh, MessageKey::InternalError), "服务器内部错误");
        messages.insert((Language::Zh, MessageKey::NotFound), "未找到");
        messages.insert((Language::Zh, MessageKey::BadRequest), "请求无效");
        messages.insert((Language::Zh, MessageKey::RateLimitExceeded), "请求频率超限");
        messages.insert((Language::Zh, MessageKey::ServiceUnavailable), "服务不可用");

        Self { messages }
    }

    /// Get message by key and language
    pub fn get(&self, lang: Language, key: MessageKey) -> &'static str {
        self.messages
            .get(&(lang, key))
            .or_else(|| self.messages.get(&(Language::En, key))) // Fallback to English
            .copied()
            .unwrap_or("Unknown error")
    }
}

/// Get a message for the given key (default: English)
pub fn msg(key: MessageKey) -> &'static str {
    get_messages().get(Language::En, key)
}

/// Get a message for the given key and language
#[allow(dead_code)]
pub fn msg_lang(lang: Language, key: MessageKey) -> &'static str {
    get_messages().get(lang, key)
}

/// Get a message for the given key and language string
#[allow(dead_code)]
pub fn msg_for(lang_str: &str, key: MessageKey) -> &'static str {
    get_messages().get(Language::from(lang_str), key)
}

