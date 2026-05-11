//! Alert Service for sending notifications via Telegram and Email

use lettre::message::header::ContentType;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Alert configuration loaded from environment variables
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AlertConfig {
    /// Enable/disable alert system
    #[serde(default)]
    pub enabled: bool,

    // Gmail configuration
    #[serde(default)]
    pub gmail_user: Option<String>,
    #[serde(default)]
    pub gmail_password: Option<String>,
    #[serde(default)]
    pub alert_email: Option<String>,

    // Telegram configuration
    #[serde(default)]
    pub telegram_bot_token: Option<String>,
    #[serde(default)]
    pub telegram_chat_id: Option<String>,
}

impl AlertConfig {
    /// Load alert configuration from environment variables
    pub fn from_env() -> Self {
        Self {
            enabled: std::env::var("ALERT_ENABLED")
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(true), // Default enabled if any credentials are set
            gmail_user: std::env::var("GMAIL_USER").ok(),
            gmail_password: std::env::var("GMAIL_PASSWORD").ok(),
            alert_email: std::env::var("ALERT_EMAIL").ok(),
            telegram_bot_token: std::env::var("TELEGRAM_BOT_TOKEN").ok(),
            telegram_chat_id: std::env::var("TELEGRAM_CHAT_ID").ok(),
        }
    }

    /// Check if Gmail is configured
    pub fn is_gmail_configured(&self) -> bool {
        self.gmail_user.is_some()
            && self.gmail_password.is_some()
            && self.alert_email.is_some()
    }

    /// Check if Telegram is configured
    pub fn is_telegram_configured(&self) -> bool {
        self.telegram_bot_token.is_some() && self.telegram_chat_id.is_some()
    }

    /// Check if any alert channel is configured
    pub fn is_any_configured(&self) -> bool {
        self.is_gmail_configured() || self.is_telegram_configured()
    }
}

/// Alert service for sending notifications
pub struct AlertService {
    config: AlertConfig,
    http_client: Client,
    /// Rate limiting: last alert timestamp
    last_alert_time: Arc<RwLock<Option<std::time::Instant>>>,
    /// Minimum interval between alerts (in seconds)
    min_alert_interval_secs: u64,
}

impl AlertService {
    /// Create a new alert service with configuration
    pub fn new(config: AlertConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
            last_alert_time: Arc::new(RwLock::new(None)),
            min_alert_interval_secs: 60, // Default: 1 minute between alerts
        }
    }

    /// Create alert service from environment variables
    pub fn from_env() -> Self {
        Self::new(AlertConfig::from_env())
    }

    /// Check if alerts are enabled and configured
    pub fn is_enabled(&self) -> bool {
        self.config.enabled && self.config.is_any_configured()
    }

    /// Check rate limiting - returns true if enough time has passed since last alert
    async fn check_rate_limit(&self) -> bool {
        let last_time = self.last_alert_time.read().await;
        match *last_time {
            Some(t) => t.elapsed().as_secs() >= self.min_alert_interval_secs,
            None => true,
        }
    }

    /// Update last alert time
    async fn update_last_alert_time(&self) {
        let mut last_time = self.last_alert_time.write().await;
        *last_time = Some(std::time::Instant::now());
    }

    /// Send message to Telegram
    async fn send_telegram_message(&self, message: &str) -> anyhow::Result<()> {
        let bot_token = self
            .config
            .telegram_bot_token
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Telegram bot token not configured"))?;

        let chat_id = self
            .config
            .telegram_chat_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Telegram chat ID not configured"))?;

        let url = format!("https://api.telegram.org/bot{}/sendMessage", bot_token);

        let response = self
            .http_client
            .post(&url)
            .json(&serde_json::json!({
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML"
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Telegram API error: {} - {}",
                status,
                body
            ));
        }

        Ok(())
    }

    /// Send email alert via Gmail SMTP
    async fn send_email_alert(&self, subject: &str, body: &str) -> anyhow::Result<()> {
        let gmail_user = self
            .config
            .gmail_user
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Gmail user not configured"))?;

        let gmail_password = self
            .config
            .gmail_password
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Gmail password not configured"))?;

        let alert_email = self
            .config
            .alert_email
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Alert email not configured"))?;

        let email = Message::builder()
            .from(gmail_user.parse()?)
            .to(alert_email.parse()?)
            .subject(subject)
            .header(ContentType::TEXT_PLAIN)
            .body(body.to_string())?;

        let creds = Credentials::new(gmail_user.clone(), gmail_password.clone());

        let mailer: AsyncSmtpTransport<Tokio1Executor> =
            AsyncSmtpTransport::<Tokio1Executor>::relay("smtp.gmail.com")?
                .credentials(creds)
                .build();

        mailer.send(email).await?;

        Ok(())
    }

    /// Send email to a specific recipient (e.g. user liquidation notification)
    pub async fn send_email_to(&self, to_email: &str, subject: &str, body: &str) -> anyhow::Result<()> {
        let gmail_user = self
            .config
            .gmail_user
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Gmail user not configured"))?;

        let gmail_password = self
            .config
            .gmail_password
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Gmail password not configured"))?;

        let email = Message::builder()
            .from(gmail_user.parse()?)
            .to(to_email.parse()?)
            .subject(subject)
            .header(ContentType::TEXT_HTML)
            .body(body.to_string())?;

        let creds = Credentials::new(gmail_user.clone(), gmail_password.clone());

        let mailer: AsyncSmtpTransport<Tokio1Executor> =
            AsyncSmtpTransport::<Tokio1Executor>::relay("smtp.gmail.com")?
                .credentials(creds)
                .build();

        mailer.send(email).await?;
        Ok(())
    }

    /// Send a generic alert message
    pub async fn send_alert(&self, subject: &str, message: &str) -> anyhow::Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        if !self.check_rate_limit().await {
            tracing::debug!("Alert rate limited, skipping");
            return Ok(());
        }

        let mut success = false;

        // Send to Telegram
        if self.config.is_telegram_configured() {
            if let Err(e) = self.send_telegram_message(message).await {
                tracing::error!("Failed to send Telegram alert: {}", e);
            } else {
                success = true;
            }
        }

        // Send to Gmail
        if self.config.is_gmail_configured() {
            if let Err(e) = self.send_email_alert(subject, message).await {
                tracing::error!("Failed to send Email alert: {}", e);
            } else {
                success = true;
            }
        }

        if success {
            self.update_last_alert_time().await;
        }

        Ok(())
    }
}

/// Global alert service instance for convenience
#[allow(dead_code)]
static ALERT_SERVICE: std::sync::OnceLock<AlertService> = std::sync::OnceLock::new();

/// Initialize the global alert service
#[allow(dead_code)]
pub fn init_alert_service() -> &'static AlertService {
    ALERT_SERVICE.get_or_init(AlertService::from_env)
}

/// Get the global alert service
#[allow(dead_code)]
pub fn get_alert_service() -> Option<&'static AlertService> {
    ALERT_SERVICE.get()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alert_config_from_env() {
        // Test with no env vars set
        let config = AlertConfig::default();
        assert!(!config.is_gmail_configured());
        assert!(!config.is_telegram_configured());
    }

}
