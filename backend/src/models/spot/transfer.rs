use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct SpotInternalTransfer {
    pub id: Uuid,
    pub user_address: String,
    pub direction: String,           // "perp_to_spot" | "spot_to_perp"
    pub token: String,
    pub amount: Decimal,
    pub perp_balance_before: Decimal,
    pub spot_balance_before: Decimal,
    pub created_at: DateTime<Utc>,
}

/// Convenience direction enum used at the service layer; the DB column is plain text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferDirection {
    PerpToSpot,
    SpotToPerp,
}

impl TransferDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            TransferDirection::PerpToSpot => "perp_to_spot",
            TransferDirection::SpotToPerp => "spot_to_perp",
        }
    }
}
