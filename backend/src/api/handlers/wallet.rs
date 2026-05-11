//! Wallet metadata endpoints.
//!
//! `GET /wallet/tokens` returns the list of tokens the platform supports for
//! deposit / withdrawal, with chain id + on-chain contract + decimals so the
//! frontend can render the deposit picker without hardcoding values.
//!
//! - USDT comes from the existing perp `collateral_token_*` config (chain is
//!   the perp chain — Arbitrum Sepolia 421614 on testnet).
//! - DF comes from the spot `SpotConfig` (chain is the BSC vault chain). It is
//!   only included when `Config::spot` is `Some` (i.e. `SPOT_ENABLED=true`).
//!
//! The endpoint is public (no auth) — it returns metadata only, no per-user
//! state.

use axum::{extract::State, Json};
use serde::Serialize;
use std::sync::Arc;

use crate::utils::response::ApiResponse;
use crate::AppState;

#[derive(Serialize)]
pub struct WalletToken {
    pub symbol: String,
    pub image: String,
    pub decimals: u8,
    #[serde(rename = "chainId")]
    pub chain_id: u64,
    pub contract: String,
}

pub async fn list_tokens(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<WalletToken>>> {
    let mut tokens: Vec<WalletToken> = Vec::with_capacity(2);

    // Perp collateral (USDT). The address comes from config; address casing in
    // .env is preserved (matches what the frontend will receive).
    tokens.push(WalletToken {
        symbol: state.config.collateral_token_symbol.clone(),
        image: String::new(),
        decimals: state.config.collateral_token_decimals,
        chain_id: state.config.chain_id,
        contract: state.config.collateral_token_address.clone(),
    });

    // DF (spot) — only when spot subsystem is enabled.
    if let Some(spot) = state.config.spot.as_ref() {
        tokens.push(WalletToken {
            symbol: "DF".to_string(),
            image: String::new(),
            decimals: spot.df_token_decimals,
            chain_id: spot.bsc_chain_id,
            // ethers `Address` Debug is the canonical 0x-prefixed lowercased form
            contract: format!("{:?}", spot.df_token_address),
        });
    }

    Json(ApiResponse::success(tokens))
}
