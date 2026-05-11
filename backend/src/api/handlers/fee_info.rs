//! Fee Info API Handler
//!
//! 返回用户当前 VIP 等级、14 天滚动交易量、费率表与折扣明细。

use axum::{extract::State, http::StatusCode, Extension, Json};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use std::sync::Arc;

use crate::auth::middleware::OptionalAuthUser;
use crate::services::vip_tier;
use crate::utils::fee_tiers::{self, TierProgress, TIERS};
use crate::utils::user_volume;
use crate::AppState;

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

#[derive(Debug, Serialize)]
pub struct FeeTierView {
    pub level: u8,
    pub label: &'static str,
    pub maker: Decimal,
    pub taker: Decimal,
    pub volume_min: Decimal,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_max: Option<Decimal>,
}

#[derive(Debug, Serialize)]
pub struct DiscountsView {
    pub referral: Decimal,
    pub token_staking: Decimal,
    pub multiplier: Decimal,
}

#[derive(Debug, Serialize)]
pub struct FeeInfoResponse {
    pub current_tier: u8,
    pub current_label: &'static str,
    pub current_maker: Decimal,
    pub current_taker: Decimal,
    /// 计入 discount_multiplier 后的有效费率（展示用）。
    pub effective_maker: Decimal,
    pub effective_taker: Decimal,
    pub volume_14d: Decimal,
    pub volume_30d: Decimal,
    pub fee_tiers: Vec<FeeTierView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_to_next: Option<TierProgress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_tier: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_effective_at: Option<DateTime<Utc>>,
    pub discounts: DiscountsView,
}

fn view(t: &fee_tiers::VipTier) -> FeeTierView {
    FeeTierView {
        level: t.level,
        label: t.label,
        maker: t.maker,
        taker: t.taker,
        volume_min: t.volume_min,
        volume_max: t.volume_max,
    }
}

/// GET /account/fee-info
///
/// Optionally authenticated. Anonymous callers receive the public VIP tier
/// schedule with VIP 0 baseline values (the /fee-vip frontend then drops the
/// user-specific block). Authenticated callers additionally get their current
/// tier, 14d/30d volume, pending downgrade, and effective discount.
pub async fn get_fee_info(
    State(state): State<Arc<AppState>>,
    Extension(OptionalAuthUser(maybe_user)): Extension<OptionalAuthUser>,
) -> Result<Json<FeeInfoResponse>, (StatusCode, Json<ErrorResponse>)> {
    let fee_tiers_view: Vec<FeeTierView> = TIERS.iter().map(view).collect();

    let Some(auth_user) = maybe_user else {
        // Anonymous path: tier table only, with safe defaults so the response
        // shape stays identical for the frontend mapper.
        let baseline = &TIERS[0];
        return Ok(Json(FeeInfoResponse {
            current_tier: baseline.level,
            current_label: baseline.label,
            current_maker: baseline.maker,
            current_taker: baseline.taker,
            effective_maker: baseline.maker,
            effective_taker: baseline.taker,
            volume_14d: Decimal::ZERO,
            volume_30d: Decimal::ZERO,
            fee_tiers: fee_tiers_view,
            progress_to_next: None,
            pending_tier: None,
            pending_effective_at: None,
            discounts: DiscountsView {
                referral: Decimal::ZERO,
                token_staking: Decimal::ZERO,
                multiplier: Decimal::ONE,
            },
        }));
    };

    let user_address = auth_user.address.to_lowercase();

    // 懒惰重算 + 写库（升档立即生效 / 降档安排 pending）。
    let effective = vip_tier::resolve(
        &state.db.pool,
        &state.vip_tier_event_sender,
        &user_address,
    )
    .await;

    let (volume_14d, volume_30d) =
        user_volume::get_volumes(&state.db.pool, &user_address).await;

    let current = effective.current;
    let multiplier = fee_tiers::discount_multiplier(&user_address, true);
    let referral = fee_tiers::referral_discount();
    let staking = fee_tiers::token_staking_discount(&user_address);

    Ok(Json(FeeInfoResponse {
        current_tier: current.level,
        current_label: current.label,
        current_maker: current.maker,
        current_taker: current.taker,
        effective_maker: fee_tiers::round_fee(current.maker * multiplier),
        effective_taker: fee_tiers::round_fee(current.taker * multiplier),
        volume_14d,
        volume_30d,
        fee_tiers: fee_tiers_view,
        progress_to_next: fee_tiers::progress_to_next(volume_14d),
        pending_tier: effective.pending_tier,
        pending_effective_at: effective.pending_effective_at,
        discounts: DiscountsView {
            referral,
            token_staking: staking,
            multiplier,
        },
    }))
}
