//! Position Management Service
//!
//! Based on GMX V2 position management logic with the following key concepts:
//! - Track both sizeInUsd AND sizeInTokens for accurate PnL calculation
//! - PnL: Long = (sizeInTokens × markPrice) - sizeInUsd
//! - PnL: Short = sizeInUsd - (sizeInTokens × markPrice)
//! - Liquidation based on remaining collateral vs min requirements
//! - Opposing position netting: when opening a position in one direction,
//!   first close/reduce any existing position in the opposite direction

use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::sync::Arc;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tokio;
use uuid::Uuid;

// Helper constants
const HUNDRED: i64 = 100;

/// Generate a stable lock key for advisory lock based on user_address and symbol
fn generate_position_lock_key(user_address: &str, symbol: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    user_address.to_lowercase().hash(&mut hasher);
    symbol.to_uppercase().hash(&mut hasher);
    // Use only the lower 63 bits to ensure positive value for PostgreSQL bigint
    (hasher.finish() & 0x7FFFFFFFFFFFFFFF) as i64
}

/// Balance-side deltas applied when `increase_position_inner` closes an opposing position.
///
/// Two call sites with different `balances.frozen` invariants:
///
/// - `open_position` (user-initiated): caller pre-froze `incoming_collateral` in frozen,
///   and the opposing position's `collateral + buffer + opening_fee` were also in frozen
///   (held from the opposing's original freeze-on-open lifecycle). Closing must release
///   those amounts from frozen, credit the opposing buffer + returned collateral back to
///   available, and release the incoming-order's frozen collateral on pure-close.
///
/// - `update_positions_after_trade` (trade fill): NEITHER the opposing's collateral nor
///   the incoming order's margin is in `balances.frozen` at this point. The opposing's
///   collateral lives in `positions.collateral_amount`; the incoming order's freeze was
///   already released by `order.rs` taker-release / orchestrator maker-release before
///   this function runs. Subtracting from frozen is a no-op (GREATEST clamps to 0) and
///   crediting buffer or `incoming_collateral` is a phantom credit to available.
///
/// Tuple semantics: `(frozen_sub, available_add, incoming_release_from_frozen)`.
/// Each value is applied to `balances` via `frozen = GREATEST(frozen - X, 0)` and
/// `available = available + Y`. `incoming_release_from_frozen` produces a second UPDATE
/// where both sides bind the same amount (the pre-frozen incoming collateral coming back).
fn opposing_close_balance_delta(
    caller_pre_froze: bool,
    is_partial_close_not_flip: bool,
    opp_collateral_to_release: Decimal,
    opp_buffer: Decimal,
    opp_opening_fee: Decimal,
    collateral_returned: Decimal,
    incoming_collateral: Decimal,
    new_position_collateral: Decimal,
) -> (Decimal, Decimal, Decimal) {
    let collateral_returned = collateral_returned.max(Decimal::ZERO);
    let new_position_collateral = new_position_collateral.max(Decimal::ZERO);
    if caller_pre_froze {
        let frozen_sub = opp_collateral_to_release + opp_buffer + opp_opening_fee;
        let available_add = if is_partial_close_not_flip {
            collateral_returned + opp_buffer
        } else {
            // Flip branch: opposing released `collateral_returned`; the new
            // (smaller-after-netting) position takes only `new_position_collateral`.
            // The leftover must return to balance — pre-fix only `opp_buffer`
            // returned, so any flip where new size < opposing size leaked the
            // difference (P0 2026-04-27, single-server retest).
            let leftover = (collateral_returned - new_position_collateral).max(Decimal::ZERO);
            opp_buffer + leftover
        };
        let incoming_release = if is_partial_close_not_flip {
            incoming_collateral
        } else {
            Decimal::ZERO
        };
        (frozen_sub, available_add, incoming_release)
    } else {
        // Trade-path: balances.frozen holds nothing attributable to this close.
        // On partial-close: returned collateral must move from positions.collateral_amount
        // back to available (decrease_position already reduced positions.collateral_amount).
        // On flip: opposing released `collateral_returned`; the new (smaller)
        // position takes only `new_position_collateral`. The difference is the
        // user's IM that should not silently disappear (P0 2026-04-27 root cause:
        // pre-fix returned ZERO, leaking ≈ original IM whenever close-fill price
        // made incoming.size_in_usd > opposing.size_in_usd by any amount).
        let available_add = if is_partial_close_not_flip {
            collateral_returned
        } else {
            (collateral_returned - new_position_collateral).max(Decimal::ZERO)
        };
        (Decimal::ZERO, available_add, Decimal::ZERO)
    }
}

use crate::cache::PositionCache;
use crate::models::{
    Position, PositionConfig, PositionResponse, PositionSide, PositionStatus,
    LiquidationInfo, IncreasePositionResult, DecreasePositionResult,
};
use crate::services::points::PointsService;

/// Result of processing opposing position during increase_position
#[allow(dead_code)]
#[derive(Debug)]
pub struct OpposingPositionResult {
    /// Realized PnL from closing opposing position
    pub realized_pnl: Decimal,
    /// Collateral returned from opposing position
    pub collateral_returned: Decimal,
    /// Fees paid for closing opposing position
    pub fees_paid: Decimal,
    /// Whether opposing position was fully closed
    pub fully_closed: bool,
    /// Remaining size to open in new direction (after netting)
    pub remaining_size_usd: Decimal,
}

/// Position Service for managing perpetual positions
pub struct PositionService {
    pool: PgPool,
    pub config: PositionConfig,
    /// Optional Redis cache for high-frequency operations
    cache: Option<Arc<PositionCache>>,
    /// Optional Points service for referral rewards
    points_service: Option<Arc<PointsService>>,
}

/// Errors that can occur during position operations
#[derive(Debug, thiserror::Error)]
pub enum PositionError {
    #[error("Position not found")]
    NotFound,
    #[error("Insufficient collateral: required {required}, available {available}")]
    InsufficientCollateral { required: Decimal, available: Decimal },
    #[error("Position size too small: minimum is {min_size}")]
    PositionTooSmall { min_size: Decimal },
    #[error("Leverage too high: maximum is {max_leverage}")]
    LeverageTooHigh { max_leverage: i32 },
    #[error("Position would be liquidatable")]
    WouldBeLiquidatable,
    #[error("Position is already closed")]
    AlreadyClosed,
    #[error("Cannot remove collateral: would trigger liquidation")]
    CollateralRemovalWouldLiquidate,
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl PositionService {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            config: PositionConfig::default(),
            cache: None,
            points_service: None,
        }
    }

    pub fn with_config(pool: PgPool, config: PositionConfig) -> Self {
        Self { pool, config, cache: None, points_service: None }
    }

    pub fn with_cache(pool: PgPool, config: PositionConfig, cache: Arc<PositionCache>) -> Self {
        Self { pool, config, cache: Some(cache), points_service: None }
    }

    /// Set cache after construction
    pub fn set_cache(&mut self, cache: Arc<PositionCache>) {
        self.cache = Some(cache);
    }

    /// Set points service after construction
    pub fn set_points_service(&mut self, points_service: Arc<PointsService>) {
        self.points_service = Some(points_service);
    }

    // ==================== Core PnL Calculations (GMX-style) ====================

    /// Calculate unrealized PnL based on GMX formula
    /// Long: (sizeInTokens × markPrice) - sizeInUsd
    /// Short: sizeInUsd - (sizeInTokens × markPrice)
    pub fn calculate_unrealized_pnl(
        position: &Position,
        mark_price: Decimal,
    ) -> Decimal {
        let position_value = position.size_in_tokens * mark_price;

        match position.side {
            PositionSide::Long => position_value - position.size_in_usd,
            PositionSide::Short => position.size_in_usd - position_value,
        }
    }

    /// Calculate unrealized PnL percentage relative to collateral
    pub fn calculate_unrealized_pnl_percent(
        position: &Position,
        mark_price: Decimal,
    ) -> Decimal {
        let pnl = Self::calculate_unrealized_pnl(position, mark_price);
        if position.collateral_amount.is_zero() {
            return Decimal::ZERO;
        }
        crate::safe_div!(
            pnl, 
            position.collateral_amount, 
            "PositionService: unrealized_pnl_percent"
        ) * Decimal::from(HUNDRED)
    }

    /// Calculate liquidation price based on position parameters (simplified).
    ///
    /// This uses the idealised formula (collateral = size/leverage, fees = 0)
    /// and is only appropriate at initial position creation.
    /// For existing positions use [`calculate_liquidation_price_from_position`]
    /// which accounts for the actual collateral and accumulated fees.
    ///
    /// For Long: entry_price × (1 - 1/leverage + maintenance_margin_rate)
    /// For Short: entry_price × (1 + 1/leverage - maintenance_margin_rate)
    pub fn calculate_liquidation_price(
        entry_price: Decimal,
        leverage: i32,
        side: PositionSide,
        maintenance_margin_rate: Decimal,
    ) -> Decimal {
        let leverage_dec = Decimal::from(leverage);
        let one = Decimal::ONE;

        match side {
            PositionSide::Long => {
                entry_price * (one - crate::safe_div!(one, leverage_dec, "PositionService: liquidation long leverage") + maintenance_margin_rate)
            }
            PositionSide::Short => {
                entry_price * (one + crate::safe_div!(one, leverage_dec, "PositionService: liquidation short leverage") - maintenance_margin_rate)
            }
        }
    }

    /// Calculate liquidation price from **actual** position state.
    ///
    /// Derives the mark price at which `remaining_collateral == min_collateral`,
    /// i.e. the exact same condition that `LiquidationService::should_liquidate`
    /// checks. This keeps the value displayed to the user consistent with the
    /// engine's trigger logic.
    ///
    /// Formula:
    ///   remaining = collateral + PnL - fees = size_usd * mmr
    ///   Long PnL  = tokens * mark - size_usd   →  mark = (size_usd * mmr - collateral + fees + size_usd) / tokens
    ///   Short PnL = size_usd - tokens * mark    →  mark = (collateral - fees + size_usd - size_usd * mmr) / tokens
    pub fn calculate_liquidation_price_from_position(
        position: &Position,
        maintenance_margin_rate: Decimal,
    ) -> Decimal {
        if position.size_in_tokens <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let fees = position.accumulated_funding_fee + position.accumulated_borrowing_fee;
        let min_collateral = position.size_in_usd * maintenance_margin_rate;

        match position.side {
            PositionSide::Long => {
                // tokens * mark = min_collateral - collateral + fees + size_usd
                let numerator = min_collateral - position.collateral_amount + fees + position.size_in_usd;
                crate::safe_div!(numerator, position.size_in_tokens, "PositionService: liq_price_from_pos long")
                    .max(Decimal::ZERO)
            }
            PositionSide::Short => {
                // tokens * mark = collateral - fees + size_usd - min_collateral
                let numerator = position.collateral_amount - fees + position.size_in_usd - min_collateral;
                crate::safe_div!(numerator, position.size_in_tokens, "PositionService: liq_price_from_pos short")
                    .max(Decimal::ZERO)
            }
        }
    }

    /// Calculate remaining collateral in USD after PnL and fees
    pub fn calculate_remaining_collateral_usd(
        position: &Position,
        mark_price: Decimal,
        collateral_price: Decimal, // Price of collateral token (usually 1 for stablecoins)
    ) -> Decimal {
        let pnl = Self::calculate_unrealized_pnl(position, mark_price);
        let collateral_usd = position.collateral_amount * collateral_price;
        let fees = position.accumulated_funding_fee + position.accumulated_borrowing_fee;

        collateral_usd + pnl - fees
    }

    /// Calculate net position value (collateral + unrealized PnL - fees)
    pub fn calculate_net_value(
        position: &Position,
        mark_price: Decimal,
    ) -> Decimal {
        let pnl = Self::calculate_unrealized_pnl(position, mark_price);
        let fees = position.accumulated_funding_fee + position.accumulated_borrowing_fee;
        position.collateral_amount + pnl - fees
    }

    /// Calculate current margin ratio (Binance-style: Maintenance Margin / Equity)
    ///
    /// Binance Formula:
    /// - Margin Ratio = Maintenance Margin / Margin Balance (Equity)
    /// - Equity = Collateral + Unrealized PnL - Fees
    /// - Maintenance Margin = Position Value × MMR
    ///
    /// Returns: 0.0 ~ 1.0+ (when >= 1.0, position should be liquidated)
    /// - 0.0 = Very safe (no risk)
    /// - 0.5 = 50% risk
    /// - 1.0 = 100% risk (liquidation threshold)
    pub fn calculate_margin_ratio(
        position: &Position,
        mark_price: Decimal,
    ) -> Decimal {
        Self::calculate_margin_ratio_with_mmr(position, mark_price, Decimal::new(5, 3)) // Default 0.5% MMR
    }

    /// Calculate margin ratio with custom maintenance margin rate
    pub fn calculate_margin_ratio_with_mmr(
        position: &Position,
        mark_price: Decimal,
        maintenance_margin_rate: Decimal,
    ) -> Decimal {
        // Calculate equity (margin balance)
        let equity = Self::calculate_remaining_collateral_usd(position, mark_price, Decimal::ONE);

        if equity <= Decimal::ZERO {
            return Decimal::ONE; // 100% risk - should be liquidated
        }

        // Calculate maintenance margin based on current position value
        // Position value = size (in tokens) × mark_price
        let size_in_tokens = if position.entry_price > Decimal::ZERO {
            crate::safe_div!(position.size_in_usd, position.entry_price, "margin_ratio: size_in_tokens")
        } else {
            Decimal::ZERO
        };
        let position_value = size_in_tokens * mark_price;
        let maintenance_margin = position_value * maintenance_margin_rate;

        // Margin Ratio = Maintenance Margin / Equity
        // Higher = more dangerous, >= 1.0 = liquidation
        crate::safe_div!(
            maintenance_margin,
            equity,
            "PositionService: margin_ratio"
        )
    }

    // ==================== Liquidation Logic ====================

    /// Check if a position is liquidatable (GMX-style)
    /// Conditions:
    /// 1. remainingCollateral <= 0
    /// 2. remainingCollateral < minCollateralUsd
    /// 3. remainingCollateral < minCollateralForLeverage (max leverage check)
    pub fn check_liquidation(
        &self,
        position: &Position,
        mark_price: Decimal,
    ) -> LiquidationInfo {
        let remaining_collateral = Self::calculate_remaining_collateral_usd(
            position,
            mark_price,
            Decimal::ONE,
        );

        let min_collateral_usd = self.config.min_collateral_usd;
        let min_collateral_for_leverage = position.size_in_usd * self.config.maintenance_margin_rate;

        // Check liquidation conditions
        if remaining_collateral <= Decimal::ZERO {
            return LiquidationInfo {
                is_liquidatable: true,
                reason: Some("Remaining collateral <= 0".to_string()),
                remaining_collateral_usd: remaining_collateral,
                min_collateral_usd,
                min_collateral_for_leverage,
            };
        }

        if remaining_collateral < min_collateral_usd {
            return LiquidationInfo {
                is_liquidatable: true,
                reason: Some("Below minimum collateral".to_string()),
                remaining_collateral_usd: remaining_collateral,
                min_collateral_usd,
                min_collateral_for_leverage,
            };
        }

        if remaining_collateral < min_collateral_for_leverage {
            return LiquidationInfo {
                is_liquidatable: true,
                reason: Some("Max leverage exceeded".to_string()),
                remaining_collateral_usd: remaining_collateral,
                min_collateral_usd,
                min_collateral_for_leverage,
            };
        }

        LiquidationInfo {
            is_liquidatable: false,
            reason: None,
            remaining_collateral_usd: remaining_collateral,
            min_collateral_usd,
            min_collateral_for_leverage,
        }
    }

    // ==================== Position Operations ====================

    /// Open a new position or increase existing position
    ///
    /// Key behavior: When opening a position in one direction, this function will
    /// FIRST check for and close any existing opposing position before proceeding.
    /// This ensures that users can't have both long and short positions on the same
    /// symbol simultaneously.
    ///
    /// `skip_min_size_check`: Set to true when creating positions from already-executed trades
    /// to bypass minimum position size validation (since the trade has already happened)
    pub async fn increase_position(
        &self,
        user_address: &str,
        symbol: &str,
        side: PositionSide,
        collateral_amount: Decimal,
        leverage: i32,
        execution_price: Decimal,
        skip_min_size_check: bool,
        trade_id: Option<Uuid>,
    ) -> Result<IncreasePositionResult, PositionError> {
        // Validate leverage
        if leverage > self.config.max_leverage || leverage < 1 {
            return Err(PositionError::LeverageTooHigh {
                max_leverage: self.config.max_leverage,
            });
        }

        // ============= CONCURRENCY CONTROL =============
        // Use a transaction-scoped advisory lock (pg_try_advisory_xact_lock) to prevent
        // race conditions when handling opposing positions.
        //
        // FIX for P0 issue: Previously used session-level pg_advisory_lock on connection A
        // while queries ran on pool connections B/C/D. Now we use a transaction that holds
        // the lock AND runs critical queries on the SAME connection, ensuring the lock
        // actually protects the operations it guards.
        //
        // The transaction-scoped lock automatically releases when the transaction
        // commits or rolls back, preventing lock leaks.
        let lock_key = generate_position_lock_key(user_address, symbol);

        // Start a transaction - the advisory lock will live within this transaction
        let mut tx = self.pool.begin().await?;

        // Set lock timeout to prevent indefinite blocking (5 seconds max wait)
        sqlx::query("SET LOCAL lock_timeout = '5s'")
            .execute(&mut *tx)
            .await?;

        // Acquire transaction-scoped advisory lock via Postgres's native
        // waiter queue. `pg_advisory_xact_lock` blocks the session until
        // the lock is held or `lock_timeout` (set above, 5s) fires.
        //
        // Postgres serves waiters on the same key in effectively FIFO
        // order, so a contested (user, symbol) — e.g. a market maker
        // generating tens of concurrent persist_trade tasks — drains
        // fairly and cheaply, one acquirer at a time.
        //
        // The previous implementation polled `pg_try_advisory_xact_lock`
        // up to 10× with 500ms sleeps (~5s worst-case per attempt) while
        // holding the sqlx connection idle-in-transaction. Under MM load
        // (incident 2026-04-21 12:47 UTC) this produced mpsc backlogs
        // of 2500+ trades and "Advisory lock timeout" retry storms,
        // because every loser in the thundering herd still consumed a
        // pool connection for the full 5s window. Native queueing
        // preserves the same upper bound without the client-side spin.
        //
        // Lock auto-releases on tx commit/rollback.
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock_key)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                tracing::warn!(
                    "Advisory xact lock not acquired for key {} (user={}, symbol={}): {}",
                    lock_key, user_address, symbol, e
                );
                // Tx rolls back automatically on drop, releasing any partial state.
                PositionError::Database(sqlx::Error::Protocol(format!(
                    "Advisory lock timeout: could not acquire lock for user={}, symbol={}",
                    user_address, symbol
                )))
            })?;

        // Use inner function to do the actual work
        // The advisory xact lock in `tx` provides server-wide mutual exclusion
        let result = self.increase_position_inner(
            user_address,
            symbol,
            side,
            collateral_amount,
            leverage,
            execution_price,
            skip_min_size_check,
            trade_id,
        ).await;

        // Commit / rollback explicitly. Do NOT rely on Transaction's Drop impl:
        // under tokio cancellation (e.g. a caller-level `tokio::time::timeout`) the
        // Drop-based async rollback may not get polled, leaving the Postgres session
        // stuck in `idle in transaction` and holding the advisory xact lock until the
        // DB kills it — which exhausts the sqlx connection pool.
        match &result {
            Ok(_) => tx.commit().await?,
            Err(_) => { let _ = tx.rollback().await; }
        }

        result
    }

    /// Inner implementation of increase_position (called with advisory xact lock held)
    ///
    /// The advisory lock (pg_advisory_xact_lock) is held in the caller's transaction,
    /// providing server-wide mutual exclusion per user+symbol. The lock auto-releases
    /// when the transaction commits/rolls back.
    #[allow(deprecated)] // reads PositionConfig.min_position_size_usd as fallback; spec §2.1
    async fn increase_position_inner(
        &self,
        user_address: &str,
        symbol: &str,
        side: PositionSide,
        collateral_amount: Decimal,
        leverage: i32,
        execution_price: Decimal,
        skip_min_size_check: bool,
        trade_id: Option<Uuid>,
    ) -> Result<IncreasePositionResult, PositionError> {
        // Calculate position size
        let size_in_usd = collateral_amount * Decimal::from(leverage);

        // NOTE: Don't validate minimum size here! We need to check for opposing positions first.
        // A trade smaller than min_size is valid if it's closing/reducing an opposing position.
        // The effective size after netting will be validated later (line ~353).

        // ============= OPPOSING POSITION HANDLING =============
        // Check for opposing position and handle netting
        let opposing_side = match side {
            PositionSide::Long => PositionSide::Short,
            PositionSide::Short => PositionSide::Long,
        };

        let mut effective_size_in_usd = size_in_usd;
        let mut effective_collateral = collateral_amount;
        let mut realized_pnl_from_closing = Decimal::ZERO;

        // Check if there's an opposing position
        if let Some(opposing_position) = self.get_user_position(user_address, symbol, opposing_side).await? {
            tracing::info!(
                "Found opposing {} position for user {} on {}: size_usd={}, closing/reducing it",
                format!("{:?}", opposing_side).to_lowercase(),
                user_address,
                symbol,
                opposing_position.size_in_usd
            );

            // Compare in token space, not USD, to decide partial-vs-flip.
            //
            // The size_in_usd of `opposing_position` was captured at OPEN
            // price; `size_in_usd` here is computed at FILL price. When the
            // caller's intent is "close exactly this many tokens" (e.g. a
            // reduceOnly order that the handler already capped to opposite
            // tokens), any price drift between open and fill flips the
            // USD-comparison even though the token quantities are equal.
            // That falls into the flip branch and creates a sub-microscopic
            // opposite-side dust position whose collateral_amount stays on
            // the books — R4 P0 #2 / #3 (residual collateral after flatten).
            //
            // Comparing in tokens: if the incoming token quantity is <= the
            // opposing's, it's an honest partial close (no flip ever, no
            // dust). When tokens match exactly the partial-close branch
            // closes the opposing fully via `Some(opposing.size_in_usd)` →
            // close_ratio = 1.0 → full close → no residue.
            let incoming_size_in_tokens = if execution_price > Decimal::ZERO {
                size_in_usd / execution_price
            } else {
                Decimal::ZERO
            };

            if opposing_position.size_in_tokens >= incoming_size_in_tokens {
                // Pass a USD amount that yields the intended token close in
                // `decrease_position` (which uses close_ratio =
                // close_size_usd / position.size_in_usd to apportion).
                //
                //   close_size_usd = opposing.size_in_usd * (incoming_tokens /
                //                                            opposing_tokens)
                //
                // In the equal-token case this reduces exactly to
                // `opposing.size_in_usd` → close_ratio = 1.0 → full close,
                // no dust position created.
                let close_size_usd = if opposing_position.size_in_tokens == incoming_size_in_tokens {
                    opposing_position.size_in_usd
                } else if opposing_position.size_in_tokens > Decimal::ZERO {
                    opposing_position.size_in_usd * incoming_size_in_tokens
                        / opposing_position.size_in_tokens
                } else {
                    size_in_usd
                };
                // Opposing position is larger or equal - reduce it only.
                // Use the token-derived `close_size_usd` so the close_ratio
                // computed inside `decrease_position` (close_size_usd /
                // position.size_in_usd) tracks the token intent exactly.
                let result = self.decrease_position(
                    opposing_position.id,
                    Some(close_size_usd),
                    execution_price,
                    trade_id,
                ).await?;

                realized_pnl_from_closing = result.pnl_realized;

                tracing::info!(
                    "Reduced opposing position by ${} (close_size_usd; incoming size_usd was ${}), realized PnL: ${}",
                    close_size_usd,
                    size_in_usd,
                    realized_pnl_from_closing
                );

                // Release balance claims from the closed/reduced opposing position.
                // Branching on caller: `skip_min_size_check` is only set by
                // `update_positions_after_trade` (trade fill), whose balances state differs
                // from `open_position` (user-initiated). See `opposing_close_balance_delta`.
                let opposing_close_ratio = crate::safe_div!(
                    close_size_usd,
                    opposing_position.size_in_usd,
                    "PositionService: opposing close_ratio for balance release"
                );
                let opp_collateral_to_release = opposing_position.collateral_amount * opposing_close_ratio;
                let opp_margin = close_size_usd / Decimal::from(opposing_position.leverage);
                let opp_buffer = opp_margin * Decimal::new(5, 3); // 0.5% buffer
                // PR 2 (2026-04-29): post-fix, place_order does NOT freeze an
                // opening_fee, so there is none to release here. The legacy
                // value is preserved as `Decimal::ZERO`; the existing
                // `opposing_close_balance_delta` already tolerates a zero here
                // (the GREATEST(frozen - X, 0) clamp). See §2.5.
                let opp_opening_fee = Decimal::ZERO;

                let caller_pre_froze = !skip_min_size_check;
                let (frozen_sub, available_add, incoming_release) = opposing_close_balance_delta(
                    caller_pre_froze,
                    true, // partial-close (not flip)
                    opp_collateral_to_release,
                    opp_buffer,
                    opp_opening_fee,
                    result.collateral_returned,
                    collateral_amount,
                    Decimal::ZERO, // partial-close: no new position, no new collateral
                );

                if frozen_sub > Decimal::ZERO || available_add > Decimal::ZERO {
                    if let Err(e) = sqlx::query(
                        "UPDATE balances SET frozen = GREATEST(frozen - $1, 0), available = available + $2, updated_at = NOW() WHERE user_address = $3 AND token = 'USDT'"
                    )
                    .bind(frozen_sub)
                    .bind(available_add)
                    .bind(user_address)
                    .execute(&self.pool)
                    .await
                    {
                        tracing::error!(
                            "Failed to release balance after opposing close for {}: {}",
                            user_address, e
                        );
                    }
                }

                // Release the incoming order's pre-frozen collateral — only when the caller
                // actually pre-froze (open_position). Trade-path (`skip_min_size_check=true`)
                // never froze `collateral_amount`; an unconditional release here was the
                // root cause of the phantom-credit leak (short position appears to use no
                // margin on close; ~$7M+ potential exposure before fix).
                if incoming_release > Decimal::ZERO {
                    if let Err(e) = sqlx::query(
                        "UPDATE balances SET frozen = GREATEST(frozen - $1, 0), available = available + $1, updated_at = NOW() WHERE user_address = $2 AND token = 'USDT'"
                    )
                    .bind(incoming_release)
                    .bind(user_address)
                    .execute(&self.pool)
                    .await
                    {
                        tracing::error!(
                            "Failed to release incoming order frozen for {}: {}",
                            user_address, e
                        );
                    }
                }

                // After full close: clean up residual frozen from price slippage
                if result.is_fully_closed {
                    let cleanup = sqlx::query(
                        r#"
                        UPDATE balances
                        SET available = available + frozen,
                            frozen = 0,
                            updated_at = NOW()
                        WHERE user_address = $1 AND token = 'USDT' AND frozen > 0
                          AND NOT EXISTS (
                            SELECT 1 FROM positions
                            WHERE user_address = $1 AND status = 'open' AND size_in_usd > 0
                          )
                          AND NOT EXISTS (
                            SELECT 1 FROM orders
                            WHERE user_address = $1 AND status = 'pending'
                          )
                        "#
                    )
                    .bind(user_address)
                    .execute(&self.pool)
                    .await;

                    if let Ok(r) = &cleanup {
                        if r.rows_affected() > 0 {
                            tracing::info!("Cleaned up residual frozen balance for user {}", user_address);
                        }
                    }
                }

                // Return early - the order just reduced the opposing position
                // No new position is created in the requested direction
                let remaining_position = if result.is_fully_closed {
                    None
                } else {
                    // Get updated opposing position
                    self.get_user_position(user_address, symbol, opposing_side).await?
                };

                return Ok(IncreasePositionResult {
                    position: remaining_position
                        .map(|p| self.position_to_response(&p, execution_price))
                        .unwrap_or_else(|| self.empty_position_response(symbol, side, execution_price)),
                    execution_price,
                    size_delta_usd: size_in_usd,
                    size_delta_tokens: crate::safe_div!(
                        size_in_usd,
                        execution_price,
                        "PositionService: increase_position size_delta_tokens (opposing)"
                    ),
                    collateral_delta: result.collateral_returned,
                    fees_paid: result.fees_paid,
                });
            } else {
                // New position is larger - close opposing fully, open remainder in new direction
                let result = self.decrease_position(
                    opposing_position.id,
                    None, // Close fully
                    execution_price,
                    trade_id,
                ).await?;

                realized_pnl_from_closing = result.pnl_realized;

                // Flip: opposing closes fully, new position opens on the requested side.
                // Same trade-path vs open_position-path distinction as partial-close branch.
                let opp_collateral_to_release = opposing_position.collateral_amount;
                let opp_margin = opposing_position.size_in_usd / Decimal::from(opposing_position.leverage);
                let opp_buffer = opp_margin * Decimal::new(5, 3); // 0.5% buffer
                // PR 2 (2026-04-29): same rationale as the partial-close branch
                // above — no opening_fee in frozen post-fix. See §2.5.
                let opp_opening_fee = Decimal::ZERO;

                // Pre-compute the new (post-flip) position's collateral so the
                // balance-delta helper can credit any unspent returned collateral
                // back to balances.available. Mirrors the formula used at
                // line ~786 (`collateral_after_fee = effective_size/leverage - fee`).
                //
                // The "remainder" is computed in tokens at the fill price so
                // it tracks the actual token netting; using a USD difference
                // would mix open-price and fill-price USD and overstate the
                // new position's notional whenever prices moved.
                let new_eff_size_tokens = (incoming_size_in_tokens
                    - opposing_position.size_in_tokens)
                    .max(Decimal::ZERO);
                let new_eff_size = new_eff_size_tokens * execution_price;
                // PR 2 (2026-04-29): legacy `new_position_fee` removed; the
                // post-flip new position no longer absorbs an open-side fee
                // out of its collateral. See §2.5 of the design spec.
                let new_position_collateral = if new_eff_size > Decimal::ZERO {
                    (new_eff_size / Decimal::from(leverage)).max(Decimal::ZERO)
                } else {
                    Decimal::ZERO
                };

                let caller_pre_froze = !skip_min_size_check;
                let (frozen_sub, available_add, _incoming_release) = opposing_close_balance_delta(
                    caller_pre_froze,
                    false, // flip (not partial-close)
                    opp_collateral_to_release,
                    opp_buffer,
                    opp_opening_fee,
                    result.collateral_returned,
                    collateral_amount,
                    new_position_collateral,
                );

                if frozen_sub > Decimal::ZERO || available_add > Decimal::ZERO {
                    if let Err(e) = sqlx::query(
                        "UPDATE balances SET frozen = GREATEST(frozen - $1, 0), available = available + $2, updated_at = NOW() WHERE user_address = $3 AND token = 'USDT'"
                    )
                    .bind(frozen_sub)
                    .bind(available_add)
                    .bind(user_address)
                    .execute(&self.pool)
                    .await
                    {
                        tracing::error!(
                            "Failed to release frozen balance after opposing full close for {}: {}",
                            user_address, e
                        );
                    }
                }

                // Calculate remaining size to open in new direction. Same
                // token-based netting as `new_eff_size` above so the new
                // position's notional uses the fill price consistently.
                effective_size_in_usd = new_eff_size;

                // Adjust collateral: add back returned collateral from closed position
                effective_collateral = collateral_amount + result.collateral_returned - result.fees_paid;

                tracing::info!(
                    "Closed opposing position fully, opening new {} position with remaining ${} (after netting)",
                    format!("{:?}", side).to_lowercase(),
                    effective_size_in_usd
                );
            }
        }
        // ============= END OPPOSING POSITION HANDLING =============

        // Check if remaining size meets minimum (unless explicitly skipped for trade-created positions)
        if !skip_min_size_check && effective_size_in_usd < self.config.min_position_size_usd {
            // The netting resulted in a position too small to open
            // This is OK - we already closed/reduced the opposing position
            tracing::warn!(
                "Position size ${} below minimum ${}, skipping position creation (user: {}, symbol: {})",
                effective_size_in_usd,
                self.config.min_position_size_usd,
                user_address,
                symbol
            );
            return Ok(IncreasePositionResult {
                position: self.empty_position_response(symbol, side, execution_price),
                execution_price,
                size_delta_usd: size_in_usd,
                size_delta_tokens: size_in_usd / execution_price,
                collateral_delta: effective_collateral,
                fees_paid: Decimal::ZERO,
            });
        }

        // Calculate size in tokens for the effective amount
        let size_in_tokens = crate::safe_div!(
            effective_size_in_usd, 
            execution_price, 
            "PositionService: increase_position size_in_tokens"
        );

        // PR 2 (2026-04-29): the legacy `position_fee = effective_size × position_fee_rate`
        // has been removed. Users no longer pay the 0.1% open-side fee out of their
        // collateral; instead the per-fill maker/taker fee is accumulated onto
        // positions.accumulated_trading_fee (orchestrator.rs) and charged
        // proportionally on close (decrease_position). Spec:
        // docs/superpowers/specs/2026-04-29-fee-unification-and-protocol-revenue-ledger-design.md §2.5
        let position_fee = Decimal::ZERO;
        let collateral_after_fee = crate::safe_div!(
            effective_size_in_usd,
            Decimal::from(leverage),
            "PositionService: increase_position collateral_after_fee"
        );

        // Skip min collateral check for trade-created positions (same as skip_min_size_check)
        if !skip_min_size_check && collateral_after_fee < self.config.min_collateral_usd {
            return Err(PositionError::InsufficientCollateral {
                required: self.config.min_collateral_usd,
                available: effective_collateral,
            });
        }

        // Calculate liquidation price
        let liquidation_price = Self::calculate_liquidation_price(
            execution_price,
            leverage,
            side,
            self.config.maintenance_margin_rate,
        );

        // Check if existing same-direction position exists
        let existing = self.get_user_position(user_address, symbol, side).await?;

        let position = if let Some(mut existing) = existing {
            // Increase existing position - calculate new average entry
            let total_size_usd = existing.size_in_usd + effective_size_in_usd;
            let total_size_tokens = existing.size_in_tokens + size_in_tokens;
            let new_entry_price = crate::safe_div_or!(
                total_size_usd, 
                total_size_tokens, 
                existing.entry_price,
                "PositionService: increase_position new_entry_price"
            );

            existing.size_in_usd = total_size_usd;
            existing.size_in_tokens = total_size_tokens;
            existing.collateral_amount = existing.collateral_amount + collateral_after_fee;
            existing.entry_price = new_entry_price;
            existing.liquidation_price = Self::calculate_liquidation_price_from_position(
                &existing,
                self.config.maintenance_margin_rate,
            );
            existing.increased_at = Some(Utc::now());
            existing.updated_at = Utc::now();

            // Validate not liquidatable (skip for trade-created positions since trade already executed)
            if !skip_min_size_check {
                let liq_check = self.check_liquidation(&existing, execution_price);
                if liq_check.is_liquidatable {
                    return Err(PositionError::WouldBeLiquidatable);
                }
            }

            self.update_position(&self.pool, &existing).await?;

            // Update cache
            self.cache_position(&existing).await;

            existing
        } else {
            // Create new position
            let now = Utc::now();
            let position = Position {
                id: Uuid::new_v4(),
                user_address: user_address.to_string(),
                symbol: symbol.to_string(),
                side,
                size_in_usd: effective_size_in_usd,
                size_in_tokens,
                collateral_amount: collateral_after_fee,
                entry_price: execution_price,
                leverage,
                liquidation_price,
                borrowing_factor: Decimal::ZERO,
                funding_fee_amount_per_size: Decimal::ZERO,
                accumulated_funding_fee: Decimal::ZERO,
                accumulated_borrowing_fee: Decimal::ZERO,
                accumulated_trading_fee: Decimal::ZERO,
                unrealized_pnl: Decimal::ZERO,
                realized_pnl: realized_pnl_from_closing,
                status: PositionStatus::Open,
                created_at: now,
                updated_at: now,
                increased_at: Some(now),
                decreased_at: None,
            };

            // Validate not immediately liquidatable (skip for trade-created positions since trade already executed)
            if !skip_min_size_check {
                let liq_check = self.check_liquidation(&position, execution_price);
                if liq_check.is_liquidatable {
                    return Err(PositionError::WouldBeLiquidatable);
                }
            }

            self.insert_position(&position).await?;

            // Update cache
            self.cache_position(&position).await;

            position
        };

        // Check for referral activation reward
        // if let Some(points_service) = &self.points_service {
        //      // Pass the total intended order size (nominal value)
        //      // We spawn this to not block the critical path
        //      let points_svc = points_service.clone();
        //      let addr = user_address.to_string();
        //      let val = size_in_usd;
        //      tokio::spawn(async move {
        //          if let Err(e) = points_svc.process_referral_activation(&addr, val).await {
        //              tracing::warn!("Failed to process referral activation: {}", e);
        //          }
        //      });
        // }

        Ok(IncreasePositionResult {
            position: self.position_to_response(&position, execution_price),
            execution_price,
            size_delta_usd: effective_size_in_usd,
            size_delta_tokens: size_in_tokens,
            collateral_delta: collateral_after_fee,
            fees_paid: position_fee,
        })
    }

    /// Create an empty position response (used when position is fully closed)
    fn empty_position_response(&self, symbol: &str, side: PositionSide, mark_price: Decimal) -> PositionResponse {
        PositionResponse {
            position_id: Uuid::nil(),
            symbol: symbol.to_string(),
            side,
            status: PositionStatus::Closed,
            size_in_usd: Decimal::ZERO,
            size: Decimal::ZERO,
            size_in_tokens: Decimal::ZERO,
            amount: Decimal::ZERO,
            collateral_amount: Decimal::ZERO,
            entry_price: Decimal::ZERO,
            mark_price,
            liquidation_price: Decimal::ZERO,
            leverage: 0,
            margin_ratio: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            unrealized_pnl_percent: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            accumulated_funding_fee: Decimal::ZERO,
            accumulated_borrowing_fee: Decimal::ZERO,
            accumulated_trading_fee: Decimal::ZERO,
            net_value: Decimal::ZERO,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// Cache a position in Redis (if cache is available)
    async fn cache_position(&self, position: &Position) {
        if let Some(cache) = &self.cache {
            if let Err(e) = cache.set_position(position).await {
                tracing::warn!("Failed to cache position {}: {}", position.id, e);
            }
        }
    }

    /// Remove position from cache (if cache is available)
    async fn uncache_position(&self, position: &Position) {
        if let Some(cache) = &self.cache {
            if let Err(e) = cache.remove_position(position).await {
                tracing::warn!("Failed to remove position {} from cache: {}", position.id, e);
            }
        }
    }

    /// Public method to invalidate position cache by ID
    /// Used after external transactions update positions
    pub async fn invalidate_position_cache(&self, position_id: Uuid) {
        if let Some(cache) = &self.cache {
            if let Err(e) = cache.remove_position_by_id(position_id).await {
                tracing::warn!("Failed to invalidate cache for position {}: {}", position_id, e);
            }
        }
    }

    /// Decrease or close a position
    ///
    /// `trade_id`: when this decrease is the closing leg of a matched trade,
    /// pass the corresponding trade row's id. The realized_pnl_event written
    /// here will reference it directly so /account/trades and analytics no
    /// longer need the legacy ±5s time-proximity fallback. User-initiated or
    /// liquidation/ADL flows that don't have a single trade should pass None.
    #[allow(deprecated)] // reads PositionConfig.min_position_size_usd as fallback; spec §2.1
    pub async fn decrease_position(
        &self,
        position_id: Uuid,
        size_delta_usd: Option<Decimal>,
        execution_price: Decimal,
        trade_id: Option<Uuid>,
    ) -> Result<DecreasePositionResult, PositionError> {
        let mut position = self.get_position_by_id(position_id).await?
            .ok_or(PositionError::NotFound)?;

        if position.status != PositionStatus::Open {
            return Err(PositionError::AlreadyClosed);
        }

        // If no size specified, close entire position
        let close_size_usd = size_delta_usd.unwrap_or(position.size_in_usd);
        let close_size_usd = close_size_usd.min(position.size_in_usd);

        let is_fully_closed = close_size_usd >= position.size_in_usd;
        // .round_dp(10): accumulated_*_fee columns are NUMERIC(36,18); without
        // capping the ratio, `fee (18-scale) * close_ratio (up to 28-scale)`
        // overflows rust_decimal's 96-bit mantissa at rescale and panics
        // (decimal.rs:550). 10 dp keeps 18+10=28 within bounds.
        let close_ratio = crate::safe_div!(
            close_size_usd,
            position.size_in_usd,
            "PositionService: decrease_position close_ratio"
        ).round_dp(10);

        // Calculate proportional values
        let close_size_tokens = position.size_in_tokens * close_ratio;
        let collateral_to_return = position.collateral_amount * close_ratio;

        // Calculate realized PnL for closed portion
        let total_pnl = Self::calculate_unrealized_pnl(&position, execution_price);
        let realized_pnl = total_pnl * close_ratio;

        // Calculate fees for closing.
        //
        // PR 2 (2026-04-29): replaced legacy `position_fee = close_size_usd ×
        // position_fee_rate` with `proportional_trading` driven by
        // `accumulated_trading_fee` (which orchestrator.rs now bumps per fill
        // using the trade's actual maker/taker_fee). The legacy path was a
        // fixed-rate static config, divorced from the user's VIP tier and
        // from the maker/taker rates the UI was already showing. See spec
        // §2.2.
        let proportional_trading   = position.accumulated_trading_fee   * close_ratio;
        let proportional_funding   = position.accumulated_funding_fee   * close_ratio;
        let proportional_borrowing = position.accumulated_borrowing_fee * close_ratio;
        let total_fees = proportional_trading + proportional_funding + proportional_borrowing;

        // Final amount to return
        let amount_returned = collateral_to_return + realized_pnl - total_fees;

        if is_fully_closed {
            // Close position entirely
            position.status = PositionStatus::Closed;
            position.size_in_usd = Decimal::ZERO;
            position.size_in_tokens = Decimal::ZERO;
            position.collateral_amount = Decimal::ZERO;
            position.accumulated_trading_fee = Decimal::ZERO;
            position.accumulated_funding_fee = Decimal::ZERO;
            position.accumulated_borrowing_fee = Decimal::ZERO;
            position.realized_pnl = position.realized_pnl + realized_pnl;
            position.decreased_at = Some(Utc::now());
            position.updated_at = Utc::now();

            // Position close + protocol_fee_ledger rows must land in the
            // same transaction. record_close_ledger feeds
            // admin_stats.actual_protocol_revenue (added in 7d68ff8); a
            // partial commit (position closes but ledger rows missing)
            // makes the position view show closed while reported revenue
            // silently undercounts by the close-time fee slice.
            let mut tx = self.pool.begin().await?;
            self.update_position(&mut *tx, &position).await?;
            // PR 2 (2026-04-29): write protocol_fee_ledger rows for the
            // close-time fees actually charged.
            self.record_close_ledger(
                &mut tx,
                &position, trade_id,
                proportional_trading, proportional_funding, proportional_borrowing,
                close_ratio, close_size_usd, true,
            ).await?;
            tx.commit().await?;

            // Record realized PnL event for accurate daily tracking.
            // trade_id is plumbed through from the matched trade so the
            // resulting row links directly to its causing trade — replaces
            // the legacy ±5s time-proximity JOIN. User-initiated /
            // liquidation paths still pass None.
            //
            // Failures here are NOT swallowed quietly: realized_pnl_events
            // backs the QA reconciliation invariant ((avail+frozen+pos_col)
            // − start = pnl − fees) and any silent miss makes the rest of
            // the system look like it's gained money it never had. R4
            // reconcile flagged a phantom-credit drift of ≈$11–18/wallet
            // whose magnitude lines up with a small number of missed PnL
            // event writes; treat these errors as observability-critical
            // until that drift is closed out.
            if let Err(e) = self.record_realized_pnl_event(
                &position, realized_pnl, execution_price, close_size_usd, true, trade_id,
            ).await {
                tracing::error!(
                    target: "audit.realized_pnl",
                    user = %position.user_address,
                    position_id = %position.id,
                    trade_id = ?trade_id,
                    realized_pnl = %realized_pnl,
                    close_size_usd = %close_size_usd,
                    "Failed to record realized PnL event (full close): {}", e
                );
            }

            // Remove from cache since position is closed
            self.uncache_position(&position).await;

            // Cancel all active trigger orders for this closed position
            // This prevents orphan trigger orders that could cause issues
            if let Err(e) = sqlx::query(
                r#"
                UPDATE trigger_orders
                SET status = 'cancelled', updated_at = NOW()
                WHERE position_id = $1 AND status = 'active'
                "#
            )
            .bind(position_id)
            .execute(&self.pool)
            .await
            {
                tracing::warn!("Failed to cancel trigger orders for closed position {}: {}", position_id, e);
            }

            // Also clean up position_tp_sl record
            if let Err(e) = sqlx::query(
                "DELETE FROM position_tp_sl WHERE position_id = $1"
            )
            .bind(position_id)
            .execute(&self.pool)
            .await
            {
                tracing::warn!("Failed to delete position_tp_sl for closed position {}: {}", position_id, e);
            }

            // Calculate points for position close (async, non-blocking)
            if let Some(ref points_svc) = self.points_service {
                let user_address = position.user_address.clone();
                let symbol = position.symbol.clone();
                crate::services::points::handle_position_close_async(
                    Arc::clone(points_svc),
                    user_address,
                    position_id,
                    realized_pnl,
                    collateral_to_return,
                    symbol,
                );
            }

            Ok(DecreasePositionResult {
                position: None,
                execution_price,
                size_delta_usd: close_size_usd,
                pnl_realized: realized_pnl,
                collateral_returned: amount_returned.max(Decimal::ZERO),
                fees_paid: total_fees,
                is_fully_closed: true,
            })
        } else {
            // Partial close
            position.size_in_usd = position.size_in_usd - close_size_usd;
            position.size_in_tokens = position.size_in_tokens - close_size_tokens;
            position.collateral_amount = position.collateral_amount - collateral_to_return;
            position.realized_pnl = position.realized_pnl + realized_pnl;
            // Clamp at 0: under concurrent partial-close races (e.g. matching
            // retries), two callers can both load the same accumulated_*
            // snapshot and each subtract their proportional slice. With no
            // floor the second write can land negative — `Position` would
            // then carry a small negative fee total which `admin_stats`
            // reads back as a credit.
            position.accumulated_trading_fee =
                (position.accumulated_trading_fee - proportional_trading).max(Decimal::ZERO);
            position.accumulated_funding_fee =
                (position.accumulated_funding_fee - proportional_funding).max(Decimal::ZERO);
            position.accumulated_borrowing_fee =
                (position.accumulated_borrowing_fee - proportional_borrowing).max(Decimal::ZERO);
            position.decreased_at = Some(Utc::now());
            position.updated_at = Utc::now();

            // Validate remaining position is not liquidatable
            let liq_check = self.check_liquidation(&position, execution_price);
            if liq_check.is_liquidatable {
                // If partial close would leave liquidatable position, force full close
                return Box::pin(self.decrease_position(position_id, None, execution_price, trade_id)).await;
            }

            // Check minimum position size
            if position.size_in_usd < self.config.min_position_size_usd {
                return Box::pin(self.decrease_position(position_id, None, execution_price, trade_id)).await;
            }

            // Same atomicity story as the full-close branch above:
            // partial closes also write fee_ledger rows that
            // admin_stats.actual_protocol_revenue aggregates from, so the
            // position update + ledger writes have to commit together.
            let mut tx = self.pool.begin().await?;
            self.update_position(&mut *tx, &position).await?;
            // PR 2 (2026-04-29): same ledger writes as the full-close branch.
            self.record_close_ledger(
                &mut tx,
                &position, trade_id,
                proportional_trading, proportional_funding, proportional_borrowing,
                close_ratio, close_size_usd, false,
            ).await?;
            tx.commit().await?;

            // Record realized PnL event for accurate daily tracking.
            // See comment on the full-close branch: trade_id is plumbed in
            // from the matched trade so the row links exactly. Same
            // observability bump as the full-close path.
            if let Err(e) = self.record_realized_pnl_event(
                &position, realized_pnl, execution_price, close_size_usd, false, trade_id,
            ).await {
                tracing::error!(
                    target: "audit.realized_pnl",
                    user = %position.user_address,
                    position_id = %position.id,
                    trade_id = ?trade_id,
                    realized_pnl = %realized_pnl,
                    close_size_usd = %close_size_usd,
                    "Failed to record realized PnL event (partial close): {}", e
                );
            }

            // Update cache with new position state
            self.cache_position(&position).await;

            // Calculate points for partial position close (async, non-blocking)
            if let Some(ref points_svc) = self.points_service {
                let user_address = position.user_address.clone();
                let symbol = position.symbol.clone();
                crate::services::points::handle_position_close_async(
                    Arc::clone(points_svc),
                    user_address,
                    position_id,
                    realized_pnl,
                    collateral_to_return,
                    symbol,
                );
            }

            Ok(DecreasePositionResult {
                position: Some(self.position_to_response(&position, execution_price)),
                execution_price,
                size_delta_usd: close_size_usd,
                pnl_realized: realized_pnl,
                collateral_returned: amount_returned.max(Decimal::ZERO),
                fees_paid: total_fees,
                is_fully_closed: false,
            })
        }
    }

    /// Add collateral to a position
    pub async fn add_collateral(
        &self,
        position_id: Uuid,
        amount: Decimal,
    ) -> Result<Position, PositionError> {
        let mut position = self.get_position_by_id(position_id).await?
            .ok_or(PositionError::NotFound)?;

        if position.status != PositionStatus::Open {
            return Err(PositionError::AlreadyClosed);
        }

        position.collateral_amount = position.collateral_amount + amount;
        position.updated_at = Utc::now();

        // Recalculate liquidation price with new collateral
        let effective_leverage = crate::safe_div!(
            position.size_in_usd, 
            position.collateral_amount, 
            "PositionService: add_collateral effective_leverage"
        )
            .to_string()
            .parse::<i32>()
            .unwrap_or(position.leverage);

        position.leverage = effective_leverage;
        position.liquidation_price = Self::calculate_liquidation_price_from_position(
            &position,
            self.config.maintenance_margin_rate,
        );

        self.update_position(&self.pool, &position).await?;
        Ok(position)
    }

    /// Remove collateral from a position
    pub async fn remove_collateral(
        &self,
        position_id: Uuid,
        amount: Decimal,
        current_price: Decimal,
    ) -> Result<Position, PositionError> {
        let mut position = self.get_position_by_id(position_id).await?
            .ok_or(PositionError::NotFound)?;

        if position.status != PositionStatus::Open {
            return Err(PositionError::AlreadyClosed);
        }

        let new_collateral = position.collateral_amount - amount;
        if new_collateral < self.config.min_collateral_usd {
            return Err(PositionError::InsufficientCollateral {
                required: self.config.min_collateral_usd,
                available: new_collateral,
            });
        }

        // Check if removal would trigger liquidation
        let mut test_position = position.clone();
        test_position.collateral_amount = new_collateral;
        let liq_check = self.check_liquidation(&test_position, current_price);
        if liq_check.is_liquidatable {
            return Err(PositionError::CollateralRemovalWouldLiquidate);
        }

        position.collateral_amount = new_collateral;
        position.updated_at = Utc::now();

        // Recalculate leverage and liquidation price
        let effective_leverage = (position.size_in_usd / position.collateral_amount)
            .to_string()
            .parse::<i32>()
            .unwrap_or(position.leverage);

        position.leverage = effective_leverage;
        position.liquidation_price = Self::calculate_liquidation_price_from_position(
            &position,
            self.config.maintenance_margin_rate,
        );

        self.update_position(&self.pool, &position).await?;
        Ok(position)
    }

    // ==================== Query Methods ====================

    /// Get all open positions for a user
    /// Uses Redis cache if available, falls back to database
    pub async fn get_user_positions(&self, user_address: &str) -> Result<Vec<Position>, PositionError> {
        // Try cache first
        if let Some(cache) = &self.cache {
            match cache.get_user_positions(user_address).await {
                Ok(positions) if !positions.is_empty() => {
                    tracing::trace!("Cache hit for user positions: {}", user_address);
                    return Ok(positions);
                }
                Err(e) => {
                    tracing::warn!("Cache error getting user positions: {}", e);
                }
                _ => {
                    tracing::trace!("Cache miss for user positions: {}", user_address);
                }
            }
        }

        // Fall back to database
        let positions = sqlx::query_as::<_, Position>(
            r#"
            SELECT * FROM positions
            WHERE user_address = $1 AND status = 'open'
            ORDER BY created_at DESC
            "#
        )
        .bind(user_address)
        .fetch_all(&self.pool)
        .await?;

        // Populate cache
        if let Some(cache) = &self.cache {
            for position in &positions {
                if let Err(e) = cache.set_position(position).await {
                    tracing::warn!("Failed to cache position {}: {}", position.id, e);
                }
            }
        }

        Ok(positions)
    }

    /// Get a specific position for a user by symbol and side
    /// Uses Redis cache if available, falls back to database
    pub async fn get_user_position(
        &self,
        user_address: &str,
        symbol: &str,
        side: PositionSide,
    ) -> Result<Option<Position>, PositionError> {
        // Try cache first
        if let Some(cache) = &self.cache {
            match cache.get_position_by_key(user_address, symbol, side).await {
                Ok(Some(position)) => {
                    tracing::trace!("Cache hit for position: {}:{}:{:?}", user_address, symbol, side);
                    return Ok(Some(position));
                }
                Err(e) => {
                    tracing::warn!("Cache error getting position: {}", e);
                }
                _ => {
                    tracing::trace!("Cache miss for position: {}:{}:{:?}", user_address, symbol, side);
                }
            }
        }

        // Fall back to database
        let position = sqlx::query_as::<_, Position>(
            r#"
            SELECT * FROM positions
            WHERE user_address = $1 AND symbol = $2 AND side = $3 AND status = 'open'
            "#
        )
        .bind(user_address)
        .bind(symbol)
        .bind(side)
        .fetch_optional(&self.pool)
        .await?;

        // Populate cache if found
        if let (Some(cache), Some(ref pos)) = (&self.cache, &position) {
            if let Err(e) = cache.set_position(pos).await {
                tracing::warn!("Failed to cache position {}: {}", pos.id, e);
            }
        }

        Ok(position)
    }

    /// Get position by ID
    /// Uses Redis cache if available, falls back to database
    pub async fn get_position_by_id(&self, id: Uuid) -> Result<Option<Position>, PositionError> {
        // Try cache first
        if let Some(cache) = &self.cache {
            match cache.get_position(id).await {
                Ok(Some(position)) => {
                    tracing::trace!("Cache hit for position by id: {}", id);
                    return Ok(Some(position));
                }
                Err(e) => {
                    tracing::warn!("Cache error getting position by id: {}", e);
                }
                _ => {}
            }
        }

        // Fall back to database
        let position = sqlx::query_as::<_, Position>(
            "SELECT * FROM positions WHERE id = $1"
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        // Populate cache if found
        if let (Some(cache), Some(ref pos)) = (&self.cache, &position) {
            if let Err(e) = cache.set_position(pos).await {
                tracing::warn!("Failed to cache position {}: {}", pos.id, e);
            }
        }

        Ok(position)
    }

    /// Get all positions that are liquidatable at a given price
    pub async fn get_liquidatable_positions(
        &self,
        symbol: &str,
        current_price: Decimal,
    ) -> Result<Vec<Position>, PositionError> {
        // Get all open positions for the symbol
        let positions = sqlx::query_as::<_, Position>(
            r#"
            SELECT * FROM positions
            WHERE symbol = $1 AND status = 'open'
            "#
        )
        .bind(symbol)
        .fetch_all(&self.pool)
        .await?;

        // Filter by liquidation check
        let liquidatable: Vec<Position> = positions
            .into_iter()
            .filter(|p| {
                let liq = self.check_liquidation(p, current_price);
                liq.is_liquidatable
            })
            .collect();

        Ok(liquidatable)
    }

    // ==================== Database Operations ====================

    async fn insert_position(&self, position: &Position) -> Result<(), PositionError> {
        sqlx::query(
            r#"
            INSERT INTO positions (
                id, user_address, symbol, side,
                size_in_usd, size_in_tokens, collateral_amount,
                entry_price, leverage, liquidation_price,
                borrowing_factor, funding_fee_amount_per_size,
                accumulated_funding_fee, accumulated_borrowing_fee,
                unrealized_pnl, realized_pnl, status,
                created_at, updated_at, increased_at, decreased_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
            )
            "#
        )
        .bind(position.id)
        .bind(&position.user_address)
        .bind(&position.symbol)
        .bind(position.side)
        .bind(position.size_in_usd)
        .bind(position.size_in_tokens)
        .bind(position.collateral_amount)
        .bind(position.entry_price)
        .bind(position.leverage)
        .bind(position.liquidation_price)
        .bind(position.borrowing_factor)
        .bind(position.funding_fee_amount_per_size)
        .bind(position.accumulated_funding_fee)
        .bind(position.accumulated_borrowing_fee)
        .bind(position.unrealized_pnl)
        .bind(position.realized_pnl)
        .bind(position.status)
        .bind(position.created_at)
        .bind(position.updated_at)
        .bind(position.increased_at)
        .bind(position.decreased_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// `executor` is generic over `Executor<Database = Postgres>` so callers
    /// can pass either `&self.pool` (auto-commit) or `&mut *tx` to thread
    /// the write into a surrounding transaction. The close paths in
    /// `decrease_position` rely on the latter — the position update has to
    /// land atomically with the matching `protocol_fee_ledger` writes so
    /// `admin_stats.actual_protocol_revenue` and the position view stay in
    /// agreement.
    async fn update_position<'e, E>(
        &self,
        executor: E,
        position: &Position,
    ) -> Result<(), PositionError>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
            UPDATE positions SET
                size_in_usd = $2,
                size_in_tokens = $3,
                collateral_amount = $4,
                entry_price = $5,
                leverage = $6,
                liquidation_price = $7,
                borrowing_factor = $8,
                funding_fee_amount_per_size = $9,
                accumulated_funding_fee = $10,
                accumulated_borrowing_fee = $11,
                accumulated_trading_fee = $12,
                unrealized_pnl = $13,
                realized_pnl = $14,
                status = $15,
                updated_at = $16,
                increased_at = $17,
                decreased_at = $18
            WHERE id = $1
            "#
        )
        .bind(position.id)
        .bind(position.size_in_usd)
        .bind(position.size_in_tokens)
        .bind(position.collateral_amount)
        .bind(position.entry_price)
        .bind(position.leverage)
        .bind(position.liquidation_price)
        .bind(position.borrowing_factor)
        .bind(position.funding_fee_amount_per_size)
        .bind(position.accumulated_funding_fee)
        .bind(position.accumulated_borrowing_fee)
        .bind(position.accumulated_trading_fee)
        .bind(position.unrealized_pnl)
        .bind(position.realized_pnl)
        .bind(position.status)
        .bind(position.updated_at)
        .bind(position.increased_at)
        .bind(position.decreased_at)
        .execute(executor)
        .await?;

        Ok(())
    }

    /// PR 2 (2026-04-29): write up to 3 protocol_fee_ledger rows (one per
    /// fee bucket — trading / funding / borrowing) for the proportional
    /// fees being charged on this close. Zero amounts are silently dropped
    /// by the ledger helper.
    ///
    /// Now accepts a `&mut Transaction` from the caller so the ledger
    /// rows commit atomically with the position update. Returns
    /// `Result<(), sqlx::Error>` instead of swallowing errors — when a
    /// fee event fails to insert under the surrounding tx, the caller
    /// must roll back the position update too. Otherwise we'd land the
    /// position close without the ledger row that
    /// `admin_stats.actual_protocol_revenue` aggregates from, and the
    /// reported protocol revenue would silently undercount.
    async fn record_close_ledger(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        position: &Position,
        trade_id: Option<Uuid>,
        proportional_trading: Decimal,
        proportional_funding: Decimal,
        proportional_borrowing: Decimal,
        close_ratio: Decimal,
        close_size_usd: Decimal,
        is_full_close: bool,
    ) -> Result<(), sqlx::Error> {
        use crate::services::protocol_fee_ledger::{record_fee_event, FeeType};

        let metadata = serde_json::json!({
            "close_ratio":    close_ratio.to_string(),
            "close_size_usd": close_size_usd.to_string(),
            "is_full_close":  is_full_close,
        });

        for (fee_type, amount) in [
            (FeeType::TradingFee,   proportional_trading),
            (FeeType::FundingFee,   proportional_funding),
            (FeeType::BorrowingFee, proportional_borrowing),
        ] {
            if let Err(e) = record_fee_event(
                &mut **tx,
                &position.user_address,
                Some(position.id),
                trade_id,
                fee_type,
                amount,
                &metadata,
            ).await {
                tracing::error!(
                    target: "audit.protocol_fee_ledger",
                    user = %position.user_address,
                    position_id = %position.id,
                    trade_id = ?trade_id,
                    fee_type = fee_type.as_str(),
                    amount = %amount,
                    "Failed to write protocol_fee_ledger row on close: {}", e
                );
                return Err(e);
            }
        }

        Ok(())
    }

    /// Record a realized PnL event for accurate daily PnL tracking
    /// This stores the incremental realized PnL from each position decrease/close
    pub async fn record_realized_pnl_event(
        &self,
        position: &Position,
        realized_pnl: Decimal,
        execution_price: Decimal,
        size_delta_usd: Decimal,
        is_full_close: bool,
        trade_id: Option<Uuid>,
    ) -> Result<(), PositionError> {
        sqlx::query(
            r#"
            INSERT INTO realized_pnl_events (
                user_address, symbol, position_id, realized_pnl,
                execution_price, size_delta_usd, is_full_close, trade_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(&position.user_address)
        .bind(&position.symbol)
        .bind(position.id)
        .bind(realized_pnl)
        .bind(execution_price)
        .bind(size_delta_usd)
        .bind(is_full_close)
        .bind(trade_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!("Failed to record realized PnL event: {}", e);
            e
        })?;

        Ok(())
    }

    // ==================== Helpers ====================

    /// Convert Position to PositionResponse with current mark price
    pub fn position_to_response(&self, position: &Position, mark_price: Decimal) -> PositionResponse {
        let unrealized_pnl = Self::calculate_unrealized_pnl(position, mark_price);
        let unrealized_pnl_percent = Self::calculate_unrealized_pnl_percent(position, mark_price);
        let margin_ratio = Self::calculate_margin_ratio_with_mmr(
            position,
            mark_price,
            self.config.maintenance_margin_rate,
        );
        let net_value = Self::calculate_net_value(position, mark_price);

        PositionResponse {
            position_id: position.id,
            symbol: position.symbol.clone(),
            side: position.side,
            status: position.status,
            size_in_usd: position.size_in_usd,
            size: position.size_in_usd,
            size_in_tokens: position.size_in_tokens,
            amount: position.size_in_tokens,
            collateral_amount: position.collateral_amount,
            entry_price: position.entry_price,
            mark_price,
            liquidation_price: position.liquidation_price,
            leverage: position.leverage,
            margin_ratio,
            unrealized_pnl,
            unrealized_pnl_percent,
            realized_pnl: position.realized_pnl,
            accumulated_funding_fee: position.accumulated_funding_fee,
            accumulated_borrowing_fee: position.accumulated_borrowing_fee,
            accumulated_trading_fee: position.accumulated_trading_fee,
            net_value,
            created_at: position.created_at,
            updated_at: position.updated_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_pnl_calculation_long() {
        let position = Position {
            id: Uuid::new_v4(),
            user_address: "0x123".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Long,
            size_in_usd: dec!(10000),      // $10,000 position at entry
            size_in_tokens: dec!(0.1),      // 0.1 BTC at $100,000
            collateral_amount: dec!(1000),  // $1,000 collateral (10x)
            entry_price: dec!(100000),
            leverage: 10,
            liquidation_price: dec!(91000),
            borrowing_factor: Decimal::ZERO,
            funding_fee_amount_per_size: Decimal::ZERO,
            accumulated_funding_fee: Decimal::ZERO,
            accumulated_borrowing_fee: Decimal::ZERO,
            accumulated_trading_fee: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            status: PositionStatus::Open,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            increased_at: None,
            decreased_at: None,
        };

        // Price goes up 10%: $100,000 -> $110,000
        let pnl = PositionService::calculate_unrealized_pnl(&position, dec!(110000));
        // PnL = 0.1 * 110000 - 10000 = 11000 - 10000 = $1000
        assert_eq!(pnl, dec!(1000));

        // Price goes down 10%: $100,000 -> $90,000
        let pnl = PositionService::calculate_unrealized_pnl(&position, dec!(90000));
        // PnL = 0.1 * 90000 - 10000 = 9000 - 10000 = -$1000
        assert_eq!(pnl, dec!(-1000));
    }

    #[test]
    fn test_pnl_calculation_short() {
        let position = Position {
            id: Uuid::new_v4(),
            user_address: "0x123".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Short,
            size_in_usd: dec!(10000),
            size_in_tokens: dec!(0.1),
            collateral_amount: dec!(1000),
            entry_price: dec!(100000),
            leverage: 10,
            liquidation_price: dec!(109000),
            borrowing_factor: Decimal::ZERO,
            funding_fee_amount_per_size: Decimal::ZERO,
            accumulated_funding_fee: Decimal::ZERO,
            accumulated_borrowing_fee: Decimal::ZERO,
            accumulated_trading_fee: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            status: PositionStatus::Open,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            increased_at: None,
            decreased_at: None,
        };

        // Price goes down 10%: $100,000 -> $90,000 (profit for short)
        let pnl = PositionService::calculate_unrealized_pnl(&position, dec!(90000));
        // PnL = 10000 - 0.1 * 90000 = 10000 - 9000 = $1000
        assert_eq!(pnl, dec!(1000));

        // Price goes up 10%: $100,000 -> $110,000 (loss for short)
        let pnl = PositionService::calculate_unrealized_pnl(&position, dec!(110000));
        // PnL = 10000 - 0.1 * 110000 = 10000 - 11000 = -$1000
        assert_eq!(pnl, dec!(-1000));
    }

    /// Root-cause regression for 2026-04-22 phantom-credit report
    /// ("持仓空单，未占用保证金，下单显示依然可以下最大的仓位").
    ///
    /// Scenario: trade fill path (`skip_min_size_check=true`) closes an opposing
    /// position. Pre-fix, the helper released `collateral_amount` unconditionally
    /// to `available`, producing a phantom credit on every trade-path close
    /// (an open_position invariant applied where it does not hold).
    #[test]
    fn test_opposing_close_delta_trade_path_no_phantom_credit() {
        // User holds LONG 1 BTC (col $9.95) opened via earlier trade. Now a SELL
        // 1 BTC taker fill triggers `update_positions_after_trade` → increase_position
        // with Short side, collateral=$10.
        let (frozen_sub, avail_add, incoming_release) = opposing_close_balance_delta(
            /* caller_pre_froze */ false,
            /* is_partial_close_not_flip */ true,
            /* opp_collateral_to_release */ dec!(9.95),
            /* opp_buffer */ dec!(0.05),
            /* opp_opening_fee */ dec!(0.05),
            /* collateral_returned */ dec!(9.90),
            /* incoming_collateral */ dec!(10.00),
            /* new_position_collateral */ Decimal::ZERO, // partial-close: no new pos
        );
        // Trade-path: nothing in frozen to subtract, no incoming release, and
        // ONLY the actually-returned collateral goes back to available.
        assert_eq!(frozen_sub, Decimal::ZERO);
        assert_eq!(avail_add, dec!(9.90));
        assert_eq!(incoming_release, Decimal::ZERO);
    }

    #[test]
    fn test_opposing_close_delta_trade_path_flip_symmetric_is_money_conserving() {
        // User holds SHORT 1 BTC (col $9.95). Places BUY 2 BTC → flip to LONG 1 BTC.
        // When the new LONG is the SAME notional as the closed SHORT, the new
        // position's collateral consumes the released collateral entirely → no
        // balance movement.
        let (frozen_sub, avail_add, incoming_release) = opposing_close_balance_delta(
            false, false,
            dec!(9.95), dec!(0.05), dec!(0.05),
            /* collateral_returned */ dec!(9.90),
            dec!(20.00),
            /* new_position_collateral */ dec!(9.90), // matches returned → leftover 0
        );
        assert_eq!(frozen_sub, Decimal::ZERO);
        assert_eq!(avail_add, Decimal::ZERO);
        assert_eq!(incoming_release, Decimal::ZERO);
    }

    /// Regression: 2026-04-27 single-server retest, P0-3 (collateral leak on
    /// reduceOnly close when fill price differs from open price). When B's
    /// MARKET BUY 0.001 @ $79059.66 was followed by reduceOnly MARKET SELL
    /// 0.001 that filled @ $79061.50, incoming.size_in_usd ($79.0615) was
    /// fractionally larger than opposing.size_in_usd ($79.0597) → flip path.
    /// Pre-fix: avail_add = 0, leaking ≈ $7.83 (the full IM) per cycle.
    #[test]
    fn test_opposing_close_delta_trade_path_flip_credits_unspent_collateral() {
        // Numbers approximate the real B-cycle on 2026-04-27.
        //   opposing LONG: size=$79.0597, leverage=10 → col ≈ $7.906
        //   close fill @ $79061.50 → incoming.size=$79.0615
        //   new SHORT eff size = $0.0018, col_after_fee ≈ $0.000180
        //   collateral_returned ≈ $7.88 (after position close fees ~$0.04)
        let collateral_returned = dec!(7.88);
        let new_position_collateral = dec!(0.000180);
        let (frozen_sub, avail_add, incoming_release) = opposing_close_balance_delta(
            /* caller_pre_froze */ false,
            /* is_partial_close_not_flip */ false,
            /* opp_collateral_to_release */ dec!(7.906),
            /* opp_buffer */ dec!(0.039530),
            /* opp_opening_fee */ dec!(0.039530),
            collateral_returned,
            /* incoming_collateral */ dec!(7.9062),
            new_position_collateral,
        );
        assert_eq!(frozen_sub, Decimal::ZERO);
        // Released col MINUS what the dust new position consumed must return.
        assert_eq!(avail_add, collateral_returned - new_position_collateral);
        assert_eq!(incoming_release, Decimal::ZERO);
        // Sanity: drift would have been ≥ $7.87 pre-fix; now < $0.001 of the
        // expected "fee + pnl" identity (caller still has fees to subtract).
        assert!(avail_add > dec!(7.87));
    }

    #[test]
    fn test_opposing_close_delta_open_position_path_partial_preserved() {
        // Regression guard: do not break the user-initiated /position handler path.
        let (frozen_sub, avail_add, incoming_release) = opposing_close_balance_delta(
            true, true,
            dec!(9.95), dec!(0.05), dec!(0.05),
            dec!(9.90), dec!(10.00),
            Decimal::ZERO,
        );
        assert_eq!(frozen_sub, dec!(10.05));
        assert_eq!(avail_add, dec!(9.95));
        assert_eq!(incoming_release, dec!(10.00));
    }

    #[test]
    fn test_opposing_close_delta_open_position_path_flip_symmetric_preserved() {
        // Symmetric flip on the user-initiated path: new pos consumes released
        // collateral fully → only buffer comes back (matches pre-fix behavior
        // for symmetric scenarios).
        let (frozen_sub, avail_add, incoming_release) = opposing_close_balance_delta(
            true, false,
            dec!(9.95), dec!(0.05), dec!(0.05),
            dec!(9.90), dec!(20.00),
            /* new_position_collateral */ dec!(9.90),
        );
        assert_eq!(frozen_sub, dec!(10.05));
        assert_eq!(avail_add, dec!(0.05)); // buffer only; new pos ate the released col
        assert_eq!(incoming_release, Decimal::ZERO);
    }

    #[test]
    fn test_opposing_close_delta_open_position_path_flip_credits_unspent_collateral() {
        // Asymmetric flip on the user-initiated path: the open_position handler
        // also leaked when the new position consumed less collateral than was
        // released (same root cause as the trade-path regression above).
        let (frozen_sub, avail_add, incoming_release) = opposing_close_balance_delta(
            true, false,
            dec!(9.95), dec!(0.05), dec!(0.05),
            /* collateral_returned */ dec!(9.90),
            dec!(20.00),
            /* new_position_collateral */ dec!(0.10), // tiny new pos
        );
        assert_eq!(frozen_sub, dec!(10.05));
        // buffer + (returned - new_pos) = 0.05 + 9.80 = 9.85
        assert_eq!(avail_add, dec!(9.85));
        assert_eq!(incoming_release, Decimal::ZERO);
    }

    #[test]
    fn test_opposing_close_delta_trade_path_negative_collateral_returned_clamped() {
        // PnL worse than collateral leads decrease_position to return 0 (not negative).
        let (_, avail_add, _) = opposing_close_balance_delta(
            false, true,
            dec!(9.95), dec!(0.05), dec!(0.05),
            dec!(-100),  // Should be clamped to 0
            dec!(10.00),
            Decimal::ZERO,
        );
        assert_eq!(avail_add, Decimal::ZERO);
    }

    #[test]
    fn test_opposing_close_delta_trade_path_flip_new_pos_larger_than_returned_no_negative() {
        // Edge case: if somehow new_position_collateral > collateral_returned
        // (e.g. funny rounding), don't underflow into negative.
        let (_, avail_add, _) = opposing_close_balance_delta(
            false, false,
            dec!(9.95), dec!(0.05), dec!(0.05),
            /* collateral_returned */ dec!(5.00),
            dec!(20.00),
            /* new_position_collateral */ dec!(8.00),
        );
        assert_eq!(avail_add, Decimal::ZERO);
    }

    #[test]
    fn test_liquidation_price_calculation() {
        // Long position: entry $100,000, 10x leverage, 0.5% maintenance margin
        let liq_price = PositionService::calculate_liquidation_price(
            dec!(100000),
            10,
            PositionSide::Long,
            dec!(0.005),
        );
        // Expected: 100000 * (1 - 0.1 + 0.005) = 100000 * 0.905 = 90500
        assert_eq!(liq_price, dec!(90500));

        // Short position: entry $100,000, 10x leverage, 0.5% maintenance margin
        let liq_price = PositionService::calculate_liquidation_price(
            dec!(100000),
            10,
            PositionSide::Short,
            dec!(0.005),
        );
        // Expected: 100000 * (1 + 0.1 - 0.005) = 100000 * 1.095 = 109500
        assert_eq!(liq_price, dec!(109500));
    }
}
