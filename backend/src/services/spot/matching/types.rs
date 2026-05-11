use rust_decimal::Decimal;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, broadcast};
use uuid::Uuid;

use crate::models::spot::{Side, SpotOrder, SpotTrade};

/// In-memory representation of an order resting on the book.
#[derive(Debug, Clone)]
pub struct RestingOrder {
    pub id: Uuid,
    pub user_address: String,
    pub side: Side,
    pub price: Decimal,
    pub original_qty: Decimal,
    pub remaining_qty: Decimal,
    /// Monotonic ns counter assigned when the order was first inserted into the
    /// in-memory book. Used as the secondary key in price-time priority
    /// (primary key is price). Lower value = earlier = takes priority at the
    /// same price.
    pub ts_ns: u128,
}

/// (side_str, price, post_mutation_total_at_price). One entry per affected
/// price level on the book. Filled by the engine; consumed by ws_publisher
/// to synthesize spot:depth diffs without re-querying.
pub type AffectedLevel = (String, rust_decimal::Decimal, rust_decimal::Decimal);

#[derive(Debug, Clone, Serialize)]
pub enum EngineEvent {
    Fill {
        trade: SpotTrade,
        maker_after: SpotOrder,
        taker_after: SpotOrder,
        book_update_id: u64,
        affected_levels: Vec<AffectedLevel>,
    },
    OrderPlaced     { order: SpotOrder, book_update_id: u64, affected_levels: Vec<AffectedLevel> },
    OrderCanceled   { order: SpotOrder, book_update_id: u64, affected_levels: Vec<AffectedLevel> },
    OrderRejected   { order: SpotOrder },
}

/// Inputs for placing one new order.
#[derive(Debug)]
pub struct PlaceOrderRequest {
    pub user_address: String,
    pub market_id: String,
    pub side: crate::models::spot::Side,
    pub r#type: crate::models::spot::OrderType,
    pub tif: crate::models::spot::Tif,
    pub price: Option<rust_decimal::Decimal>,
    pub quantity: Option<rust_decimal::Decimal>,
    pub quote_quantity: Option<rust_decimal::Decimal>,
}

#[derive(Debug)]
pub enum OrderCommand {
    NewOrder(PlaceOrderRequest, oneshot::Sender<PlaceOrderResult>),
    Cancel { id: uuid::Uuid, user_address: String, reply: oneshot::Sender<CancelResult> },
    CancelAll {
        user_address: String,
        market_id: Option<String>,
        reply: oneshot::Sender<Vec<SpotOrder>>,
    },
    /// Admin-only: cancel every open / partially-filled order for a market
    /// (used when delisting). No per-user check; engine cancels on behalf
    /// of each owner so balances unfreeze normally.
    CancelMarket {
        market_id: String,
        reply: oneshot::Sender<Vec<SpotOrder>>,
    },
    ReloadMarket { market_id: String },
    Snapshot {
        /// (depth) — engine returns at most this many levels per side.
        depth: usize,
        reply: oneshot::Sender<(Vec<(rust_decimal::Decimal, rust_decimal::Decimal)>,
                                Vec<(rust_decimal::Decimal, rust_decimal::Decimal)>,
                                u64)>,
    },
}

pub type PlaceOrderResult = Result<PlaceOrderOk, PlaceOrderError>;
pub type CancelResult     = Result<SpotOrder, CancelError>;

#[derive(Debug)]
pub struct PlaceOrderOk {
    pub order: SpotOrder,
    pub fills: Vec<SpotTrade>,
}

#[derive(Debug, thiserror::Error)]
pub enum PlaceOrderError {
    #[error("MARKET_NOT_FOUND")]    MarketNotFound,
    #[error("MARKET_HALTED")]       MarketHalted,
    #[error("MARKET_DELISTED")]     MarketDelisted,
    #[error("INVALID_TICK")]        InvalidTick,
    #[error("INVALID_LOT")]         InvalidLot,
    #[error("BELOW_MIN_NOTIONAL")]  BelowMinNotional,
    #[error("INSUFFICIENT_BALANCE")] InsufficientBalance,
    #[error("POST_ONLY_REJECT")]    PostOnlyReject,
    #[error("SELF_TRADE")]          SelfTrade,
    #[error("INVALID_REQUEST: {0}")] InvalidRequest(&'static str),
    #[error("DB_ERROR")]            DbError(#[from] sqlx::Error),
    #[error("ENGINE_BUSY")]         EngineBusy,
    #[error("ENGINE_RESTARTING")]   EngineRestarting,
}

#[derive(Debug, thiserror::Error)]
pub enum CancelError {
    #[error("ORDER_NOT_FOUND")] NotFound,
    #[error("FORBIDDEN")]       Forbidden,
    #[error("DB_ERROR")]        Db(#[from] sqlx::Error),
}

/// Public handle the API layer uses to talk to the engine.
#[derive(Clone)]
pub struct EngineHandle {
    pub cmd_tx: mpsc::Sender<OrderCommand>,
    pub event_tx: broadcast::Sender<EngineEvent>,
}

impl EngineHandle {
    pub async fn place(&self, req: PlaceOrderRequest) -> PlaceOrderResult {
        let (tx, rx) = oneshot::channel();
        if self.cmd_tx.try_send(OrderCommand::NewOrder(req, tx)).is_err() {
            return Err(PlaceOrderError::EngineBusy);
        }
        rx.await.map_err(|_| PlaceOrderError::EngineRestarting)?
    }

    pub async fn cancel(&self, id: uuid::Uuid, user: String) -> CancelResult {
        let (tx, rx) = oneshot::channel();
        if self.cmd_tx.try_send(OrderCommand::Cancel {
            id, user_address: user, reply: tx,
        }).is_err() {
            return Err(CancelError::Db(sqlx::Error::PoolClosed));
        }
        rx.await.map_err(|_| CancelError::Db(sqlx::Error::PoolClosed))?
    }

    pub async fn cancel_all(&self, user: String, market: Option<String>) -> Vec<SpotOrder> {
        let (tx, rx) = oneshot::channel();
        if self.cmd_tx.try_send(OrderCommand::CancelAll {
            user_address: user, market_id: market, reply: tx,
        }).is_err() {
            return vec![];
        }
        rx.await.unwrap_or_default()
    }

    /// Admin: cancel every open / partially-filled order for `market_id`.
    /// Returns the canceled rows as observed by the engine. Used by the
    /// admin "delist" status transition.
    pub async fn cancel_market(&self, market_id: String) -> Vec<SpotOrder> {
        let (tx, rx) = oneshot::channel();
        if self.cmd_tx.try_send(OrderCommand::CancelMarket { market_id, reply: tx }).is_err() {
            return vec![];
        }
        rx.await.unwrap_or_default()
    }

    pub async fn depth(
        &self,
        depth: usize,
    ) -> Result<(Vec<(rust_decimal::Decimal, rust_decimal::Decimal)>,
                 Vec<(rust_decimal::Decimal, rust_decimal::Decimal)>,
                 u64), &'static str>
    {
        let (tx, rx) = oneshot::channel();
        if self.cmd_tx.try_send(OrderCommand::Snapshot { depth, reply: tx }).is_err() {
            return Err("ENGINE_BUSY");
        }
        rx.await.map_err(|_| "ENGINE_RESTARTING")
    }
}
