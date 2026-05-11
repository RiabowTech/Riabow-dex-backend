//! Spot domain models.

pub mod balance;
pub mod deposit;
pub mod withdrawal;
pub mod transfer;

pub mod market;
pub mod order;
pub mod trade;
pub mod kline;
pub mod ticker;
pub mod admin_credit;

pub use balance::SpotBalance;
pub use deposit::SpotDeposit;
pub use withdrawal::SpotWithdrawal;
pub use transfer::{SpotInternalTransfer, TransferDirection};

pub use market::{SpotMarket, MarketStatus};
pub use order::{SpotOrder, Side, OrderType, Tif, OrderStatus};
pub use trade::SpotTrade;
pub use kline::SpotKline;
pub use ticker::SpotTicker24h;
pub use admin_credit::SpotAdminCredit;
