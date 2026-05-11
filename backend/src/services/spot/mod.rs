//! Spot subsystem.
//!
//! Sub-project 1 of 4: DF wallet (deposit/withdraw on BSC) + perp↔spot
//! USDT internal transfer. Hard isolation from perp code: never import
//! crate::services::{blockchain,withdraw,matching,position,liquidation,
//! funding,mm_pool,unified_margin}, never import crate::models::{Position,
//! Order,Trade}. The single explicit boundary point is wallet.rs::transfer
//! which reads/writes the `balances` table directly via SQL.
//!
//! Reference: docs/superpowers/specs/2026-05-09-spot-df-wallet-design.md

pub mod blockchain;
pub mod config;
pub mod markets;
pub mod reaper;
pub mod reconciler;
pub mod wallet;
pub mod withdraw_signer;
pub mod matching;
pub mod market_data;
pub mod kline_aggregator;
pub mod ticker_aggregator;
pub mod ws_messages;
pub mod ws_publisher;
