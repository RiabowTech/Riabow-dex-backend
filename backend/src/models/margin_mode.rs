//! Margin Mode enum — isolated vs unified (portfolio margin)
//!
//! See design_docs/08_统一保证金模式设计.md

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MarginMode {
    Isolated,
    Unified,
}

impl Default for MarginMode {
    fn default() -> Self {
        MarginMode::Isolated
    }
}

impl fmt::Display for MarginMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MarginMode::Isolated => write!(f, "isolated"),
            MarginMode::Unified => write!(f, "unified"),
        }
    }
}

impl FromStr for MarginMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "isolated" => Ok(MarginMode::Isolated),
            "unified" => Ok(MarginMode::Unified),
            other => Err(format!("invalid margin_mode: {}", other)),
        }
    }
}
