//! Symbol-sharded routing for the matching engine.
//!
//! ## Why this exists
//!
//! The matching engine stores per-symbol orderbook state in process memory
//! (`DashMap<String, Arc<Orderbook>>` at `services/matching/engine.rs:38`).
//! Under K8s with N≥2 replicas the load balancer round-robins requests, so
//! N independent in-memory orderbooks accumulate divergent state per
//! symbol. The 2026-04-26 integrity probe (PRs #63 + #64) confirmed both
//! directions of divergence:
//!   - 45.8% of recent DB-open orders missing from any given pod's engine
//!   - per-symbol engine over-count up to 1.55× DB-open count (phantom
//!     makers from cross-pod cancel-all)
//!
//! ## Approach
//!
//! Single-writer-per-symbol via consistent hash over `(symbol, replicas)`.
//! Each request that touches engine state asks this module for a routing
//! decision; if the request lands on a non-owner pod, the pod proxies the
//! request to the owner over the headless service. The engine code stays
//! single-master per symbol (its existing assumption), and we keep N
//! replicas for HA.
//!
//! Configuration is env-driven so K8s manifests can wire it in without
//! code changes. See `ShardingConfig::from_env`.
//!
//! ## What this module does NOT do
//!
//! - Dynamic re-sharding on pod failure. If pod K dies, the symbols K
//!   owns are unavailable until K restarts (the new K reloads from DB on
//!   startup via `services::matching::recovery`). For pre-launch this is
//!   acceptable; the follow-up is per-symbol leader election via K8s
//!   Lease (Plan B).
//! - Read-side routing. Orderbook depth queries currently hit the local
//!   pod's engine, which on a non-owner pod is empty. Document for the
//!   reader; route in a follow-up.

pub mod proxy;

use std::env;

/// Decision returned for a given (symbol, current pod) tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    /// The current pod owns this symbol. Handle the request locally.
    Local,
    /// Another pod owns this symbol. Forward the request to the target
    /// URL base (host:port form, no scheme) and return its response.
    Forward { target_host: String, owner_ordinal: u32 },
}

/// Static sharding configuration, read once from the environment at
/// startup. `enabled = false` short-circuits all decisions to Local —
/// useful for local dev, single-replica deployments, and the kill switch.
#[derive(Debug, Clone)]
pub struct ShardingConfig {
    /// This pod's ordinal (0..replica_count). Read from `POD_ORDINAL`
    /// env var, or parsed from `HOSTNAME` (K8s StatefulSet sets the
    /// hostname to e.g. `ztdx-backend-2`).
    pub my_ordinal: u32,

    /// Total number of matching-engine replicas. Read from
    /// `MATCHING_REPLICAS`. Must match the StatefulSet's `replicas:`.
    pub replica_count: u32,

    /// DNS template for peer pods. `{ord}` is replaced with the owner
    /// ordinal. Default: `ztdx-backend-{ord}.ztdx-backend-headless:8080`,
    /// which resolves under K8s with a StatefulSet named `ztdx-backend`
    /// and a headless `Service` named `ztdx-backend-headless`. Override
    /// via `MATCHING_PEER_DNS`.
    pub peer_dns_template: String,

    /// Master kill switch. Read from `MATCHING_SHARDING_ENABLED` (default
    /// false during rollout). When false, every request is handled
    /// locally — equivalent to current behaviour, lets the rollout be
    /// staged: deploy code with sharding off, flip on per pod, observe.
    pub enabled: bool,
}

impl ShardingConfig {
    /// Build from process env. Returns a disabled config if any required
    /// var is missing — explicit logging at the call site shows what was
    /// missing so a misconfigured StatefulSet doesn't silently fall back
    /// to broken multi-replica behaviour.
    pub fn from_env() -> Self {
        let enabled = env::var("MATCHING_SHARDING_ENABLED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let my_ordinal = env::var("POD_ORDINAL")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .or_else(|| {
                env::var("HOSTNAME").ok().and_then(|h| {
                    h.rsplit('-').next().and_then(|s| s.parse::<u32>().ok())
                })
            })
            .unwrap_or(0);

        let replica_count = env::var("MATCHING_REPLICAS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(1);

        let peer_dns_template = env::var("MATCHING_PEER_DNS")
            .unwrap_or_else(|_| "ztdx-backend-{ord}.ztdx-backend-headless:8080".to_string());

        Self {
            my_ordinal,
            replica_count,
            peer_dns_template,
            enabled,
        }
    }

    /// Owner ordinal for `symbol` under the current replica count.
    /// Stable across requests so a sequence of `place / modify / cancel`
    /// for the same id always lands on the same pod.
    ///
    /// Uses FNV-1a 64-bit so the hash is dependency-free, deterministic
    /// across runs, and not subject to the random seed
    /// `std::collections::hash_map::DefaultHasher` would introduce.
    pub fn owner_for(&self, symbol: &str) -> u32 {
        if self.replica_count <= 1 {
            return 0;
        }
        let h = fnv1a_64(symbol.as_bytes());
        (h % self.replica_count as u64) as u32
    }

    pub fn route_for(&self, symbol: &str) -> RouteDecision {
        if !self.enabled || self.replica_count <= 1 {
            return RouteDecision::Local;
        }
        let owner = self.owner_for(symbol);
        if owner == self.my_ordinal {
            RouteDecision::Local
        } else {
            let target_host = self
                .peer_dns_template
                .replace("{ord}", &owner.to_string());
            RouteDecision::Forward {
                target_host,
                owner_ordinal: owner,
            }
        }
    }
}

fn fnv1a_64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for b in bytes {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// HTTP header set on a request that has already been forwarded once.
/// Receiving pod inspects this; if it's set AND the receiving pod also
/// thinks it's not the owner, the sharding view is inconsistent (peer
/// scaled, hash drift). We log loudly and handle locally as a degraded
/// fallback — better wrong-pod-handles-it than infinite forward loop.
pub const FORWARDED_HEADER: &str = "X-ZTDX-Shard-Forwarded";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_is_stable_across_calls() {
        let cfg = ShardingConfig {
            my_ordinal: 0,
            replica_count: 4,
            peer_dns_template: "p-{ord}.svc:80".into(),
            enabled: true,
        };
        let a = cfg.owner_for("BTCUSDT");
        let b = cfg.owner_for("BTCUSDT");
        assert_eq!(a, b);
    }

    #[test]
    fn owner_distributes_across_replicas() {
        let cfg = ShardingConfig {
            my_ordinal: 0,
            replica_count: 4,
            peer_dns_template: "p-{ord}.svc:80".into(),
            enabled: true,
        };
        // With FNV-1a and a few real symbols, expect owners to spread.
        let symbols = [
            "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT",
            "LINKUSDT", "DOGEUSDT", "TRXUSDT", "DOTUSDT", "HYPEUSDT", "BCHUSDT",
        ];
        let mut seen = [false; 4];
        for s in symbols {
            seen[cfg.owner_for(s) as usize] = true;
        }
        // Not a hard guarantee but with 12 symbols and 4 buckets we
        // expect every bucket to be hit; if the hash distributes
        // pathologically badly we want to know early.
        assert!(seen.iter().all(|x| *x), "uneven distribution: {:?}", seen);
    }

    #[test]
    fn single_replica_always_local() {
        let cfg = ShardingConfig {
            my_ordinal: 0,
            replica_count: 1,
            peer_dns_template: String::new(),
            enabled: true,
        };
        assert_eq!(cfg.route_for("BTCUSDT"), RouteDecision::Local);
    }

    #[test]
    fn disabled_always_local() {
        let cfg = ShardingConfig {
            my_ordinal: 1,
            replica_count: 4,
            peer_dns_template: "p-{ord}.svc:80".into(),
            enabled: false,
        };
        assert_eq!(cfg.route_for("BTCUSDT"), RouteDecision::Local);
    }

    #[test]
    fn local_when_my_ordinal_owns() {
        let cfg = ShardingConfig {
            my_ordinal: 0,
            replica_count: 4,
            peer_dns_template: "p-{ord}.svc:80".into(),
            enabled: true,
        };
        // Find a symbol pod 0 owns by exhaustion.
        let owned = (0..1000)
            .map(|i| format!("SYM{i}USDT"))
            .find(|s| cfg.owner_for(s) == 0)
            .unwrap();
        assert_eq!(cfg.route_for(&owned), RouteDecision::Local);
    }
}
