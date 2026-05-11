//! Prometheus Metrics Module
//!
//! Centralized metrics definitions for all background workers and services.
//! Exposed via GET /metrics endpoint in Prometheus text format.

use prometheus::{
    register_counter_vec, register_gauge, register_gauge_vec, register_histogram_vec,
    CounterVec, Gauge, GaugeVec, HistogramVec, Encoder, TextEncoder,
};

lazy_static::lazy_static! {
    // ========================================================================
    // Generic task-level metrics (all workers)
    // ========================================================================

    /// Unix timestamp of the last time a task's loop body ran
    pub static ref TASK_LAST_RUN_TIMESTAMP: GaugeVec = register_gauge_vec!(
        "task_last_run_timestamp",
        "Unix timestamp of the last heartbeat for a background task",
        &["task"]
    ).unwrap();

    /// Duration of a single task execution cycle
    pub static ref TASK_EXECUTION_DURATION: HistogramVec = register_histogram_vec!(
        "task_execution_duration_seconds",
        "Time spent executing a single cycle of a background task",
        &["task", "symbol"],
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    ).unwrap();

    /// Successful task executions
    pub static ref TASK_SUCCESS_TOTAL: CounterVec = register_counter_vec!(
        "task_update_success_total",
        "Total number of successful task executions",
        &["task", "symbol"]
    ).unwrap();

    /// Failed task executions
    pub static ref TASK_FAILED_TOTAL: CounterVec = register_counter_vec!(
        "task_update_failed_total",
        "Total number of failed task executions",
        &["task", "symbol", "error"]
    ).unwrap();

    // ========================================================================
    // ADL-specific metrics
    // ========================================================================

    /// Number of positions participating in ADL ranking calculation
    pub static ref ADL_RANKING_POSITION_COUNT: GaugeVec = register_gauge_vec!(
        "adl_ranking_position_count",
        "Number of profitable positions in ADL ranking per market/side",
        &["symbol", "side"]
    ).unwrap();

    // ========================================================================
    // Hyperliquid price sync metrics
    // ========================================================================

    /// Number of active trading pairs on the exchange
    pub static ref PRICE_SYNC_SYMBOLS_TOTAL: Gauge = register_gauge!(
        "price_sync_symbols_total",
        "Number of active trading pairs on the exchange"
    ).unwrap();

    // ========================================================================
    // Trade persistence metrics
    // ========================================================================

    /// Total trades processed by persistence worker
    pub static ref TRADE_PERSISTENCE_TOTAL: CounterVec = register_counter_vec!(
        "trade_persistence_total",
        "Total trades processed by persistence/points worker",
        &["status"]
    ).unwrap();

    // ========================================================================
    // Fee adjustment metrics
    // ========================================================================

    /// Current OI imbalance ratio per market
    pub static ref FEE_IMBALANCE_RATIO: GaugeVec = register_gauge_vec!(
        "fee_imbalance_ratio",
        "Current open interest imbalance ratio per market",
        &["symbol"]
    ).unwrap();

    /// Current dynamic long taker fee per market
    pub static ref FEE_LONG_TAKER: GaugeVec = register_gauge_vec!(
        "fee_dynamic_long_taker",
        "Current dynamic long taker fee rate per market",
        &["symbol"]
    ).unwrap();

    /// Current dynamic short taker fee per market
    pub static ref FEE_SHORT_TAKER: GaugeVec = register_gauge_vec!(
        "fee_dynamic_short_taker",
        "Current dynamic short taker fee rate per market",
        &["symbol"]
    ).unwrap();

    // ========================================================================
    // Withdrawal expiry metrics
    // ========================================================================

    /// Total expired withdrawals processed
    pub static ref WITHDRAWAL_EXPIRED_TOTAL: CounterVec = register_counter_vec!(
        "withdrawal_expired_total",
        "Total expired withdrawals processed",
        &["status"]
    ).unwrap();

    // ========================================================================
    // Broadcast channel health
    // ========================================================================

    /// Total lagged events across broadcast channels
    pub static ref CHANNEL_LAGGED_TOTAL: CounterVec = register_counter_vec!(
        "broadcast_channel_lagged_total",
        "Total lagged messages on broadcast channels",
        &["worker"]
    ).unwrap();

    // ========================================================================
    // Keeper trigger orders metrics (business-critical)
    // ========================================================================

    // --- 2.1 Queue / pending orders (DB-based queue) ---

    /// Number of active (pending) trigger orders across all markets
    pub static ref KEEPER_TRIGGER_PENDING_ORDERS: GaugeVec = register_gauge_vec!(
        "keeper_trigger_pending_orders",
        "Current count of active trigger orders pending execution",
        &["side"]
    ).unwrap();

    /// Total pending trigger orders (all sides combined)
    pub static ref KEEPER_TRIGGER_PENDING_ORDERS_TOTAL: Gauge = register_gauge!(
        "keeper_trigger_pending_orders_total",
        "Total count of active trigger orders pending execution"
    ).unwrap();

    // --- 2.2 Task health ---

    /// Actual interval between keeper scans in seconds
    pub static ref KEEPER_TRIGGER_SCAN_INTERVAL: Gauge = register_gauge!(
        "keeper_trigger_scan_interval_seconds",
        "Actual seconds between two consecutive keeper scan cycles"
    ).unwrap();

    // --- 2.3 Scan execution ---

    /// Duration of a full keeper scan cycle (all markets)
    pub static ref KEEPER_TRIGGER_SCAN_DURATION: HistogramVec = register_histogram_vec!(
        "keeper_trigger_scan_duration_seconds",
        "Time spent on a full keeper trigger order scan cycle",
        &["scope"],
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0]
    ).unwrap();

    /// Number of orders matched (trigger condition met) per scan cycle
    pub static ref KEEPER_TRIGGER_MATCHED_PER_SCAN: Gauge = register_gauge!(
        "keeper_trigger_matched_per_scan",
        "Number of trigger orders that matched price condition in last scan"
    ).unwrap();

    // --- 2.4 Trigger results ---

    /// Cumulative count of successfully fired trigger orders
    pub static ref KEEPER_TRIGGER_FIRED_TOTAL: CounterVec = register_counter_vec!(
        "keeper_trigger_fired_total",
        "Total successfully fired trigger orders",
        &["side", "type"]
    ).unwrap();

    /// Cumulative count of failed trigger order executions
    pub static ref KEEPER_TRIGGER_FAILED_TOTAL: CounterVec = register_counter_vec!(
        "keeper_trigger_failed_total",
        "Total failed trigger order executions",
        &["reason"]
    ).unwrap();

    /// Count of orders that missed their trigger window
    pub static ref KEEPER_TRIGGER_MISSED_TOTAL: prometheus::Counter = prometheus::register_counter!(
        "keeper_trigger_missed_total",
        "Total trigger orders that missed their trigger window"
    ).unwrap();

    /// Orders expired by keeper
    pub static ref KEEPER_TRIGGER_EXPIRED_TOTAL: prometheus::Counter = prometheus::register_counter!(
        "keeper_trigger_expired_total",
        "Total trigger orders expired by keeper"
    ).unwrap();

    /// Per-market active trigger order count
    pub static ref KEEPER_TRIGGER_ACTIVE_PER_MARKET: GaugeVec = register_gauge_vec!(
        "keeper_trigger_active_per_market",
        "Active trigger orders per market symbol",
        &["symbol"]
    ).unwrap();

    // --- 2.1 Async queue metrics (DB-based queue) ---

    /// Current depth of the trigger order processing queue
    pub static ref KEEPER_TRIGGER_QUEUE_DEPTH: Gauge = register_gauge!(
        "keeper_trigger_queue_depth",
        "Current number of active trigger orders queued for processing"
    ).unwrap();

    /// Maximum capacity of the trigger order processing queue
    pub static ref KEEPER_TRIGGER_QUEUE_CAPACITY: Gauge = register_gauge!(
        "keeper_trigger_queue_capacity",
        "Maximum capacity of the trigger order processing queue"
    ).unwrap();

    // ========================================================================
    // Persistence worker health metrics (P0 action item #7)
    // ========================================================================

    /// Current number of trades waiting in the persistence queue
    pub static ref PERSISTENCE_QUEUE_DEPTH: Gauge = register_gauge!(
        "persistence_queue_depth",
        "Current number of trades waiting in the persistence mpsc queue"
    ).unwrap();

    /// Number of concurrent persistence tasks currently running
    pub static ref PERSISTENCE_CONCURRENT_TASKS: Gauge = register_gauge!(
        "persistence_concurrent_tasks",
        "Number of concurrent trade persistence tasks currently running"
    ).unwrap();

    /// Counter incremented each time the matching engine's try_send to the
    /// persistence mpsc queue returns Full and we fall back to `spawn(send().await)`.
    /// Non-zero means the persistence pipeline cannot keep up with the trade
    /// firehose at some moment — operators should widen PERSISTENCE_MAX_CONCURRENT_TASKS
    /// or raise FALLBACK_QUEUE_CAPACITY. Pre-fix (before fix/trade-persistence-drop)
    /// this path silently dropped the trade and produced filled-orders-without-trades
    /// orphans (34k+ in 24h as of 2026-04-22).
    pub static ref PERSISTENCE_QUEUE_OVERFLOW_TOTAL: prometheus::Counter = prometheus::register_counter!(
        "persistence_queue_overflow_total",
        "Trade events that overflowed the persistence mpsc queue and were handed off to an async spawn(send)."
    ).unwrap();

    /// Trade persistence latency histogram
    pub static ref PERSISTENCE_LATENCY: HistogramVec = register_histogram_vec!(
        "persistence_latency_seconds",
        "Time spent persisting a single trade (including position updates)",
        &["status"],
        vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0]
    ).unwrap();

    /// Number of orders marked 'filled' 1–10 minutes ago with no
    /// corresponding trade row — i.e. orphans created when the trade
    /// persistence path died mid-flight. The 1-minute lower bound is a
    /// grace window for the async persistence worker; trades still being
    /// written are NOT counted here. Window upper bound tracks the
    /// detector tick interval (see `keeper/mod.rs`). Steady-state value
    /// is 0; any non-zero reading means the persistence pipeline is
    /// dropping events and requires human investigation (no auto-fix on
    /// purpose).
    pub static ref ORPHAN_ORDERS_RECENT: Gauge = register_gauge!(
        "orphan_orders_recent",
        "Orders filled 1–10 minutes ago with no corresponding trade row"
    ).unwrap();

    /// HTTP request duration per route + status — QA batch 4 引入，之前无 API
    /// 级别延迟 histogram 无法观测 p50/p95/p99。路径使用 axum MatchedPath
    /// (路由模板而非具体 URL) 避免 UUID/参数造成基数爆炸。
    pub static ref HTTP_REQUEST_DURATION: HistogramVec = register_histogram_vec!(
        "http_request_duration_seconds",
        "HTTP request handling time per route / method / status",
        &["method", "path", "status"],
        vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    ).unwrap();

    /// Total trades that timed out during persistence
    pub static ref PERSISTENCE_TIMEOUT_TOTAL: prometheus::Counter = prometheus::register_counter!(
        "persistence_timeout_total",
        "Total trades that timed out during persistence (120s limit)"
    ).unwrap();

    /// Total trades written to dead letter queue
    pub static ref PERSISTENCE_DLQ_TOTAL: prometheus::Counter = prometheus::register_counter!(
        "persistence_dlq_total",
        "Total trades written to dead letter queue after all retries failed"
    ).unwrap();

    /// Count of matched trades the matching engine attempted to emit to the
    /// persistence mpsc queue. The point of this counter is pairwise-comparable
    /// with `persistence_latency_seconds_count{status="success"}` and
    /// `trade_persist_entry_total`; if they diverge we can localise the gap:
    ///
    ///   trade_emit_total{outcome="ok"}         – enqueue OK (expected == persisted)
    ///   trade_emit_total{outcome="overflow"}   – queue Full, handed to spawn(send)
    ///   trade_emit_total{outcome="queue_closed"} – receiver gone; trade lost (CRITICAL)
    ///
    /// `orphan_orders_recent` with `persistence_queue_overflow_total=0` and
    /// `persistence_dlq_total=0` told us "nothing is falling off the edge of
    /// the known pipeline", but didn't reveal whether matched trades even
    /// reached the mpsc in the first place — this counter closes that gap.
    pub static ref TRADE_EMIT_TOTAL: CounterVec = register_counter_vec!(
        "trade_emit_total",
        "Matched trades the engine attempted to enqueue to the persistence mpsc queue",
        &["outcome"]
    ).unwrap();

    /// Count of trades that reached `persist_trade_helper` (i.e. were pulled
    /// off the mpsc and have acquired a semaphore permit). Diverging from
    /// `trade_emit_total{outcome in (ok, overflow)}` means the spawned task
    /// died before doing any work — typical culprit is a panic or a
    /// receiver-closed drop during graceful shutdown.
    pub static ref TRADE_PERSIST_ENTRY_TOTAL: prometheus::Counter = prometheus::register_counter!(
        "trade_persist_entry_total",
        "Trades that entered persist_trade_helper (post-semaphore)"
    ).unwrap();

    /// Count of persist_trade_helper invocations that unwound via panic
    /// inside the spawned tokio task. Pre-patch these vanished silently —
    /// tokio returns the panic via the JoinHandle, which we never awaited.
    /// The catch_unwind wrapper in orchestrator converts panics into an
    /// increment of this counter plus a logged stack trace, so operators
    /// can see the shape without reproducing the race.
    pub static ref TRADE_PERSIST_PANIC_TOTAL: prometheus::Counter = prometheus::register_counter!(
        "trade_persist_panic_total",
        "Persist-task invocations that panicked; captured via catch_unwind"
    ).unwrap();

    /// Global process-wide panic counter. Bumped from a custom `std::panic::set_hook`
    /// in main.rs so panics that escape any catch_unwind (e.g. from background
    /// workers we haven't wrapped yet) are at least visible as a non-zero
    /// number, not just silently eaten.
    pub static ref PROCESS_PANIC_TOTAL: CounterVec = register_counter_vec!(
        "process_panic_total",
        "Panics observed by the global panic hook",
        &["thread"]
    ).unwrap();

    // ========================================================================
    // Orderbook depth metrics (P0 action item #8)
    // ========================================================================

    /// Total orders in all orderbooks
    pub static ref ORDERBOOK_TOTAL_ORDERS: Gauge = register_gauge!(
        "orderbook_total_orders",
        "Total number of orders across all orderbooks"
    ).unwrap();

    /// Per-symbol bid depth (total bid volume in USD)
    pub static ref ORDERBOOK_BID_DEPTH: GaugeVec = register_gauge_vec!(
        "orderbook_bid_depth",
        "Total bid volume per symbol in the orderbook",
        &["symbol"]
    ).unwrap();

    /// Per-symbol ask depth (total ask volume in USD)
    pub static ref ORDERBOOK_ASK_DEPTH: GaugeVec = register_gauge_vec!(
        "orderbook_ask_depth",
        "Total ask volume per symbol in the orderbook",
        &["symbol"]
    ).unwrap();

    /// Per-symbol order count
    pub static ref ORDERBOOK_ORDER_COUNT: GaugeVec = register_gauge_vec!(
        "orderbook_order_count",
        "Number of orders per symbol in the orderbook",
        &["symbol"]
    ).unwrap();

    /// Per-symbol bid-ask spread
    pub static ref ORDERBOOK_SPREAD: GaugeVec = register_gauge_vec!(
        "orderbook_spread",
        "Bid-ask spread per symbol",
        &["symbol"]
    ).unwrap();

    /// Orderbook empty alert (1 = empty, 0 = has orders)
    pub static ref ORDERBOOK_EMPTY: GaugeVec = register_gauge_vec!(
        "orderbook_empty",
        "Whether orderbook is empty (1=empty, 0=has orders). Empty orderbook = all market orders cancelled!",
        &["symbol"]
    ).unwrap();

    // ========================================================================
    // Tokio runtime metrics
    // ========================================================================

    /// Number of currently alive async tasks in the Tokio runtime
    pub static ref TOKIO_ALIVE_TASKS: Gauge = register_gauge!(
        "tokio_alive_tasks_count",
        "Number of currently alive async tasks in the Tokio runtime"
    ).unwrap();

    /// Depth of the Tokio runtime global task queue
    pub static ref TOKIO_GLOBAL_QUEUE_DEPTH: Gauge = register_gauge!(
        "tokio_global_queue_depth",
        "Depth of the Tokio runtime global task queue"
    ).unwrap();

    /// Depth of the Tokio spawn_blocking wait queue
    pub static ref TOKIO_BLOCKING_QUEUE_DEPTH: Gauge = register_gauge!(
        "tokio_blocking_queue_depth",
        "Number of blocking threads spawned by the Tokio runtime"
    ).unwrap();

    /// Mean delay between task creation and first poll
    pub static ref TOKIO_MEAN_FIRST_POLL_DELAY: Gauge = register_gauge!(
        "tokio_mean_first_poll_delay_seconds",
        "Mean delay between task spawn and first poll in seconds"
    ).unwrap();

    /// Number of Tokio worker threads
    pub static ref TOKIO_WORKERS_COUNT: Gauge = register_gauge!(
        "tokio_workers_count",
        "Number of Tokio runtime worker threads"
    ).unwrap();

    // ========================================================================
    // Unified Margin Mode metrics (Phase 2+)
    // ========================================================================

    /// Total users currently in unified (portfolio) margin mode.
    pub static ref UNIFIED_ACCOUNTS_TOTAL: Gauge = register_gauge!(
        "unified_accounts_total",
        "Number of users with margin_mode='unified'"
    ).unwrap();

    /// Unified accounts count bucketed by live status
    /// (normal / warning_1 / warning_2 / reduce_only / liquidating).
    pub static ref UNIFIED_ACCOUNTS_BY_STATUS: GaugeVec = register_gauge_vec!(
        "unified_accounts_by_status",
        "Unified-margin accounts count by risk status",
        &["status"]
    ).unwrap();

    /// Distribution of live uniMMR values across all unified accounts,
    /// observed each risk-worker tick. Non-capped buckets above 10
    /// (safe) and tight buckets near the liquidation threshold 1.05.
    pub static ref UNIFIED_UNI_MMR: HistogramVec = register_histogram_vec!(
        "unified_uni_mmr",
        "Distribution of uniMMR across unified accounts",
        &["bucket"],
        vec![1.05, 1.10, 1.20, 1.50, 2.0, 3.0, 5.0, 10.0, 100.0, 1000.0]
    ).unwrap();

    /// Count of forced-liquidation steps fired by the unified risk
    /// worker (one step = one position fully closed via ADL/insurance).
    pub static ref UNIFIED_LIQUIDATION_STEPS_TOTAL: CounterVec = register_counter_vec!(
        "unified_liquidation_steps_total",
        "Number of unified-margin liquidation steps executed",
        &["symbol", "adl"]
    ).unwrap();

    /// Cumulative insurance-fund shortfall on unified liquidations that
    /// had to be covered by ADL (or left uncovered if ADL rejected).
    pub static ref UNIFIED_INSURANCE_SHORTFALL_USD_TOTAL: CounterVec = register_counter_vec!(
        "unified_insurance_shortfall_usd_total",
        "Cumulative USD amount of bad debt not covered by insurance fund",
        &["symbol"]
    ).unwrap();

    /// Number of orders cancelled by reduce_only enforcement across
    /// unified accounts, cumulative since process start.
    pub static ref UNIFIED_REDUCE_ONLY_CANCELLED_TOTAL: Gauge = register_gauge!(
        "unified_reduce_only_cancelled_total",
        "Cumulative count of orders cancelled by reduce_only enforcement"
    ).unwrap();

    /// Per-symbol counter incremented whenever the risk worker observes
    /// a missing mark price for a position. Drives the alert that says
    /// "the price feed for SYMBOL is stale and unified accounts holding
    /// it are being held in reduce_only".
    pub static ref UNIFIED_MISSING_MARK_PRICE_TOTAL: CounterVec = register_counter_vec!(
        "unified_missing_mark_price_total",
        "Risk worker observations of a missing mark price (forces reduce_only)",
        &["symbol"]
    ).unwrap();

    /// Number of unified-margin DB writes the risk worker actually
    /// performed in a tick (vs. dirty-checked away). Used to verify
    /// the write-amplification fix is biting.
    pub static ref UNIFIED_PERSIST_WRITES_TOTAL: CounterVec = register_counter_vec!(
        "unified_persist_writes_total",
        "unified_margin_accounts UPDATE outcomes per tick",
        &["outcome"]   // "written" | "skipped_clean"
    ).unwrap();

    // ========================================================================
    // Orderbook engine instrumentation (orphan / silent-remove investigation)
    // ========================================================================

    /// Makers fully consumed by the matcher and silently removed from the
    /// orderbook. Pairs with `orderbook_cancel_not_found_total` so ops can
    /// tell whether a `cancel_order: TRUE ORPHAN` warn corresponds to a
    /// legitimately-filled maker (matcher_maker_consumed bumps just before
    /// the cancel) or an unexplained removal.
    pub static ref ORDERBOOK_MATCHER_MAKER_CONSUMED_TOTAL: CounterVec = register_counter_vec!(
        "orderbook_matcher_maker_consumed_total",
        "Total fully-filled makers removed from the orderbook by the matcher (per symbol)",
        &["symbol"]
    ).unwrap();

    /// `cancel_order` returned `Ok(false)` — engine had no record of the id.
    /// Categorised by the diagnostic check the cancel path performs.
    /// Categories:
    ///   "fast_path"    — index hit, normal cancel succeeded (NOT counted here)
    ///   "slow_scan"    — index miss but queue scan recovered the order (NOT counted here; logged as `orphan recovered via slow scan`)
    ///   "true_orphan"  — index miss AND queue miss; the warn we're chasing
    pub static ref ORDERBOOK_CANCEL_NOT_FOUND_TOTAL: CounterVec = register_counter_vec!(
        "orderbook_cancel_not_found_total",
        "cancel_order returns Ok(false) categorised by cause (per symbol)",
        &["symbol", "category"]
    ).unwrap();

    /// Engine ↔ DB integrity probe (PR #63: 2026-04-26).
    ///
    /// A periodic worker scans `orders` rows that the DB believes are
    /// `open` / `partially_filled` and limit-typed, and asks the live
    /// orderbook whether each id is still in the index. The PR #59
    /// orphan investigation concluded the cancel-not-found counter was
    /// dominated by benign double-cancel; T46c repro on 2026-04-26
    /// disproved that — placing a fresh far-from-market LIMIT BUY then
    /// an immediate modify or DELETE returned -2011 ~30-50% of the
    /// time, with no trade rows for the order. This counter exposes the
    /// silent-remove population directly so we can chase it without
    /// going through the cancel path.
    ///
    /// Categories:
    ///   "missing_from_engine"  — DB says open, engine has no entry → silent remove
    ///   "present"              — both agree (sanity)
    pub static ref ENGINE_DB_INTEGRITY_TOTAL: CounterVec = register_counter_vec!(
        "engine_db_integrity_total",
        "Periodic check of DB-open orders vs engine.has_order (per symbol, kind)",
        &["symbol", "kind"]
    ).unwrap();

    /// Engine size vs DB count snapshot. Per integrity tick, set the gauge
    /// to the engine's `orderbook_order_count` and the DB's count of
    /// `status IN ('open','partially_filled')` rows. Their gap is the
    /// silent-leak population (orders the engine thinks rest but DB
    /// already terminalised). Discovered 2026-04-26: BNBUSDT engine
    /// reported 10211 while DB had 4506 rows open — engine was carrying
    /// ~5700 phantom makers. Track both numbers as gauges so we can
    /// chart the divergence over time.
    pub static ref ENGINE_DB_SIZE_GAUGE: GaugeVec = register_gauge_vec!(
        "engine_db_size_gauge",
        "Snapshot of engine orderbook size vs DB-open count, per symbol",
        &["symbol", "source"]
    ).unwrap();
}

/// Idempotently materialise `CounterVec` metrics at startup so Grafana
/// dashboards referencing the series don't 404 before the first real
/// event. A zero-increment with no-op labels is enough for Prometheus
/// to emit the series. Called once from `bootstrap`.
pub fn touch_counter_vec_labels() {
    // Seed the two labels the risk worker already produces; the third
    // label vec for insurance shortfall and the liquidation steps vec
    // are per-symbol and would balloon the label cardinality if
    // pre-seeded, so we leave those to real traffic.
    UNIFIED_PERSIST_WRITES_TOTAL
        .with_label_values(&["written"])
        .inc_by(0.0);
    UNIFIED_PERSIST_WRITES_TOTAL
        .with_label_values(&["skipped_clean"])
        .inc_by(0.0);
    // Explicitly NOT touching:
    //   - UNIFIED_LIQUIDATION_STEPS_TOTAL    — per-symbol × per-adl
    //   - UNIFIED_INSURANCE_SHORTFALL_USD_TOTAL — per-symbol
    //   - UNIFIED_MISSING_MARK_PRICE_TOTAL   — per-symbol (66+ markets)
    // These materialize naturally on first real event and pre-seeding
    // them with every symbol would triple the /metrics payload.
}

/// Collect Tokio runtime metrics and update Prometheus gauges.
/// Called periodically by a background worker.
///
/// Note: Most `RuntimeMetrics` methods require `tokio_unstable` cfg.
/// Build with `RUSTFLAGS="--cfg tokio_unstable" cargo build` to enable
/// all metrics (alive tasks, global queue depth, blocking threads, poll delay).
/// Without unstable, only `num_workers()` is available.
#[allow(unexpected_cfgs)]
pub fn collect_tokio_runtime_metrics() {
    let handle = tokio::runtime::Handle::current();
    let metrics = handle.metrics();

    // num_workers() is stable in all tokio versions
    TOKIO_WORKERS_COUNT.set(metrics.num_workers() as f64);

    // The following require tokio_unstable cfg
    #[cfg(tokio_unstable)]
    {
        TOKIO_ALIVE_TASKS.set(metrics.num_alive_tasks() as f64);
        TOKIO_BLOCKING_QUEUE_DEPTH.set(metrics.num_blocking_threads() as f64);
        TOKIO_GLOBAL_QUEUE_DEPTH.set(metrics.global_queue_depth() as f64);

        // Approximate mean first-poll delay from per-worker mean poll time
        let n = metrics.num_workers();
        if n > 0 {
            let mut total_poll_secs: f64 = 0.0;
            for i in 0..n {
                total_poll_secs += metrics.worker_mean_poll_time(i).as_secs_f64();
            }
            TOKIO_MEAN_FIRST_POLL_DELAY.set(total_poll_secs / n as f64);
        }
    }
}

/// Force-initialize all lazy_static metrics so they appear in /metrics output
/// even before their first .inc() / .set() call. Must be called once at startup.
pub fn init_all_metrics() {
    // Touch every lazy_static to trigger registration with Prometheus.

    // --- Keeper trigger results (Counter / CounterVec) ---
    // CounterVec: initialise every expected label combination with 0
    for side in &["buy", "sell"] {
        for typ in &["stop_loss", "take_profit", "stop_limit", "take_profit_limit", "trailing_stop"] {
            KEEPER_TRIGGER_FIRED_TOTAL.with_label_values(&[side, typ]);
        }
    }
    for reason in &["rpc_timeout", "db_error", "price_stale", "invalid_size", "connection", "unknown"] {
        KEEPER_TRIGGER_FAILED_TOTAL.with_label_values(&[reason]);
    }
    // Plain Counter – just deref to trigger lazy_static init
    let _ = KEEPER_TRIGGER_MISSED_TOTAL.get();
    let _ = KEEPER_TRIGGER_EXPIRED_TOTAL.get();

    // --- Task success/failed counters ---
    // Pre-initialise all task × error combinations so they appear in /metrics from startup
    let tasks_with_fixed_symbol = &[
        "orderbook-depth-monitor",
        "volume-refresh",
        "withdrawal-expiry",
        "hyperliquid-price-sync",
        "trade-points-worker",
    ];
    let tasks_with_dynamic_symbol = &[
        "fee-adjustment",
        "adl-ranking-update",
        "kline-update",
        "price-feed",
        "keeper-trigger-orders",
    ];
    let error_classes = &["RowNotFound", "timeout", "connection", "other"];

    for task in tasks_with_fixed_symbol {
        TASK_SUCCESS_TOTAL.with_label_values(&[task, "all"]);
        for err in error_classes {
            TASK_FAILED_TOTAL.with_label_values(&[task, "all", err]);
        }
    }
    for task in tasks_with_dynamic_symbol {
        // Use "_total" as a sentinel symbol; per-symbol labels are created dynamically
        TASK_SUCCESS_TOTAL.with_label_values(&[task, "_total"]);
        for err in error_classes {
            TASK_FAILED_TOTAL.with_label_values(&[task, "_total", err]);
        }
    }

    // --- Queue gauges ---
    let _ = KEEPER_TRIGGER_QUEUE_DEPTH.get();
    let _ = KEEPER_TRIGGER_QUEUE_CAPACITY.get();

    // --- Tokio runtime gauges ---
    let _ = TOKIO_ALIVE_TASKS.get();
    let _ = TOKIO_GLOBAL_QUEUE_DEPTH.get();
    let _ = TOKIO_BLOCKING_QUEUE_DEPTH.get();
    let _ = TOKIO_MEAN_FIRST_POLL_DELAY.get();
    let _ = TOKIO_WORKERS_COUNT.get();

    // --- Persistence worker health gauges ---
    let _ = PERSISTENCE_QUEUE_DEPTH.get();
    let _ = PERSISTENCE_CONCURRENT_TASKS.get();
    let _ = PERSISTENCE_QUEUE_OVERFLOW_TOTAL.get();
    for status in &["success", "failure"] {
        PERSISTENCE_LATENCY.with_label_values(&[status]);
    }
    let _ = PERSISTENCE_TIMEOUT_TOTAL.get();
    let _ = PERSISTENCE_DLQ_TOTAL.get();
    let _ = ORPHAN_ORDERS_RECENT.get();
    // Force HTTP histogram registration (empty labels will show up only after first request).
    let _ = &*HTTP_REQUEST_DURATION;

    // --- Orderbook depth gauges ---
    let _ = ORDERBOOK_TOTAL_ORDERS.get();

    tracing::info!("All Prometheus metrics initialized and registered");
}

/// Render all metrics as Prometheus text format
pub fn gather_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Classify trigger order execution error into a low-cardinality reason label
pub fn classify_trigger_error(error: &str) -> &'static str {
    let e = error.to_lowercase();
    if e.contains("timeout") || e.contains("timed out") {
        "rpc_timeout"
    } else if e.contains("database") || e.contains("sqlx") || e.contains("db") || e.contains("rownotfound") {
        "db_error"
    } else if e.contains("price") && (e.contains("stale") || e.contains("invalid") || e.contains("zero")) {
        "price_stale"
    } else if e.contains("size") || e.contains("amount") {
        "invalid_size"
    } else if e.contains("connection") {
        "connection"
    } else {
        "unknown"
    }
}

/// Helper: record a timed operation for a task+symbol and increment success/failure counters
pub struct TaskTimer {
    task: &'static str,
    symbol: String,
    start: std::time::Instant,
}

impl TaskTimer {
    pub fn start(task: &'static str, symbol: &str) -> Self {
        Self {
            task,
            symbol: symbol.to_string(),
            start: std::time::Instant::now(),
        }
    }

    pub fn success(self) {
        let elapsed = self.start.elapsed().as_secs_f64();
        TASK_EXECUTION_DURATION
            .with_label_values(&[self.task, &self.symbol])
            .observe(elapsed);
        TASK_SUCCESS_TOTAL
            .with_label_values(&[self.task, &self.symbol])
            .inc();
    }

    pub fn failure(self, error: &str) {
        let elapsed = self.start.elapsed().as_secs_f64();
        TASK_EXECUTION_DURATION
            .with_label_values(&[self.task, &self.symbol])
            .observe(elapsed);
        // Truncate error to avoid cardinality explosion
        let error_class = if error.contains("RowNotFound") {
            "RowNotFound"
        } else if error.contains("timeout") || error.contains("Timeout") {
            "timeout"
        } else if error.contains("connection") {
            "connection"
        } else {
            "other"
        };
        TASK_FAILED_TOTAL
            .with_label_values(&[self.task, &self.symbol, error_class])
            .inc();
    }
}
