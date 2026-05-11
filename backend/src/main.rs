//! Backend - Main Entry Point
//!
//! This is the application entry point. All business logic is organized in submodules:
//! - `app::bootstrap` - Service initialization
//! - `app::workers` - Background workers
//! - `app::routes` - HTTP router and CORS
//! - `app::state` - Application state

use std::net::SocketAddr;

use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod api;
mod app;
mod auth;
mod cache;
mod config;
mod constants;
mod db;
mod i18n;
mod models;
mod services;
mod utils;
mod websocket;

use crate::config::AppConfig;

// Re-export AppState and OrderUpdateEvent for backward compatibility
// Other modules use `crate::AppState` to access these types
pub use app::{AppState, OrderUpdateEvent};

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name_fn(|| {
            static ATOMIC_ID: std::sync::atomic::AtomicUsize =
                std::sync::atomic::AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            format!("ztdx-worker-{}", id)
        })
        .enable_all()
        .build()
        .expect("Failed to build Tokio runtime");

    runtime.block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ztdx_backend=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Install a process-wide panic hook BEFORE spawning any workers. Tokio
    // task panics default to landing in the JoinHandle and vanishing when
    // the handle is dropped — there are several places we `tokio::spawn` and
    // never `.await` the result (e.g. trade persistence, worker loops), so
    // those panics were invisible until the 2026-04-24 `orphan_orders_recent`
    // spike forced us to look. The hook here bumps a labelled counter and
    // logs the panic payload + backtrace to stderr, so we have at least one
    // observable trace when things go sideways.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let thread_name = std::thread::current()
            .name()
            .unwrap_or("unnamed")
            .to_string();
        crate::services::metrics::PROCESS_PANIC_TOTAL
            .with_label_values(&[&thread_name])
            .inc();
        tracing::error!("🔥 panic on thread {}: {}", thread_name, info);
        default_hook(info);
    }));

    // Load and validate configuration
    dotenvy::dotenv().ok();
    // Initialize EIP-712 domain names from env before anything reads them
    crate::constants::eip712_domains::init_from_env();
    let config = AppConfig::load()?;

    if config.validate_config_on_start {
        config::validator::ConfigValidator::validate(&config)?;
        tracing::info!("✅ Configuration validated for {} environment", config.environment);
    }

    tracing::info!("Starting Backend v{}", env!("CARGO_PKG_VERSION"));
    tracing::info!("Environment: {}", config.environment);

    // Initialize all services
    let state = app::initialize_services(config.clone()).await?;

    // Start background workers
    app::start_workers(state.clone()).await;

    // Build router and start server
    let app = app::create_router(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    tracing::info!("Server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;

    // Use graceful shutdown to ensure pending trades are persisted
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tracing::info!("Server shutdown complete");
    Ok(())
}

/// Graceful shutdown signal handler
///
/// Listens for SIGTERM (from systemd) or SIGINT (Ctrl+C) and initiates graceful shutdown.
/// This gives the persistence worker time to finish processing pending trades.
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("⚠️ Received Ctrl+C, initiating graceful shutdown...");
        }
        _ = terminate => {
            tracing::info!("⚠️ Received SIGTERM, initiating graceful shutdown...");
        }
    }

    // Give the persistence worker a grace period to finish processing
    tracing::info!("🔄 Waiting 3 seconds for pending trades to be persisted...");
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    tracing::info!("✅ Grace period complete, shutting down...");
}
