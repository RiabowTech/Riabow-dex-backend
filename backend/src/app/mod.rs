//! Application module
//!
//! Contains application state, service bootstrap, background workers, and router configuration.

pub mod bootstrap;
pub mod routes;
pub mod state;
pub mod workers;

pub use bootstrap::initialize_services;
pub use routes::create_router;
pub use state::{AppState, OrderUpdateEvent};
pub use workers::start_workers;
