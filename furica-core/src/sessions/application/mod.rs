//! Session use cases and host data exchange; no storage dependencies.
mod acp_records;
mod data;
mod manager;
mod state;
pub use data::SessionData;
pub use manager::{SessionLimits, SessionManager};

mod bindings;
pub use bindings::{AgentBinding, WorkspaceOption};
