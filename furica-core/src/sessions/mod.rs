//! Long-lived business sessions with one main context and independent ACP contexts.
mod application;
pub mod domain;
pub mod ports;
pub use application::{AgentBinding, SessionData, SessionLimits, SessionManager, WorkspaceOption};
pub use domain::*;
pub use ports::{ExecutionEvent, ExecutionInput, Executor};
