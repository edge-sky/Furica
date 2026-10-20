//! ACP v1 executor adapter, enabled with feature `acp`.
mod config;
mod infrastructure;
pub use config::AcpConfig;
pub use infrastructure::AcpExecutor;
