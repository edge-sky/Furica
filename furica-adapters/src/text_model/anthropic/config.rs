//! Existing provider configuration; credentials are intentionally not Debug.
use std::time::Duration;

#[derive(Clone)]
pub struct Config {
    pub endpoint: String,
    pub api_key: String,
    pub model: String,
    pub isolation_scope: String,
    pub connect_timeout: Duration,
    pub version: String,
    /// Explicit model capabilities; unsupported request controls fail before I/O.
    pub supports_adaptive_thinking: bool,
    pub supports_thinking_budget: bool,
}
impl Config {
    pub fn new(endpoint: String, api_key: String, model: String, isolation_scope: String) -> Self {
        Self {
            endpoint,
            api_key,
            model,
            isolation_scope,
            connect_timeout: Duration::from_secs(10),
            version: "2023-06-01".into(),
            supports_adaptive_thinking: false,
            supports_thinking_budget: false,
        }
    }
}
