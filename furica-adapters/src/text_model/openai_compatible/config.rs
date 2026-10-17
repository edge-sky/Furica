//! Existing provider configuration; credentials are intentionally not Debug.
use std::time::Duration;

/// Full endpoint (including path). No automatic endpoint/parameter fallback.
#[derive(Clone)]
pub struct Config {
    pub endpoint: String,
    pub api_key: String,
    pub model: String,
    /// Credential-free account/tenant identifier; change it when changing accounts.
    pub isolation_scope: String,
    pub connect_timeout: Duration,
    /// Exactly `max_completion_tokens` or `max_tokens`.
    pub max_tokens_field: String,
    pub supports_reasoning_effort: bool,
}
impl Config {
    pub fn new(endpoint: String, api_key: String, model: String, isolation_scope: String) -> Self {
        Self {
            endpoint,
            api_key,
            model,
            isolation_scope,
            connect_timeout: Duration::from_secs(10),
            max_tokens_field: "max_completion_tokens".into(),
            supports_reasoning_effort: false,
        }
    }
}
