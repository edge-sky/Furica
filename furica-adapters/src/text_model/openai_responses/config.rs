//! Existing provider configuration; credentials are intentionally not Debug.
use std::time::Duration;

#[derive(Clone)]
pub struct Config {
    pub endpoint: String,
    pub api_key: String,
    pub model: String,
    pub isolation_scope: String,
    pub connect_timeout: Duration,
    pub supports_reasoning: bool,
}
impl Config {
    pub fn new(endpoint: String, api_key: String, model: String, isolation_scope: String) -> Self {
        Self {
            endpoint,
            api_key,
            model,
            isolation_scope,
            connect_timeout: Duration::from_secs(10),
            supports_reasoning: false,
        }
    }
}
