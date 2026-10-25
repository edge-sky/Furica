//! Host-selected ACP process and timeout configuration.
use std::{path::PathBuf, time::Duration};

#[derive(Clone, Debug)]
pub struct AcpConfig {
    pub command: PathBuf,
    /// Stable host configuration reference, included in session provenance.
    pub agent_id: String,
    pub args: Vec<String>,
    pub auth_method: Option<String>,
    pub handshake_timeout: Duration,
    /// Maximum duration of one prompt round.
    pub run_timeout: Duration,
    /// Time allowed for the agent to confirm cancellation or session close.
    pub cancel_timeout: Duration,
    /// Connections with no open sessions are released after this period.
    pub idle_timeout: Duration,
}

impl AcpConfig {
    pub fn new(command: impl Into<PathBuf>) -> Self {
        let command = command.into();
        let agent_id = command.to_string_lossy().into_owned();
        Self {
            command,
            agent_id,
            args: Vec::new(),
            auth_method: None,
            handshake_timeout: Duration::from_secs(15),
            run_timeout: Duration::from_secs(600),
            cancel_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(1_800),
        }
    }
}
