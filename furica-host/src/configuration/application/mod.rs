use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf, time::Duration};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostConfig {
    pub http: HttpConfig,
    pub workspace: PathBuf,
    pub model: ModelConfig,
    pub speech: Option<SpeechConfig>,
    pub acp: AcpConfig,
    #[serde(default)]
    pub limits: RuntimeLimits,
    pub instructions: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    #[serde(default = "default_listen")]
    pub listen: SocketAddr,
    pub token_env: String,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "protocol", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelConfig {
    OpenaiCompatible {
        endpoint: String,
        model: String,
        isolation_scope: String,
        api_key_env: String,
        #[serde(default = "ten_seconds")]
        connect_timeout_ms: u64,
        #[serde(default = "default_token_field")]
        max_tokens_field: String,
        #[serde(default)]
        supports_reasoning_effort: bool,
    },
    OpenaiResponses {
        endpoint: String,
        model: String,
        isolation_scope: String,
        api_key_env: String,
        #[serde(default = "ten_seconds")]
        connect_timeout_ms: u64,
        #[serde(default)]
        supports_reasoning: bool,
    },
    Anthropic {
        endpoint: String,
        model: String,
        isolation_scope: String,
        api_key_env: String,
        #[serde(default = "ten_seconds")]
        connect_timeout_ms: u64,
        #[serde(default = "default_anthropic_version")]
        version: String,
        #[serde(default)]
        supports_adaptive_thinking: bool,
        #[serde(default)]
        supports_thinking_budget: bool,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpeechConfig {
    pub vad_model: PathBuf,
    pub stt: SttConfig,
    pub tts: TtsConfig,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum SttConfig {
    Sensevoice {
        model_path: PathBuf,
        tokens_path: PathBuf,
    },
    Aliyun {
        endpoint: String,
        model: String,
        api_key_env: String,
    },
    Volcengine {
        endpoint: String,
        model: String,
        api_key_env: String,
        resource_id: String,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum TtsConfig {
    Aliyun {
        endpoint: String,
        model: String,
        voice: String,
        api_key_env: String,
    },
    Volcengine {
        endpoint: String,
        model: String,
        voice: String,
        api_key_env: String,
        resource_id: String,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpConfig {
    pub command: PathBuf,
    pub agent_id: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub auth_method: Option<String>,
    #[serde(default = "fifteen_seconds")]
    pub handshake_timeout_ms: u64,
    #[serde(default = "ten_minutes")]
    pub run_timeout_ms: u64,
    #[serde(default = "five_seconds")]
    pub cancel_timeout_ms: u64,
    #[serde(default = "thirty_minutes")]
    pub idle_timeout_ms: u64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeLimits {
    pub channel_capacity: usize,
    pub connect_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub inference_timeout_ms: u64,
    pub max_history_messages: usize,
    pub max_context_bytes: usize,
    pub max_output_tokens: u32,
    pub max_utterance_ms: u64,
    pub max_generated_audio_ms: u64,
    pub max_acp_sessions: usize,
    pub max_runs: usize,
    pub question_timeout_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub upload_retry_limit: u32,
    pub upload_retry_window_ms: u64,
    pub snapshot_max_bytes: u64,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            channel_capacity: 128,
            connect_timeout_ms: 10_000,
            idle_timeout_ms: 60_000,
            inference_timeout_ms: 60_000,
            max_history_messages: 128,
            max_context_bytes: 256 * 1024,
            max_output_tokens: 4096,
            max_utterance_ms: 30_000,
            max_generated_audio_ms: 60_000,
            max_acp_sessions: 4,
            max_runs: 32,
            question_timeout_ms: 300_000,
            heartbeat_timeout_ms: 15_000,
            upload_retry_limit: 3,
            upload_retry_window_ms: 2000,
            snapshot_max_bytes: 64 * 1024 * 1024,
        }
    }
}

fn default_listen() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 8765))
}
fn default_token_field() -> String {
    "max_completion_tokens".into()
}
fn default_anthropic_version() -> String {
    "2023-06-01".into()
}
fn five_seconds() -> u64 {
    5000
}
fn ten_seconds() -> u64 {
    10000
}
fn fifteen_seconds() -> u64 {
    15000
}
fn ten_minutes() -> u64 {
    600_000
}
fn thirty_minutes() -> u64 {
    1_800_000
}

fn present(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 8192 && !value.chars().any(char::is_control)
}
pub fn env_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .enumerate()
            .all(|(i, b)| b == b'_' || b.is_ascii_alphabetic() || (i > 0 && b.is_ascii_digit()))
}
fn endpoint(value: &str) -> bool {
    provider_endpoint(value, false)
}
fn provider_endpoint(value: &str, websocket: bool) -> bool {
    if !present(value) || value.contains('#') {
        return false;
    }
    let Ok(uri) = value.parse::<hyper::Uri>() else {
        return false;
    };
    let Some(authority) = uri.authority() else {
        return false;
    };
    if authority.as_str().contains('@') || authority.host().is_empty() {
        return false;
    }
    let host = authority.host().trim_matches(['[', ']']);
    let local = host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    let (secure, insecure) = if websocket {
        ("wss", "ws")
    } else {
        ("https", "http")
    };
    uri.scheme_str() == Some(secure) || (local && uri.scheme_str() == Some(insecure))
}

impl HostConfig {
    pub fn validate(&self) -> Result<()> {
        let l = &self.limits;
        if !self.http.listen.ip().is_loopback()
            || !env_name(&self.http.token_env)
            || !self.workspace.is_absolute()
            || !self.acp.command.is_absolute()
            || !present(&self.acp.agent_id)
            || self.acp.args.len() > 128
            || self.acp.args.iter().any(|s| s.len() > 8192)
            || self.acp.auth_method.as_ref().is_some_and(|s| !present(s))
            || [
                self.acp.handshake_timeout_ms,
                self.acp.run_timeout_ms,
                self.acp.cancel_timeout_ms,
                self.acp.idle_timeout_ms,
                l.connect_timeout_ms,
                l.idle_timeout_ms,
                l.inference_timeout_ms,
                l.question_timeout_ms,
            ]
            .iter()
            .any(|value| *value == 0 || *value > 86_400_000)
            || l.channel_capacity == 0
            || l.channel_capacity > 4096
            || l.max_history_messages == 0
            || l.max_history_messages > 4096
            || l.max_context_bytes == 0
            || l.max_context_bytes > 4 * 1024 * 1024
            || l.max_output_tokens == 0
            || l.max_utterance_ms == 0
            || l.max_utterance_ms > 120_000
            || l.max_generated_audio_ms == 0
            || l.max_generated_audio_ms > 120_000
            || l.max_acp_sessions == 0
            || l.max_acp_sessions > 64
            || l.max_runs == 0
            || l.max_runs > 1024
            || l.heartbeat_timeout_ms <= 5000
            || l.heartbeat_timeout_ms > 300_000
            || l.upload_retry_limit == 0
            || l.upload_retry_limit > 32
            || l.upload_retry_window_ms == 0
            || l.upload_retry_window_ms > 30_000
            || l.snapshot_max_bytes == 0
            || l.snapshot_max_bytes > 1024 * 1024 * 1024
            || self
                .instructions
                .as_ref()
                .is_some_and(|s| s.len() > l.max_context_bytes)
            || self.http.allowed_origins.len() > 32
            || self.http.allowed_origins.iter().any(|s| !valid_origin(s))
        {
            return Err(Error::config());
        }
        let (url, name, scope, key, timeout) = match &self.model {
            ModelConfig::OpenaiCompatible {
                endpoint,
                model,
                isolation_scope,
                api_key_env,
                connect_timeout_ms,
                max_tokens_field,
                ..
            } => {
                if !matches!(
                    max_tokens_field.as_str(),
                    "max_tokens" | "max_completion_tokens"
                ) {
                    return Err(Error::config());
                }
                (
                    endpoint,
                    model,
                    isolation_scope,
                    api_key_env,
                    connect_timeout_ms,
                )
            }
            ModelConfig::OpenaiResponses {
                endpoint,
                model,
                isolation_scope,
                api_key_env,
                connect_timeout_ms,
                ..
            } => (
                endpoint,
                model,
                isolation_scope,
                api_key_env,
                connect_timeout_ms,
            ),
            ModelConfig::Anthropic {
                endpoint,
                model,
                isolation_scope,
                api_key_env,
                connect_timeout_ms,
                version,
                ..
            } => {
                if !present(version) {
                    return Err(Error::config());
                }
                (
                    endpoint,
                    model,
                    isolation_scope,
                    api_key_env,
                    connect_timeout_ms,
                )
            }
        };
        if !endpoint(url)
            || !present(name)
            || !present(scope)
            || !env_name(key)
            || *timeout == 0
            || *timeout > 86_400_000
        {
            return Err(Error::config());
        }
        if let Some(s) = &self.speech {
            if !s.vad_model.is_absolute() {
                return Err(Error::config());
            }
            match &s.stt {
                SttConfig::Sensevoice {
                    model_path,
                    tokens_path,
                } if !model_path.is_absolute() || !tokens_path.is_absolute() => {
                    return Err(Error::config());
                }
                SttConfig::Aliyun {
                    endpoint: url,
                    model,
                    api_key_env,
                }
                | SttConfig::Volcengine {
                    endpoint: url,
                    model,
                    api_key_env,
                    ..
                } if !endpoint(url) || !present(model) || !env_name(api_key_env) => {
                    return Err(Error::config());
                }
                _ => (),
            }
            match &s.tts {
                TtsConfig::Aliyun {
                    endpoint: url,
                    model,
                    voice,
                    api_key_env,
                }
                | TtsConfig::Volcengine {
                    endpoint: url,
                    model,
                    voice,
                    api_key_env,
                    ..
                } => {
                    if !provider_endpoint(url, matches!(s.tts, TtsConfig::Aliyun { .. }))
                        || !present(model)
                        || !present(voice)
                        || !env_name(api_key_env)
                    {
                        return Err(Error::config());
                    }
                }
            }
            if matches!(&s.stt, SttConfig::Volcengine {resource_id,..} if !present(resource_id))
                || matches!(&s.tts, TtsConfig::Volcengine {resource_id,..} if !present(resource_id))
            {
                return Err(Error::config());
            }
        }
        Ok(())
    }
    pub fn core_config(&self) -> furica_core::config::CoreConfig {
        furica_core::config::CoreConfig {
            sample_rate: 16_000,
            connect_timeout: Duration::from_millis(self.limits.connect_timeout_ms),
            idle_timeout: Duration::from_millis(self.limits.idle_timeout_ms),
            channel_capacity: self.limits.channel_capacity,
            ..Default::default()
        }
    }
    pub fn session_limits(&self) -> furica_core::sessions::SessionLimits {
        furica_core::sessions::SessionLimits {
            max_acp_sessions: self.limits.max_acp_sessions,
            max_runs: self.limits.max_runs,
            question_timeout: Duration::from_millis(self.limits.question_timeout_ms),
        }
    }
}

fn valid_origin(value: &str) -> bool {
    let Some((scheme, authority)) = value.split_once("://") else {
        return false;
    };
    matches!(scheme, "http" | "https")
        && !authority.is_empty()
        && authority.len() <= 512
        && !authority
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '/' | '?' | '#' | '@' | '*'))
}
