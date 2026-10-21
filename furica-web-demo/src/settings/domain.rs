//! Host-owned service settings; Core contracts remain unchanged.
use crate::bootstrap::{SAMPLE_RATE, local_model_path, resolve_path};
use furica_core::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub stt: SpeechConfig,
    pub text_model: ModelConfig,
    pub tts: SpeechConfig,
    pub vad_model_path: String,
    pub acp: AgentConfig,
    pub instructions: String,
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpeechConfig {
    pub provider: String,
    pub endpoint: String,
    pub model: String,
    pub api_key: String,
    pub resource_id: String,
    pub voice: String,
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ModelConfig {
    pub protocol: String,
    pub endpoint: String,
    pub model: String,
    pub api_key: String,
    pub isolation_scope: String,
    pub max_tokens_field: String,
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentConfig {
    pub command: String,
    pub args: Vec<String>,
    pub auth_method: Option<String>,
    pub workspace: String,
}

impl Config {
    pub fn missing(&self, voice: bool) -> Vec<String> {
        let mut missing = Vec::new();
        let mut require = |key: &str, valid: bool| {
            if !valid {
                missing.push(key.to_owned());
            }
        };
        for (name, config) in [("stt", &self.stt), ("tts", &self.tts)] {
            if name == "stt" && config.provider.is_empty() {
                require(
                    "本地 ASR 模型文件",
                    local_model_path("sensevoice-small.int8.onnx").is_file(),
                );
                require(
                    "本地 ASR 词表文件",
                    local_model_path("sensevoice-tokens.txt").is_file(),
                );
                continue;
            }
            require(
                &format!("{name}.provider (aliyun / volcengine)"),
                matches!(config.provider.as_str(), "aliyun" | "volcengine"),
            );
            require(
                &format!("{name}.endpoint"),
                !config.endpoint.trim().is_empty(),
            );
            require(&format!("{name}.model"), !config.model.trim().is_empty());
            require(
                &format!("{name}.api_key"),
                !config.api_key.trim().is_empty(),
            );
            if config.provider == "volcengine" {
                require(
                    &format!("{name}.resource_id"),
                    !config.resource_id.trim().is_empty(),
                );
            }
        }
        require("tts.voice", !self.tts.voice.trim().is_empty());
        let model = &self.text_model;
        require(
            "text_model.protocol (openai-compatible / openai-responses / anthropic)",
            matches!(
                model.protocol.as_str(),
                "openai-compatible" | "openai-responses" | "anthropic"
            ),
        );
        require("text_model.endpoint", !model.endpoint.trim().is_empty());
        require("text_model.model", !model.model.trim().is_empty());
        require("text_model.api_key", !model.api_key.trim().is_empty());
        require(
            "text_model.isolation_scope",
            !model.isolation_scope.trim().is_empty(),
        );
        require("acp.command", !self.acp.command.trim().is_empty());
        require(
            "acp.workspace (已存在的绝对目录)",
            Path::new(&self.acp.workspace).is_absolute() && Path::new(&self.acp.workspace).is_dir(),
        );
        if voice {
            require("本地 VAD 模型文件", self.vad_model_file().is_file());
        }
        missing
    }

    pub fn public_status(&self) -> Value {
        json!({"type":"configuration", "sample_rate":SAMPLE_RATE, "model":self.text_model.model,
            "text_missing":self.missing(false), "voice_missing":self.missing(true),
            "speaker_verifier":"测试校验器；不验证真实身份"})
    }

    pub fn vad_model_file(&self) -> PathBuf {
        if self.vad_model_path.is_empty() {
            local_model_path("silero_vad.onnx")
        } else {
            resolve_path(&self.vad_model_path)
        }
    }

    /// Incomplete drafts may be saved. Reject invalid choices and endpoints before persistence.
    pub fn validate(&self) -> Result<()> {
        for (name, speech) in [("stt", &self.stt), ("tts", &self.tts)] {
            if name == "stt" && speech.provider.is_empty() {
                continue;
            }
            if !["", "aliyun", "volcengine"].contains(&speech.provider.as_str()) {
                return Err(Error::Config(format!("{name}.provider 无效")));
            }
            endpoint(
                &speech.endpoint,
                name == "tts" && speech.provider == "aliyun",
                name,
            )?;
            key(&speech.api_key)?;
        }
        if !["", "openai-compatible", "openai-responses", "anthropic"]
            .contains(&self.text_model.protocol.as_str())
        {
            return Err(Error::Config("text_model.protocol 无效".into()));
        }
        if !["", "max_tokens", "max_completion_tokens"]
            .contains(&self.text_model.max_tokens_field.as_str())
        {
            return Err(Error::Config("max_tokens_field 无效".into()));
        }
        endpoint(&self.text_model.endpoint, false, "text_model")?;
        key(&self.text_model.api_key)?;
        if !self.acp.workspace.is_empty()
            && (!Path::new(&self.acp.workspace).is_absolute()
                || !Path::new(&self.acp.workspace).is_dir())
        {
            return Err(Error::Config("ACP 工作目录必须是已存在的绝对目录".into()));
        }
        if !self.vad_model_path.is_empty() && !resolve_path(&self.vad_model_path).is_file() {
            return Err(Error::Config("VAD 模型文件不存在".into()));
        }
        Ok(())
    }
}

fn key(value: &str) -> Result<()> {
    if !value.is_ascii() || value.bytes().any(|b| b.is_ascii_control()) || value.trim() != value {
        return Err(Error::Config("API Key 不应包含空白边界或控制字符".into()));
    }
    Ok(())
}

fn endpoint(value: &str, websocket: bool, name: &str) -> Result<()> {
    if value.is_empty() {
        return Ok(());
    }
    let invalid = || {
        Error::Config(format!(
            "{name}.endpoint 需要完整的 {} 地址，不含凭据、查询参数或片段",
            if websocket { "WSS" } else { "HTTPS" }
        ))
    };
    let url = url::Url::parse(value).map_err(|_| invalid())?;
    let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    let secure = if websocket { "wss" } else { "https" };
    let plain = if websocket { "ws" } else { "http" };
    if url.host_str().is_none()
        || (url.scheme() != secure && !(local && url.scheme() == plain))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_speech_needs_no_service_fields_and_vad_defaults_to_bundled_file() {
        let mut config = Config::default();
        assert_eq!(config.vad_model_file(), local_model_path("silero_vad.onnx"));
        // Local availability is reported separately from editable cloud configuration.
        assert!(
            !config
                .missing(false)
                .iter()
                .any(|field| field.starts_with("stt."))
        );
        config.stt.provider = "aliyun".into();
        let missing = config.missing(false);
        assert!(missing.contains(&"stt.api_key".into()));
        assert!(missing.contains(&"stt.endpoint".into()));
        config.stt.provider.clear();
        assert!(
            !config
                .missing(false)
                .iter()
                .any(|field| field.starts_with("stt."))
        );
    }
}
