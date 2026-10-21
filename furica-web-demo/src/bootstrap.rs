//! Host configuration and composition root. No configuration is sent to the browser.
use furica_adapters::{
    acp::{AcpConfig, AcpExecutor},
    aliyun,
    text_model::{anthropic, openai_compatible, openai_responses},
    volcengine,
};
use furica_core::{
    Error, Result,
    orchestration::{CascadeConfig, CascadedBackend},
    sessions::{SessionLimits, SessionManager},
    stt::Stt,
    text_model::TextModel,
    tts::Tts,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

pub const SAMPLE_RATE: u32 = 16_000;
pub const HOST_ROOT: &str = env!("CARGO_MANIFEST_DIR");

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub stt: SpeechConfig,
    pub text_model: ModelConfig,
    pub tts: SpeechConfig,
    pub vad_model_path: String,
    pub acp: AgentConfig,
    pub instructions: String,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpeechConfig {
    pub provider: String,
    pub endpoint: String,
    pub model: String,
    pub api_key_env: String,
    pub resource_id: String,
    pub voice: String,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ModelConfig {
    pub protocol: String,
    pub endpoint: String,
    pub model: String,
    pub api_key_env: String,
    pub isolation_scope: String,
    pub max_tokens_field: String,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentConfig {
    pub command: String,
    pub args: Vec<String>,
    pub auth_method: Option<String>,
    pub workspace: String,
}

impl Config {
    pub fn load(
        path: &Path,
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|_| "invalid host JSON configuration; check field names and types".into()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn missing(&self, voice: bool) -> Vec<String> {
        let mut missing = Vec::new();
        let mut require = |key: &str, valid: bool| {
            if !valid {
                missing.push(key.to_owned());
            }
        };
        for (name, config) in [("stt", &self.stt), ("tts", &self.tts)] {
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
                &format!("{name}.api_key_env / 环境变量值"),
                credential(&config.api_key_env).is_ok(),
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
        require(
            "text_model.api_key_env / 环境变量值",
            credential(&model.api_key_env).is_ok(),
        );
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
            require(
                "vad_model_path (本地 ONNX 文件)",
                !self.vad_model_path.is_empty() && resolve_path(&self.vad_model_path).is_file(),
            );
        }
        missing
    }

    pub fn public_status(&self) -> Value {
        json!({"type":"configuration", "sample_rate":SAMPLE_RATE, "model":self.text_model.model,
            "text_missing":self.missing(false), "voice_missing":self.missing(true),
            "speaker_verifier":"测试校验器；不验证真实身份"})
    }

    pub fn compose(
        &self,
    ) -> Result<(
        CascadedBackend,
        SessionManager,
        Arc<crate::sessions::SnapshotStore>,
    )> {
        let missing = self.missing(false);
        if !missing.is_empty() {
            return Err(Error::Config(format!(
                "configuration incomplete: {}",
                missing.join(", ")
            )));
        }
        let stt: Arc<dyn Stt> = match self.stt.provider.as_str() {
            "aliyun" => Arc::new(aliyun::AliyunStt::new(aliyun::SttConfig {
                endpoint: self.stt.endpoint.clone(),
                model: self.stt.model.clone(),
                api_key: credential(&self.stt.api_key_env)?,
            })?),
            _ => Arc::new(volcengine::VolcengineStt::new(volcengine::SttConfig {
                endpoint: self.stt.endpoint.clone(),
                model: self.stt.model.clone(),
                api_key: credential(&self.stt.api_key_env)?,
                resource_id: self.stt.resource_id.clone(),
            })?),
        };
        let tts: Arc<dyn Tts> = match self.tts.provider.as_str() {
            "aliyun" => Arc::new(aliyun::AliyunTts::new(aliyun::TtsConfig {
                endpoint: self.tts.endpoint.clone(),
                model: self.tts.model.clone(),
                api_key: credential(&self.tts.api_key_env)?,
                voice: self.tts.voice.clone(),
                sample_rate: SAMPLE_RATE,
            })?),
            _ => Arc::new(volcengine::VolcengineTts::new(volcengine::TtsConfig {
                endpoint: self.tts.endpoint.clone(),
                model: self.tts.model.clone(),
                api_key: credential(&self.tts.api_key_env)?,
                voice: self.tts.voice.clone(),
                sample_rate: SAMPLE_RATE,
                resource_id: self.tts.resource_id.clone(),
            })?),
        };
        let m = &self.text_model;
        let key = credential(&m.api_key_env)?;
        let model: Arc<dyn TextModel> = match m.protocol.as_str() {
            "openai-compatible" => {
                let mut c = openai_compatible::Config::new(
                    m.endpoint.clone(),
                    key,
                    m.model.clone(),
                    m.isolation_scope.clone(),
                );
                if !m.max_tokens_field.is_empty() {
                    c.max_tokens_field = m.max_tokens_field.clone();
                }
                Arc::new(openai_compatible::OpenAiCompatible::new(c)?)
            }
            "openai-responses" => Arc::new(openai_responses::OpenAiResponses::new(
                openai_responses::Config::new(
                    m.endpoint.clone(),
                    key,
                    m.model.clone(),
                    m.isolation_scope.clone(),
                ),
            )?),
            _ => Arc::new(anthropic::Anthropic::new(anthropic::Config::new(
                m.endpoint.clone(),
                key,
                m.model.clone(),
                m.isolation_scope.clone(),
            ))?),
        };
        let cascade = CascadedBackend::new(
            CascadeConfig {
                sample_rate: SAMPLE_RATE,
                instructions: Some(if self.instructions.trim().is_empty() {
                    "你是 Furica，一位安静、可靠的工作伙伴。简洁回答用户。需要发声时调用 speak 工具，普通回复文本用于显示。".into()
                } else {
                    self.instructions.clone()
                }),
                ..Default::default()
            },
            stt,
            model,
            tts,
        )?;
        let mut agent = AcpConfig::new(&self.acp.command);
        agent.args = self.acp.args.clone();
        agent.auth_method = self.acp.auth_method.clone();
        let store = Arc::new(crate::sessions::SnapshotStore::configured());
        let manager = SessionManager::new(
            store.load()?,
            Arc::new(AcpExecutor::new(agent)?),
            self.acp.workspace.clone().into(),
            SessionLimits::default(),
        )?;
        Ok((cascade, manager, store))
    }
}

pub fn resolve_path(value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        Path::new(HOST_ROOT).join(path)
    }
}

fn credential(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::Config("credential environment variable is missing".into()))
}
