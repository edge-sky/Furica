use crate::{
    Components, Error, Host, Result,
    configuration::application::{HostConfig, ModelConfig, SttConfig, TtsConfig},
    infrastructure::{audio::VadBackend, model_policy::SpeechPolicy, sqlite::ConfigStore},
};
use furica_adapters::{
    acp, aliyun,
    text_model::{anthropic, openai_compatible, openai_responses},
    volcengine,
};
use furica_core::{
    orchestration::{CascadeConfig, CascadedBackend, VoiceBackend},
    stt::Stt,
    text_model::TextModel,
    tts::Tts,
};
use std::io::Read;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

pub fn secret(name: &str) -> Result<String> {
    let value = std::env::var(name).map_err(|_| {
        Error::new(
            503,
            "credential_missing",
            "required credential environment variable is missing",
        )
    })?;
    if value.trim().is_empty() || value.len() > 8192 || value.contains(['\r', '\n']) {
        return Err(Error::new(
            503,
            "credential_invalid",
            "credential environment variable is invalid",
        ));
    }
    Ok(value)
}

pub fn prepare_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path).map_err(|_| Error::storage())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| Error::storage())?;
    }
    Ok(())
}

pub async fn start(
    data_dir: PathBuf,
    initial: Option<PathBuf>,
) -> Result<(Arc<Host>, ConfigStore, String)> {
    let data_dir = if data_dir.is_absolute() {
        data_dir
    } else {
        std::env::current_dir()
            .map_err(|_| Error::config())?
            .join(data_dir)
    };
    prepare_directory(&data_dir)?;
    // Acquire before accessing the session database. A second process must not
    // overwrite the live host's data while discovering that its port is busy.
    let directory_lock = tokio::task::spawn_blocking({
        let path = data_dir.join(".host.lock.sqlite3");
        move || {
            let connection = rusqlite::Connection::open(path).map_err(|_| Error::storage())?;
            connection.execute_batch("BEGIN EXCLUSIVE").map_err(|_| {
                Error::new(
                    503,
                    "host_data_in_use",
                    "another host owns this data directory",
                )
            })?;
            Ok::<_, Error>(connection)
        }
    })
    .await
    .map_err(|_| Error::storage())??;
    let (store, config) = tokio::task::spawn_blocking({
        let dir = data_dir.clone();
        move || {
            let config = initial
                .map(|path| {
                    let mut bytes = Vec::new();
                    std::fs::File::open(path)
                        .map_err(|_| Error::config())?
                        .take(1024 * 1024 + 1)
                        .read_to_end(&mut bytes)
                        .map_err(|_| Error::config())?;
                    if bytes.len() > 1024 * 1024 {
                        return Err(Error::config());
                    }
                    serde_json::from_slice::<HostConfig>(&bytes).map_err(|_| Error::config())
                })
                .transpose()?;
            ConfigStore::open(dir.join("configuration.sqlite3"), config.as_ref())
        }
    })
    .await
    .map_err(|_| Error::storage())??;
    std::fs::create_dir_all(&config.workspace).map_err(|_| Error::config())?;
    let token = secret(&config.http.token_env)?;
    if token.len() < 32 {
        return Err(Error::new(
            400,
            "weak_access_token",
            "host access token must contain at least 32 bytes",
        ));
    }
    let instructions = if config.model.is_some() {
        tokio::task::spawn_blocking({
            let directory = data_dir.clone();
            let workspace = config.workspace.clone();
            let max_bytes = config.limits.max_context_bytes;
            move || crate::infrastructure::prompts::load_system(&directory, &workspace, max_bytes)
        })
        .await
        .map_err(|_| Error::config())??
    } else {
        String::new()
    };
    let components = assemble(&config, instructions).await?;
    let path = data_dir.join("configuration.sqlite3");
    let host = Host::new(config, components, path).await?;
    host.hold_directory_lock(directory_lock).await;
    if let Err(error) = host.flush().await {
        let _ = host.shutdown().await;
        return Err(error);
    }
    Ok((host, store, token))
}

pub async fn assemble(config: &HostConfig, instructions: String) -> Result<Components> {
    config.validate()?;
    let model = config
        .model
        .as_ref()
        .map(|config| -> Result<Arc<dyn TextModel>> {
            Ok(match config {
                ModelConfig::OpenaiCompatible {
                    endpoint,
                    model,
                    isolation_scope,
                    api_key_env,
                    connect_timeout_ms,
                    max_tokens_field,
                    supports_reasoning_effort,
                } => {
                    let mut c = openai_compatible::Config::new(
                        endpoint.clone(),
                        secret(api_key_env)?,
                        model.clone(),
                        isolation_scope.clone(),
                    );
                    c.connect_timeout = Duration::from_millis(*connect_timeout_ms);
                    c.max_tokens_field = max_tokens_field.clone();
                    c.supports_reasoning_effort = *supports_reasoning_effort;
                    Arc::new(openai_compatible::OpenAiCompatible::new(c)?)
                }
                ModelConfig::OpenaiResponses {
                    endpoint,
                    model,
                    isolation_scope,
                    api_key_env,
                    connect_timeout_ms,
                    supports_reasoning,
                } => {
                    let mut c = openai_responses::Config::new(
                        endpoint.clone(),
                        secret(api_key_env)?,
                        model.clone(),
                        isolation_scope.clone(),
                    );
                    c.connect_timeout = Duration::from_millis(*connect_timeout_ms);
                    c.supports_reasoning = *supports_reasoning;
                    Arc::new(openai_responses::OpenAiResponses::new(c)?)
                }
                ModelConfig::Anthropic {
                    endpoint,
                    model,
                    isolation_scope,
                    api_key_env,
                    connect_timeout_ms,
                    version,
                    supports_adaptive_thinking,
                    supports_thinking_budget,
                } => {
                    let mut c = anthropic::Config::new(
                        endpoint.clone(),
                        secret(api_key_env)?,
                        model.clone(),
                        isolation_scope.clone(),
                    );
                    c.connect_timeout = Duration::from_millis(*connect_timeout_ms);
                    c.version = version.clone();
                    c.supports_adaptive_thinking = *supports_adaptive_thinking;
                    c.supports_thinking_budget = *supports_thinking_budget;
                    Arc::new(anthropic::Anthropic::new(c)?)
                }
            })
        })
        .transpose()?;
    let mut agents = Vec::new();
    if let Some(c) = &config.acp {
        let mut execution = acp::AcpConfig::new(c.command.clone());
        execution.agent_id = c.agent_id.clone();
        execution.args = c.args.clone();
        execution.auth_method = c.auth_method.clone();
        execution.handshake_timeout = Duration::from_millis(c.handshake_timeout_ms);
        execution.run_timeout = Duration::from_millis(c.run_timeout_ms);
        execution.cancel_timeout = Duration::from_millis(c.cancel_timeout_ms);
        execution.idle_timeout = Duration::from_millis(c.idle_timeout_ms);
        let executor = Arc::new(acp::AcpExecutor::new(execution)?);
        agents.push(furica_core::sessions::AgentBinding {
            id: c.agent_id.clone(),
            description: "Configured coding agent".into(),
            executor,
            workspaces: vec![furica_core::sessions::WorkspaceOption {
                id: "default".into(),
                description: "Configured agent workspace".into(),
                path: config.workspace.clone(),
            }],
        });
    }
    let speech_enabled = Arc::new(AtomicBool::new(false));
    let stt: Option<Arc<dyn Stt>> =
        if let Some(stt) = config.speech.as_ref().and_then(|s| s.stt.as_ref()) {
            Some(match stt {
                SttConfig::Sensevoice {
                    model_path,
                    tokens_path,
                } => {
                    let model = model_path.clone();
                    let tokens = tokens_path.clone();
                    Arc::new(
                        tokio::task::spawn_blocking(move || {
                            furica_local::asr::SenseVoiceStt::from_files(model, tokens)
                        })
                        .await
                        .map_err(|_| Error::config())??,
                    )
                }
                SttConfig::Aliyun {
                    endpoint,
                    model,
                    api_key_env,
                } => Arc::new(aliyun::AliyunStt::new(aliyun::SttConfig {
                    endpoint: endpoint.clone(),
                    model: model.clone(),
                    api_key: secret(api_key_env)?,
                })?),
                SttConfig::Volcengine {
                    endpoint,
                    model,
                    api_key_env,
                    resource_id,
                } => Arc::new(volcengine::VolcengineStt::new(volcengine::SttConfig {
                    endpoint: endpoint.clone(),
                    model: model.clone(),
                    api_key: secret(api_key_env)?,
                    resource_id: resource_id.clone(),
                })?),
            })
        } else {
            None
        };
    let tts: Option<Arc<dyn Tts>> =
        if let Some(tts) = config.speech.as_ref().and_then(|s| s.tts.as_ref()) {
            Some(match tts {
                TtsConfig::Aliyun {
                    endpoint,
                    model,
                    voice,
                    api_key_env,
                } => Arc::new(aliyun::AliyunTts::new(aliyun::TtsConfig {
                    endpoint: endpoint.clone(),
                    model: model.clone(),
                    voice: voice.clone(),
                    api_key: secret(api_key_env)?,
                    sample_rate: 16000,
                })?),
                TtsConfig::Volcengine {
                    endpoint,
                    model,
                    voice,
                    api_key_env,
                    resource_id,
                } => Arc::new(volcengine::VolcengineTts::new(volcengine::TtsConfig {
                    endpoint: endpoint.clone(),
                    model: model.clone(),
                    voice: voice.clone(),
                    api_key: secret(api_key_env)?,
                    sample_rate: 16000,
                    resource_id: resource_id.clone(),
                })?),
            })
        } else {
            None
        };
    let voice_available = model.is_some()
        && config
            .speech
            .as_ref()
            .is_some_and(|s| s.stt.is_some() && s.vad_model.is_some());
    let l = &config.limits;
    let backend: Option<Arc<dyn VoiceBackend>> = if let Some(model) = model {
        let model = Arc::new(SpeechPolicy::new(model, speech_enabled.clone()));
        let cascade = CascadeConfig {
            sample_rate: 16000,
            channel_capacity: l.channel_capacity,
            max_utterance: Duration::from_millis(l.max_utterance_ms),
            max_generated_audio: Duration::from_millis(l.max_generated_audio_ms),
            inference_timeout: Duration::from_millis(l.inference_timeout_ms),
            max_history_messages: l.max_history_messages,
            max_context_bytes: l.max_context_bytes,
            instructions: Some(instructions),
            max_output_tokens: l.max_output_tokens,
            reasoning: None,
        };
        crate::configuration::application::runtime::validate_cascade(
            &cascade,
            stt.is_some(),
            tts.is_some(),
        )?;
        let backend = Arc::new(CascadedBackend::with_speech(cascade, stt, model, tts));
        if voice_available {
            let path = config
                .speech
                .as_ref()
                .and_then(|s| s.vad_model.clone())
                .expect("voice has a VAD model");
            let model_path = path.clone();
            tokio::task::spawn_blocking(move || {
                furica_local::vad::SileroVad::from_file(path, Default::default())
            })
            .await
            .map_err(|_| Error::config())?
            .map_err(|_| {
                Error::new(400, "invalid_vad_model", "Silero model could not be loaded")
            })?;
            Some(Arc::new(VadBackend {
                inner: backend,
                model_path,
                capacity: l.channel_capacity,
                max_samples: (l.max_utterance_ms * 16) as usize,
            }))
        } else {
            Some(backend)
        }
    } else {
        None
    };
    Ok(Components {
        backend,
        agents,
        speech_enabled,
        voice_available,
    })
}
