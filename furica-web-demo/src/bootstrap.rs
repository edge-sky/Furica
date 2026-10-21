//! Composition root: configured adapters are injected into Core.
use crate::settings::Config;
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
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

pub const SAMPLE_RATE: u32 = 16_000;
pub const HOST_ROOT: &str = env!("CARGO_MANIFEST_DIR");

impl Config {
    async fn speech_recognizer(&self) -> Result<Arc<dyn Stt>> {
        match self.stt.provider.as_str() {
            "" => tokio::task::spawn_blocking(|| {
                furica_local::asr::SenseVoiceStt::from_files(
                    local_model_path("sensevoice-small.int8.onnx"),
                    local_model_path("sensevoice-tokens.txt"),
                )
                .map(|model| Arc::new(model) as Arc<dyn Stt>)
            })
            .await
            .map_err(|_| Error::State("本地 ASR 加载任务失败".into()))?,
            "aliyun" => Ok(Arc::new(aliyun::AliyunStt::new(aliyun::SttConfig {
                endpoint: self.stt.endpoint.clone(),
                model: self.stt.model.clone(),
                api_key: self.stt.api_key.clone(),
            })?)),
            "volcengine" => Ok(Arc::new(volcengine::VolcengineStt::new(
                volcengine::SttConfig {
                    endpoint: self.stt.endpoint.clone(),
                    model: self.stt.model.clone(),
                    api_key: self.stt.api_key.clone(),
                    resource_id: self.stt.resource_id.clone(),
                },
            )?)),
            _ => Err(Error::Config("stt.provider 无效".into())),
        }
    }

    pub async fn compose(
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
        let stt = self.speech_recognizer().await?;
        let tts: Arc<dyn Tts> = match self.tts.provider.as_str() {
            "aliyun" => Arc::new(aliyun::AliyunTts::new(aliyun::TtsConfig {
                endpoint: self.tts.endpoint.clone(),
                model: self.tts.model.clone(),
                api_key: self.tts.api_key.clone(),
                voice: self.tts.voice.clone(),
                sample_rate: SAMPLE_RATE,
            })?),
            _ => Arc::new(volcengine::VolcengineTts::new(volcengine::TtsConfig {
                endpoint: self.tts.endpoint.clone(),
                model: self.tts.model.clone(),
                api_key: self.tts.api_key.clone(),
                voice: self.tts.voice.clone(),
                sample_rate: SAMPLE_RATE,
                resource_id: self.tts.resource_id.clone(),
            })?),
        };
        let m = &self.text_model;
        let key = m.api_key.clone();
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

pub fn local_model_path(name: &str) -> PathBuf {
    Path::new(HOST_ROOT)
        .join("../furica-local/model")
        .join(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use furica_core::{audio::AudioFrame, stt::Utterance};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    #[ignore = "requires bundled SenseVoice model and recorded fixture"]
    async fn no_stt_settings_uses_real_local_recognizer() {
        let config = Config::default();
        let model = config.speech_recognizer().await.unwrap();
        assert_eq!(
            model.source(),
            Some("sensevoice-small-int8/sherpa-onnx-2024-07-17")
        );
        let mut wav = hound::WavReader::open(
            Path::new(HOST_ROOT).join("../furica-local/tests/fixtures/sensevoice-zh.wav"),
        )
        .unwrap();
        assert_eq!(wav.spec().sample_rate, SAMPLE_RATE);
        let pcm = wav
            .samples::<i16>()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let frames = pcm
            .chunks(512)
            .enumerate()
            .map(|(sequence, chunk)| AudioFrame {
                sequence: sequence as u64,
                sample_rate: SAMPLE_RATE,
                samples: chunk.to_vec(),
            })
            .collect();
        let text = model
            .transcribe(
                Utterance {
                    sample_rate: SAMPLE_RATE,
                    frames,
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(text, "开饭时间早上9点至下午5点。");
    }
}
