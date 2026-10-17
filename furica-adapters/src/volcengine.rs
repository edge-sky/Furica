//! Doubao speech: flash file recognition and V3 unidirectional HTTP TTS.
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use furica_core::{
    Error, Result,
    voice::cascade::{Stt, Tts, TtsStream, Utterance},
};
use futures_util::stream;
use reqwest::{Client, header::HeaderValue};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::cloud::{self, JsonStream, Pcm, cancellable};

#[derive(Clone)]
pub struct SttConfig {
    pub api_key: String,
    /// Full /api/v3/auc/bigmodel/recognize/flash HTTP endpoint.
    pub endpoint: String,
    /// Resource permission, e.g. volc.bigasr.auc_turbo.
    pub resource_id: String,
    pub model: String,
}

pub struct VolcengineStt {
    config: SttConfig,
    client: Client,
    key: HeaderValue,
}

impl VolcengineStt {
    pub fn new(config: SttConfig) -> Result<Self> {
        cloud::endpoint(&config.endpoint, false)?;
        cloud::text(&config.model, "ASR model")?;
        let key = cloud::secret(&config.api_key, false)?;
        cloud::secret(&config.resource_id, false)?;
        let client = cloud::client(&config.endpoint)?;
        Ok(Self {
            config,
            client,
            key,
        })
    }
}

#[async_trait]
impl Stt for VolcengineStt {
    async fn transcribe(&self, utterance: Utterance, cancel: CancellationToken) -> Result<String> {
        cancellable(&cancel, async {
            let audio = cloud::wav_base64(utterance)?;
            let response = self
                .client
                .post(&self.config.endpoint)
                .header("X-Api-Key", self.key.clone())
                .header("X-Api-Resource-Id", &self.config.resource_id)
                .header("X-Api-Request-Id", Uuid::new_v4().to_string())
                .header("X-Api-Sequence", "-1")
                .json(&json!({"user": {"uid": "furica"}, "audio": {"data": audio},
                    "request": {"model_name": self.config.model}}))
                .send()
                .await
                .map_err(cloud::transport)?;
            cloud::status(&response)?;
            match response
                .headers()
                .get("X-Api-Status-Code")
                .and_then(|value| value.to_str().ok())
            {
                Some("20000003") => Ok(String::new()),
                Some("20000000") => {
                    let value = cloud::json_response(response, &cancel).await?;
                    value
                        .pointer("/result/text")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .ok_or_else(|| {
                            Error::Protocol("Volcengine STT response is missing final text".into())
                        })
                }
                _ => Err(Error::Protocol(
                    "Volcengine STT rejected the request or omitted its status".into(),
                )),
            }
        })
        .await
    }
}

#[derive(Clone)]
pub struct TtsConfig {
    pub api_key: String,
    /// HTTP Chunked endpoint (not the /sse variant).
    pub endpoint: String,
    pub resource_id: String,
    /// Explicit model version. Must be compatible with the voice and resource.
    pub model: String,
    pub voice: String,
    /// Native mono PCM16 output rate.
    pub sample_rate: u32,
}

pub struct VolcengineTts {
    config: TtsConfig,
    client: Client,
    key: HeaderValue,
}

impl VolcengineTts {
    pub fn new(config: TtsConfig) -> Result<Self> {
        cloud::endpoint(&config.endpoint, false)?;
        cloud::text(&config.model, "TTS model")?;
        cloud::text(&config.voice, "TTS voice")?;
        cloud::secret(&config.resource_id, false)?;
        if ![8000, 16000, 22050, 24000, 32000, 44100, 48000].contains(&config.sample_rate) {
            return Err(Error::Config(
                "unsupported Volcengine PCM sample rate".into(),
            ));
        }
        let key = cloud::secret(&config.api_key, false)?;
        let client = cloud::client(&config.endpoint)?;
        Ok(Self {
            config,
            client,
            key,
        })
    }
}

#[async_trait]
impl Tts for VolcengineTts {
    async fn synthesize_stream(
        &self,
        text: String,
        cancel: CancellationToken,
    ) -> Result<TtsStream> {
        cloud::validate_tts(&text, 1024)?;
        let response = cancellable(&cancel, async {
            self.client
                .post(&self.config.endpoint)
                .header("X-Api-Key", self.key.clone())
                .header("X-Api-Resource-Id", &self.config.resource_id)
                .header("X-Api-Request-Id", Uuid::new_v4().to_string())
                .json(&json!({"user": {"uid": "furica"}, "req_params": {
                    "text": text, "speaker": self.config.voice, "model": self.config.model,
                    "audio_params": {"format": "pcm", "sample_rate": self.config.sample_rate}
                }}))
                .send()
                .await
                .map_err(cloud::transport)
        })
        .await?;
        cloud::status(&response)?;
        let state = (
            JsonStream::new(response),
            Pcm::new(self.config.sample_rate),
            cancel,
            false,
        );
        let output = stream::try_unfold(state, |(mut source, mut pcm, cancel, done)| async move {
            if done {
                return Ok(None);
            }
            loop {
                let value = source.next(&cancel).await?.ok_or_else(|| {
                    Error::Protocol("Volcengine stream ended without completion".into())
                })?;
                let done = match value.get("code").and_then(Value::as_i64) {
                    Some(0) => false,
                    Some(20_000_000) => true,
                    _ => {
                        return Err(Error::Protocol(
                            "Volcengine synthesis failed or omitted its status".into(),
                        ));
                    }
                };
                let audio = match value.get("data") {
                    None | Some(Value::Null) => None,
                    Some(Value::String(encoded)) if encoded.is_empty() => None,
                    Some(Value::String(encoded)) => {
                        let bytes = STANDARD.decode(encoded).map_err(|_| {
                            Error::Protocol("invalid Volcengine audio Base64".into())
                        })?;
                        pcm.push(&bytes)?
                    }
                    _ => return Err(Error::Protocol("invalid Volcengine audio field".into())),
                };
                if done {
                    pcm.finish()?;
                }
                if let Some(audio) = audio {
                    return Ok(Some((audio, (source, pcm, cancel, done))));
                }
                if done {
                    return Ok(None);
                }
            }
        });
        Ok(Box::pin(output))
    }
}
