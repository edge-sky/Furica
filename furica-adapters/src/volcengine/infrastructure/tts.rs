//! TTS wire protocol and implementation of the Core capability port.
use super::super::config::TtsConfig;
use crate::infrastructure::{
    http::{self, cancellable},
    speech::{self, JsonStream, Pcm},
};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use furica_core::{
    Error, Result,
    tts::{Tts, TtsStream},
};
use futures_util::stream;
use reqwest::{Client, header::HeaderValue};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub struct VolcengineTts {
    config: TtsConfig,
    client: Client,
    key: HeaderValue,
}

impl VolcengineTts {
    pub fn new(config: TtsConfig) -> Result<Self> {
        http::endpoint(&config.endpoint, false)?;
        http::text(&config.model, "TTS model")?;
        http::text(&config.voice, "TTS voice")?;
        http::secret(&config.resource_id, false)?;
        if ![8000, 16000, 22050, 24000, 32000, 44100, 48000].contains(&config.sample_rate) {
            return Err(Error::Config(
                "unsupported Volcengine PCM sample rate".into(),
            ));
        }
        let key = http::secret(&config.api_key, false)?;
        let client = http::client(&config.endpoint)?;
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
        speech::validate_tts(&text, 1024)?;
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
                .map_err(http::transport)
        })
        .await?;
        http::status(&response)?;
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
