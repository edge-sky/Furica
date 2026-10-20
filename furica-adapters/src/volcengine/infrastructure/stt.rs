//! STT wire protocol and implementation of the Core capability port.
use super::super::config::SttConfig;
use crate::infrastructure::{
    http::{self, cancellable},
    speech,
};
use async_trait::async_trait;
use furica_core::{
    Error, Result,
    stt::{Stt, Utterance},
};
use reqwest::{Client, header::HeaderValue};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub struct VolcengineStt {
    config: SttConfig,
    client: Client,
    key: HeaderValue,
}

impl VolcengineStt {
    pub fn new(config: SttConfig) -> Result<Self> {
        http::endpoint(&config.endpoint, false)?;
        http::text(&config.model, "ASR model")?;
        let key = http::secret(&config.api_key, false)?;
        http::secret(&config.resource_id, false)?;
        let client = http::client(&config.endpoint)?;
        Ok(Self {
            config,
            client,
            key,
        })
    }
}

#[async_trait]
impl Stt for VolcengineStt {
    fn source(&self) -> Option<&str> {
        Some(&self.config.model)
    }
    async fn transcribe(&self, utterance: Utterance, cancel: CancellationToken) -> Result<String> {
        cancellable(&cancel, async {
            let audio = speech::wav_base64(utterance)?;
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
                .map_err(http::transport)?;
            http::status(&response)?;
            match response
                .headers()
                .get("X-Api-Status-Code")
                .and_then(|value| value.to_str().ok())
            {
                Some("20000003") => Ok(String::new()),
                Some("20000000") => {
                    let value = speech::json_response(response, &cancel).await?;
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
