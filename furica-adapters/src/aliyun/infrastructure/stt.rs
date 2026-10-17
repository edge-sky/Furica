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

pub struct AliyunStt {
    config: SttConfig,
    client: Client,
    authorization: HeaderValue,
}

impl AliyunStt {
    pub fn new(config: SttConfig) -> Result<Self> {
        http::endpoint(&config.endpoint, false)?;
        http::text(&config.model, "ASR model")?;
        let authorization = http::secret(&config.api_key, true)?;
        let client = http::client(&config.endpoint)?;
        Ok(Self {
            config,
            client,
            authorization,
        })
    }
}

#[async_trait]
impl Stt for AliyunStt {
    async fn transcribe(&self, utterance: Utterance, cancel: CancellationToken) -> Result<String> {
        cancellable(&cancel, async {
            let audio = speech::wav_base64(utterance)?;
            let response = self.client.post(&self.config.endpoint)
                .header("Authorization", self.authorization.clone()).header("X-DashScope-SSE", "disable")
                .json(&json!({
                    "model": self.config.model,
                    "input": {"messages": [{"role": "user", "content": [{"audio": format!("data:audio/wav;base64,{audio}")}]}]},
                    "parameters": {"format": "wav", "vad_enabled": false}, "resources": []
                })).send().await.map_err(http::transport)?;
            let value = speech::json_response(response, &cancel).await?;
            if value.get("code").is_some() { return Err(Error::Protocol("Aliyun STT rejected the request".into())); }
            value.pointer("/output/text").and_then(Value::as_str).map(str::to_owned)
                .ok_or_else(|| Error::Protocol("Aliyun STT response is missing final text".into()))
        }).await
    }
}
