//! Alibaba Cloud Model Studio: Fun-ASR HTTP and CosyVoice WebSocket.
//! Select model IDs and endpoints when constructing the adapter, not in Core.
use async_trait::async_trait;
use furica_core::{
    Error, Result,
    stt::{Stt, Utterance},
    tts::{Tts, TtsStream},
};
use futures_util::{SinkExt, StreamExt, stream};
use reqwest::{Client, header::HeaderValue};
use serde_json::{Value, json};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, client::IntoClientRequest, protocol::WebSocketConfig},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::cloud::{self, Pcm, cancellable};

/// Credentials intentionally do not implement Debug or Serialize.
#[derive(Clone)]
pub struct SttConfig {
    pub api_key: String,
    /// HTTP endpoint, including the generation path. Beijing region only.
    pub endpoint: String,
    /// Fun-ASR-Realtime model supporting the non-realtime HTTP protocol.
    pub model: String,
}

pub struct AliyunStt {
    config: SttConfig,
    client: Client,
    authorization: HeaderValue,
}

impl AliyunStt {
    pub fn new(config: SttConfig) -> Result<Self> {
        cloud::endpoint(&config.endpoint, false)?;
        cloud::text(&config.model, "ASR model")?;
        let authorization = cloud::secret(&config.api_key, true)?;
        let client = cloud::client(&config.endpoint)?;
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
            let audio = cloud::wav_base64(utterance)?;
            let response = self.client.post(&self.config.endpoint)
                .header("Authorization", self.authorization.clone()).header("X-DashScope-SSE", "disable")
                .json(&json!({
                    "model": self.config.model,
                    "input": {"messages": [{"role": "user", "content": [{"audio": format!("data:audio/wav;base64,{audio}")}]}]},
                    "parameters": {"format": "wav", "vad_enabled": false}, "resources": []
                })).send().await.map_err(cloud::transport)?;
            let value = cloud::json_response(response, &cancel).await?;
            if value.get("code").is_some() { return Err(Error::Protocol("Aliyun STT rejected the request".into())); }
            value.pointer("/output/text").and_then(Value::as_str).map(str::to_owned)
                .ok_or_else(|| Error::Protocol("Aliyun STT response is missing final text".into()))
        }).await
    }
}

#[derive(Clone)]
pub struct TtsConfig {
    pub api_key: String,
    /// WebSocket inference endpoint, including the path.
    pub endpoint: String,
    pub model: String,
    pub voice: String,
    /// Native mono PCM16 rate: 8000, 16000, 22050, 24000, 44100 or 48000 Hz.
    pub sample_rate: u32,
}

pub struct AliyunTts {
    config: TtsConfig,
    authorization: HeaderValue,
}

impl AliyunTts {
    pub fn new(config: TtsConfig) -> Result<Self> {
        cloud::endpoint(&config.endpoint, true)?;
        cloud::text(&config.model, "TTS model")?;
        cloud::text(&config.voice, "TTS voice")?;
        if ![8000, 16000, 22050, 24000, 44100, 48000].contains(&config.sample_rate) {
            return Err(Error::Config(
                "unsupported CosyVoice PCM sample rate".into(),
            ));
        }
        let authorization = cloud::secret(&config.api_key, true)?;
        Ok(Self {
            config,
            authorization,
        })
    }
}

fn event(value: &Value, task_id: &str) -> Result<String> {
    if value.pointer("/header/task_id").and_then(Value::as_str) != Some(task_id) {
        return Err(Error::Protocol("CosyVoice task ID mismatch".into()));
    }
    match value.pointer("/header/event").and_then(Value::as_str) {
        Some("task-failed") => Err(Error::Protocol("CosyVoice synthesis task failed".into())),
        Some(name) => Ok(name.to_owned()),
        None => Err(Error::Protocol(
            "CosyVoice response is missing an event".into(),
        )),
    }
}

#[async_trait]
impl Tts for AliyunTts {
    async fn synthesize_stream(
        &self,
        text: String,
        cancel: CancellationToken,
    ) -> Result<TtsStream> {
        // Byte bound also limits encoded request size (provider character limit is 20k).
        cloud::validate_tts(&text, 20_000)?;
        let mut request = self
            .config
            .endpoint
            .as_str()
            .into_client_request()
            .map_err(|_| Error::Config("invalid CosyVoice WebSocket request".into()))?;
        request
            .headers_mut()
            .insert("Authorization", self.authorization.clone());
        let mut limits = WebSocketConfig::default();
        limits.max_message_size = Some(cloud::MESSAGE_LIMIT);
        limits.max_frame_size = Some(cloud::MESSAGE_LIMIT);
        let (mut socket, _) = cancellable(&cancel, async {
            connect_async_with_config(request, Some(limits), false)
                .await
                .map_err(|_| Error::Protocol("CosyVoice WebSocket handshake failed".into()))
        })
        .await?;
        let task_id = Uuid::new_v4().to_string();
        let header =
            |action: &str| json!({"action": action, "task_id": task_id, "streaming": "duplex"});
        cancellable(&cancel, async {
            socket.send(Message::Text(json!({
                "header": header("run-task"),
                "payload": {"task_group": "audio", "task": "tts", "function": "SpeechSynthesizer",
                    "model": self.config.model, "parameters": {"text_type": "PlainText", "voice": self.config.voice,
                        "format": "pcm", "sample_rate": self.config.sample_rate}, "input": {}}
            }).to_string().into())).await.map_err(|_| Error::Protocol("CosyVoice start failed".into()))?;
            loop {
                match socket.next().await {
                    Some(Ok(Message::Text(value))) => {
                        let value: Value = serde_json::from_str(&value).map_err(|_| Error::Protocol("invalid CosyVoice JSON".into()))?;
                        if event(&value, &task_id)? != "task-started" {
                            return Err(Error::Protocol("CosyVoice task did not start".into()));
                        }
                        break;
                    }
                    Some(Ok(Message::Ping(bytes))) => { socket.send(Message::Pong(bytes)).await.map_err(|_| Error::Closed)?; }
                    Some(Ok(Message::Pong(_))) => (),
                    _ => return Err(Error::Protocol("CosyVoice disconnected before task start".into())),
                }
            }
            for value in [
                json!({"header": header("continue-task"), "payload": {"input": {"text": text}}}),
                json!({"header": header("finish-task"), "payload": {"input": {}}}),
            ] {
                socket.send(Message::Text(value.to_string().into())).await
                    .map_err(|_| Error::Protocol("CosyVoice input failed".into()))?;
            }
            Ok(())
        }).await?;

        // The stream owns the socket. Dropping a pending read or the stream closes
        // its transport; no detached producer or unbounded audio queue exists.
        let state = (socket, Pcm::new(self.config.sample_rate), task_id, cancel);
        let output =
            stream::try_unfold(state, |(mut socket, mut pcm, task_id, cancel)| async move {
                loop {
                    let message = cancellable(&cancel, async {
                        socket
                            .next()
                            .await
                            .transpose()
                            .map_err(|_| Error::Protocol("CosyVoice receive failed".into()))
                    })
                    .await?
                    .ok_or_else(|| {
                        Error::Protocol("CosyVoice stream ended without task-finished".into())
                    })?;
                    match message {
                        Message::Binary(bytes) => {
                            if let Some(audio) = pcm.push(&bytes)? {
                                return Ok(Some((audio, (socket, pcm, task_id, cancel))));
                            }
                        }
                        Message::Text(value) => {
                            let value: Value = serde_json::from_str(&value)
                                .map_err(|_| Error::Protocol("invalid CosyVoice JSON".into()))?;
                            match event(&value, &task_id)?.as_str() {
                                "result-generated" => (),
                                "task-finished" => {
                                    pcm.finish()?;
                                    return Ok(None);
                                }
                                _ => {
                                    return Err(Error::Protocol(
                                        "unexpected CosyVoice event".into(),
                                    ));
                                }
                            }
                        }
                        Message::Ping(bytes) => {
                            cancellable(&cancel, async {
                                socket
                                    .send(Message::Pong(bytes))
                                    .await
                                    .map_err(|_| Error::Closed)
                            })
                            .await?;
                        }
                        Message::Pong(_) => (),
                        _ => {
                            return Err(Error::Protocol(
                                "CosyVoice closed before task completion".into(),
                            ));
                        }
                    }
                }
            });
        Ok(Box::pin(output))
    }
}
