//! STT -> text model -> TTS contracts and backend assembly.
//! Commands, cancellation and turn scheduling are in worker; a single inference is in inference.

mod inference;
mod worker;

use super::{VoiceBackend, VoiceSession};
use crate::{Error, Result, audio::AudioFrame};
use async_trait::async_trait;
use serde_json::Value;
use std::{pin::Pin, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct Utterance {
    pub sample_rate: u32,
    pub frames: Vec<AudioFrame>,
}

#[async_trait]
pub trait Stt: Send + Sync {
    /// Return one final transcript. Empty/whitespace means no recognized speech.
    /// Implementations must release owned resources when the future is dropped.
    async fn transcribe(&self, utterance: Utterance, cancel: CancellationToken) -> Result<String>;
}

#[derive(Clone, Debug)]
pub struct ToolCall {
    pub call_id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Clone, Debug)]
pub enum ModelMessage {
    User(String),
    Assistant(String),
    AssistantTools(Vec<ToolCall>),
    ToolResult {
        call_id: String,
        result: Value,
    },
    /// Metadata and agent output are data, never system instructions.
    TaskContext(Value),
    TaskUpdate(Value),
}

#[derive(Clone, Debug)]
pub struct ModelRequest {
    pub messages: Vec<ModelMessage>,
    pub tools: Vec<Value>,
}

/// A turn either replies or requests tools. Tool acceptance is fed back before
/// the next model turn; requesting a task does not mean the task has completed.
#[derive(Clone, Debug)]
pub enum ModelResponse {
    Text(String),
    Tools(Vec<ToolCall>),
}

#[async_trait]
pub trait TextModel: Send + Sync {
    /// Cooperate with cancellation and release resources on future drop.
    async fn generate(
        &self,
        request: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelResponse>;
}

/// Mono PCM16 audio; a stream chunk or the pipeline's collected result.
#[derive(Clone, Debug)]
pub struct SynthesizedAudio {
    pub sample_rate: u32,
    pub samples: Vec<i16>,
}

/// Ordered audio chunks. `None` means successful completion; consumers must
/// stop and drop the stream on the first error. A successful synthesis contains
/// at least one nonempty chunk, all at the configured pipeline sample rate.
///
/// Native streaming adapters deliver chunks as they become available. Buffered
/// providers may yield their complete result as a single chunk. Adapters must
/// bound any internal buffering and stop owned work when this stream is dropped
/// or the synthesis cancellation token is cancelled.
pub type TtsStream =
    Pin<Box<dyn futures_core::Stream<Item = Result<SynthesizedAudio>> + Send + 'static>>;

/// Complete text input with streaming mono PCM16 output.
/// Adapters own format conversion and resampling to the pipeline sample rate.
/// Implementations must release owned work on cancellation or future/stream drop.
#[async_trait]
pub trait Tts: Send + Sync {
    /// Success opens a stream, not a completed
    /// synthesis. Opening failures use the outer Result; later failures use
    /// stream items. The cancellation token covers opening and consumption.
    /// Consumers must validate chunks and bound cumulative audio duration.
    async fn synthesize_stream(&self, text: String, cancel: CancellationToken)
    -> Result<TtsStream>;
}

#[derive(Clone, Debug)]
pub struct CascadeConfig {
    pub sample_rate: u32,
    pub channel_capacity: usize,
    pub max_utterance: Duration,
    pub max_generated_audio: Duration,
    /// Covers the entire STT/model/TTS turn, not each stage separately.
    pub inference_timeout: Duration,
    pub max_history_messages: usize,
    pub max_context_bytes: usize,
}

impl Default for CascadeConfig {
    fn default() -> Self {
        Self {
            sample_rate: 24_000,
            channel_capacity: 128,
            max_utterance: Duration::from_secs(30),
            max_generated_audio: Duration::from_secs(60),
            inference_timeout: Duration::from_secs(60),
            max_history_messages: 128,
            max_context_bytes: 256 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct CascadedBackend {
    config: CascadeConfig,
    stt: Arc<dyn Stt>,
    model: Arc<dyn TextModel>,
    tts: Arc<dyn Tts>,
}

impl CascadedBackend {
    pub fn new(
        config: CascadeConfig,
        stt: Arc<dyn Stt>,
        model: Arc<dyn TextModel>,
        tts: Arc<dyn Tts>,
    ) -> Result<Self> {
        if !(8_000..=48_000).contains(&config.sample_rate)
            || config.channel_capacity == 0
            || config.channel_capacity > 4096
            || config.max_utterance.is_zero()
            || config.max_utterance > Duration::from_secs(120)
            || config.max_generated_audio.is_zero()
            || config.max_generated_audio > Duration::from_secs(120)
            || config.inference_timeout.is_zero()
            || config.max_history_messages == 0
            || config.max_history_messages > 4096
            || config.max_context_bytes == 0
            || config.max_context_bytes > 4 * 1024 * 1024
        {
            return Err(Error::Config("invalid cascaded pipeline limits".into()));
        }
        Ok(Self {
            config,
            stt,
            model,
            tts,
        })
    }
}

#[async_trait]
impl VoiceBackend for CascadedBackend {
    fn sample_rate(&self) -> u32 {
        self.config.sample_rate
    }

    async fn connect(&self) -> Result<VoiceSession> {
        let (session, channels) = VoiceSession::channels(self.config.channel_capacity)?;
        worker::spawn(self.clone(), channels);
        Ok(session)
    }
}
