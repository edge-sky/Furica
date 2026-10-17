//! Cascaded workflow assembly. Capability implementations live in sibling modules;
//! this module owns turn scheduling, history, tool continuations and cancellation.

mod inference;
mod worker;

use crate::{
    Error, Result,
    orchestration::{VoiceBackend, VoiceSession},
    stt::Stt,
    text_model::TextModel,
    tts::Tts,
};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};

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
