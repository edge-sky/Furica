//! Input capability assembly, limits and per-input collection bookkeeping.

use super::{SpeakerVerifier, Vad};
use crate::{Error, Result, config::CoreConfig};
use std::{sync::Arc, time::Duration};

#[derive(Clone, Debug)]
pub struct InputConfig {
    /// Amount of voiced PCM collected before requesting speaker verification.
    /// An earlier endpoint submits the shorter segment for an explicit decision.
    pub verification_window: Duration,
    pub verification_timeout: Duration,
    pub endpoint_silence: Duration,
}

impl Default for InputConfig {
    fn default() -> Self {
        Self {
            verification_window: Duration::from_secs(1),
            verification_timeout: Duration::from_secs(5),
            endpoint_silence: Duration::from_millis(300),
        }
    }
}

pub struct SpeechInput {
    pub vad: Arc<dyn Vad>,
    pub speaker: Arc<dyn SpeakerVerifier>,
    pub config: InputConfig,
}

impl SpeechInput {
    pub(crate) fn validate(&self, core: &CoreConfig) -> Result<()> {
        if self.config.verification_window.is_zero()
            || self.config.verification_window >= core.connect_buffer
            || self.config.verification_timeout.is_zero()
            || self.config.endpoint_silence.is_zero()
            || self.config.endpoint_silence > core.connect_buffer
        {
            return Err(Error::Config("invalid speech input limits".into()));
        }
        Ok(())
    }
}

pub(crate) struct InputGate {
    pub input: SpeechInput,
    pub speaking: bool,
    pub voiced_samples: usize,
    pub silent_samples: usize,
    pub requested: bool,
}

impl InputGate {
    pub fn new(input: SpeechInput) -> Self {
        Self {
            input,
            speaking: false,
            voiced_samples: 0,
            silent_samples: 0,
            requested: false,
        }
    }

    pub fn reset(&mut self) {
        self.speaking = false;
        self.voiced_samples = 0;
        self.silent_samples = 0;
        self.requested = false;
    }
}
