mod domain;
mod infrastructure;

use std::{error::Error, path::Path, time::Duration};

use domain::trigger::Trigger;
use infrastructure::model::Model;

/// Required sample rate. Callers perform resampling and channel mixing.
pub const SAMPLE_RATE: u32 = 16_000;
/// Required samples per frame (32 ms at [`SAMPLE_RATE`]).
pub const FRAME_SAMPLES: usize = 512;

/// Trigger policy; durations are rounded up to whole inference frames.
#[derive(Clone, Debug)]
pub struct VadConfig {
    pub speech_threshold: f32,
    pub silence_threshold: f32,
    pub min_speech: Duration,
    pub min_silence: Duration,
}

impl Default for VadConfig {
    fn default() -> Self {
        Self {
            speech_threshold: 0.5,
            silence_threshold: 0.35,
            min_speech: Duration::from_millis(64),
            min_silence: Duration::from_millis(300),
        }
    }
}

/// Raw model probability and at most one confirmed boundary for this frame.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VadOutput {
    pub speech_probability: f32,
    pub event: Option<VadEvent>,
}

/// Boundaries in 16 kHz samples, relative to the most recent reset or finish.
/// Events arrive after confirmation; retain PCM externally to use these offsets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VadEvent {
    /// Inclusive start, backdated to the first consecutive candidate frame.
    SpeechStarted { start_sample: u64 },
    /// Exclusive end, excluding the consecutive trailing silence.
    SpeechEnded { end_sample: u64 },
}

#[derive(Debug, thiserror::Error)]
pub enum VadError {
    #[error("invalid VAD configuration: {0}")]
    Config(&'static str),
    #[error("failed to load the Silero model: {source}")]
    Model {
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    #[error("unsupported Silero model contract: {0}")]
    ModelContract(&'static str),
    #[error("expected {FRAME_SAMPLES} PCM16 samples, received {actual}")]
    InvalidFrame { actual: usize },
    #[error("Silero inference failed: {source}")]
    Inference {
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    #[error("invalid Silero inference output: {0}")]
    InvalidOutput(&'static str),
    #[error("reset the VAD after the previous inference failure")]
    NeedsReset,
    #[error("audio sample position exceeds u64")]
    PositionOverflow,
}

/// Local, synchronous CPU inference for one continuous stream.
///
/// Loading and processing block the calling thread. Use your own worker thread
/// when integrating with an async runtime or an audio device callback.
pub struct SileroVad {
    model: Model,
    trigger: Trigger,
    needs_reset: bool,
}

impl SileroVad {
    /// Load a local streaming ONNX model with the documented Silero contract.
    /// Validates configuration before loading, and checks the model with a
    /// silence frame before resetting its state. Never downloads a model.
    pub fn from_file(model_path: impl AsRef<Path>, config: VadConfig) -> Result<Self, VadError> {
        let trigger = Trigger::new(config)?;
        let model = Model::load(model_path.as_ref())?;
        Ok(Self {
            model,
            trigger,
            needs_reset: false,
        })
    }

    /// Process exactly one mono PCM16 frame at 16 kHz in capture order.
    /// Invalid length does not advance any state. Inference/output errors
    /// require [`Self::reset`] before the stream can be used again.
    pub fn process(&mut self, pcm: &[i16]) -> Result<VadOutput, VadError> {
        self.ensure_ready()?;
        if pcm.len() != FRAME_SAMPLES {
            return Err(VadError::InvalidFrame { actual: pcm.len() });
        }
        // Check before advancing model state, including at the u64 boundary.
        self.trigger.next_position()?;
        let result = self.model.infer(pcm).and_then(|probability| {
            Ok(VadOutput {
                speech_probability: probability,
                event: self.trigger.process(probability)?,
            })
        });
        if result.is_err() {
            self.needs_reset = true;
        }
        result
    }

    /// Close the current stream without inference, emitting an end only if a
    /// start was confirmed. Success resets the model and timeline to zero.
    pub fn finish(&mut self) -> Result<Option<VadEvent>, VadError> {
        self.ensure_ready()?;
        let event = self.trigger.finish();
        self.reset();
        Ok(event)
    }

    /// Discard the stream without events, clearing model state and timeline.
    pub fn reset(&mut self) {
        self.model.reset();
        self.trigger.reset();
        self.needs_reset = false;
    }

    fn ensure_ready(&self) -> Result<(), VadError> {
        if self.needs_reset {
            Err(VadError::NeedsReset)
        } else {
            Ok(())
        }
    }
}
