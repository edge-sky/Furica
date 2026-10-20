use std::time::Duration;

use crate::{FRAME_SAMPLES, SAMPLE_RATE, VadConfig, VadError, VadEvent};

pub(crate) struct Trigger {
    config: VadConfig,
    min_speech: u64,
    min_silence: u64,
    position: u64,
    speaking: bool,
    candidate_start: Option<u64>,
    silence_start: Option<u64>,
}

impl Trigger {
    pub fn new(config: VadConfig) -> Result<Self, VadError> {
        if !config.speech_threshold.is_finite()
            || !config.silence_threshold.is_finite()
            || config.silence_threshold <= 0.0
            || config.silence_threshold >= config.speech_threshold
            || config.speech_threshold >= 1.0
        {
            return Err(VadError::Config(
                "thresholds must satisfy 0 < silence < speech < 1 and be finite",
            ));
        }
        Ok(Self {
            min_speech: duration_samples(config.min_speech)?,
            min_silence: duration_samples(config.min_silence)?,
            config,
            position: 0,
            speaking: false,
            candidate_start: None,
            silence_start: None,
        })
    }

    pub fn next_position(&self) -> Result<u64, VadError> {
        self.position
            .checked_add(FRAME_SAMPLES as u64)
            .ok_or(VadError::PositionOverflow)
    }

    pub fn process(&mut self, probability: f32) -> Result<Option<VadEvent>, VadError> {
        if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
            return Err(VadError::InvalidOutput(
                "probability must be finite in [0, 1]",
            ));
        }
        let start = self.position;
        self.position = self.next_position()?;
        if !self.speaking {
            if probability >= self.config.speech_threshold {
                let candidate = *self.candidate_start.get_or_insert(start);
                if self.position - candidate >= self.min_speech {
                    self.speaking = true;
                    self.candidate_start = None;
                    return Ok(Some(VadEvent::SpeechStarted {
                        start_sample: candidate,
                    }));
                }
            } else {
                self.candidate_start = None;
            }
        } else if probability < self.config.silence_threshold {
            let silence = *self.silence_start.get_or_insert(start);
            if self.position - silence >= self.min_silence {
                self.speaking = false;
                self.silence_start = None;
                return Ok(Some(VadEvent::SpeechEnded {
                    end_sample: silence,
                }));
            }
        } else {
            self.silence_start = None;
        }
        Ok(None)
    }

    pub fn finish(&self) -> Option<VadEvent> {
        self.speaking.then_some(VadEvent::SpeechEnded {
            end_sample: self.silence_start.unwrap_or(self.position),
        })
    }

    pub fn reset(&mut self) {
        self.position = 0;
        self.speaking = false;
        self.candidate_start = None;
        self.silence_start = None;
    }
}

fn duration_samples(duration: Duration) -> Result<u64, VadError> {
    if duration.is_zero() {
        return Err(VadError::Config("confirmation durations must be positive"));
    }
    // Round upward, including sub-sample durations; never trigger early because
    // of truncation or floating-point conversion of very large durations.
    u64::try_from((duration.as_nanos() * u128::from(SAMPLE_RATE)).div_ceil(1_000_000_000))
        .map_err(|_| VadError::Config("confirmation duration exceeds u64 samples"))
}

#[cfg(test)]
mod tests;
