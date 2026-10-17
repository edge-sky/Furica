//! Bounded recognition use case; conversation decisions belong to orchestration.

use super::{Stt, Utterance};
use crate::{Error, Result};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Recognize a complete utterance without starting a conversation or another
/// capability. The caller owns the deadline; cancellation drops the adapter future.
pub async fn transcribe(
    adapter: &dyn Stt,
    utterance: Utterance,
    max_utterance: Duration,
    max_text_bytes: usize,
    cancel: CancellationToken,
) -> Result<String> {
    if max_text_bytes == 0 || utterance.frames.is_empty() {
        return Err(Error::Config("invalid STT input or text limit".into()));
    }
    let mut samples = 0usize;
    let mut previous = None;
    for (index, frame) in utterance.frames.iter().enumerate() {
        frame.validate(utterance.sample_rate)?;
        if previous.is_some_and(|sequence| frame.sequence <= sequence) {
            return Err(Error::Audio("STT frame sequence did not increase".into()));
        }
        previous = Some(frame.sequence);
        samples = samples.saturating_add(frame.samples.len());
        validate_buffer(utterance.sample_rate, samples, index + 1, max_utterance)?;
    }
    let text = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(Error::Cancelled),
        result = adapter.transcribe(utterance, cancel.clone()) => result?,
    };
    if text.len() > max_text_bytes {
        return Err(Error::Busy("STT transcript"));
    }
    Ok(text)
}

/// Shared by complete calls and the orchestrator's incremental audio buffer.
pub(crate) fn validate_buffer(
    sample_rate: u32,
    samples: usize,
    frames: usize,
    max_utterance: Duration,
) -> Result<()> {
    if !(8_000..=48_000).contains(&sample_rate)
        || max_utterance.is_zero()
        || max_utterance > Duration::from_secs(120)
    {
        return Err(Error::Config("invalid STT audio limits".into()));
    }
    let limit = max_utterance.as_nanos() * u128::from(sample_rate) / 1_000_000_000;
    if samples as u128 > limit || frames > 4096 {
        return Err(Error::Busy("STT utterance buffer"));
    }
    Ok(())
}
