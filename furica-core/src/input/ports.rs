//! Input detection and speaker verification ports supplied by the host.

use super::domain::{SpeakerDecision, VadActivity};
use crate::{Result, audio::AudioFrame};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// Called on the ingress actor. Implementations must be bounded and nonblocking;
/// device capture, model loading and blocking inference belong outside this call.
pub trait Vad: Send + Sync {
    fn detect(&self, frame: &AudioFrame) -> Result<VadActivity>;
}

/// Owns enrollment/matching policy. Scores and enrollment storage are adapter
/// concerns; the framework accepts only an explicit identity decision.
/// Implementations must stop owned work on cancellation or future drop.
#[async_trait]
pub trait SpeakerVerifier: Send + Sync {
    async fn verify(
        &self,
        frames: Vec<AudioFrame>,
        cancel: CancellationToken,
    ) -> Result<SpeakerDecision>;
}
