//! Outbound speech recognition port implemented by adapters.

use super::domain::Utterance;
use crate::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait Stt: Send + Sync {
    /// Return one final transcript. Empty/whitespace means no recognized speech.
    /// Implementations must release owned resources when the future is dropped.
    async fn transcribe(&self, utterance: Utterance, cancel: CancellationToken) -> Result<String>;
}
