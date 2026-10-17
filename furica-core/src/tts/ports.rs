//! Outbound synthesis port and asynchronous stream contract.

use super::domain::SynthesizedAudio;
use crate::Result;
use async_trait::async_trait;
use std::pin::Pin;
use tokio_util::sync::CancellationToken;

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
