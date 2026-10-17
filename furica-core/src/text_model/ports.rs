//! Outbound text generation port implemented by adapters.

use super::domain::{ModelRequest, ModelResponse};
use crate::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait TextModel: Send + Sync {
    /// Cooperate with cancellation and release resources on future drop.
    async fn generate(
        &self,
        request: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelResponse>;
}
