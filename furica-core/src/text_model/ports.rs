//! Outbound streaming port. Implementations keep no hidden conversation history.
use super::{ModelEvent, ModelMessage, Reasoning, ToolDefinition};
use crate::Result;
use async_trait::async_trait;
use std::pin::Pin;
use tokio_util::sync::CancellationToken;

/// Application input owned by the caller, which validates its configuration.
/// Core checks message/protocol consistency and enforces runtime budgets; the
/// outbound adapter translates the request to its wire protocol.
#[derive(Clone, Debug)]
pub struct ModelRequest {
    pub messages: Vec<ModelMessage>,
    pub tools: Vec<ToolDefinition>,
    pub instructions: Option<String>,
    pub max_output_tokens: u32,
    pub reasoning: Option<Reasoning>,
}

pub type ModelStream =
    Pin<Box<dyn futures_core::Stream<Item = Result<ModelEvent>> + Send + 'static>>;

#[async_trait]
pub trait TextModel: Send + Sync {
    /// Credential-free stable scope; incompatible continuations fail before I/O.
    fn continuation_source(&self) -> &str;

    /// Cancellation covers opening and reading. Drop releases local resources.
    /// EOF without Finished is an error; stop after the first error or Finished.
    async fn generate_stream(
        &self,
        request: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelStream>;
}
