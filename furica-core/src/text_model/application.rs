//! One bounded model invocation; history ownership and tool execution remain with the caller.

use super::{ModelRequest, ModelResponse, TextModel, domain::message_size};
use crate::{Error, Result};
use std::collections::HashSet;
use tokio_util::sync::CancellationToken;

/// Generate one bounded response. Empty tools are allowed; supplied tools are
/// passed through unchanged. Tool execution and subsequent calls belong to the caller.
/// The caller owns the deadline; cancellation drops the adapter future.
pub async fn generate(
    adapter: &dyn TextModel,
    request: ModelRequest,
    max_messages: usize,
    max_context_bytes: usize,
    cancel: CancellationToken,
) -> Result<ModelResponse> {
    if max_messages == 0 || max_context_bytes == 0 {
        return Err(Error::Config("invalid text model limits".into()));
    }
    let history_len = request.messages.len();
    let history_bytes = request.messages.iter().map(message_size).sum::<usize>();
    if history_len > max_messages || history_bytes > max_context_bytes {
        return Err(Error::Busy("pipeline conversation context"));
    }
    let response = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(Error::Cancelled),
        result = adapter.generate(request, cancel.clone()) => result?,
    };
    let response_bytes = match &response {
        ModelResponse::Text(text) => text.len(),
        ModelResponse::Tools(calls) => calls
            .iter()
            .map(|call| call.call_id.len() + call.name.len() + call.arguments.to_string().len())
            .sum(),
    };
    if history_len >= max_messages
        || history_bytes.saturating_add(response_bytes) > max_context_bytes
    {
        return Err(Error::Busy("pipeline conversation context"));
    }
    if let ModelResponse::Tools(calls) = &response {
        let mut ids = HashSet::new();
        if calls.is_empty() || calls.len() > 32 {
            return Err(Error::Protocol("invalid model tool batch".into()));
        }
        for call in calls {
            if call.call_id.is_empty()
                || call.call_id.len() > 256
                || !ids.insert(&call.call_id)
                || call.name.len() > 128
                || call.arguments.to_string().len() > max_context_bytes
            {
                return Err(Error::Protocol("invalid model tool call".into()));
            }
        }
    }
    Ok(response)
}

pub(crate) fn validate_text(text: &str, limit: usize) -> Result<()> {
    if text.len() > limit {
        return Err(Error::Busy("pipeline text"));
    }
    Ok(())
}
