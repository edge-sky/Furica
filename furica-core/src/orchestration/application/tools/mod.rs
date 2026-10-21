//! Session-bound built-in tool declarations, dispatch and correlated results.
mod acp;
mod speech;

use super::cascade::SpeechQueue;
use crate::{
    Error, Result,
    sessions::SessionManager,
    text_model::{ModelMessage, ToolCall, ToolDefinition},
};
use serde_json::{Value, json};
use uuid::Uuid;

struct ToolEntry {
    definition: ToolDefinition,
    handler: fn(&CoreTools, &ToolCall, Option<&mut SpeechQueue>) -> Result<Value>,
}

pub(super) struct CoreTools {
    sessions: SessionManager,
    session_id: Uuid,
    entries: Vec<ToolEntry>,
}

impl CoreTools {
    pub(super) fn new(sessions: SessionManager, session_id: Uuid, speech: bool) -> Self {
        let mut entries = acp::entries();
        if speech {
            entries.push(speech::entry());
        }
        Self {
            sessions,
            session_id,
            entries,
        }
    }

    pub(super) fn definitions(&self) -> Vec<ToolDefinition> {
        self.entries
            .iter()
            .map(|entry| entry.definition.clone())
            .collect()
    }

    /// Returns acceptance/error immediately; business completion remains owned
    /// by the ACP runtime or speech queue. The caller records this message once.
    pub(super) fn invoke(&self, call: ToolCall, speech: Option<&mut SpeechQueue>) -> ModelMessage {
        let result = (|| {
            if call.call_id.is_empty() || call.call_id.len() > 256 {
                return Err(Error::Protocol("invalid tool call ID".into()));
            }
            let entry = self
                .entries
                .iter()
                .find(|entry| entry.definition.name == call.name)
                .ok_or_else(|| Error::Protocol("unknown core tool".into()))?;
            (entry.handler)(self, &call, speech)
        })()
        .unwrap_or_else(|error| json!({"error": error.to_string()}));
        ModelMessage::ToolResult {
            call_id: call.call_id,
            result,
        }
    }
}

/// ACP tools available to custom voice backends. Derived from dispatch entries.
pub fn execution_tools() -> Vec<ToolDefinition> {
    acp::entries()
        .into_iter()
        .map(|entry| entry.definition)
        .collect()
}

#[cfg(test)]
#[path = "../../../../tests/behavior/core_tools.rs"]
mod tests;
