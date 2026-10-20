//! Provider-independent values. Continuations are opaque data, never instructions.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: Option<String>,
    pub parameters: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    pub call_id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelContinuation {
    /// Stable, credential-free identity of protocol, service, model and isolation scope.
    pub source: String,
    pub data: Value,
}

impl std::fmt::Debug for ModelContinuation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModelContinuation")
            .field("source", &self.source)
            .field("data", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ModelContent {
    Text(String),
    ToolCall(ToolCall),
    ReasoningSummary(String),
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AssistantMessage {
    pub content: Vec<ModelContent>,
    pub continuation: Option<ModelContinuation>,
}

impl AssistantMessage {
    pub fn text(&self) -> String {
        self.content
            .iter()
            .filter_map(|part| match part {
                ModelContent::Text(text) => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }

    pub fn tool_calls(&self) -> impl Iterator<Item = &ToolCall> {
        self.content.iter().filter_map(|part| match part {
            ModelContent::ToolCall(call) => Some(call),
            _ => None,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ModelMessage {
    User(String),
    Assistant(AssistantMessage),
    ToolResult {
        call_id: String,
        result: Value,
    },
    /// Metadata and agent output are data, never system instructions.
    ExecutionContext(Value),
    ExecutionUpdate(Value),
}

/// Effort is a qualitative hint; a token budget is a different, explicit setting.
/// Adapters reject settings their configured protocol/model does not support.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reasoning {
    Low,
    Medium,
    High,
    BudgetTokens(u32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FinishReason {
    Complete,
    ToolCalls,
    Truncated,
    Refused,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ModelUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ModelResponse {
    pub message: AssistantMessage,
    pub finish_reason: FinishReason,
    pub usage: Option<ModelUsage>,
}

/// Indices determine final content order, independently of delta arrival order.
/// Deltas remain provisional until Finished has passed validation.
#[derive(Clone, Debug, PartialEq)]
pub enum ModelEvent {
    TextStart {
        index: usize,
    },
    ReasoningStart {
        index: usize,
    },
    ToolStart {
        index: usize,
        call_id: String,
        name: String,
    },
    TextDelta {
        index: usize,
        text: String,
    },
    ReasoningDelta {
        index: usize,
        text: String,
    },
    ToolArgumentsDelta {
        index: usize,
        json: String,
    },
    ContentEnd {
        index: usize,
    },
    Finished {
        reason: FinishReason,
        usage: Option<ModelUsage>,
        continuation: Option<ModelContinuation>,
    },
}

pub(crate) fn json_size(value: &Value) -> usize {
    // Count without allocating a second serialized copy of retained JSON.
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len());
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Counter(0);
    serde_json::to_writer(&mut count, value).expect("JSON value serialization");
    count.0
}

pub(crate) fn continuation_size(c: &ModelContinuation) -> usize {
    c.source.len().saturating_add(json_size(&c.data))
}

pub(crate) fn message_size(message: &ModelMessage) -> usize {
    match message {
        ModelMessage::User(text) => text.len(),
        ModelMessage::Assistant(message) => message
            .content
            .iter()
            .map(|part| match part {
                ModelContent::Text(s) | ModelContent::ReasoningSummary(s) => s.len(),
                ModelContent::ToolCall(c) => c
                    .call_id
                    .len()
                    .saturating_add(c.name.len())
                    .saturating_add(json_size(&c.arguments)),
            })
            .fold(
                message.continuation.as_ref().map_or(0, continuation_size),
                usize::saturating_add,
            ),
        ModelMessage::ToolResult { call_id, result } => {
            call_id.len().saturating_add(json_size(result))
        }
        ModelMessage::ExecutionContext(v) | ModelMessage::ExecutionUpdate(v) => json_size(v),
    }
}
