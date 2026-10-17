//! Model messages, tool calls and response values; no scheduling or provider I/O.

use serde_json::Value;

#[derive(Clone, Debug)]
pub struct ToolCall {
    pub call_id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Clone, Debug)]
pub enum ModelMessage {
    User(String),
    Assistant(String),
    AssistantTools(Vec<ToolCall>),
    ToolResult {
        call_id: String,
        result: Value,
    },
    /// Metadata and agent output are data, never system instructions.
    TaskContext(Value),
    TaskUpdate(Value),
}

#[derive(Clone, Debug)]
pub struct ModelRequest {
    pub messages: Vec<ModelMessage>,
    pub tools: Vec<Value>,
}

/// A turn either replies or requests tools. Tool acceptance is fed back before
/// the next model turn; requesting a task does not mean the task has completed.
#[derive(Clone, Debug)]
pub enum ModelResponse {
    Text(String),
    Tools(Vec<ToolCall>),
}

pub(crate) fn message_size(message: &ModelMessage) -> usize {
    match message {
        ModelMessage::User(text) | ModelMessage::Assistant(text) => text.len(),
        ModelMessage::AssistantTools(calls) => calls
            .iter()
            .map(|call| call.call_id.len() + call.name.len() + call.arguments.to_string().len())
            .sum(),
        ModelMessage::ToolResult { call_id, result } => call_id.len() + result.to_string().len(),
        ModelMessage::TaskContext(value) | ModelMessage::TaskUpdate(value) => {
            value.to_string().len()
        }
    }
}
