use crate::text_model::ModelMessage;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageRole {
    User,
    Assistant,
    System,
    Tool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageSource {
    UserText,
    UserVoice { stt: Option<String> },
    Model { source: String },
    Agent { agent_id: String },
    Tool { name: Option<String> },
    System,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageState {
    Streaming,
    Complete,
    Interrupted,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MessageContent {
    /// Includes tool arguments and opaque provider continuation data.
    Model(ModelMessage),
    System(String),
    /// Complete ACP content blocks, including non-text content.
    Blocks(Vec<Value>),
    ToolCall(Uuid),
    ToolResult(Uuid),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub id: Uuid,
    pub session_id: Uuid,
    /// None selects the one main-model context of the business session.
    pub acp_session_id: Option<Uuid>,
    pub run_id: Option<Uuid>,
    pub source_message_id: Option<String>,
    /// Stable ordering within the business session, independent of timestamps.
    pub sequence: u64,
    pub role: MessageRole,
    pub source: MessageSource,
    pub content: MessageContent,
    pub state: MessageState,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolCallState {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}
impl ToolCallState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: Uuid,
    pub session_id: Uuid,
    pub acp_session_id: Option<Uuid>,
    pub run_id: Option<Uuid>,
    pub source_call_id: String,
    pub name: Option<String>,
    pub title: Option<String>,
    pub kind: Option<String>,
    pub input: Option<Value>,
    pub output: Option<Value>,
    pub content: Vec<Value>,
    pub locations: Vec<Value>,
    pub state: ToolCallState,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

/// Local memory limit per complete message, independent of protocol chunk sizes.
pub const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
