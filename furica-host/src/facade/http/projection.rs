use furica_core::sessions::{Message, MessageContent};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub offset: usize,
    pub limit: usize,
    pub total: usize,
}

pub fn page<T>(all: Vec<T>, offset: usize, limit: usize) -> Page<T> {
    let total = all.len();
    Page {
        items: all.into_iter().skip(offset).take(limit).collect(),
        offset,
        limit,
        total,
    }
}

pub fn message_view(message: &Message) -> Value {
    use furica_core::text_model::ModelMessage;
    let content = match &message.content {
        MessageContent::Model(ModelMessage::User(text)) => json!({"kind":"user","text":text}),
        MessageContent::Model(ModelMessage::Assistant(assistant)) => {
            json!({"kind":"assistant","parts":assistant.content})
        }
        MessageContent::Model(ModelMessage::ToolResult { call_id, result }) => {
            json!({"kind":"model_tool_result","call_id":call_id,"result":result})
        }
        MessageContent::Model(ModelMessage::ExecutionContext(data)) => {
            json!({"kind":"execution_context","blocks":data})
        }
        MessageContent::Model(ModelMessage::ExecutionUpdate(data)) => {
            json!({"kind":"execution_update","blocks":data})
        }
        MessageContent::System(text) => json!({"kind":"system","text":text}),
        MessageContent::Blocks(blocks) => json!({"kind":"blocks","blocks":blocks}),
        MessageContent::ToolCall(id) => json!({"kind":"tool_call","tool_call_id":id}),
        MessageContent::ToolResult(id) => json!({"kind":"tool_result","tool_call_id":id}),
    };
    json!({"id":message.id,"session_id":message.session_id,"acp_session_id":message.acp_session_id,
        "run_id":message.run_id,"sequence":message.sequence,"role":message.role,"source":message.source,
        "state":message.state,"created_at_ms":message.created_at_ms,"updated_at_ms":message.updated_at_ms,"content":content})
}
