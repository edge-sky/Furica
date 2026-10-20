//! ACP wire updates mapped into Core message and tool records.
use furica_core::sessions::ExecutionEvent;
use serde_json::Value;
use uuid::Uuid;

pub(super) fn normalize_update(run_id: Option<Uuid>, value: &Value) -> Option<ExecutionEvent> {
    use furica_core::sessions::MessageRole;
    match value.get("sessionUpdate")?.as_str()? {
        "agent_message_chunk" | "user_message_chunk" | "agent_thought_chunk" => {
            let mut content = value.get("content")?.clone();
            let thought = value["sessionUpdate"] == "agent_thought_chunk";
            if thought {
                if content.get("text").and_then(Value::as_str).is_some() {
                    content["type"] = Value::String("reasoning".into());
                } else {
                    content = serde_json::json!({"type":"reasoning","content":content});
                }
            }
            Some(ExecutionEvent::Message {
                run_id,
                source_message_id: value
                    .get("messageId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                role: if value["sessionUpdate"] == "user_message_chunk" {
                    MessageRole::User
                } else {
                    MessageRole::Assistant
                },
                content,
            })
        }
        "tool_call" | "tool_call_update" => normalize_tool(run_id, value),
        _ => None,
    }
}
pub(super) fn normalize_tool(run_id: Option<Uuid>, v: &Value) -> Option<ExecutionEvent> {
    use furica_core::sessions::ToolCallState;
    let string = |name| v.get(name).and_then(Value::as_str).map(str::to_owned);
    let state = match v.get("status").and_then(Value::as_str) {
        Some("pending") => Some(ToolCallState::Pending),
        Some("in_progress") => Some(ToolCallState::Running),
        Some("completed") => Some(ToolCallState::Completed),
        Some("failed") => Some(ToolCallState::Failed),
        _ => None,
    };
    Some(ExecutionEvent::ToolCall {
        run_id,
        source_call_id: string("toolCallId")?,
        name: string("name"),
        title: string("title"),
        kind: string("kind"),
        state,
        input: v.get("rawInput").filter(|v| !v.is_null()).cloned(),
        output: v.get("rawOutput").filter(|v| !v.is_null()).cloned(),
        content: v.get("content").and_then(Value::as_array).cloned(),
        locations: v.get("locations").and_then(Value::as_array).cloned(),
    })
}
