//! Assemble ACP message chunks and merge tool updates into final domain records.
use super::super::domain::*;
use super::state::{Memory, missing, next_sequence, push_message, touch};
use crate::{Error, Result};
use serde_json::Value;
use uuid::Uuid;

impl Memory {
    pub fn append_acp(
        &self,
        acp_id: Uuid,
        run_id: Option<Uuid>,
        source_id: Option<String>,
        role: MessageRole,
        content: Value,
    ) -> Result<()> {
        if !matches!(role, MessageRole::User | MessageRole::Assistant) {
            return Err(Error::Protocol("invalid agent message role".into()));
        }
        let incoming_bytes = crate::text_model::json_size(&content);
        if incoming_bytes > MAX_MESSAGE_BYTES {
            return Err(Error::Busy("ACP message content"));
        }
        self.change(|data| {
            let acp=data.acp_sessions.iter().find(|session|session.id==acp_id)
                .cloned().ok_or_else(||missing(acp_id))?;
            let candidate=if let Some(id)=&source_id {
                data.messages.iter_mut().find(|message| {
                    message.acp_session_id==Some(acp_id) && message.run_id==run_id
                        && message.source_message_id.as_ref()==Some(id) && message.role==role
                        && message.state==MessageState::Streaming
                        && matches!(message.content,MessageContent::Blocks(_))
                })
            } else {
                data.messages.iter_mut().rev()
                    .find(|message|message.acp_session_id==Some(acp_id) && message.run_id==run_id)
                    .filter(|message| {
                        message.source_message_id.is_none() && message.role==role
                            && message.state==MessageState::Streaming
                            && matches!(&message.content,MessageContent::Blocks(blocks)
                                if blocks.last().and_then(|block|block.get("type"))==content.get("type"))
                    })
            };
            if let Some(message)=candidate {
                if let MessageContent::Blocks(blocks)=&mut message.content {
                    let existing_bytes=blocks.iter().map(crate::text_model::json_size).sum::<usize>();
                    if existing_bytes.saturating_add(incoming_bytes)>MAX_MESSAGE_BYTES {
                        return Err(Error::Busy("ACP message content"));
                    }
                    let mut merged=false;
                    if let Some(last)=blocks.last_mut()
                        && same_block_metadata(last,&content)
                        && let Some(text)=content.get("text").and_then(Value::as_str)
                        && let Some(Value::String(previous))=last.get_mut("text") {
                        previous.push_str(text);
                        merged=true;
                    }
                    if !merged { blocks.push(content); }
                }
                message.updated_at_ms=now_ms();
                touch(data,acp.session_id);
            } else {
                push_message(data,acp.session_id,Some(acp_id),run_id,source_id,role,
                    MessageSource::Agent{agent_id:acp.agent_id},MessageContent::Blocks(vec![content]),MessageState::Streaming)?;
            }
            if let Some(session)=data.acp_sessions.iter_mut().find(|session|session.id==acp_id) { session.touch(); }
            Ok(())
        })
    }
    #[allow(clippy::too_many_arguments)]
    pub fn apply_tool(
        &self,
        acp_id: Uuid,
        run_id: Option<Uuid>,
        source_call_id: String,
        name: Option<String>,
        title: Option<String>,
        kind: Option<String>,
        state: Option<ToolCallState>,
        input: Option<Value>,
        output: Option<Value>,
        content: Option<Vec<Value>>,
        locations: Option<Vec<Value>>,
    ) -> Result<()> {
        self.change(|d| {
            let owner = d
                .acp_sessions
                .iter()
                .find(|s| s.id == acp_id)
                .ok_or_else(|| missing(acp_id))?
                .session_id;
            // At most two timeline entries (call/result) may be created. Check
            // capacity before merging any fields so an error leaves data unchanged.
            next_sequence(d, owner)?
                .checked_add(1)
                .ok_or_else(|| Error::State("message sequence exhausted".into()))?;
            let index = if let Some(index) = d.tool_calls.iter().position(|t| {
                t.acp_session_id == Some(acp_id)
                    && t.run_id == run_id
                    && t.source_call_id == source_call_id
            }) {
                index
            } else {
                if source_call_id.is_empty() {
                    return Err(Error::Protocol("tool call id is empty".into()));
                }
                let now = now_ms();
                let id = Uuid::new_v4();
                push_message(
                    d,
                    owner,
                    Some(acp_id),
                    run_id,
                    None,
                    MessageRole::Assistant,
                    MessageSource::Agent {
                        agent_id: d
                            .acp_sessions
                            .iter()
                            .find(|s| s.id == acp_id)
                            .expect("known")
                            .agent_id
                            .clone(),
                    },
                    MessageContent::ToolCall(id),
                    MessageState::Complete,
                )?;
                d.tool_calls.push(ToolCall {
                    id,
                    session_id: owner,
                    acp_session_id: Some(acp_id),
                    run_id,
                    source_call_id,
                    name: None,
                    title: None,
                    kind: None,
                    input: None,
                    output: None,
                    content: vec![],
                    locations: vec![],
                    state: ToolCallState::Pending,
                    created_at_ms: now,
                    updated_at_ms: now,
                });
                d.tool_calls.len() - 1
            };
            let call = &mut d.tool_calls[index];
            if let Some(v) = name {
                call.name = Some(v);
            }
            if let Some(v) = title {
                call.title = Some(v);
            }
            if let Some(v) = kind {
                call.kind = Some(v);
            }
            if let Some(v) = input {
                call.input = Some(v);
            }
            if let Some(v) = output {
                call.output = Some(v);
            }
            if let Some(v) = content {
                call.content = v;
            }
            if let Some(v) = locations {
                call.locations = v;
            }
            if let Some(v) = state
                && !call.state.is_terminal()
            {
                call.state = v;
            }
            call.updated_at_ms = now_ms();
            let id = call.id;
            let name = call.name.clone();
            let terminal = call.state.is_terminal();
            if terminal
                && !d
                    .messages
                    .iter()
                    .any(|m| matches!(m.content,MessageContent::ToolResult(v) if v==id))
            {
                push_message(
                    d,
                    owner,
                    Some(acp_id),
                    run_id,
                    None,
                    MessageRole::Tool,
                    MessageSource::Tool { name },
                    MessageContent::ToolResult(id),
                    MessageState::Complete,
                )?;
            }
            touch(d, owner);
            if let Some(s) = d.acp_sessions.iter_mut().find(|s| s.id == acp_id) {
                s.touch();
            }
            Ok(())
        })
    }
}

fn same_block_metadata(left: &Value, right: &Value) -> bool {
    match (left.as_object(), right.as_object()) {
        (Some(left), Some(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .all(|(key, value)| key == "text" || right.get(key) == Some(value))
        }
        _ => false,
    }
}
