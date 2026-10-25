//! Host data values and relationship validation; no storage or migration code.
use super::super::domain::*;
use super::state::model_role;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use uuid::Uuid;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SessionData {
    pub sessions: Vec<Session>,
    pub messages: Vec<Message>,
    pub acp_sessions: Vec<AcpSession>,
    pub runs: Vec<Run>,
    pub tool_calls: Vec<ToolCall>,
    pub questions: Vec<Question>,
}

impl SessionData {
    pub fn validate(&self) -> Result<()> {
        fn unique(ids: impl Iterator<Item = Uuid>) -> Result<()> {
            let mut seen = HashSet::new();
            for id in ids {
                if id.is_nil() || !seen.insert(id) {
                    return Err(Error::Config("nil or duplicate data id".into()));
                }
            }
            Ok(())
        }
        unique(self.sessions.iter().map(|x| x.id))?;
        unique(self.messages.iter().map(|x| x.id))?;
        unique(self.acp_sessions.iter().map(|x| x.id))?;
        unique(self.runs.iter().map(|x| x.id))?;
        unique(self.tool_calls.iter().map(|x| x.id))?;
        unique(self.questions.iter().map(|x| x.id))?;
        let belongs = |id| self.sessions.iter().any(|s| s.id == id);
        let scope = |owner, acp: Option<Uuid>| {
            belongs(owner)
                && acp.is_none_or(|id| {
                    self.acp_sessions
                        .iter()
                        .any(|s| s.id == id && s.session_id == owner)
                })
        };
        for s in &self.sessions {
            if s.name.trim().is_empty() || s.name.len() > 1024 {
                return Err(Error::Config("invalid session name".into()));
            }
        }
        for s in &self.acp_sessions {
            if !belongs(s.session_id) {
                return Err(Error::State("invalid ACP session ownership".into()));
            }
        }
        let mut sequences = HashSet::new();
        for m in &self.messages {
            let bytes = match &m.content {
                MessageContent::Model(message) => crate::text_model::message_size(message),
                MessageContent::System(text) => text.len(),
                MessageContent::Blocks(blocks) => {
                    blocks.iter().map(crate::text_model::json_size).sum()
                }
                _ => 0,
            };
            if bytes > MAX_MESSAGE_BYTES {
                return Err(Error::Config(
                    "message content exceeds the local limit".into(),
                ));
            }
            if !scope(m.session_id, m.acp_session_id)
                || m.sequence == 0
                || !sequences.insert((m.session_id, m.sequence))
                || m.run_id.is_some_and(|id| {
                    !self.runs.iter().any(|r| {
                        r.id == id
                            && r.session_id == m.session_id
                            && Some(r.acp_session_id) == m.acp_session_id
                    })
                })
            {
                return Err(Error::Config(
                    "invalid message ownership, order or execution".into(),
                ));
            }
            let valid = match &m.content {
                MessageContent::Model(v) => m.acp_session_id.is_none() && m.role == model_role(v),
                MessageContent::System(_) => {
                    m.role == MessageRole::System && m.acp_session_id.is_none()
                }
                MessageContent::Blocks(_) => m.acp_session_id.is_some(),
                MessageContent::ToolCall(id) | MessageContent::ToolResult(id) => {
                    self.tool_calls.iter().any(|t| {
                        t.id == *id
                            && t.session_id == m.session_id
                            && t.acp_session_id == m.acp_session_id
                            && t.run_id == m.run_id
                    }) && m.role
                        == if matches!(m.content, MessageContent::ToolCall(_)) {
                            MessageRole::Assistant
                        } else {
                            MessageRole::Tool
                        }
                }
            };
            if !valid {
                return Err(Error::Config(
                    "message role or content does not match its context".into(),
                ));
            }
        }
        let mut requests = HashSet::new();
        for r in &self.runs {
            if !scope(r.session_id, Some(r.acp_session_id))
                || !requests.insert(&r.request_key)
                || !r.request_key.starts_with(&format!("{}:", r.session_id))
                || !self.messages.iter().any(|m| {
                    m.id == r.input_message_id
                        && m.session_id == r.session_id
                        && m.acp_session_id == Some(r.acp_session_id)
                        && m.run_id == Some(r.id)
                        && m.role == MessageRole::User
                })
            {
                return Err(Error::Config(
                    "invalid run ownership, input or request key".into(),
                ));
            }
        }
        let mut calls = HashSet::new();
        for t in &self.tool_calls {
            if !scope(t.session_id, t.acp_session_id)
                || t.source_call_id.is_empty()
                || (t.acp_session_id.is_some()
                    && !calls.insert((t.session_id, t.acp_session_id, t.run_id, &t.source_call_id)))
                || t.run_id.is_some_and(|id| {
                    !self.runs.iter().any(|r| {
                        r.id == id
                            && r.session_id == t.session_id
                            && Some(r.acp_session_id) == t.acp_session_id
                    })
                })
            {
                return Err(Error::Config(
                    "invalid tool call ownership or identity".into(),
                ));
            }
        }
        for q in &self.questions {
            if !self.runs.iter().any(|r| {
                r.id == q.run_id
                    && r.session_id == q.session_id
                    && r.acp_session_id == q.acp_session_id
            }) || q.tool_call_id.is_some_and(|id| {
                !self
                    .tool_calls
                    .iter()
                    .any(|t| t.id == id && t.run_id == Some(q.run_id))
            }) {
                return Err(Error::Config("invalid question ownership".into()));
            }
        }
        Ok(())
    }
}
