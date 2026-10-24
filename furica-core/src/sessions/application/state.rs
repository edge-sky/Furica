//! Canonical in-memory session records and coalescing change notifications.
use super::super::domain::*;
use super::data::SessionData;
use crate::{Error, Result, text_model::ModelMessage};
use serde_json::Value;
use std::{
    collections::{HashSet, VecDeque},
    sync::{Mutex, MutexGuard},
};
use tokio::sync::watch;
use uuid::Uuid;

pub(super) struct Memory {
    data: Mutex<SessionData>,
    changes: watch::Sender<u64>,
    // Ordered, acknowledged delivery is separate from lossy UI notifications.
    pending: Mutex<DeliveryQueue>,
}
struct DeliveryQueue {
    generated: u64,
    acknowledged: u64,
    groups: VecDeque<(u64, Vec<SessionEvent>)>,
}
impl Memory {
    pub fn new(mut data: SessionData) -> Result<Self> {
        data.validate()?;
        // Imported records do not recreate process handles or execution effects.
        for s in &mut data.acp_sessions {
            if !s.state.is_terminal() {
                s.disconnected("execution connection is not attached".into());
            }
        }
        for r in &mut data.runs {
            if !r.state.is_terminal() {
                r.interrupt("execution is not attached; reconcile effects before retrying".into());
            }
        }
        for q in &mut data.questions {
            if q.state == QuestionState::Pending {
                q.resolve(QuestionState::Cancelled, None);
            }
        }
        for m in &mut data.messages {
            if m.state == MessageState::Streaming {
                m.state = MessageState::Interrupted;
                m.updated_at_ms = now_ms();
            }
        }
        for t in &mut data.tool_calls {
            if !t.state.is_terminal() {
                t.state = ToolCallState::Interrupted;
                t.updated_at_ms = now_ms();
            }
        }
        // Complete orphaned main-model tool exchanges with an explicit failure,
        // never by replaying their side effects.
        let missing: Vec<_> = data
            .tool_calls
            .iter()
            .filter(|t| {
                t.acp_session_id.is_none()
                    && !data
                        .messages
                        .iter()
                        .any(|m| matches!(m.content,MessageContent::ToolResult(id) if id==t.id))
            })
            .map(|t| (t.id, t.session_id))
            .collect();
        for (id, owner) in missing {
            let t = data
                .tool_calls
                .iter_mut()
                .find(|t| t.id == id)
                .expect("known call");
            if t.output.is_none() {
                t.output = Some(
                    serde_json::json!({"error":"tool result unavailable after context import"}),
                );
            }
            push_message(
                &mut data,
                owner,
                None,
                None,
                None,
                MessageRole::Tool,
                MessageSource::Tool { name: None },
                MessageContent::ToolResult(id),
                MessageState::Complete,
            )?;
        }
        let initial = context_messages(&data);
        let mut pending = VecDeque::new();
        let sequence = u64::from(!initial.is_empty());
        if sequence != 0 {
            pending.push_back((sequence, initial));
        }
        let (changes, _) = watch::channel(sequence);
        Ok(Self {
            data: Mutex::new(data),
            changes,
            pending: Mutex::new(DeliveryQueue {
                generated: sequence,
                acknowledged: 0,
                groups: pending,
            }),
        })
    }
    fn lock(&self) -> Result<MutexGuard<'_, SessionData>> {
        self.data
            .lock()
            .map_err(|_| Error::State("session data lock poisoned".into()))
    }
    #[cfg(test)]
    pub fn snapshot(&self) -> Result<SessionData> {
        Ok(self.lock()?.clone())
    }
    pub fn pending_messages(&self) -> Result<Option<(u64, Vec<SessionEvent>)>> {
        Ok(self
            .pending
            .lock()
            .map_err(|_| Error::State("message delivery lock poisoned".into()))?
            .groups
            .front()
            .cloned())
    }
    pub fn acknowledge_messages(&self, sequence: u64) -> Result<()> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| Error::State("message delivery lock poisoned".into()))?;
        if sequence <= pending.acknowledged {
            return Ok(());
        }
        if pending.groups.front().map(|v| v.0) != Some(sequence) {
            return Err(Error::State(
                "message acknowledgement is out of order".into(),
            ));
        }
        pending.groups.pop_front();
        pending.acknowledged = sequence;
        Ok(())
    }
    pub fn source_tool(&self, run_id: Uuid, source: Option<&str>) -> Result<Option<Uuid>> {
        Ok(self
            .lock()?
            .tool_calls
            .iter()
            .find(|t| t.run_id == Some(run_id) && Some(t.source_call_id.as_str()) == source)
            .map(|t| t.id))
    }
    pub fn switching_allowed(&self) -> Result<bool> {
        let data = self.lock()?;
        Ok(!data.runs.iter().any(|r| !r.state.is_terminal())
            && !data
                .questions
                .iter()
                .any(|q| q.state == QuestionState::Pending))
    }
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }
    pub fn change<T>(
        &self,
        f: impl FnOnce(&mut SessionData, &mut Vec<SessionEvent>) -> Result<T>,
    ) -> Result<T> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| Error::State("message delivery lock poisoned".into()))?;
        // Never evict unacknowledged messages. Host must continuously receive this queue.
        let sequence = pending
            .generated
            .checked_add(1)
            .ok_or_else(|| Error::State("delivery sequence exhausted".into()))?;
        let mut data = self.lock()?;
        let mut events = Vec::new();
        let result = f(&mut data, &mut events)?;
        if !events.is_empty() {
            pending.generated = sequence;
            pending.groups.push_back((sequence, events));
            self.changes.send_replace(sequence);
        }
        Ok(result)
    }
    pub fn owner(&self, id: Uuid) -> Result<Session> {
        self.lock()?
            .sessions
            .iter()
            .find(|s| s.id == id)
            .cloned()
            .ok_or_else(|| missing(id))
    }
    pub fn add_owner(&self, session: Session) -> Result<()> {
        self.change(|d, events| {
            if session.id.is_nil()
                || session.name.trim().is_empty()
                || session.name.len() > 1024
                || d.sessions.iter().any(|s| s.id == session.id)
            {
                return Err(Error::Config("invalid or duplicate session".into()));
            }
            events.push(SessionEvent::SessionChanged(session.clone()));
            d.sessions.push(session);
            Ok(())
        })
    }
    pub fn rename(&self, id: Uuid, name: String) -> Result<Session> {
        self.change(|d, events| {
            if name.trim().is_empty() || name.len() > 1024 {
                return Err(Error::Config("invalid session name".into()));
            }
            let s = d
                .sessions
                .iter_mut()
                .find(|s| s.id == id)
                .ok_or_else(|| missing(id))?;
            s.name = name;
            s.updated_at_ms = now_ms();
            events.push(SessionEvent::SessionChanged(s.clone()));
            Ok(s.clone())
        })
    }
    pub fn archive(&self, id: Uuid) -> Result<Session> {
        self.change(|d, events| {
            let s = d
                .sessions
                .iter_mut()
                .find(|s| s.id == id)
                .ok_or_else(|| missing(id))?;
            s.state = SessionState::Archived;
            s.updated_at_ms = now_ms();
            events.push(SessionEvent::SessionChanged(s.clone()));
            Ok(s.clone())
        })
    }
    pub fn get_session(&self, id: Uuid) -> Result<AcpSession> {
        self.lock()?
            .acp_sessions
            .iter()
            .find(|s| s.id == id)
            .cloned()
            .ok_or_else(|| missing(id))
    }
    pub fn list_sessions(&self, owner: Uuid) -> Result<Vec<AcpSession>> {
        let data = self.lock()?;
        if !data.sessions.iter().any(|s| s.id == owner) {
            return Err(missing(owner));
        }
        Ok(data
            .acp_sessions
            .iter()
            .filter(|s| s.session_id == owner)
            .cloned()
            .collect())
    }
    pub fn get_run(&self, id: Uuid) -> Result<Run> {
        self.lock()?
            .runs
            .iter()
            .find(|s| s.id == id)
            .cloned()
            .ok_or_else(|| missing(id))
    }
    pub fn get_question(&self, id: Uuid) -> Result<Question> {
        self.lock()?
            .questions
            .iter()
            .find(|s| s.id == id)
            .cloned()
            .ok_or_else(|| missing(id))
    }
    pub fn find_run_by_request(&self, key: &str) -> Result<Option<Run>> {
        Ok(self
            .lock()?
            .runs
            .iter()
            .find(|r| r.request_key == key)
            .cloned())
    }
    pub fn list_runs(&self, id: Uuid) -> Result<Vec<Run>> {
        Ok(self
            .lock()?
            .runs
            .iter()
            .filter(|r| r.acp_session_id == id)
            .cloned()
            .collect())
    }
    pub fn list_pending_questions(&self) -> Result<Vec<Question>> {
        Ok(self
            .lock()?
            .questions
            .iter()
            .filter(|q| q.state == QuestionState::Pending)
            .cloned()
            .collect())
    }
    pub fn recent_runs(&self, owner: Option<Uuid>, limit: usize) -> Result<Vec<Run>> {
        if limit == 0 || limit > 1000 {
            return Err(Error::Config("invalid run limit".into()));
        }
        Ok(self
            .lock()?
            .runs
            .iter()
            .rev()
            .filter(|r| owner.is_none_or(|id| r.session_id == id))
            .take(limit)
            .cloned()
            .collect())
    }
    pub fn update_session(&self, id: Uuid, f: impl FnOnce(&mut AcpSession)) -> Result<AcpSession> {
        self.change(|d, events| {
            let r = d
                .acp_sessions
                .iter_mut()
                .find(|r| r.id == id)
                .ok_or_else(|| missing(id))?;
            f(r);
            let result = r.clone();
            touch(d, result.session_id);
            events.push(SessionEvent::AcpSessionChanged(result.clone()));
            emit_owner(d, result.session_id, events);
            Ok(result)
        })
    }
    pub fn update_run(&self, id: Uuid, f: impl FnOnce(&mut Run)) -> Result<Run> {
        self.change(|d, events| {
            let r = d
                .runs
                .iter_mut()
                .find(|r| r.id == id)
                .ok_or_else(|| missing(id))?;
            f(r);
            r.updated_at_ms = now_ms();
            let result = r.clone();
            touch(d, result.session_id);
            events.push(SessionEvent::RunChanged {
                session_id: result.session_id,
                run: result.clone(),
            });
            emit_owner(d, result.session_id, events);
            Ok(result)
        })
    }
    pub fn update_question(&self, id: Uuid, f: impl FnOnce(&mut Question)) -> Result<Question> {
        self.change(|d, events| {
            let r = d
                .questions
                .iter_mut()
                .find(|r| r.id == id)
                .ok_or_else(|| missing(id))?;
            f(r);
            let result = r.clone();
            touch(d, result.session_id);
            events.push(SessionEvent::QuestionChanged(result.clone()));
            emit_owner(d, result.session_id, events);
            Ok(result)
        })
    }
    pub fn insert_question(&self, q: &Question) -> Result<()> {
        self.change(|d, events| {
            if d.questions.iter().any(|x| x.id == q.id) {
                return Err(Error::State("duplicate question".into()));
            }
            d.questions.push(q.clone());
            events.push(SessionEvent::QuestionChanged(q.clone()));
            touch(d, q.session_id);
            emit_owner(d, q.session_id, events);
            Ok(())
        })
    }
    pub fn prompt(&self, id: Uuid) -> Result<String> {
        let d = self.lock()?;
        let r = d
            .runs
            .iter()
            .find(|r| r.id == id)
            .ok_or_else(|| missing(id))?;
        let m = d
            .messages
            .iter()
            .find(|m| m.id == r.input_message_id)
            .ok_or_else(|| missing(r.input_message_id))?;
        match &m.content {
            MessageContent::Blocks(v) => Ok(v
                .iter()
                .filter_map(|b| b.get("text").and_then(|s| s.as_str()))
                .collect()),
            _ => Err(Error::State("run input is not text".into())),
        }
    }
    pub fn main_history(&self, id: Uuid) -> Result<Vec<ModelMessage>> {
        let d = self.lock()?;
        let mut records: Vec<_> = d
            .messages
            .iter()
            .filter(|m| {
                m.session_id == id
                    && m.acp_session_id.is_none()
                    && m.state == MessageState::Complete
            })
            .collect();
        records.sort_by_key(|m| m.sequence);
        let mut out = Vec::new();
        for m in records {
            match &m.content {
                MessageContent::Model(v) => out.push(v.clone()),
                MessageContent::ToolResult(id) => {
                    let t = d
                        .tool_calls
                        .iter()
                        .find(|t| t.id == *id)
                        .ok_or_else(|| missing(*id))?;
                    out.push(ModelMessage::ToolResult {
                        call_id: t.source_call_id.clone(),
                        result: t.output.clone().unwrap_or(Value::Null),
                    });
                }
                _ => {}
            }
        }
        Ok(out)
    }
    pub fn append_model(
        &self,
        id: Uuid,
        source: MessageSource,
        message: ModelMessage,
        state: MessageState,
    ) -> Result<Message> {
        if crate::text_model::message_size(&message) > MAX_MESSAGE_BYTES {
            return Err(Error::Busy("message content"));
        }
        self.change(|d, events| {
            require_active(d, id)?;
            next_sequence(d, id)?;
            if let ModelMessage::Assistant(a) = &message {
                let mut ids = HashSet::new();
                for call in a.tool_calls() {
                    if !ids.insert(&call.call_id)
                        || d.tool_calls.iter().any(|t| {
                            t.session_id == id
                                && t.acp_session_id.is_none()
                                && t.source_call_id == call.call_id
                                && t.output.is_none()
                                && !t.state.is_terminal()
                        })
                    {
                        return Err(Error::Protocol("duplicate main-context tool call".into()));
                    }
                }
            }
            if let ModelMessage::User(text) = &message
                && !d.messages.iter().any(|m| {
                    m.session_id == id && m.acp_session_id.is_none() && m.role == MessageRole::User
                })
            {
                let s = d.sessions.iter_mut().find(|s| s.id == id).expect("active");
                if s.name == "新会话" && !text.trim().is_empty() {
                    s.name = text
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .chars()
                        .take(48)
                        .collect();
                }
            }
            let role = model_role(&message);
            let content = match message {
                ModelMessage::Assistant(a) => {
                    let first_tool = d.tool_calls.len();
                    for c in a.tool_calls() {
                        let now = now_ms();
                        d.tool_calls.push(ToolCall {
                            id: Uuid::new_v4(),
                            session_id: id,
                            acp_session_id: None,
                            run_id: None,
                            source_call_id: c.call_id.clone(),
                            name: Some(c.name.clone()),
                            title: None,
                            kind: None,
                            input: Some(c.arguments.clone()),
                            output: None,
                            content: vec![],
                            locations: vec![],
                            state: ToolCallState::Pending,
                            created_at_ms: now,
                            updated_at_ms: now,
                        });
                    }
                    events.extend(
                        d.tool_calls[first_tool..]
                            .iter()
                            .cloned()
                            .map(SessionEvent::ToolCallRecorded),
                    );
                    MessageContent::Model(ModelMessage::Assistant(a))
                }
                ModelMessage::ToolResult { call_id, result } => {
                    let t = d
                        .tool_calls
                        .iter_mut()
                        .rev()
                        .find(|t| {
                            t.session_id == id
                                && t.acp_session_id.is_none()
                                && t.source_call_id == call_id
                                && t.output.is_none()
                        })
                        .ok_or_else(|| {
                            Error::Protocol("unknown main-context tool result".into())
                        })?;
                    t.state = if result.get("error").is_some() {
                        ToolCallState::Failed
                    } else {
                        ToolCallState::Completed
                    };
                    t.output = Some(result);
                    t.updated_at_ms = now_ms();
                    events.push(SessionEvent::ToolCallRecorded(t.clone()));
                    MessageContent::ToolResult(t.id)
                }
                other => MessageContent::Model(other),
            };
            let message = push_message(d, id, None, None, None, role, source, content, state)?;
            events.push(SessionEvent::MessageRecorded(message.clone()));
            emit_owner(d, id, events);
            Ok(message)
        })
    }
    pub fn set_instructions(&self, id: Uuid, text: Option<&str>) -> Result<()> {
        if text.is_some_and(|text| text.len() > MAX_MESSAGE_BYTES) {
            return Err(Error::Busy("system instructions"));
        }
        self.change(|d, events| {
            require_active(d, id)?;
            let current = d
                .messages
                .iter()
                .filter(|m| m.session_id == id && m.acp_session_id.is_none())
                .rev()
                .find_map(|m| match &m.content {
                    MessageContent::System(t) => Some(t.as_str()),
                    _ => None,
                });
            if current == text {
                return Ok(());
            }
            push_message(
                d,
                id,
                None,
                None,
                None,
                MessageRole::System,
                MessageSource::System,
                MessageContent::System(text.unwrap_or_default().to_owned()),
                MessageState::Complete,
            )?;
            events.push(SessionEvent::MessageRecorded(
                d.messages.last().expect("appended message").clone(),
            ));
            emit_owner(d, id, events);
            Ok(())
        })
    }
    pub fn finish_messages(&self, run: Uuid, interrupted: bool) -> Result<()> {
        self.change(|d, events| {
            for m in &mut d.messages {
                if m.run_id == Some(run) && m.state == MessageState::Streaming {
                    m.state = if interrupted {
                        MessageState::Interrupted
                    } else {
                        MessageState::Complete
                    };
                    m.updated_at_ms = now_ms();
                    events.push(SessionEvent::MessageRecorded(m.clone()));
                }
            }
            for t in &mut d.tool_calls {
                if t.run_id == Some(run) && !t.state.is_terminal() {
                    t.state = ToolCallState::Interrupted;
                    t.updated_at_ms = now_ms();
                    events.push(SessionEvent::ToolCallRecorded(t.clone()));
                }
            }
            Ok(())
        })
    }
}

pub(super) fn require_active(d: &SessionData, id: Uuid) -> Result<()> {
    match d.sessions.iter().find(|s| s.id == id) {
        Some(s) if s.state == SessionState::Active => Ok(()),
        Some(_) => Err(Error::State("session is archived".into())),
        None => Err(missing(id)),
    }
}
pub(super) fn touch(d: &mut SessionData, id: Uuid) {
    if let Some(s) = d.sessions.iter_mut().find(|s| s.id == id) {
        s.updated_at_ms = now_ms();
    }
}
pub(super) fn model_role(m: &ModelMessage) -> MessageRole {
    match m {
        ModelMessage::User(_) => MessageRole::User,
        ModelMessage::Assistant(_) => MessageRole::Assistant,
        _ => MessageRole::Tool,
    }
}
#[allow(clippy::too_many_arguments)]
pub(super) fn push_message(
    d: &mut SessionData,
    owner: Uuid,
    acp: Option<Uuid>,
    run: Option<Uuid>,
    source_id: Option<String>,
    role: MessageRole,
    source: MessageSource,
    content: MessageContent,
    state: MessageState,
) -> Result<Message> {
    let sequence = next_sequence(d, owner)?;
    let now = now_ms();
    let message = Message {
        id: Uuid::new_v4(),
        session_id: owner,
        acp_session_id: acp,
        run_id: run,
        source_message_id: source_id,
        sequence,
        role,
        source,
        content,
        state,
        created_at_ms: now,
        updated_at_ms: now,
    };
    d.messages.push(message.clone());
    touch(d, owner);
    Ok(message)
}
pub(super) fn missing(id: Uuid) -> Error {
    Error::NotFound(id.to_string())
}

pub(super) fn next_sequence(d: &SessionData, owner: Uuid) -> Result<u64> {
    d.messages
        .iter()
        .filter(|m| m.session_id == owner)
        .map(|m| m.sequence)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| Error::State("message sequence exhausted".into()))
}

/// A host-provided context is emitted once after runtime recovery normalization.
fn context_messages(data: &SessionData) -> Vec<SessionEvent> {
    data.sessions
        .iter()
        .cloned()
        .map(SessionEvent::SessionChanged)
        .chain(
            data.acp_sessions
                .iter()
                .cloned()
                .map(SessionEvent::AcpSessionChanged),
        )
        .chain(
            data.messages
                .iter()
                .cloned()
                .map(SessionEvent::MessageRecorded),
        )
        .chain(
            data.runs
                .iter()
                .cloned()
                .map(|run| SessionEvent::RunChanged {
                    session_id: run.session_id,
                    run,
                }),
        )
        .chain(
            data.tool_calls
                .iter()
                .cloned()
                .map(SessionEvent::ToolCallRecorded),
        )
        .chain(
            data.questions
                .iter()
                .cloned()
                .map(SessionEvent::QuestionChanged),
        )
        .collect()
}
pub(super) fn emit_owner(data: &SessionData, id: Uuid, events: &mut Vec<SessionEvent>) {
    if let Some(session) = data.sessions.iter().find(|s| s.id == id) {
        events.push(SessionEvent::SessionChanged(session.clone()));
    }
}
