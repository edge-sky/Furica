//! ACP execution lifecycles and user interactions.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{Error, Result};

pub const MAX_SUMMARY_CHARS: usize = 16_384;
pub const MAX_QUESTION_TEXT_CHARS: usize = 8_192;
pub const MAX_ANSWER_BYTES: usize = 64 * 1024;

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn truncate(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

/// Lifecycle of one ACP session. A session may execute many prompt rounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcpSessionState {
    Opening,
    Ready,
    Closing,
    Closed,
    Disconnected,
}

impl AcpSessionState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Closed | Self::Disconnected)
    }

    /// Opening sessions accept work so the first round can be queued while the
    /// handshake and `session/new` are still in flight.
    pub fn accepts_runs(self) -> bool {
        matches!(self, Self::Opening | Self::Ready)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcpSession {
    pub id: Uuid,
    pub session_id: Uuid,
    pub remote_session_id: Option<String>,
    pub state: AcpSessionState,
    pub workspace: PathBuf,
    pub agent_id: String,
    pub updated_at_ms: u64,
    pub created_at_ms: u64,
    pub last_activity_ms: u64,
    pub error: Option<String>,
}

impl AcpSession {
    pub(crate) fn opened(&mut self, remote_session_id: String) -> bool {
        if self.state != AcpSessionState::Opening {
            return false;
        }
        self.state = AcpSessionState::Ready;
        self.remote_session_id = Some(remote_session_id);
        self.touch();
        true
    }

    pub(crate) fn request_close(&mut self) -> bool {
        if self.state.is_terminal() {
            return false;
        }
        self.state = AcpSessionState::Closing;
        self.touch();
        true
    }

    pub(crate) fn closed(&mut self) -> bool {
        if self.state == AcpSessionState::Closed {
            return false;
        }
        self.state = AcpSessionState::Closed;
        self.touch();
        true
    }

    pub(crate) fn disconnected(&mut self, error: String) -> bool {
        if self.state.is_terminal() {
            return false;
        }
        self.state = AcpSessionState::Disconnected;
        self.error = Some(truncate(&error, MAX_QUESTION_TEXT_CHARS));
        self.touch();
        true
    }

    pub(crate) fn touch(&mut self) {
        self.last_activity_ms = now_ms();
        self.updated_at_ms = self.last_activity_ms;
    }
}

/// Lifecycle of one prompt round. A run belongs to exactly one ACP session.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Prepared,
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

impl RunState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Run {
    pub id: Uuid,
    pub session_id: Uuid,
    pub input_message_id: Uuid,
    pub updated_at_ms: u64,
    /// Local `AcpSession::id`; the remote ACP id lives on the session.
    pub acp_session_id: Uuid,
    /// Idempotency key derived by SessionManager, not by the model.
    pub request_key: String,
    pub state: RunState,
    pub summary: String,
    pub error: Option<String>,
    pub stop_reason: Option<String>,
    pub created_at_ms: u64,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
}

/// Terminal result reported by an executor for one round.
#[derive(Clone, Debug)]
pub struct RunOutcome {
    pub state: RunState,
    pub summary: String,
    pub stop_reason: Option<String>,
    pub error: Option<String>,
}

impl Run {
    pub(crate) fn mark_running(&mut self) -> bool {
        if self.state != RunState::Queued {
            return false;
        }
        self.state = RunState::Running;
        self.updated_at_ms = now_ms();
        self.started_at_ms = Some(now_ms());
        true
    }

    pub(crate) fn finish(&mut self, outcome: RunOutcome) -> bool {
        if self.state.is_terminal() {
            return false;
        }
        if !outcome.state.is_terminal() {
            return self.interrupt("executor returned a nonterminal run outcome".into());
        }
        self.state = outcome.state;
        self.summary = truncate(&outcome.summary, MAX_SUMMARY_CHARS);
        self.stop_reason = outcome.stop_reason;
        self.error = outcome
            .error
            .map(|error| truncate(&error, MAX_QUESTION_TEXT_CHARS));
        self.updated_at_ms = now_ms();
        self.finished_at_ms = Some(self.updated_at_ms);
        true
    }

    pub(crate) fn interrupt(&mut self, error: String) -> bool {
        if self.state.is_terminal() {
            return false;
        }
        self.state = RunState::Interrupted;
        self.error = Some(truncate(&error, MAX_QUESTION_TEXT_CHARS));
        self.updated_at_ms = now_ms();
        self.finished_at_ms = Some(self.updated_at_ms);
        true
    }

    pub(crate) fn cancel_before_start(&mut self, reason: &str) -> bool {
        if !matches!(self.state, RunState::Prepared | RunState::Queued) {
            return false;
        }
        self.state = RunState::Cancelled;
        self.stop_reason = Some(reason.to_owned());
        self.updated_at_ms = now_ms();
        self.finished_at_ms = Some(self.updated_at_ms);
        true
    }
}

/// Lifecycle of one agent question. Pending is the only nonterminal state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuestionState {
    Pending,
    Answered,
    Declined,
    Cancelled,
    Expired,
}

impl QuestionState {
    pub fn is_terminal(self) -> bool {
        self != Self::Pending
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionOptionKind {
    AllowOnce,
    AllowAlways,
    RejectOnce,
    RejectAlways,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PermissionOption {
    pub id: String,
    pub name: String,
    pub kind: PermissionOptionKind,
}

/// Question payload. Permission decisions and ordinary information requests
/// stay distinct so the model can answer each in its native shape.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QuestionKind {
    Permission {
        title: String,
        summary: Option<String>,
        options: Vec<PermissionOption>,
    },
    Elicitation {
        message: String,
        schema: Value,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Answer {
    Submit { content: Value },
    Decline,
    Cancel,
}

impl Answer {
    pub fn state(&self) -> QuestionState {
        match self {
            Self::Submit { .. } => QuestionState::Answered,
            Self::Decline => QuestionState::Declined,
            Self::Cancel => QuestionState::Cancelled,
        }
    }

    fn permission_option_id(&self) -> Option<&str> {
        match self {
            Self::Submit {
                content: Value::String(id),
            } => Some(id),
            Self::Submit {
                content: Value::Object(content),
            } => content.get("option_id").and_then(Value::as_str),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Question {
    pub id: Uuid,
    pub tool_call_id: Option<Uuid>,
    pub updated_at_ms: u64,
    pub run_id: Uuid,
    pub session_id: Uuid,
    pub acp_session_id: Uuid,
    pub kind: QuestionKind,
    pub state: QuestionState,
    pub answer: Option<Answer>,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    pub resolved_at_ms: Option<u64>,
}

impl Question {
    pub fn validate_answer(&self, answer: &Answer) -> Result<()> {
        if self.state != QuestionState::Pending {
            return Err(Error::State(format!(
                "question is not pending: {:?}",
                self.state
            )));
        }
        if self.expires_at_ms <= now_ms() {
            return Err(Error::State("question has expired".into()));
        }
        match (&self.kind, answer) {
            (QuestionKind::Permission { options, .. }, Answer::Submit { .. }) => {
                let Some(option_id) = answer.permission_option_id() else {
                    return Err(Error::Config(
                        "permission answer must contain an option id".into(),
                    ));
                };
                if !options.iter().any(|option| option.id == option_id) {
                    return Err(Error::Config("permission option was not advertised".into()));
                }
            }
            (QuestionKind::Elicitation { .. }, Answer::Submit { content }) => {
                validate_elicitation_content(content)?;
            }
            (QuestionKind::Permission { .. }, Answer::Decline | Answer::Cancel) => {}
            (QuestionKind::Elicitation { .. }, Answer::Decline | Answer::Cancel) => {}
        }
        Ok(())
    }

    pub(crate) fn resolve(&mut self, state: QuestionState, answer: Option<Answer>) -> bool {
        if self.state != QuestionState::Pending {
            return false;
        }
        self.state = state;
        self.answer = answer;
        self.updated_at_ms = now_ms();
        self.resolved_at_ms = Some(self.updated_at_ms);
        true
    }
}

fn validate_elicitation_content(content: &Value) -> Result<()> {
    match content {
        Value::Null => Ok(()),
        Value::Object(values) => {
            if values.len() > 64 {
                return Err(Error::Config(
                    "elicitation answer has too many fields".into(),
                ));
            }
            for (key, value) in values {
                if key.len() > 128 {
                    return Err(Error::Config(
                        "elicitation answer field name is too long".into(),
                    ));
                }
                if !is_elicitation_value(value) {
                    return Err(Error::Config("unsupported elicitation answer value".into()));
                }
            }
            if serde_json::to_vec(content)?.len() > MAX_ANSWER_BYTES {
                return Err(Error::Config("elicitation answer exceeds limit".into()));
            }
            Ok(())
        }
        _ => Err(Error::Config(
            "elicitation answer must be a JSON object".into(),
        )),
    }
}

fn is_elicitation_value(value: &Value) -> bool {
    match value {
        Value::String(_) | Value::Bool(_) | Value::Number(_) => true,
        Value::Array(items) => items.iter().all(Value::is_string),
        _ => false,
    }
}

/// A new question proposed by the executor. SessionManager adds binding and time.
#[derive(Clone, Debug)]
pub struct QuestionDraft {
    pub id: Uuid,
    pub source_tool_call_id: Option<String>,
    pub run_id: Uuid,
    pub kind: QuestionKind,
}

/// Read-only ACP work status for the host and the main-model status tool.
#[derive(Clone, Debug, Serialize)]
pub struct AcpStatus {
    pub session: AcpSession,
    pub runs: Vec<Run>,
    pub pending_questions: Vec<Question>,
}

/// Complete business messages for host organization, plus transient runtime notifications.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SessionEvent {
    SessionChanged(super::Session),
    MessageRecorded(super::Message),
    ToolCallRecorded(super::ToolCall),
    QuestionChanged(Question),
    AcpSessionChanged(AcpSession),
    RunChanged {
        session_id: Uuid,
        run: Run,
    },
    Progress {
        session_id: Uuid,
        run_id: Uuid,
        update: Value,
    },
    QuestionAsked {
        session_id: Uuid,
        question: Question,
    },
    QuestionResolved {
        session_id: Uuid,
        run_id: Uuid,
        question_id: Uuid,
        state: QuestionState,
    },
    Error {
        session_id: Option<Uuid>,
        acp_session_id: Option<Uuid>,
        run_id: Option<Uuid>,
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn question(kind: QuestionKind) -> Question {
        Question {
            id: Uuid::new_v4(),
            tool_call_id: None,
            updated_at_ms: now_ms(),
            run_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            acp_session_id: Uuid::new_v4(),
            kind,
            state: QuestionState::Pending,
            answer: None,
            created_at_ms: now_ms(),
            expires_at_ms: now_ms() + 60_000,
            resolved_at_ms: None,
        }
    }

    #[test]
    fn permission_answers_must_use_advertised_options() {
        let question = question(QuestionKind::Permission {
            title: "Run shell".into(),
            summary: None,
            options: vec![PermissionOption {
                id: "allow".into(),
                name: "Allow".into(),
                kind: PermissionOptionKind::AllowOnce,
            }],
        });
        assert!(
            question
                .validate_answer(&Answer::Submit {
                    content: Value::String("deny".into())
                })
                .is_err()
        );
        assert!(
            question
                .validate_answer(&Answer::Submit {
                    content: Value::String("allow".into())
                })
                .is_ok()
        );
        assert!(question.validate_answer(&Answer::Cancel).is_ok());
    }

    #[test]
    fn elicitation_answers_are_flat_objects() {
        let question = question(QuestionKind::Elicitation {
            message: "Name".into(),
            schema: Value::Null,
        });
        assert!(
            question
                .validate_answer(&Answer::Submit {
                    content: serde_json::json!({"name": "Ada", "tags": ["one"]})
                })
                .is_ok()
        );
        assert!(
            question
                .validate_answer(&Answer::Submit {
                    content: serde_json::json!({"nested": {"x": 1}})
                })
                .is_err()
        );
    }

    #[test]
    fn runs_ignore_late_terminal_updates() {
        let mut run = Run {
            id: Uuid::new_v4(),
            acp_session_id: Uuid::new_v4(),
            request_key: "key".into(),
            session_id: Uuid::new_v4(),
            input_message_id: Uuid::new_v4(),
            updated_at_ms: now_ms(),
            state: RunState::Running,
            summary: String::new(),
            error: None,
            stop_reason: None,
            created_at_ms: now_ms(),
            started_at_ms: Some(now_ms()),
            finished_at_ms: None,
        };
        assert!(run.finish(RunOutcome {
            state: RunState::Completed,
            summary: "done".into(),
            stop_reason: Some("end_turn".into()),
            error: None,
        }));
        assert!(!run.interrupt("late".into()));
        assert_eq!(run.state, RunState::Completed);
    }
}
