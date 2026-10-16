//! Task lifecycle rules and durable snapshots, independent of execution and storage.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::Result;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Queued,
    Running,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

impl TaskState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: Uuid,
    pub request_key: String,
    pub prompt: String,
    pub workspace: PathBuf,
    pub state: TaskState,
    pub session_id: Option<String>,
    pub summary: String,
    pub error: Option<String>,
    pub stop_reason: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug)]
pub enum TaskEvent {
    Changed(TaskRecord),
    Progress { task_id: Uuid, update: Value },
    Error { task_id: Uuid, message: String },
}

pub struct TaskOutcome {
    pub state: TaskState,
    pub summary: String,
    pub stop_reason: String,
}

impl TaskRecord {
    pub(super) fn request_cancellation(&mut self) -> bool {
        if self.state.is_terminal() {
            return false;
        }
        self.state = TaskState::Cancelling;
        true
    }

    pub(super) fn mark_running(&mut self) {
        if self.state == TaskState::Queued {
            self.state = TaskState::Running;
        }
    }

    pub(super) fn interrupt(&mut self, error: String) {
        self.state = TaskState::Interrupted;
        self.error = Some(error);
    }

    pub(super) fn recover(&mut self) {
        self.interrupt("core restarted; execution effects require reconciliation".into());
    }

    pub(super) fn finish(&mut self, outcome: Result<TaskOutcome>) {
        match outcome {
            Ok(outcome) if outcome.state.is_terminal() => {
                self.state = outcome.state;
                self.summary = outcome.summary.chars().take(16_384).collect();
                self.stop_reason = Some(outcome.stop_reason);
            }
            Ok(_) => self.interrupt("executor returned a nonterminal outcome".into()),
            Err(error) => self.interrupt(error.to_string()),
        }
    }
}
