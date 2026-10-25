//! Execution port implemented by ACP or a host-supplied executor.
//!
//! One executor invocation owns one ACP session and many prompt rounds. SessionManager
//! owns scheduling, runtime state and question deadlines; the executor owns protocol
//! framing, cancellation acknowledgement and process cleanup.

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::domain::{AcpSession, Answer, QuestionDraft, QuestionState, RunOutcome};
use super::domain::{MessageRole, ToolCallState};
use crate::Result;

#[derive(Clone, Debug)]
pub enum ExecutionInput {
    /// Start the next round on the already-open ACP session.
    Prompt { run_id: Uuid, prompt: String },
    /// Answer one agent request on its original JSON-RPC request.
    Answer { question_id: Uuid, answer: Answer },
    /// Cancel the round, never the session.
    CancelRun { run_id: Uuid },
    /// Close the session after confirming an in-flight round is stopped.
    Close,
}

#[derive(Clone, Debug)]
pub enum ExecutionEvent {
    /// Remote cleanup failed; the connection is draining before process cleanup.
    CloseFailed {
        error: String,
    },
    Opened {
        remote_session_id: String,
    },
    Update {
        run_id: Uuid,
        update: Value,
    },
    Message {
        run_id: Option<Uuid>,
        source_message_id: Option<String>,
        role: MessageRole,
        content: Value,
    },
    ToolCall {
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
    },
    RunFinished {
        run_id: Uuid,
        outcome: RunOutcome,
    },
    QuestionAsked {
        question: QuestionDraft,
    },
    QuestionResolved {
        question_id: Uuid,
        state: QuestionState,
    },
}

/// Implementations must keep the ACP connection alive across `Prompt` inputs,
/// answer questions through their original request handles, and clean up their
/// owned session on completion. Shared processes belong to the executor and
/// survive other managers. Ok means cleanup was confirmed. Dropping the future
/// initiates cleanup without affecting other sessions. Once a prompt is sent,
/// `Cancelled` requires agent confirmation; an unsent prompt can be cancelled locally.
#[async_trait]
pub trait Executor: Send + Sync {
    fn agent_id(&self) -> &str;
    /// Program shutdown only. Stop accepting work and await all owned processes.
    /// SessionManager shutdown deliberately does not call this method.
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn run_session(
        &self,
        session: AcpSession,
        inputs: mpsc::UnboundedReceiver<ExecutionInput>,
        events: mpsc::Sender<ExecutionEvent>,
        cancel: CancellationToken,
    ) -> Result<()>;
}
