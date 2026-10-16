//! Execution port implemented by ACP or a host-supplied executor.

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::domain::{TaskOutcome, TaskRecord};
use crate::Result;

pub enum ExecutionEvent {
    SessionCreated(String),
    Update(Value),
}

/// Implementations must cooperate with cancellation and clean up owned resources
/// when the execution future is dropped. Cancelled means confirmed termination.
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    async fn execute(
        &self,
        task: TaskRecord,
        cancel: CancellationToken,
        progress: mpsc::Sender<ExecutionEvent>,
    ) -> Result<TaskOutcome>;
}
