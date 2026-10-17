//! ACP v1 client backed by the official SDK's owned stdio transport.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_client_protocol::schema::{
    ProtocolVersion,
    v1::{
        AuthenticateRequest, CancelNotification, ContentBlock, InitializeRequest,
        NewSessionRequest, PromptRequest, RequestPermissionOutcome, RequestPermissionRequest,
        RequestPermissionResponse, SelectedPermissionOutcome, SessionNotification, SessionUpdate,
        StopReason, TextContent,
    },
};
use agent_client_protocol::{AcpAgent, AcpAgentConfig, Agent, ConnectionTo};
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    Error, Result,
    tasks::{ExecutionEvent, TaskExecutor, TaskOutcome, TaskRecord, TaskState},
};

#[async_trait]
pub trait PermissionPolicy: Send + Sync {
    /// Return one of the advertised option IDs, or None to cancel the request.
    async fn decide(
        &self,
        task_id: Uuid,
        request: RequestPermissionRequest,
        cancel: CancellationToken,
    ) -> Option<String>;
}

pub struct DenyPermissions;

#[async_trait]
impl PermissionPolicy for DenyPermissions {
    async fn decide(
        &self,
        _: Uuid,
        _: RequestPermissionRequest,
        _: CancellationToken,
    ) -> Option<String> {
        None
    }
}

/// An explicit permission request for the host. Dropping it cancels the request.
pub struct PermissionPrompt {
    pub task_id: Uuid,
    pub request: RequestPermissionRequest,
    response: oneshot::Sender<Option<String>>,
}

impl PermissionPrompt {
    pub fn respond(self, option_id: Option<String>) -> Result<()> {
        if option_id.as_ref().is_some_and(|id| {
            !self
                .request
                .options
                .iter()
                .any(|o| o.option_id.to_string() == *id)
        }) {
            return Err(Error::Protocol(
                "permission option was not advertised".into(),
            ));
        }
        self.response.send(option_id).map_err(|_| Error::Closed)
    }
}

pub struct InteractivePermissions {
    requests: mpsc::Sender<PermissionPrompt>,
    timeout: Duration,
}

impl InteractivePermissions {
    pub fn new(
        capacity: usize,
        timeout: Duration,
    ) -> Result<(Self, mpsc::Receiver<PermissionPrompt>)> {
        if capacity == 0 || capacity > 1024 || timeout.is_zero() {
            return Err(Error::Config("invalid permission limits".into()));
        }
        let (requests, rx) = mpsc::channel(capacity);
        Ok((Self { requests, timeout }, rx))
    }
}

#[async_trait]
impl PermissionPolicy for InteractivePermissions {
    async fn decide(
        &self,
        task_id: Uuid,
        request: RequestPermissionRequest,
        cancel: CancellationToken,
    ) -> Option<String> {
        let (response, rx) = oneshot::channel();
        let future = async {
            self.requests
                .send(PermissionPrompt {
                    task_id,
                    request,
                    response,
                })
                .await
                .ok()?;
            rx.await.ok().flatten()
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => None,
            result = tokio::time::timeout(self.timeout, future) => result.ok().flatten(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AcpConfig {
    pub command: PathBuf,
    pub args: Vec<String>,
    pub auth_method: Option<String>,
    pub handshake_timeout: Duration,
    pub cancel_timeout: Duration,
}

impl AcpConfig {
    pub fn new(command: impl Into<PathBuf>) -> Self {
        Self {
            command: command.into(),
            args: Vec::new(),
            auth_method: None,
            handshake_timeout: Duration::from_secs(15),
            cancel_timeout: Duration::from_secs(5),
        }
    }
}

pub struct AcpExecutor {
    config: AcpConfig,
    permissions: Arc<dyn PermissionPolicy>,
}

impl AcpExecutor {
    pub fn new(config: AcpConfig, permissions: Arc<dyn PermissionPolicy>) -> Result<Self> {
        if config.command.as_os_str().is_empty()
            || config.handshake_timeout.is_zero()
            || config.cancel_timeout.is_zero()
        {
            return Err(Error::Config("invalid ACP process or timeout".into()));
        }
        Ok(Self {
            config,
            permissions,
        })
    }
}

fn protocol_error(message: impl Into<String>) -> agent_client_protocol::Error {
    agent_client_protocol::Error::internal_error().data(serde_json::Value::String(message.into()))
}

fn cancelled_before_execution() -> TaskOutcome {
    TaskOutcome {
        state: TaskState::Cancelled,
        summary: String::new(),
        stop_reason: "cancelled_before_execution".into(),
    }
}

#[async_trait]
impl TaskExecutor for AcpExecutor {
    async fn execute(
        &self,
        task: TaskRecord,
        cancel: CancellationToken,
        progress: mpsc::Sender<ExecutionEvent>,
    ) -> Result<TaskOutcome> {
        let summary = Arc::new(Mutex::new(String::new()));
        let expected_session = Arc::new(Mutex::new(None::<String>));
        let summary_out = summary.clone();
        let expected_update = expected_session.clone();
        let expected_permission = expected_session.clone();
        let policy = self.permissions.clone();
        let permission_cancel = cancel.clone();
        let callback_progress = progress.clone();
        let task_id = task.id;
        let config = self.config.clone();
        let transport =
            AcpAgent::new(AcpAgentConfig::new(&config.command).args(config.args.clone()));

        let result = agent_client_protocol::Client.builder()
            .name("furica-core")
            .on_receive_notification(async move |notification: SessionNotification, _cx| {
                let expected = expected_update.lock().map_err(|_| protocol_error("session lock poisoned"))?.clone();
                if expected.as_ref().is_some_and(|id| *id != notification.session_id.to_string()) {
                    return Err(protocol_error("update for an unknown session"));
                }
                if let SessionUpdate::AgentMessageChunk(chunk) = &notification.update
                    && let ContentBlock::Text(text) = &chunk.content {
                        let mut summary = summary_out.lock().map_err(|_| protocol_error("summary lock poisoned"))?;
                        let remaining = 16_384usize.saturating_sub(summary.chars().count());
                        summary.extend(text.text.chars().take(remaining));
                    }
                // Internal thought chunks are deliberately not forwarded.
                if !matches!(notification.update, SessionUpdate::AgentThoughtChunk(_)) {
                    let update = serde_json::to_value(notification.update).map_err(|e| protocol_error(e.to_string()))?;
                    if update.to_string().len() > 64 * 1024 { return Err(protocol_error("ACP update exceeds limit")); }
                    callback_progress.send(ExecutionEvent::Update(update)).await.map_err(|_| protocol_error("progress receiver closed"))?;
                }
                Ok(())
            }, agent_client_protocol::on_receive_notification!())
            .on_receive_request(async move |request: RequestPermissionRequest, responder, _cx| {
                let expected = expected_permission.lock().map_err(|_| protocol_error("session lock poisoned"))?.clone();
                let selected = if expected.as_deref() == Some(&request.session_id.to_string()) {
                    policy.decide(task_id, request.clone(), permission_cancel.clone()).await
                } else { None };
                let outcome = match selected {
                    Some(id) if request.options.iter().any(|o| o.option_id.to_string() == id) =>
                        RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(id)),
                    _ => RequestPermissionOutcome::Cancelled,
                };
                responder.respond(RequestPermissionResponse::new(outcome))
            }, agent_client_protocol::on_receive_request!())
            .connect_with(transport, async move |connection: ConnectionTo<Agent>| {
                let init = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Ok(cancelled_before_execution()),
                    init = tokio::time::timeout(config.handshake_timeout,
                        connection.send_request(InitializeRequest::new(ProtocolVersion::V1)).block_task()) =>
                        init.map_err(|_| protocol_error("ACP initialization timed out"))??,
                };
                if init.protocol_version != ProtocolVersion::V1 {
                    return Err(protocol_error("agent selected unsupported ACP protocol version"));
                }
                if let Some(method) = &config.auth_method {
                    if !init.auth_methods.iter().any(|m| m.id().to_string() == *method) {
                        return Err(protocol_error("configured auth method was not advertised"));
                    }
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Ok(cancelled_before_execution()),
                        result = tokio::time::timeout(config.handshake_timeout,
                            connection.send_request(AuthenticateRequest::new(method.clone())).block_task()) => {
                            result.map_err(|_| protocol_error("ACP authentication timed out"))??;
                        }
                    }
                }
                let session = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Ok(cancelled_before_execution()),
                    result = tokio::time::timeout(config.handshake_timeout,
                        connection.send_request(NewSessionRequest::new(task.workspace)).block_task()) =>
                        result.map_err(|_| protocol_error("ACP session creation timed out"))??,
                };
                *expected_session.lock().map_err(|_| protocol_error("session lock poisoned"))? = Some(session.session_id.to_string());
                progress.send(ExecutionEvent::SessionCreated(session.session_id.to_string())).await
                    .map_err(|_| protocol_error("progress receiver closed"))?;
                if cancel.is_cancelled() { return Ok(cancelled_before_execution()); }
                let prompt = connection.send_request(PromptRequest::new(session.session_id.clone(),
                    vec![ContentBlock::Text(TextContent::new(task.prompt))])).block_task();
                tokio::pin!(prompt);
                let response = tokio::select! {
                    result = &mut prompt => result?,
                    _ = cancel.cancelled() => {
                        connection.send_notification(CancelNotification::new(session.session_id))?;
                        tokio::time::timeout(config.cancel_timeout, &mut prompt).await
                            .map_err(|_| protocol_error("cancellation unconfirmed; execution effects require reconciliation"))??
                    }
                };
                let state = match response.stop_reason {
                    StopReason::EndTurn => TaskState::Completed,
                    StopReason::Cancelled => TaskState::Cancelled,
                    StopReason::Refusal => TaskState::Failed,
                    _ => TaskState::Interrupted,
                };
                let stop_reason = serde_json::to_value(response.stop_reason).map_err(|e| protocol_error(e.to_string()))?
                    .as_str().unwrap_or("unknown").to_owned();
                let summary = summary.lock().map_err(|_| protocol_error("summary lock poisoned"))?.clone();
                Ok(TaskOutcome { state, summary, stop_reason })
            }).await;
        result.map_err(|error| Error::Protocol(error.to_string()))
    }
}
