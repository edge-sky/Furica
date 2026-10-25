//! ACP v1 client backed by the official SDK's owned stdio transport.
//!
//! Executors own reusable connections. Each invocation owns only its task session.

use super::config::AcpConfig;
mod connection;
mod session;
mod updates;
use updates::{normalize_tool, normalize_update};

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_client_protocol::schema::{
    ProtocolVersion,
    v1::{
        AuthMethod, AuthenticateRequest, CancelNotification, ClientCapabilities,
        CloseSessionRequest, ContentBlock, CreateElicitationRequest, CreateElicitationResponse,
        ElicitationAcceptAction, ElicitationAction, ElicitationCapabilities,
        ElicitationContentValue, ElicitationFormCapabilities, ElicitationMode, ElicitationScope,
        Implementation, InitializeRequest, NewSessionRequest,
        PermissionOptionKind as AcpPermissionOptionKind, PromptRequest, PromptResponse,
        RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
        SelectedPermissionOutcome, SessionId, SessionNotification, SessionUpdate, StopReason,
        TextContent,
    },
};
use agent_client_protocol::{AcpAgent, AcpAgentConfig, Agent, ConnectionTo, UntypedMessage};
use async_trait::async_trait;
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use furica_core::{
    Error, Result,
    sessions::{
        AcpSession, Answer, ExecutionEvent, ExecutionInput, Executor, PermissionOption,
        PermissionOptionKind, QuestionDraft, QuestionKind, QuestionState, RunOutcome, RunState,
    },
};

const MAX_UPDATE_BYTES: usize = 64 * 1024;
const MAX_SUMMARY_CHARS: usize = 16_384;
const MAX_SUMMARY_SOURCE_CHARS: usize = 4_096;
const MAX_QUESTION_TEXT_CHARS: usize = 8_192;
const MAX_QUESTION_OPTIONS: usize = 32;

pub struct AcpExecutor {
    config: AcpConfig,
    connections: tokio::sync::Mutex<Vec<Arc<connection::Runtime>>>,
    shutdown: CancellationToken,
}

impl AcpExecutor {
    pub fn new(config: AcpConfig) -> Result<Self> {
        if config.agent_id.trim().is_empty()
            || config.command.as_os_str().is_empty()
            || config.handshake_timeout.is_zero()
            || config.run_timeout.is_zero()
            || config.cancel_timeout.is_zero()
            || config.idle_timeout.is_zero()
        {
            return Err(Error::Config("invalid ACP process or timeout".into()));
        }
        Ok(Self {
            config,
            connections: tokio::sync::Mutex::new(Vec::new()),
            shutdown: CancellationToken::new(),
        })
    }
}

struct ActiveRun {
    id: Uuid,
    summary: String,
}

struct QuestionWaiter {
    run_id: Uuid,
    sender: oneshot::Sender<Answer>,
}

type ActiveState = Arc<Mutex<Option<ActiveRun>>>;
type Waiters = Arc<Mutex<HashMap<Uuid, QuestionWaiter>>>;
type QuestionChanges = tokio::sync::watch::Sender<u64>;

fn question_changed(changes: &QuestionChanges) {
    changes.send_modify(|value| *value = value.wrapping_add(1));
}

enum PromptEnd {
    Response(PromptResponse),
    Timeout(Result<PromptResponse>),
    CancelRun(Result<PromptResponse>),
    Close(Result<PromptResponse>),
    SessionCancel(Result<PromptResponse>),
}

enum NextAction {
    Continue,
    Close,
    Fail(String),
}

fn protocol_error(message: impl Into<String>) -> agent_client_protocol::Error {
    agent_client_protocol::Error::internal_error().data(Value::String(message.into()))
}

fn truncate(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

fn map_permission_kind(kind: AcpPermissionOptionKind) -> PermissionOptionKind {
    match kind {
        AcpPermissionOptionKind::AllowOnce => PermissionOptionKind::AllowOnce,
        AcpPermissionOptionKind::AllowAlways => PermissionOptionKind::AllowAlways,
        AcpPermissionOptionKind::RejectOnce => PermissionOptionKind::RejectOnce,
        AcpPermissionOptionKind::RejectAlways => PermissionOptionKind::RejectAlways,
        _ => PermissionOptionKind::Unknown,
    }
}

fn permission_draft(request: &RequestPermissionRequest, run_id: Uuid) -> Option<QuestionDraft> {
    if request.options.len() > MAX_QUESTION_OPTIONS {
        return None;
    }
    let mut options = Vec::with_capacity(request.options.len());
    for option in &request.options {
        let id = option.option_id.to_string();
        if id.is_empty() || id.len() > 256 {
            return None;
        }
        options.push(PermissionOption {
            id,
            name: truncate(&option.name, 256),
            kind: map_permission_kind(option.kind),
        });
    }
    let title = request
        .tool_call
        .fields
        .title
        .as_deref()
        .or(request.tool_call.fields.name.as_deref())
        .map(str::to_owned)
        .unwrap_or_else(|| request.tool_call.tool_call_id.to_string());
    let summary = request
        .tool_call
        .fields
        .raw_input
        .as_ref()
        .and_then(|value| serde_json::to_string(value).ok())
        .map(|value| truncate(&value, MAX_SUMMARY_SOURCE_CHARS));
    Some(QuestionDraft {
        source_tool_call_id: Some(request.tool_call.tool_call_id.to_string()),
        id: Uuid::new_v4(),
        run_id,
        kind: QuestionKind::Permission {
            title: truncate(&title, MAX_QUESTION_TEXT_CHARS),
            summary,
            options,
        },
    })
}

fn elicitation_draft(request: &CreateElicitationRequest, run_id: Uuid) -> Option<QuestionDraft> {
    let ElicitationMode::Form(form) = &request.mode else {
        return None;
    };
    let ElicitationScope::Session(scope) = &form.scope else {
        return None;
    };
    if scope.session_id.to_string().is_empty() {
        return None;
    }
    let schema = serde_json::to_value(&form.requested_schema).ok()?;
    if serde_json::to_vec(&schema).ok()?.len() > MAX_UPDATE_BYTES {
        return None;
    }
    Some(QuestionDraft {
        source_tool_call_id: None,
        id: Uuid::new_v4(),
        run_id,
        kind: QuestionKind::Elicitation {
            message: truncate(&request.message, MAX_QUESTION_TEXT_CHARS),
            schema,
        },
    })
}

fn permission_response(
    request: &RequestPermissionRequest,
    answer: &Answer,
) -> (RequestPermissionOutcome, QuestionState) {
    match answer {
        Answer::Submit { content } => {
            let option_id = match content {
                Value::String(id) => Some(id.as_str()),
                Value::Object(content) => content.get("option_id").and_then(Value::as_str),
                _ => None,
            };
            if let Some(option_id) = option_id
                && request
                    .options
                    .iter()
                    .any(|option| option.option_id.to_string() == option_id)
            {
                return (
                    RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(
                        option_id.to_owned(),
                    )),
                    QuestionState::Answered,
                );
            }
            (
                RequestPermissionOutcome::Cancelled,
                QuestionState::Cancelled,
            )
        }
        Answer::Decline => (RequestPermissionOutcome::Cancelled, QuestionState::Declined),
        Answer::Cancel => (
            RequestPermissionOutcome::Cancelled,
            QuestionState::Cancelled,
        ),
    }
}

fn elicitation_response(answer: &Answer) -> (CreateElicitationResponse, QuestionState) {
    match answer {
        Answer::Submit { content } => match elicitation_content(content) {
            Some(content) => {
                let mut action = ElicitationAcceptAction::new();
                if let Some(content) = content {
                    action = action.content(content);
                }
                (
                    CreateElicitationResponse::new(ElicitationAction::Accept(action)),
                    QuestionState::Answered,
                )
            }
            None => (
                CreateElicitationResponse::new(ElicitationAction::Cancel),
                QuestionState::Cancelled,
            ),
        },
        Answer::Decline => (
            CreateElicitationResponse::new(ElicitationAction::Decline),
            QuestionState::Declined,
        ),
        Answer::Cancel => (
            CreateElicitationResponse::new(ElicitationAction::Cancel),
            QuestionState::Cancelled,
        ),
    }
}

fn elicitation_content(
    content: &Value,
) -> Option<Option<BTreeMap<String, ElicitationContentValue>>> {
    match content {
        Value::Null => Some(None),
        Value::Object(values) => {
            let mut converted = BTreeMap::new();
            for (key, value) in values {
                converted.insert(key.clone(), elicitation_value(value)?);
            }
            Some(Some(converted))
        }
        _ => None,
    }
}

fn elicitation_value(value: &Value) -> Option<ElicitationContentValue> {
    match value {
        Value::String(value) => Some(ElicitationContentValue::String(value.clone())),
        Value::Bool(value) => Some(ElicitationContentValue::Boolean(*value)),
        Value::Number(value) => value
            .as_i64()
            .map(ElicitationContentValue::Integer)
            .or_else(|| value.as_f64().map(ElicitationContentValue::Number)),
        Value::Array(values) => values
            .iter()
            .map(|value| value.as_str().map(str::to_owned))
            .collect::<Option<Vec<_>>>()
            .map(ElicitationContentValue::StringArray),
        _ => None,
    }
}

fn deliver_answer(
    waiters: &Waiters,
    question_id: Uuid,
    answer: Answer,
    changes: &QuestionChanges,
) -> bool {
    let waiter = waiters
        .lock()
        .ok()
        .and_then(|mut waiters| waiters.remove(&question_id));
    if waiter.is_some() {
        question_changed(changes);
    }
    waiter.is_some_and(|waiter| waiter.sender.send(answer).is_ok())
}

fn cancel_waiters(waiters: &Waiters, run_id: Uuid, changes: &QuestionChanges) {
    let mut removed = Vec::new();
    if let Ok(mut waiters) = waiters.lock() {
        let ids: Vec<Uuid> = waiters
            .iter()
            .filter(|(_, waiter)| waiter.run_id == run_id)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            if let Some(waiter) = waiters.remove(&id) {
                removed.push(waiter);
            }
        }
    }
    if !removed.is_empty() {
        question_changed(changes);
    }
    for waiter in removed {
        let _ = waiter.sender.send(Answer::Cancel);
    }
}

fn cancel_all_waiters(waiters: &Waiters, changes: &QuestionChanges) {
    let removed: Vec<QuestionWaiter> = waiters
        .lock()
        .map(|mut waiters| waiters.drain().map(|(_, waiter)| waiter).collect())
        .unwrap_or_default();
    if !removed.is_empty() {
        question_changed(changes);
    }
    for waiter in removed {
        let _ = waiter.sender.send(Answer::Cancel);
    }
}

fn response_outcome(response: PromptResponse, summary: String, timed_out: bool) -> RunOutcome {
    let stop_reason = serde_json::to_value(response.stop_reason)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned));
    let (state, error) = match response.stop_reason {
        StopReason::EndTurn => (RunState::Completed, None),
        StopReason::Cancelled if timed_out => (RunState::Failed, Some("run timed out".to_owned())),
        StopReason::Cancelled => (RunState::Cancelled, None),
        StopReason::Refusal => (
            RunState::Failed,
            Some("agent refused to continue".to_owned()),
        ),
        StopReason::MaxTokens => (
            RunState::Failed,
            Some("agent stopped at max_tokens".to_owned()),
        ),
        StopReason::MaxTurnRequests => (
            RunState::Failed,
            Some("agent stopped at max_turn_requests".to_owned()),
        ),
        _ => (
            RunState::Failed,
            Some(format!(
                "agent stopped: {}",
                stop_reason.as_deref().unwrap_or("unknown")
            )),
        ),
    };
    RunOutcome {
        state,
        summary,
        stop_reason,
        error,
    }
}

fn interrupted_outcome(summary: String, error: String) -> RunOutcome {
    RunOutcome {
        state: RunState::Interrupted,
        summary,
        stop_reason: None,
        error: Some(error),
    }
}

async fn close_remote_session(
    connection: &ConnectionTo<Agent>,
    session_id: &SessionId,
    supports_close: bool,
    timeout: Duration,
) -> Result<()> {
    if !supports_close {
        return Ok(());
    }
    let request = connection
        .send_request(CloseSessionRequest::new(session_id.clone()))
        .block_task();
    tokio::time::timeout(timeout, request)
        .await
        .map_err(|_| Error::Timeout("ACP session close"))?
        .map_err(|error| Error::Protocol(error.to_string()))?;
    Ok(())
}

async fn await_cancel<F>(
    request: std::pin::Pin<&mut F>,
    timeout: Duration,
) -> Result<PromptResponse>
where
    F: std::future::Future<
            Output = std::result::Result<PromptResponse, agent_client_protocol::Error>,
        >,
{
    tokio::time::timeout(timeout, request)
        .await
        .map_err(|_| Error::Timeout("ACP cancellation"))?
        .map_err(|error| Error::Protocol(error.to_string()))
}

fn send_cancel(
    connection: &ConnectionTo<Agent>,
    session_id: &SessionId,
    run_id: Uuid,
    waiters: &Waiters,
    changes: &QuestionChanges,
) -> Result<()> {
    connection
        .send_notification(CancelNotification::new(session_id.clone()))
        .map_err(|error| Error::Protocol(error.to_string()))?;
    cancel_waiters(waiters, run_id, changes);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_prompt(
    connection: &ConnectionTo<Agent>,
    session_id: &SessionId,
    run_id: Uuid,
    prompt: String,
    inputs: &mut mpsc::UnboundedReceiver<ExecutionInput>,
    waiters: &Waiters,
    changes: &QuestionChanges,
    question_rx: &mut tokio::sync::watch::Receiver<u64>,
    cancel: &CancellationToken,
    config: &AcpConfig,
) -> Result<PromptEnd> {
    let request = connection
        .send_request(PromptRequest::new(
            session_id.clone(),
            vec![ContentBlock::Text(TextContent::new(prompt))],
        ))
        .block_task();
    tokio::pin!(request);
    let mut deadline = tokio::time::Instant::now() + config.run_timeout;
    let mut paused_at: Option<tokio::time::Instant> = None;
    loop {
        // Time spent waiting for questions does not consume the run budget.
        let waiting = waiters
            .lock()
            .map(|waiters| waiters.values().any(|waiter| waiter.run_id == run_id))
            .unwrap_or(false);
        if waiting {
            if paused_at.is_none() {
                paused_at = Some(tokio::time::Instant::now());
            }
        } else if let Some(started) = paused_at.take() {
            deadline += started.elapsed();
        }
        let timeout = async move {
            match paused_at {
                Some(_) => std::future::pending::<()>().await,
                None => tokio::time::sleep_until(deadline).await,
            }
        };
        tokio::select! {
            biased;
            result = &mut request => {
                return Ok(PromptEnd::Response(result.map_err(|error| Error::Protocol(error.to_string()))?));
            }
            _ = cancel.cancelled() => {
                send_cancel(connection, session_id, run_id, waiters, changes)?;
                let result = await_cancel(request.as_mut(), config.cancel_timeout).await;
                return Ok(PromptEnd::SessionCancel(result));
            }
            changed = question_rx.changed() => {
                if changed.is_err() {
                    return Err(Error::Closed);
                }
                continue;
            }
            _ = timeout => {
                send_cancel(connection, session_id, run_id, waiters, changes)?;
                let result = await_cancel(request.as_mut(), config.cancel_timeout).await;
                return Ok(PromptEnd::Timeout(result));
            }
            input = inputs.recv() => match input {
                Some(ExecutionInput::Answer { question_id, answer }) => {
                    deliver_answer(waiters, question_id, answer, changes);
                }
                Some(ExecutionInput::CancelRun { run_id: target }) if target == run_id => {
                    send_cancel(connection, session_id, run_id, waiters, changes)?;
                    let result = await_cancel(request.as_mut(), config.cancel_timeout).await;
                    return Ok(PromptEnd::CancelRun(result));
                }
                Some(ExecutionInput::CancelRun { .. }) => {}
                Some(ExecutionInput::Close) => {
                    send_cancel(connection, session_id, run_id, waiters, changes)?;
                    let result = await_cancel(request.as_mut(), config.cancel_timeout).await;
                    return Ok(PromptEnd::Close(result));
                }
                Some(ExecutionInput::Prompt { .. }) => {
                    return Err(Error::Protocol("prompt submitted while a run is active".into()));
                }
                None => return Err(Error::Closed),
            },
        }
    }
}
