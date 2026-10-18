//! ACP v1 client backed by the official SDK's owned stdio transport.
//!
//! One `run_session` invocation owns one agent process and one ACP session. Prompt
//! rounds are serial, but the same remote session is reused until TaskManager sends
//! `Close` or the idle timeout releases it.

use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_client_protocol::schema::{
    ProtocolVersion,
    v1::{
        AuthenticateRequest, CancelNotification, ClientCapabilities, CloseSessionRequest,
        ContentBlock, CreateElicitationRequest, CreateElicitationResponse, ElicitationAcceptAction,
        ElicitationAction, ElicitationCapabilities, ElicitationContentValue,
        ElicitationFormCapabilities, ElicitationMode, ElicitationScope, InitializeRequest,
        NewSessionRequest, PermissionOptionKind as AcpPermissionOptionKind, PromptRequest,
        PromptResponse, RequestPermissionOutcome, RequestPermissionRequest,
        RequestPermissionResponse, SelectedPermissionOutcome, SessionId, SessionNotification,
        SessionUpdate, StopReason, TextContent,
    },
};
use agent_client_protocol::{AcpAgent, AcpAgentConfig, Agent, ConnectionTo};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    Error, Result,
    tasks::{
        AcpSessionRecord, ExecutionEvent, PermissionOption, PermissionOptionKind, QuestionState,
        RunOutcome, SessionInput, TaskAnswer, TaskExecutor, TaskQuestionDraft, TaskQuestionKind,
        TaskRunState,
    },
};

const MAX_UPDATE_BYTES: usize = 64 * 1024;
const MAX_SUMMARY_CHARS: usize = 16_384;
const MAX_SUMMARY_SOURCE_CHARS: usize = 4_096;
const MAX_QUESTION_TEXT_CHARS: usize = 8_192;
const MAX_QUESTION_OPTIONS: usize = 32;

#[derive(Clone, Debug)]
pub struct AcpConfig {
    pub command: PathBuf,
    pub args: Vec<String>,
    pub auth_method: Option<String>,
    pub handshake_timeout: Duration,
    /// Maximum duration of one prompt round.
    pub run_timeout: Duration,
    /// Time allowed for the agent to confirm cancellation or session close.
    pub cancel_timeout: Duration,
    /// Idle sessions are released after this period without a round.
    pub idle_timeout: Duration,
}

impl AcpConfig {
    pub fn new(command: impl Into<PathBuf>) -> Self {
        Self {
            command: command.into(),
            args: Vec::new(),
            auth_method: None,
            handshake_timeout: Duration::from_secs(15),
            run_timeout: Duration::from_secs(600),
            cancel_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(1_800),
        }
    }
}

pub struct AcpExecutor {
    config: AcpConfig,
}

impl AcpExecutor {
    pub fn new(config: AcpConfig) -> Result<Self> {
        if config.command.as_os_str().is_empty()
            || config.handshake_timeout.is_zero()
            || config.run_timeout.is_zero()
            || config.cancel_timeout.is_zero()
            || config.idle_timeout.is_zero()
        {
            return Err(Error::Config("invalid ACP process or timeout".into()));
        }
        Ok(Self { config })
    }
}

struct ActiveRun {
    id: Uuid,
    summary: String,
}

struct QuestionWaiter {
    run_id: Uuid,
    sender: oneshot::Sender<TaskAnswer>,
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

fn permission_draft(request: &RequestPermissionRequest, run_id: Uuid) -> Option<TaskQuestionDraft> {
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
    Some(TaskQuestionDraft {
        id: Uuid::new_v4(),
        run_id,
        kind: TaskQuestionKind::Permission {
            title: truncate(&title, MAX_QUESTION_TEXT_CHARS),
            summary,
            options,
        },
    })
}

fn elicitation_draft(
    request: &CreateElicitationRequest,
    run_id: Uuid,
) -> Option<TaskQuestionDraft> {
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
    Some(TaskQuestionDraft {
        id: Uuid::new_v4(),
        run_id,
        kind: TaskQuestionKind::Elicitation {
            message: truncate(&request.message, MAX_QUESTION_TEXT_CHARS),
            schema,
        },
    })
}

fn permission_response(
    request: &RequestPermissionRequest,
    answer: &TaskAnswer,
) -> (RequestPermissionOutcome, QuestionState) {
    match answer {
        TaskAnswer::Submit { content } => {
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
        TaskAnswer::Decline => (RequestPermissionOutcome::Cancelled, QuestionState::Declined),
        TaskAnswer::Cancel => (
            RequestPermissionOutcome::Cancelled,
            QuestionState::Cancelled,
        ),
    }
}

fn elicitation_response(answer: &TaskAnswer) -> (CreateElicitationResponse, QuestionState) {
    match answer {
        TaskAnswer::Submit { content } => match elicitation_content(content) {
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
        TaskAnswer::Decline => (
            CreateElicitationResponse::new(ElicitationAction::Decline),
            QuestionState::Declined,
        ),
        TaskAnswer::Cancel => (
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
    answer: TaskAnswer,
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
        let _ = waiter.sender.send(TaskAnswer::Cancel);
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
        let _ = waiter.sender.send(TaskAnswer::Cancel);
    }
}

fn response_outcome(response: PromptResponse, summary: String, timed_out: bool) -> RunOutcome {
    let stop_reason = serde_json::to_value(response.stop_reason)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned));
    let (state, error) = match response.stop_reason {
        StopReason::EndTurn => (TaskRunState::Completed, None),
        StopReason::Cancelled if timed_out => {
            (TaskRunState::Failed, Some("run timed out".to_owned()))
        }
        StopReason::Cancelled => (TaskRunState::Cancelled, None),
        StopReason::Refusal => (
            TaskRunState::Failed,
            Some("agent refused to continue".to_owned()),
        ),
        StopReason::MaxTokens => (
            TaskRunState::Failed,
            Some("agent stopped at max_tokens".to_owned()),
        ),
        StopReason::MaxTurnRequests => (
            TaskRunState::Failed,
            Some("agent stopped at max_turn_requests".to_owned()),
        ),
        _ => (
            TaskRunState::Failed,
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
        state: TaskRunState::Interrupted,
        summary,
        stop_reason: None,
        error: Some(error),
    }
}

async fn close_remote_session(
    connection: &ConnectionTo<Agent>,
    session_id: &SessionId,
    timeout: Duration,
) -> Result<()> {
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
    inputs: &mut mpsc::UnboundedReceiver<SessionInput>,
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
                Some(SessionInput::Answer { question_id, answer }) => {
                    deliver_answer(waiters, question_id, answer, changes);
                }
                Some(SessionInput::CancelRun { run_id: target }) if target == run_id => {
                    send_cancel(connection, session_id, run_id, waiters, changes)?;
                    let result = await_cancel(request.as_mut(), config.cancel_timeout).await;
                    return Ok(PromptEnd::CancelRun(result));
                }
                Some(SessionInput::CancelRun { .. }) => {}
                Some(SessionInput::Close) => {
                    send_cancel(connection, session_id, run_id, waiters, changes)?;
                    let result = await_cancel(request.as_mut(), config.cancel_timeout).await;
                    return Ok(PromptEnd::Close(result));
                }
                Some(SessionInput::Prompt { .. }) => {
                    return Err(Error::Protocol("prompt submitted while a run is active".into()));
                }
                None => return Err(Error::Closed),
            },
        }
    }
}

#[async_trait]
impl TaskExecutor for AcpExecutor {
    async fn run_session(
        &self,
        session: AcpSessionRecord,
        mut inputs: mpsc::UnboundedReceiver<SessionInput>,
        events: mpsc::Sender<ExecutionEvent>,
        cancel: CancellationToken,
    ) -> Result<()> {
        let config = self.config.clone();
        let expected_session = Arc::new(Mutex::new(None::<String>));
        let active: ActiveState = Arc::new(Mutex::new(None));
        let waiters: Waiters = Arc::new(Mutex::new(HashMap::new()));
        let (question_tx, mut question_rx) = tokio::sync::watch::channel(0u64);
        let transport =
            AcpAgent::new(AcpAgentConfig::new(&config.command).args(config.args.clone()));

        let notification_events = events.clone();
        let notification_expected = expected_session.clone();
        let notification_active = active.clone();
        let permission_events = events.clone();
        let permission_expected = expected_session.clone();
        let permission_active = active.clone();
        let permission_waiters = waiters.clone();
        let permission_questions = question_tx.clone();
        let permission_cancel = cancel.clone();
        let elicitation_events = events.clone();
        let elicitation_expected = expected_session.clone();
        let elicitation_active = active.clone();
        let elicitation_waiters = waiters.clone();
        let elicitation_questions = question_tx.clone();
        let elicitation_cancel = cancel.clone();

        let result = agent_client_protocol::Client
            .builder()
            .name("furica-core")
            .on_receive_notification(
                async move |notification: SessionNotification, _cx| {
                    let expected = notification_expected
                        .lock()
                        .map_err(|_| protocol_error("session lock poisoned"))?
                        .clone();
                    if expected.as_deref() != Some(&notification.session_id.to_string()) {
                        return Err(protocol_error("update for an unknown session"));
                    }
                    let (run_id, value) = {
                        let mut current = notification_active
                            .lock()
                            .map_err(|_| protocol_error("active run lock poisoned"))?;
                        let Some(current) = current.as_mut() else {
                            return Ok(());
                        };
                        if let SessionUpdate::AgentMessageChunk(chunk) = &notification.update
                            && let ContentBlock::Text(text) = &chunk.content
                        {
                            let remaining =
                                MAX_SUMMARY_CHARS.saturating_sub(current.summary.chars().count());
                            current.summary.extend(text.text.chars().take(remaining));
                        }
                        if matches!(&notification.update, SessionUpdate::AgentThoughtChunk(_)) {
                            return Ok(());
                        }
                        let value = serde_json::to_value(&notification.update)
                            .map_err(|error| protocol_error(error.to_string()))?;
                        if serde_json::to_vec(&value)
                            .map_err(|error| protocol_error(error.to_string()))?
                            .len()
                            > MAX_UPDATE_BYTES
                        {
                            return Err(protocol_error("ACP update exceeds limit"));
                        }
                        (current.id, value)
                    };
                    notification_events
                        .send(ExecutionEvent::Update {
                            run_id,
                            update: value,
                        })
                        .await
                        .map_err(|_| protocol_error("progress receiver closed"))?;
                    Ok(())
                },
                agent_client_protocol::on_receive_notification!(),
            )
            .on_receive_request(
                async move |request: RequestPermissionRequest, responder, _cx| {
                    let expected = permission_expected
                        .lock()
                        .map_err(|_| protocol_error("session lock poisoned"))?
                        .clone();
                    if expected.as_deref() != Some(&request.session_id.to_string()) {
                        return responder.respond(RequestPermissionResponse::new(
                            RequestPermissionOutcome::Cancelled,
                        ));
                    }
                    let run_id = permission_active
                        .lock()
                        .map_err(|_| protocol_error("active run lock poisoned"))?
                        .as_ref()
                        .map(|run| run.id);
                    let Some(run_id) = run_id else {
                        return responder.respond(RequestPermissionResponse::new(
                            RequestPermissionOutcome::Cancelled,
                        ));
                    };
                    let Some(draft) = permission_draft(&request, run_id) else {
                        return responder.respond(RequestPermissionResponse::new(
                            RequestPermissionOutcome::Cancelled,
                        ));
                    };
                    let question_id = draft.id;
                    let (sender, receiver) = oneshot::channel();
                    if permission_waiters
                        .lock()
                        .map_err(|_| protocol_error("question lock poisoned"))?
                        .insert(question_id, QuestionWaiter { run_id, sender })
                        .is_some()
                    {
                        return responder.respond(RequestPermissionResponse::new(
                            RequestPermissionOutcome::Cancelled,
                        ));
                    }
                    question_changed(&permission_questions);
                    if permission_events
                        .send(ExecutionEvent::QuestionAsked { question: draft })
                        .await
                        .is_err()
                    {
                        if let Ok(mut waiters) = permission_waiters.lock() {
                            waiters.remove(&question_id);
                        }
                        question_changed(&permission_questions);
                        return responder.respond(RequestPermissionResponse::new(
                            RequestPermissionOutcome::Cancelled,
                        ));
                    }
                    let answer = tokio::select! {
                        answer = receiver => answer.unwrap_or(TaskAnswer::Cancel),
                        _ = permission_cancel.cancelled() => TaskAnswer::Cancel,
                    };
                    if let Ok(mut waiters) = permission_waiters.lock() {
                        waiters.remove(&question_id);
                    }
                    question_changed(&permission_questions);
                    let (outcome, mut state) = permission_response(&request, &answer);
                    let result = responder.respond(RequestPermissionResponse::new(outcome));
                    if result.is_err() {
                        state = QuestionState::Cancelled;
                    }
                    let _ = permission_events
                        .send(ExecutionEvent::QuestionResolved { question_id, state })
                        .await;
                    result
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |request: CreateElicitationRequest, responder, _cx| {
                    let session_matches = match &request.mode {
                        ElicitationMode::Form(form) => match &form.scope {
                            ElicitationScope::Session(scope) => elicitation_expected
                                .lock()
                                .map(|expected| {
                                    expected.as_deref() == Some(&scope.session_id.to_string())
                                })
                                .unwrap_or(false),
                            _ => false,
                        },
                        _ => false,
                    };
                    if !session_matches {
                        return responder.respond(CreateElicitationResponse::new(
                            ElicitationAction::Cancel,
                        ));
                    }
                    let run_id = elicitation_active
                        .lock()
                        .map_err(|_| protocol_error("active run lock poisoned"))?
                        .as_ref()
                        .map(|run| run.id);
                    let Some(run_id) = run_id else {
                        return responder.respond(CreateElicitationResponse::new(
                            ElicitationAction::Cancel,
                        ));
                    };
                    let Some(draft) = elicitation_draft(&request, run_id) else {
                        return responder.respond(CreateElicitationResponse::new(
                            ElicitationAction::Cancel,
                        ));
                    };
                    let question_id = draft.id;
                    let (sender, receiver) = oneshot::channel();
                    if elicitation_waiters
                        .lock()
                        .map_err(|_| protocol_error("question lock poisoned"))?
                        .insert(question_id, QuestionWaiter { run_id, sender })
                        .is_some()
                    {
                        return responder.respond(CreateElicitationResponse::new(
                            ElicitationAction::Cancel,
                        ));
                    }
                    question_changed(&elicitation_questions);
                    if elicitation_events
                        .send(ExecutionEvent::QuestionAsked { question: draft })
                        .await
                        .is_err()
                    {
                        if let Ok(mut waiters) = elicitation_waiters.lock() {
                            waiters.remove(&question_id);
                        }
                        question_changed(&elicitation_questions);
                        return responder.respond(CreateElicitationResponse::new(
                            ElicitationAction::Cancel,
                        ));
                    }
                    let answer = tokio::select! {
                        answer = receiver => answer.unwrap_or(TaskAnswer::Cancel),
                        _ = elicitation_cancel.cancelled() => TaskAnswer::Cancel,
                    };
                    if let Ok(mut waiters) = elicitation_waiters.lock() {
                        waiters.remove(&question_id);
                    }
                    question_changed(&elicitation_questions);
                    let (response, mut state) = elicitation_response(&answer);
                    let result = responder.respond(response);
                    if result.is_err() {
                        state = QuestionState::Cancelled;
                    }
                    let _ = elicitation_events
                        .send(ExecutionEvent::QuestionResolved { question_id, state })
                        .await;
                    result
                },
                agent_client_protocol::on_receive_request!(),
            )
            .connect_with(transport, async move |connection: ConnectionTo<Agent>| {
                let capabilities = ClientCapabilities::default().elicitation(
                    ElicitationCapabilities::new()
                        .form(ElicitationFormCapabilities::new()),
                );
                let initialize =
                    InitializeRequest::new(ProtocolVersion::V1).client_capabilities(capabilities);
                let init = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Ok(()),
                    result = tokio::time::timeout(
                        config.handshake_timeout,
                        connection.send_request(initialize).block_task(),
                    ) => result
                        .map_err(|_| protocol_error("ACP initialization timed out"))??,
                };
                if init.protocol_version != ProtocolVersion::V1 {
                    return Err(protocol_error(
                        "agent selected unsupported ACP protocol version",
                    ));
                }
                if let Some(method) = &config.auth_method {
                    if !init.auth_methods.iter().any(|m| m.id().to_string() == *method) {
                        return Err(protocol_error(
                            "configured auth method was not advertised",
                        ));
                    }
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Ok(()),
                        result = tokio::time::timeout(
                            config.handshake_timeout,
                            connection.send_request(AuthenticateRequest::new(method.clone())).block_task(),
                        ) => {
                            result.map_err(|_| protocol_error("ACP authentication timed out"))??;
                        }
                    }
                }
                let supports_close = init.agent_capabilities.session_capabilities.close.is_some();
                let opened = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Ok(()),
                    result = tokio::time::timeout(
                        config.handshake_timeout,
                        connection.send_request(NewSessionRequest::new(session.workspace.clone())).block_task(),
                    ) => result
                        .map_err(|_| protocol_error("ACP session creation timed out"))??,
                };
                let remote_session_id = opened.session_id;
                if cancel.is_cancelled() {
                    let _ = close_remote_session(
                        &connection,
                        &remote_session_id,
                        config.cancel_timeout,
                    )
                    .await;
                    return Ok(());
                }
                *expected_session
                    .lock()
                    .map_err(|_| protocol_error("session lock poisoned"))? =
                    Some(remote_session_id.to_string());
                events
                    .send(ExecutionEvent::Opened {
                        remote_session_id: remote_session_id.to_string(),
                    })
                    .await
                    .map_err(|_| protocol_error("event receiver closed"))?;

                loop {
                    if cancel.is_cancelled() {
                        cancel_all_waiters(&waiters, &question_tx);
                        let _ = close_remote_session(
                            &connection,
                            &remote_session_id,
                            config.cancel_timeout,
                        )
                        .await;
                        return Ok(());
                    }
                    let input = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            cancel_all_waiters(&waiters, &question_tx);
                            let _ = close_remote_session(
                                &connection,
                                &remote_session_id,
                                config.cancel_timeout,
                            ).await;
                            return Ok(());
                        }
                        input = inputs.recv() => input,
                        _ = tokio::time::sleep(config.idle_timeout) => {
                            cancel_all_waiters(&waiters, &question_tx);
                            if supports_close {
                                close_remote_session(
                                    &connection,
                                    &remote_session_id,
                                    config.cancel_timeout,
                                ).await.map_err(|error| protocol_error(error.to_string()))?;
                            }
                            return Ok(());
                        }
                    };
                    match input {
                        Some(SessionInput::Prompt { run_id, prompt }) => {
                            *active
                                .lock()
                                .map_err(|_| protocol_error("active run lock poisoned"))? =
                                Some(ActiveRun {
                                    id: run_id,
                                    summary: String::new(),
                                });
                            let end = match run_prompt(
                                &connection,
                                &remote_session_id,
                                run_id,
                                prompt,
                                &mut inputs,
                                &waiters,
                                &question_tx,
                                &mut question_rx,
                                &cancel,
                                &config,
                            )
                            .await
                            {
                                Ok(end) => end,
                                Err(error) => {
                                    *active
                                        .lock()
                                        .map_err(|_| protocol_error("active run lock poisoned"))? =
                                        None;
                                    cancel_all_waiters(&waiters, &question_tx);
                                    return Err(protocol_error(error.to_string()));
                                }
                            };
                            let summary = active
                                .lock()
                                .map_err(|_| protocol_error("active run lock poisoned"))?
                                .take()
                                .map(|run| run.summary)
                                .unwrap_or_default();
                            cancel_all_waiters(&waiters, &question_tx);
                            let (outcome, next) = match end {
                                PromptEnd::Response(response) => (
                                    response_outcome(response, summary, false),
                                    NextAction::Continue,
                                ),
                                PromptEnd::Timeout(Ok(response)) => (
                                    response_outcome(response, summary, true),
                                    NextAction::Continue,
                                ),
                                PromptEnd::Timeout(Err(error)) => (
                                    interrupted_outcome(
                                        summary,
                                        format!("run timeout cancellation unconfirmed: {error}"),
                                    ),
                                    NextAction::Fail(
                                        "run timeout cancellation unconfirmed".into(),
                                    ),
                                ),
                                PromptEnd::CancelRun(Ok(response)) => (
                                    response_outcome(response, summary, false),
                                    NextAction::Continue,
                                ),
                                PromptEnd::CancelRun(Err(error)) => (
                                    interrupted_outcome(
                                        summary,
                                        format!("cancellation unconfirmed: {error}"),
                                    ),
                                    NextAction::Fail("cancellation unconfirmed".into()),
                                ),
                                PromptEnd::Close(Ok(response)) => (
                                    response_outcome(response, summary, false),
                                    NextAction::Close,
                                ),
                                PromptEnd::Close(Err(error)) => (
                                    interrupted_outcome(
                                        summary,
                                        format!("close cancellation unconfirmed: {error}"),
                                    ),
                                    NextAction::Close,
                                ),
                                PromptEnd::SessionCancel(Ok(response)) => (
                                    response_outcome(response, summary, false),
                                    NextAction::Close,
                                ),
                                PromptEnd::SessionCancel(Err(error)) => (
                                    interrupted_outcome(
                                        summary,
                                        format!("shutdown cancellation unconfirmed: {error}"),
                                    ),
                                    NextAction::Close,
                                ),
                            };
                            events
                                .send(ExecutionEvent::RunFinished { run_id, outcome })
                                .await
                                .map_err(|_| protocol_error("event receiver closed"))?;
                            match next {
                                NextAction::Continue => {}
                                NextAction::Close => {
                                    cancel_all_waiters(&waiters, &question_tx);
                                    let close = if supports_close {
                                        close_remote_session(
                                            &connection,
                                            &remote_session_id,
                                            config.cancel_timeout,
                                        )
                                        .await
                                    } else {
                                        Ok(())
                                    };
                                    return close.map_err(|error| protocol_error(error.to_string()));
                                }
                                NextAction::Fail(reason) => {
                                    cancel_all_waiters(&waiters, &question_tx);
                                    return Err(protocol_error(reason));
                                }
                            }
                        }
                        Some(SessionInput::Answer { question_id, answer }) => {
                            deliver_answer(&waiters, question_id, answer, &question_tx);
                        }
                        Some(SessionInput::CancelRun { .. }) => {}
                        Some(SessionInput::Close) => {
                            cancel_all_waiters(&waiters, &question_tx);
                            let close = if supports_close {
                                close_remote_session(
                                    &connection,
                                    &remote_session_id,
                                    config.cancel_timeout,
                                )
                                .await
                            } else {
                                Ok(())
                            };
                            return close.map_err(|error| protocol_error(error.to_string()));
                        }
                        None => {
                            cancel_all_waiters(&waiters, &question_tx);
                            if supports_close {
                                let _ = close_remote_session(
                                    &connection,
                                    &remote_session_id,
                                    config.cancel_timeout,
                                )
                                .await;
                            }
                            return Ok(());
                        }
                    }
                }
            })
            .await;
        result.map_err(|error| Error::Protocol(error.to_string()))
    }
}
