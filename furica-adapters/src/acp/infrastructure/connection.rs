//! Executor-local process ownership, admission, and session routing.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Notify, watch};

pub(super) type Routes = Arc<Mutex<HashMap<String, Arc<Route>>>>;
pub(super) struct Route {
    pub events: mpsc::Sender<ExecutionEvent>,
    pub active: ActiveState,
    pub waiters: Waiters,
    pub questions: QuestionChanges,
    pub cancel: CancellationToken,
    pub close_failed: AtomicBool,
}
fn lookup(routes: &Routes, id: &str) -> Option<Arc<Route>> {
    routes.lock().ok()?.get(id).cloned()
}
struct Admission {
    leases: usize,
    retiring: bool,
}
pub(super) struct Runtime {
    admission: Mutex<Admission>,
    changed: Notify,
    requests: mpsc::UnboundedSender<SessionJob>,
    finished: watch::Receiver<Option<std::result::Result<(), String>>>,
    pub prompt_gate: tokio::sync::Mutex<()>,
    shared: AtomicBool,
}
impl Runtime {
    fn reserve(self: &Arc<Self>) -> Option<Lease> {
        let mut state = self.admission.lock().ok()?;
        if state.retiring
            || self.finished.borrow().is_some()
            || !self.shared.load(Ordering::Acquire)
        {
            return None;
        }
        state.leases += 1;
        self.changed.notify_one();
        Some(Lease(self.clone()))
    }
    pub fn supports_close(&self) -> bool {
        self.shared.load(Ordering::Acquire)
    }
    pub fn retire(&self) {
        if let Ok(mut state) = self.admission.lock() {
            state.retiring = true;
        }
        self.changed.notify_one();
    }
    async fn stopped(&self) -> Result<()> {
        let mut finished = self.finished.clone();
        loop {
            if let Some(result) = finished.borrow().clone() {
                return result.map_err(Error::Protocol);
            }
            finished.changed().await.map_err(|_| Error::Closed)?;
        }
    }
}
struct Lease(Arc<Runtime>);
impl Drop for Lease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.admission.lock() {
            state.leases -= 1;
        }
        self.0.changed.notify_one();
    }
}
struct SessionJob {
    session: AcpSession,
    inputs: mpsc::UnboundedReceiver<ExecutionInput>,
    events: mpsc::Sender<ExecutionEvent>,
    cancel: CancellationToken,
    reply: oneshot::Sender<(Result<()>, bool)>,
    lease: Lease,
}

impl Drop for AcpExecutor {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}
#[async_trait]
impl Executor for AcpExecutor {
    fn agent_id(&self) -> &str {
        &self.config.agent_id
    }
    async fn shutdown(&self) -> Result<()> {
        self.shutdown.cancel();
        let connections = self.connections.lock().await.clone();
        let mut failure = None;
        for runtime in connections {
            if let Err(error) = runtime.stopped().await {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
    async fn run_session(
        &self,
        session: AcpSession,
        inputs: mpsc::UnboundedReceiver<ExecutionInput>,
        events: mpsc::Sender<ExecutionEvent>,
        cancel: CancellationToken,
    ) -> Result<()> {
        let local_cancel = cancel.child_token();
        let _cleanup_on_drop = local_cancel.clone().drop_guard();
        let (runtime, lease) = {
            // Serialize discovery and initialization. Only this executor instance shares it.
            let mut connections = self.connections.lock().await;
            if self.shutdown.is_cancelled() {
                return Err(Error::Closed);
            }
            connections.retain(|r| r.finished.borrow().is_none());
            if let Some((runtime, lease)) = connections
                .iter()
                .find_map(|r| r.reserve().map(|lease| (r.clone(), lease)))
            {
                (runtime, lease)
            } else {
                let (requests, receiver) = mpsc::unbounded_channel();
                let (finished, finished_rx) = watch::channel(None);
                let runtime = Arc::new(Runtime {
                    admission: Mutex::new(Admission {
                        leases: 1,
                        retiring: false,
                    }),
                    changed: Notify::new(),
                    requests,
                    finished: finished_rx,
                    prompt_gate: tokio::sync::Mutex::new(()),
                    shared: AtomicBool::new(false),
                });
                let lease = Lease(runtime.clone());
                let (ready, ready_rx) = oneshot::channel();
                let worker = runtime.clone();
                let config = self.config.clone();
                let stop = self.shutdown.child_token();
                connections.push(runtime.clone());
                tokio::spawn(async move {
                    let result = serve(worker.clone(), config, receiver, ready, stop).await;
                    worker.retire();
                    finished.send_replace(Some(result.map_err(|e| e.to_string())));
                });
                // A cancelled caller must not strand the reservation while initializing.
                tokio::select! {
                    ready = ready_rx => {
                        if ready.is_err() {
                            runtime.stopped().await?;
                            return Err(Error::Protocol("ACP initialization ended before ready".into()));
                        }
                    },
                    _ = local_cancel.cancelled() => return Err(Error::Closed),
                }
                (runtime, lease)
            }
        };
        let (reply, response) = oneshot::channel();
        runtime
            .requests
            .send(SessionJob {
                session,
                inputs,
                events,
                cancel: local_cancel,
                reply,
                lease,
            })
            .map_err(|_| Error::Closed)?;
        match response.await {
            Ok((result, wait_exit)) => {
                if wait_exit {
                    runtime.stopped().await?;
                }
                result
            }
            Err(_) => {
                runtime.stopped().await?;
                Err(Error::Protocol(
                    "ACP connection ended while session was open".into(),
                ))
            }
        }
    }
}

async fn serve(
    runtime: Arc<Runtime>,
    config: AcpConfig,
    mut requests: mpsc::UnboundedReceiver<SessionJob>,
    ready: oneshot::Sender<()>,
    stop: CancellationToken,
) -> Result<()> {
    let routes: Routes = Arc::new(Mutex::new(HashMap::new()));
    let notification_routes = routes.clone();
    let permission_routes = routes.clone();
    let elicitation_routes = routes.clone();
    let transport = AcpAgent::new(AcpAgentConfig::new(&config.command).args(config.args.clone()));
    let result = agent_client_protocol::Client
            .builder()
            .name("furica-core")
            .on_receive_notification(
                async move |notification: SessionNotification, _cx| {
                    let Some(route) = lookup(&notification_routes, &notification.session_id.to_string()) else { return Ok(()); };
                    let notification_events = &route.events;
                    let notification_active = &route.active;
                    let value=serde_json::to_value(&notification.update).map_err(|e|protocol_error(e.to_string()))?;
                    if serde_json::to_vec(&value).map_err(|e|protocol_error(e.to_string()))?.len()>MAX_UPDATE_BYTES {return Err(protocol_error("ACP update exceeds limit"));}
                    let run_id={
                        let mut active=notification_active.lock().map_err(|_|protocol_error("active run lock poisoned"))?;
                        if let Some(current)=active.as_mut()
                            && let SessionUpdate::AgentMessageChunk(chunk)=&notification.update
                            && let ContentBlock::Text(text)=&chunk.content {
                            let remaining=MAX_SUMMARY_CHARS.saturating_sub(current.summary.chars().count());
                            current.summary.extend(text.text.chars().take(remaining));
                        }
                        active.as_ref().map(|run|run.id)
                    };
                    if let Some(event)=normalize_update(run_id,&value){notification_events.send(event).await.map_err(|_|protocol_error("event receiver closed"))?;}
                    if let Some(run_id)=run_id {
                        notification_events.send(ExecutionEvent::Update{run_id,update:value}).await.map_err(|_|protocol_error("progress receiver closed"))?;
                    }
                    Ok(())
                },
                agent_client_protocol::on_receive_notification!(),
            )
            .on_receive_request(
                async move |request: RequestPermissionRequest, responder, _cx| {
                    let Some(route) = lookup(&permission_routes, &request.session_id.to_string()) else {
                        return responder.respond(RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled));
                    };
                    let permission_active = &route.active;
                    let permission_events = &route.events;
                    let permission_waiters = &route.waiters;
                    let permission_questions = &route.questions;
                    let permission_cancel = &route.cancel;
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
                    let tool=serde_json::to_value(&request.tool_call).map_err(|e|protocol_error(e.to_string()))?;
                    if let Some(event)=normalize_tool(Some(run_id),&tool){permission_events.send(event).await.map_err(|_|protocol_error("event receiver closed"))?;}
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
                    question_changed(permission_questions);
                    if permission_events
                        .send(ExecutionEvent::QuestionAsked { question: draft })
                        .await
                        .is_err()
                    {
                        if let Ok(mut waiters) = permission_waiters.lock() {
                            waiters.remove(&question_id);
                        }
                        question_changed(permission_questions);
                        return responder.respond(RequestPermissionResponse::new(
                            RequestPermissionOutcome::Cancelled,
                        ));
                    }
                    let answer = tokio::select! {
                        answer = receiver => answer.unwrap_or(Answer::Cancel),
                        _ = permission_cancel.cancelled() => Answer::Cancel,
                    };
                    if let Ok(mut waiters) = permission_waiters.lock() {
                        waiters.remove(&question_id);
                    }
                    question_changed(permission_questions);
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
                    let id = match &request.mode {
                        ElicitationMode::Form(form) => match &form.scope {
                            ElicitationScope::Session(scope) => Some(scope.session_id.to_string()),
                            _ => None,
                        },
                        _ => None,
                    };
                    let Some(route) = id.and_then(|id| lookup(&elicitation_routes, &id)) else {
                        return responder.respond(CreateElicitationResponse::new(ElicitationAction::Cancel));
                    };
                    let elicitation_active = &route.active;
                    let elicitation_events = &route.events;
                    let elicitation_waiters = &route.waiters;
                    let elicitation_questions = &route.questions;
                    let elicitation_cancel = &route.cancel;
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
                    question_changed(elicitation_questions);
                    if elicitation_events
                        .send(ExecutionEvent::QuestionAsked { question: draft })
                        .await
                        .is_err()
                    {
                        if let Ok(mut waiters) = elicitation_waiters.lock() {
                            waiters.remove(&question_id);
                        }
                        question_changed(elicitation_questions);
                        return responder.respond(CreateElicitationResponse::new(
                            ElicitationAction::Cancel,
                        ));
                    }
                    let answer = tokio::select! {
                        answer = receiver => answer.unwrap_or(Answer::Cancel),
                        _ = elicitation_cancel.cancelled() => Answer::Cancel,
                    };
                    if let Ok(mut waiters) = elicitation_waiters.lock() {
                        waiters.remove(&question_id);
                    }
                    question_changed(elicitation_questions);
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
            // The SDK defers unhandled session requests for dynamic session
            // handlers. This executor installs its handlers up front, so reject
            // unsupported methods immediately rather than leaving them queued.
            .on_receive_request(
                async |request: UntypedMessage, responder, _cx| {
                    responder.respond_with_error(
                        agent_client_protocol::Error::method_not_found().data(request.method),
                    )
                },
                agent_client_protocol::on_receive_request!(),
            )

        .connect_with(transport, async move |connection: ConnectionTo<Agent>| {
                let capabilities = ClientCapabilities::default().elicitation(
                    ElicitationCapabilities::new()
                        .form(ElicitationFormCapabilities::new()),
                );
                let initialize = InitializeRequest::new(ProtocolVersion::V1)
                    .client_capabilities(capabilities)
                    .client_info(Implementation::new("furica-core", env!("CARGO_PKG_VERSION")));
                let init = tokio::select! {
                    biased;
                    _ = stop.cancelled() => return Ok(()),
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
                    let advertised = init.auth_methods.iter().find(|m| m.id().to_string() == *method)
                        .ok_or_else(|| protocol_error("configured auth method was not advertised"))?;
                    if !matches!(advertised, AuthMethod::Agent(_)) {
                        return Err(protocol_error("configured auth method requires unsupported terminal authentication"));
                    }
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => return Ok(()),
                        result = tokio::time::timeout(
                            config.handshake_timeout,
                            connection.send_request(AuthenticateRequest::new(method.clone())).block_task(),
                        ) => {
                            result.map_err(|_| protocol_error("ACP authentication timed out"))??;
                        }
                    }
                }
                let supports_close = init.agent_capabilities.session_capabilities.close.is_some();

            runtime.shared.store(supports_close, Ordering::Release);
            let _ = ready.send(());
            let mut sessions = FuturesUnordered::new();
            let mut idle_since = None;
            loop {
                let (empty, retiring) = {
                    let state = runtime.admission.lock().map_err(|_| protocol_error("admission lock poisoned"))?;
                    (state.leases == 0, state.retiring)
                };
                if empty && (retiring || !supports_close) { return Ok(()); }
                if empty { idle_since.get_or_insert_with(tokio::time::Instant::now); }
                else { idle_since = None; }
                let idle = async {
                    match idle_since {
                        Some(since) => tokio::time::sleep_until(since + config.idle_timeout).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => return Ok(()),
                    job = requests.recv() => {
                        let Some(job) = job else { return Ok(()); };
                        let connection = connection.clone();
                        let runtime = runtime.clone();
                        let routes = routes.clone();
                        let config = config.clone();
                        sessions.push(async move {
                            let (questions, _) = watch::channel(0);
                            let route = Arc::new(Route {
                                events: job.events, active: Arc::new(Mutex::new(None)),
                                waiters: Arc::new(Mutex::new(HashMap::new())), questions,
                                cancel: job.cancel, close_failed: AtomicBool::new(false),
                            });
                            let _cancel_handlers = route.cancel.clone().drop_guard();
                            let result = session::drive(connection, runtime.clone(), routes, route.clone(), config, job.session, job.inputs).await;
                            route.cancel.cancel();
                            cancel_all_waiters(&route.waiters, &route.questions);
                            let failed_close = route.close_failed.load(Ordering::Acquire);
                            // Unknown remote state is never reused. Other live sessions may finish.
                            if result.is_err() { runtime.retire(); }
                            let wait_exit = !supports_close || failed_close;
                            let result = if failed_close { Ok(()) } else { result.map_err(|e| Error::Protocol(e.to_string())) };
                            drop(job.lease);
                            let _ = job.reply.send((result, wait_exit));
                        });
                    }
                    _ = sessions.next(), if !sessions.is_empty() => {},
                    _ = runtime.changed.notified() => {},
                    _ = idle => {
                        let mut state = runtime.admission.lock().map_err(|_| protocol_error("admission lock poisoned"))?;
                        if state.leases == 0 {
                            // Same lock as reservation: a retiring connection cannot be assigned.
                            state.retiring = true;
                            return Ok(());
                        }
                    }
                }
            }
        }).await;
    result.map_err(|e| Error::Protocol(e.to_string()))
}
