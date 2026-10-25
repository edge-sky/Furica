//! One task's remote session and serial prompt rounds.
use super::connection::{Route, Routes, Runtime};
use super::*;
struct Registration {
    routes: Routes,
    id: String,
}
impl Drop for Registration {
    fn drop(&mut self) {
        if let Ok(mut routes) = self.routes.lock() {
            routes.remove(&self.id);
        }
    }
}

async fn cleanup(
    runtime: &Runtime,
    route: &Route,
    connection: &ConnectionTo<Agent>,
    id: &SessionId,
    supports_close: bool,
    timeout: Duration,
) -> std::result::Result<(), agent_client_protocol::Error> {
    if let Err(error) = close_remote_session(connection, id, supports_close, timeout).await {
        runtime.retire();
        route
            .close_failed
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = route
            .events
            .send(ExecutionEvent::CloseFailed {
                error: error.to_string(),
            })
            .await;
        return Err(protocol_error(error.to_string()));
    }
    Ok(())
}

pub(super) async fn drive(
    connection: ConnectionTo<Agent>,
    runtime: Arc<Runtime>,
    routes: Routes,
    route: Arc<Route>,
    config: AcpConfig,
    session: AcpSession,
    mut inputs: mpsc::UnboundedReceiver<ExecutionInput>,
) -> std::result::Result<(), agent_client_protocol::Error> {
    let supports_close = runtime.supports_close();
    let events = route.events.clone();
    let cancel = route.cancel.clone();
    let active = route.active.clone();
    let waiters = route.waiters.clone();
    let question_tx = route.questions.clone();
    let mut question_rx = question_tx.subscribe();
    if cancel.is_cancelled() {
        return Ok(());
    }
    // Once sent, await session/new so cancellation can close the returned ID.
    // A timeout leaves unknown remote state and retires this connection.
    let opened = tokio::time::timeout(
        config.handshake_timeout,
        connection
            .send_request(NewSessionRequest::new(session.workspace.clone()))
            .block_task(),
    )
    .await
    .map_err(|_| protocol_error("ACP session creation timed out"))??;
    let remote_session_id = opened.session_id;
    let result = async {
        if cancel.is_cancelled() {
            cleanup(
                &runtime, &route, &connection,
                &remote_session_id,
                supports_close,
                config.cancel_timeout,
            )
            .await?;
            return Ok(());
        }
        routes.lock().map_err(|_| protocol_error("route lock poisoned"))?
            .insert(remote_session_id.to_string(), route.clone());
        let _registration = Registration { routes: routes.clone(), id: remote_session_id.to_string() };
        events
            .send(ExecutionEvent::Opened {
                remote_session_id: remote_session_id.to_string(),
            })
            .await
            .map_err(|_| protocol_error("event receiver closed"))?;

        loop {
            if cancel.is_cancelled() {
                cancel_all_waiters(&waiters, &question_tx);
                cleanup(
                    &runtime, &route, &connection,
                    &remote_session_id,
                    supports_close,
                    config.cancel_timeout,
                )
                .await?;
                return Ok(());
            }
            let input = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    cancel_all_waiters(&waiters, &question_tx);
                    cleanup(
                        &runtime, &route, &connection,
                        &remote_session_id,
                        supports_close,
                        config.cancel_timeout,
                    ).await?;
                    return Ok(());
                }
                input = inputs.recv() => input,

            };
            match input {
                Some(ExecutionInput::Prompt { run_id, prompt }) => {
                    let _prompt_guard = tokio::select! {
                        biased;
                        guard = runtime.prompt_gate.lock() => guard,
                        _ = cancel.cancelled() => {
                            cleanup(&runtime, &route, &connection, &remote_session_id, supports_close, config.cancel_timeout).await?;
                            return Ok(());
                        }
                        input = inputs.recv() => {
                            match input {
                                Some(ExecutionInput::CancelRun { run_id: target }) if target == run_id => {
                                    events.send(ExecutionEvent::RunFinished { run_id, outcome: RunOutcome {
                                        state: RunState::Cancelled, summary: String::new(), error: None,
                                        stop_reason: Some("cancelled_before_prompt".into()),
                                    }}).await.map_err(|_| protocol_error("event receiver closed"))?;
                                    continue;
                                }
                                Some(ExecutionInput::Close) | None => {
                                    cleanup(&runtime, &route, &connection, &remote_session_id, supports_close, config.cancel_timeout).await?;
                                    return Ok(());
                                }
                                _ => return Err(protocol_error("unexpected input while waiting for prompt slot")),
                            }
                        }
                    };
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
                            let close = cleanup(
                                &runtime, &route, &connection,
                                &remote_session_id,
                                supports_close,
                                config.cancel_timeout,
                            ).await;
                            return close.map_err(|error| protocol_error(error.to_string()));
                        }
                        NextAction::Fail(reason) => {
                            cancel_all_waiters(&waiters, &question_tx);
                            return Err(protocol_error(reason));
                        }
                    }
                }
                Some(ExecutionInput::Answer { question_id, answer }) => {
                    deliver_answer(&waiters, question_id, answer, &question_tx);
                }
                Some(ExecutionInput::CancelRun { .. }) => {}
                Some(ExecutionInput::Close) => {
                    cancel_all_waiters(&waiters, &question_tx);
                    let close = cleanup(
                        &runtime, &route, &connection,
                        &remote_session_id,
                        supports_close,
                        config.cancel_timeout,
                    ).await;
                    return close.map_err(|error| protocol_error(error.to_string()));
                }
                None => {
                    cancel_all_waiters(&waiters, &question_tx);
                    cleanup(
                        &runtime, &route, &connection,
                        &remote_session_id,
                        supports_close,
                        config.cancel_timeout,
                    ).await?;
                    return Ok(());
                }
            }
        }

        }.await;
    if result.is_err()
        && !route
            .close_failed
            .load(std::sync::atomic::Ordering::Acquire)
    {
        // A round or local consumer failure still owns a remote session.
        cleanup(
            &runtime,
            &route,
            &connection,
            &remote_session_id,
            supports_close,
            config.cancel_timeout,
        )
        .await?;
    }
    result
}
