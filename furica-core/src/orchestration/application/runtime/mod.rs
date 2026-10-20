//! One actor owns conversation state. This module assembles and drives its channels.

mod input;
mod playback;
mod session;
mod startup;
use super::tools::CoreTools;

pub use session::{VoiceChannels, VoiceSession};

use super::{Action, CoreEvent, CoreState};

use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use serde_json::{Value, json};
use tokio::{
    sync::{broadcast, mpsc, oneshot, watch},
    task::JoinHandle,
    time::Instant,
};
use uuid::Uuid;

use crate::{
    Error, Result,
    audio::{AudioBuffer, AudioFrame},
    config::CoreConfig,
    input::{InputGate, SpeakerDecision},
    orchestration::{VoiceBackend, VoiceCommand, VoiceEvent},
    playback::{PlaybackChunk, PlaybackPosition, PlaybackState},
    sessions::{QuestionState, SessionEvent, SessionManager},
};

struct Actor {
    config: CoreConfig,
    backend: Arc<dyn VoiceBackend>,
    sessions: SessionManager,
    tools: CoreTools,
    commands: mpsc::Receiver<(Action, oneshot::Sender<Result<()>>)>,
    audio: mpsc::Receiver<AudioFrame>,
    events: broadcast::Sender<CoreEvent>,
    state: watch::Sender<CoreState>,
    playback: watch::Sender<PlaybackState>,
    output: mpsc::Sender<PlaybackChunk>,
    connected_tx: mpsc::Sender<(Uuid, Result<VoiceSession>)>,
    connected_rx: mpsc::Receiver<(Uuid, Result<VoiceSession>)>,
    verified_tx: mpsc::Sender<(Uuid, Result<SpeakerDecision>)>,
    verified_rx: mpsc::Receiver<(Uuid, Result<SpeakerDecision>)>,
    gate: Option<InputGate>,
    verification: Option<(JoinHandle<()>, tokio_util::sync::CancellationToken)>,
    execution_events: broadcast::Receiver<SessionEvent>,
    recent: AudioBuffer,
    pending: AudioBuffer,
    session: Option<VoiceSession>,
    connection: Option<JoinHandle<()>>,
    epoch: Uuid,
    session_id: Uuid,
    last_sequence: Option<u64>,
    last_activity: Instant,
    playing: Option<PlaybackPosition>,
    received_samples: u64,
    discarded_items: HashSet<String>,
    authorized_from: u64,
    capture_sequence: Arc<Mutex<Option<u64>>>,
    speaking: bool,
    responding: bool,
    execution_results: VecDeque<Value>,
    execution_questions: VecDeque<Value>,
}

impl Actor {
    async fn run(mut self) -> Result<()> {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        loop {
            let sender = self.session.as_ref().map(|s| s.commands.clone());
            tokio::select! {
                biased;
                command = self.commands.recv() => {
                    let Some((action, response)) = command else { break; };
                    if matches!(action, Action::Shutdown) {
                        self.end(CoreState::Stopped);
                        let result = self.sessions.shutdown(Duration::from_secs(10)).await;
                        let failed = result.is_err();
                        let _ = response.send(result);
                        return if failed { Err(Error::Timeout("core task shutdown")) } else { Ok(()) };
                    }
                    let result = self.action(action);
                    let _ = response.send(result);
                }
                Some((epoch, result)) = self.connected_rx.recv() => {
                    if epoch == self.epoch && *self.state.borrow() == CoreState::Connecting {
                        self.connection.take();
                        match result {
                            Ok(session) => {
                                let history: Vec<Value> = self.sessions.recent_runs_for_session(self.session_id, 8)?.into_iter().map(|run| -> Result<Value> { Ok(json!({
                                    "acp_session_id":run.acp_session_id,
                                    "run_id":run.id,
                                    "prompt":truncate(&self.sessions.run_prompt(run.id)?, 512),
                                    "state":run.state,
                                    "summary":truncate(&run.summary, 512),
                                    "error":run.error.as_deref().map(|text| truncate(text, 256)),
                                    "stop_reason":run.stop_reason,
                                    "created_at_ms":run.created_at_ms,
                                })) }).collect::<Result<Vec<_>>>()?;
                                if !history.is_empty() {
                                    session.try_send(VoiceCommand::ExecutionContext(json!(history)))?;
                                }
                                self.session = Some(session);
                                self.last_activity = Instant::now();
                                self.set_state(CoreState::Active);
                                self.playback.send_modify(|state| state.active = true);
                            }
                            Err(error) => self.fail(error),
                        }
                    }
                }
                Some((epoch, result)) = self.verified_rx.recv() => {
                    if epoch == self.epoch && *self.state.borrow() == CoreState::Verifying {
                        self.verification.take();
                        match result {
                            Ok(SpeakerDecision::Accepted { speaker_id }) => {
                                if speaker_id.trim().is_empty() || speaker_id.len() > 512 {
                                    self.fail(Error::Protocol("invalid verified speaker identity".into()));
                                } else {
                                    self.emit(CoreEvent::Speaker(SpeakerDecision::Accepted { speaker_id }));
                                    self.open_backend();
                                }
                            }
                            Ok(decision) => {
                                self.emit(CoreEvent::Speaker(decision));
                                self.end(CoreState::Standby);
                            }
                            Err(error) => self.fail(error),
                        }
                    }
                }
                event = async { match &mut self.session {
                    Some(session) => session.receive().await,
                    None => std::future::pending().await,
                }} => match event {
                    Some(event) => if let Err(error) = self.voice_event(event) { self.fail(error); },
                    None => self.fail(Error::Closed),
                },
                event = self.execution_events.recv() => match event {
                    Ok(event) => {
                        self.route_execution_event(&event)?;
                        self.emit(CoreEvent::Execution(event));
                        if let Err(error) = self.announce_result() { self.fail(error); }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => self.emit(CoreEvent::Error("task events lagged; reload task snapshots".into())),
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                _ = tick.tick() => {
                    if *self.state.borrow() == CoreState::Verifying
                        && self.verification.is_none()
                        && self.last_activity.elapsed() >= self.gate.as_ref().expect("gated state").input.config.verification_timeout {
                        self.fail(Error::Timeout("speaker audio collection"));
                    }
                    if *self.state.borrow() == CoreState::Active
                        && self.last_activity.elapsed() >= self.config.idle_timeout
                        && !self.sessions.has_pending_questions(self.session_id)? {
                        self.end(CoreState::Standby);
                    }
                }
                permit = async { sender.expect("guarded sender").reserve_owned().await },
                    if self.session.is_some() && !self.pending.is_empty() => {
                    match permit {
                        Ok(permit) => if let Some(buffered) = self.pending.pop() {
                            permit.send(match buffered.activity {
                                Some(activity) => VoiceCommand::InputAudio { frame: buffered.frame, activity },
                                None => VoiceCommand::Audio(buffered.frame),
                            });
                        },
                        Err(_) => self.fail(Error::Closed),
                    }
                }
                Some(frame) = self.audio.recv() => {
                    if let Err(error) = self.input(frame) { self.fail(error); }
                }
            }
        }
        self.end(CoreState::Stopped);
        self.sessions.shutdown(Duration::from_secs(10)).await
    }

    fn emit(&self, event: CoreEvent) {
        let _ = self.events.send(event);
    }
    fn set_state(&self, state: CoreState) {
        self.state.send_replace(state);
        self.emit(CoreEvent::State(state));
    }

    fn end(&mut self, state: CoreState) {
        if let Some((verification, cancel)) = self.verification.take() {
            cancel.cancel();
            verification.abort();
        }
        if let Some(gate) = &mut self.gate {
            gate.reset();
            // Audio accepted before this control action must not start the next
            // conversation, even if it is still queued in the capture channel.
            self.authorized_from = self
                .capture_sequence
                .lock()
                .ok()
                .and_then(|latest| *latest)
                .or(self.last_sequence)
                .map_or(0, |sequence| sequence.saturating_add(1));
        }
        if let Some(connection) = self.connection.take() {
            connection.abort();
        }
        self.session.take();
        self.epoch = Uuid::new_v4();
        self.recent.clear();
        self.pending.clear();
        self.playing = None;
        self.received_samples = 0;
        self.discarded_items.clear();
        self.speaking = false;
        self.responding = false;
        self.execution_results.clear();
        self.execution_questions.clear();
        self.clear_playback();
        self.playback.send_modify(|state| state.active = false);
        self.set_state(state);
    }

    fn fail(&mut self, error: Error) {
        let state = self.state.borrow().after_end();
        self.emit(CoreEvent::Error(error.to_string()));
        self.end(state);
    }

    fn action(&mut self, action: Action) -> Result<()> {
        match action {
            Action::Wake(first) => {
                if !self
                    .state
                    .borrow()
                    .validate_manual_wake(self.gate.is_some())?
                {
                    return Ok(());
                }
                if let Some(first) = first
                    && self.last_sequence.is_some_and(|last| first <= last)
                    && self
                        .recent
                        .oldest_sequence()
                        .is_none_or(|oldest| first < oldest)
                {
                    return Err(Error::Audio(
                        "requested wake audio is no longer buffered".into(),
                    ));
                }
                for frame in self.recent.take_from(first) {
                    self.pending.push(frame)?;
                }
                self.authorized_from = first.unwrap_or(0);
                self.open_backend();
            }
            Action::EndConversation => {
                self.sessions.cancel_pending_questions(self.session_id);
                let state = self.state.borrow().after_end();
                self.end(state);
            }
            Action::Mute(muted) => self.end(if muted {
                CoreState::Muted
            } else {
                CoreState::Standby
            }),
            Action::Text(text) => {
                if text.trim().is_empty() || text.len() > 64 * 1024 {
                    return Err(Error::Config("invalid voice text".into()));
                }
                self.send(VoiceCommand::Text(text))?;
                self.responding = true;
                self.last_activity = Instant::now();
            }
            Action::Interrupt => self.interrupt(true)?,
            Action::Playback {
                generation,
                position,
            } => {
                self.report_playback(generation, position)?;
            }
            Action::Shutdown => unreachable!("handled by run"),
        }
        Ok(())
    }

    fn send(&self, command: VoiceCommand) -> Result<()> {
        self.session
            .as_ref()
            .ok_or_else(|| Error::State("voice session is not active".into()))?
            .try_send(command)
    }

    fn open_backend(&mut self) {
        let backend = self.backend.clone();
        let tx = self.connected_tx.clone();
        let timeout = self.config.connect_timeout;
        let epoch = self.epoch;
        let sessions = self.sessions.clone();
        let session_id = self.session_id;
        self.connection = Some(tokio::spawn(async move {
            let result = tokio::time::timeout(timeout, backend.connect(sessions, session_id))
                .await
                .unwrap_or(Err(Error::Timeout("voice connection")));
            let _ = tx.send((epoch, result)).await;
        }));
        self.set_state(CoreState::Connecting);
    }

    fn voice_event(&mut self, event: VoiceEvent) -> Result<()> {
        match event {
            VoiceEvent::SpeechStarted => {
                self.last_activity = Instant::now();
                if self.speaking {
                    return Ok(());
                }
                self.speaking = true;
                self.interrupt(false)?;
            }
            VoiceEvent::SpeechStopped => self.speaking = false,
            VoiceEvent::ResponseStarted => self.responding = true,
            VoiceEvent::Audio {
                item_id,
                content_index,
                sample_rate,
                samples,
            } => {
                self.output_audio(item_id, content_index, sample_rate, samples)?;
            }
            VoiceEvent::Transcript {
                user,
                text,
                final_chunk,
            } => {
                if user {
                    self.last_activity = Instant::now();
                }
                self.emit(CoreEvent::Transcript {
                    user,
                    text,
                    final_chunk,
                });
            }
            VoiceEvent::ToolCall {
                call_id,
                name,
                arguments,
            } => {
                if call_id.is_empty() || call_id.len() > 256 {
                    return Err(Error::Protocol("invalid tool call ID".into()));
                }
                let crate::text_model::ModelMessage::ToolResult { call_id, result } =
                    self.tools.invoke(
                        crate::text_model::ToolCall {
                            call_id,
                            name,
                            arguments,
                        },
                        None,
                    )
                else {
                    unreachable!("tool invocation returns a result")
                };
                self.send(VoiceCommand::ToolResult { call_id, result })?;
                self.responding = true;
            }
            VoiceEvent::ResponseFinished => {
                self.responding = false;
                self.announce_result()?;
            }
            VoiceEvent::Error(error) => return Err(Error::Protocol(error)),
            VoiceEvent::Closed => return Err(Error::Closed),
        }
        Ok(())
    }

    fn route_execution_event(&mut self, event: &SessionEvent) -> Result<()> {
        match event {
            SessionEvent::RunChanged { session_id, run }
                if *session_id == self.session_id && run.state.is_terminal() =>
            {
                let update = json!({
                    "kind": "run",
                    "acp_session_id": run.acp_session_id,
                    "run_id": run.id,
                    "state": run.state,
                    "prompt": truncate(&self.sessions.run_prompt(run.id)?, 512),
                    "summary": truncate(&run.summary, 512),
                    "stop_reason": run.stop_reason,
                    "error": run.error.as_deref().map(|text| truncate(text, 256)),
                });
                self.push_execution_result(update);
            }
            SessionEvent::QuestionAsked {
                session_id,
                question,
            } if *session_id == self.session_id => {
                let update = json!({
                    "kind": "question",
                    "acp_session_id": question.acp_session_id,
                    "run_id": question.run_id,
                    "question_id": question.id,
                    "question": question,
                });
                if self.execution_questions.len() < 32 {
                    self.execution_questions.push_back(update);
                } else {
                    self.emit(CoreEvent::Error(
                        "task question announcements full; read status instead".into(),
                    ));
                }
            }
            SessionEvent::QuestionResolved {
                session_id,
                run_id,
                question_id,
                state,
            } if *session_id == self.session_id && state.is_terminal() => {
                // Give the model and user a fresh idle window after waiting for
                // input, including when a host answers an already-announced question.
                self.last_activity = Instant::now();
                // If the question never reached the model, drop it instead of
                // announcing a stale question followed by its resolution.
                let id = question_id.to_string();
                let queued = self
                    .execution_questions
                    .iter()
                    .any(|value| value["question_id"].as_str() == Some(&id));
                self.execution_questions
                    .retain(|value| value["question_id"].as_str() != Some(&id));
                if !queued {
                    let update = json!({
                        "kind": "question_resolved",
                        "run_id": run_id,
                        "question_id": question_id,
                        "state": state,
                    });
                    self.push_execution_result(update);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn push_execution_result(&mut self, update: Value) {
        if self.execution_results.len() < 32 {
            self.execution_results.push_back(update);
        } else {
            self.emit(CoreEvent::Error(
                "task result announcements full; results remain in session data".into(),
            ));
        }
    }

    fn announce_result(&mut self) -> Result<()> {
        if crate::orchestration::domain::can_announce_result(
            self.speaking || self.gate.as_ref().is_some_and(|gate| gate.speaking),
            self.responding,
            self.session.is_some(),
            crate::playback::has_finished(
                self.playing.as_ref(),
                self.received_samples,
                self.config.sample_rate,
            ),
        ) {
            // Resolution events can still be behind voice events in the actor's
            // queues. Recheck the authoritative snapshot before asking the user.
            let mut question = None;
            while let Some(update) = self.execution_questions.pop_front() {
                let id = update["question_id"]
                    .as_str()
                    .and_then(|id| Uuid::parse_str(id).ok())
                    .ok_or_else(|| Error::State("invalid queued task question id".into()))?;
                let current = self.sessions.question(id)?;
                if current.session_id == self.session_id
                    && current.state == QuestionState::Pending
                    && current.expires_at_ms > crate::sessions::domain::now_ms()
                {
                    question = Some(update);
                    break;
                }
            }
            if let Some(result) = question.or_else(|| self.execution_results.pop_front()) {
                self.send(VoiceCommand::ExecutionUpdate(result))?;
                self.responding = true;
                self.last_activity = Instant::now();
            }
        }
        Ok(())
    }
}

fn truncate(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

impl Drop for Actor {
    fn drop(&mut self) {
        if let Some((verification, cancel)) = self.verification.take() {
            cancel.cancel();
            verification.abort();
        }
        if let Some(connection) = self.connection.take() {
            connection.abort();
        }
        self.sessions.request_shutdown();
    }
}
