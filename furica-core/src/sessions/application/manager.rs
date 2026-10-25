//! SessionManager coordinates ACP sessions, serial rounds and agent questions.
//!
//! The host manages business sessions, receives business messages and reads execution state.
//! Only the main-model workflow can prepare and control ACP work.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use tokio::sync::{Notify, broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::super::domain::{
    Message, MessageContent, MessageRole, MessageSource, MessageState, Session, SessionState,
};
use super::super::{
    domain::{
        AcpSession, AcpSessionState, AcpStatus, Answer, Question, QuestionDraft, QuestionState,
        Run, RunState, SessionEvent, now_ms,
    },
    ports::{ExecutionEvent, ExecutionInput, Executor},
};
use super::bindings::{AgentBinding, WorkspaceOption};
use super::data::SessionData;
use super::state::{Memory, push_message, require_active};
use crate::text_model::ModelMessage;
use crate::{Error, Result};

#[derive(Clone, Debug)]
pub struct SessionLimits {
    /// Concurrent ACP sessions; the executor owns their connection allocation.
    pub max_acp_sessions: usize,
    /// Prepared, queued and running rounds across all sessions.
    pub max_runs: usize,
    /// Independent deadline for each pending agent question.
    pub question_timeout: Duration,
}

impl Default for SessionLimits {
    fn default() -> Self {
        Self {
            max_acp_sessions: 4,
            max_runs: 32,
            question_timeout: Duration::from_secs(300),
        }
    }
}

/// Host-facing session context, message acknowledgement and read-only ACP status.
/// ACP commands are reserved for the main-model workflow.
///
/// ```compile_fail
/// use furica_core::sessions::SessionManager;
/// use uuid::Uuid;
/// fn host_cannot_prepare_acp(manager: &SessionManager, session_id: Uuid) {
///     manager.prepare_start(session_id, "request", "work".into());
/// }
/// ```
///
/// ```compile_fail
/// use furica_core::sessions::SessionManager;
/// use uuid::Uuid;
/// fn host_cannot_launch_acp(manager: &SessionManager, session_id: Uuid, run_id: Uuid) {
///     manager.launch(session_id, run_id);
/// }
/// ```
#[derive(Clone)]
pub struct SessionManager(Arc<Inner>);

struct Inner {
    data: Memory,
    delivery_id: Uuid,
    acknowledgements: Mutex<()>,
    agents: Vec<AgentBinding>,
    limits: SessionLimits,
    connections: Mutex<HashMap<Uuid, SessionControl>>,
    questions: Mutex<HashMap<Uuid, PendingQuestion>>,
    detached: Mutex<HashSet<Uuid>>,
    shutdown: CancellationToken,
    idle: Notify,
    events: broadcast::Sender<SessionEvent>,
}

struct SessionControl {
    session_id: Uuid,
    input: mpsc::UnboundedSender<ExecutionInput>,
    active_run: Option<Uuid>,
    queue: VecDeque<QueuedRun>,
    detached: bool,
    closing: bool,
}

struct QueuedRun {
    id: Uuid,
    prompt: String,
}

#[derive(Clone)]
struct PendingQuestion {
    session_id: Uuid,
    acp_session_id: Uuid,
    run_id: Uuid,
    expires_at_ms: u64,
}

impl SessionManager {
    pub fn new(
        data: SessionData,
        executor: Arc<dyn Executor>,
        workspace: PathBuf,
        limits: SessionLimits,
    ) -> Result<Self> {
        Self::with_agents(
            data,
            vec![AgentBinding {
                id: executor.agent_id().to_owned(),
                description: "Default agent".into(),
                executor,
                workspaces: vec![WorkspaceOption {
                    id: "default".into(),
                    description: "Default workspace".into(),
                    path: workspace,
                }],
            }],
            limits,
        )
    }

    pub fn with_agents(
        data: SessionData,
        agents: Vec<AgentBinding>,
        limits: SessionLimits,
    ) -> Result<Self> {
        let data = Memory::new(data)?;
        let (events, _) = broadcast::channel(128);
        Ok(Self(Arc::new(Inner {
            data,
            delivery_id: Uuid::new_v4(),
            acknowledgements: Mutex::new(()),
            agents,
            limits,
            connections: Mutex::new(HashMap::new()),
            questions: Mutex::new(HashMap::new()),
            detached: Mutex::new(HashSet::new()),
            shutdown: CancellationToken::new(),
            idle: Notify::new(),
            events,
        })))
    }

    /// Immutable candidates supplied by the host for model selection.
    pub fn agents(&self) -> &[AgentBinding] {
        &self.0.agents
    }

    #[cfg(test)]
    pub fn snapshot(&self) -> Result<SessionData> {
        self.0.data.snapshot()
    }

    /// Identity of this runtime's ordered host message stream.
    pub fn delivery_id(&self) -> Uuid {
        self.0.delivery_id
    }
    /// The oldest unacknowledged group. Reading never removes it.
    pub fn pending_messages(&self) -> Result<Option<(u64, Vec<SessionEvent>)>> {
        self.0.data.pending_messages()
    }
    /// Confirm host receipt of one group. Core does not perform storage I/O.
    /// Prepared tasks in the confirmed group may now be scheduled, once only.
    /// Call inside a Tokio runtime when the group contains prepared work.
    pub fn acknowledge_messages(&self, sequence: u64) -> Result<()> {
        let _serial = self
            .0
            .acknowledgements
            .lock()
            .map_err(|_| Error::State("acknowledgement lock poisoned".into()))?;
        // Retain the group if scheduling fails so the same acknowledgement can be retried.
        if let Some((pending, events)) = self.0.data.pending_messages()?
            && pending == sequence
        {
            for event in events {
                if let SessionEvent::RunChanged { run, .. } = event
                    && run.state == RunState::Prepared
                    && !self.0.shutdown.is_cancelled()
                    && self.0.data.get_run(run.id)?.state == RunState::Prepared
                    && self.session(run.session_id)?.state == SessionState::Active
                {
                    self.launch(run.session_id, run.id)?;
                }
            }
        }
        self.0.data.acknowledge_messages(sequence)
    }
    /// A wake-up hint only. Drain pending_messages; this notification may coalesce.
    pub fn messages_available(&self) -> watch::Receiver<u64> {
        self.0.data.subscribe()
    }
    pub fn switching_allowed(&self) -> Result<bool> {
        self.0.data.switching_allowed()
    }
    pub fn create_session(&self) -> Result<Session> {
        let s = Session::new();
        self.add_session(s.clone())?;
        Ok(s)
    }
    pub fn add_session(&self, session: Session) -> Result<()> {
        self.0.data.add_owner(session)
    }
    pub fn session(&self, id: Uuid) -> Result<Session> {
        self.0.data.owner(id)
    }
    pub fn rename_session(&self, id: Uuid, name: String) -> Result<Session> {
        self.0.data.rename(id, name)
    }
    pub fn archive_session(&self, id: Uuid) -> Result<Session> {
        let s = self.0.data.archive(id)?;
        self.detach_session(id);
        Ok(s)
    }
    pub fn run_prompt(&self, id: Uuid) -> Result<String> {
        self.0.data.prompt(id)
    }
    pub fn main_history(&self, id: Uuid) -> Result<Vec<ModelMessage>> {
        self.session(id)?;
        self.0.data.main_history(id)
    }
    pub fn append_message(
        &self,
        id: Uuid,
        source: MessageSource,
        message: ModelMessage,
        state: MessageState,
    ) -> Result<Message> {
        self.0.data.append_model(id, source, message, state)
    }
    pub fn set_instructions(&self, id: Uuid, text: Option<&str>) -> Result<()> {
        self.0.data.set_instructions(id, text)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SessionEvent> {
        self.0.events.subscribe()
    }

    pub fn acp_session(&self, id: Uuid) -> Result<AcpSession> {
        self.0.data.get_session(id)
    }

    /// List the ACP contexts belonging to one business session for host display.
    pub fn acp_sessions(&self, session_id: Uuid) -> Result<Vec<AcpSession>> {
        self.0.data.list_sessions(session_id)
    }

    pub fn run(&self, id: Uuid) -> Result<Run> {
        self.0.data.get_run(id)
    }

    pub fn question(&self, id: Uuid) -> Result<Question> {
        self.0.data.get_question(id)
    }

    pub fn recent_runs(&self, limit: usize) -> Result<Vec<Run>> {
        self.0.data.recent_runs(None, limit)
    }

    pub fn recent_runs_for_session(&self, session_id: Uuid, limit: usize) -> Result<Vec<Run>> {
        self.0.data.recent_runs(Some(session_id), limit)
    }

    /// Pending questions keep only their owning conversation alive. The manager
    /// releases this hold when an answer, cancellation or deadline resolves them.
    pub fn has_pending_questions(&self, session_id: Uuid) -> Result<bool> {
        Ok(self
            .lock_questions()?
            .values()
            .any(|question| question.session_id == session_id))
    }

    /// Main-model tool entry point. Only prepare data; the host must receive and
    /// acknowledge its ordered messages before Core starts execution.
    pub(crate) fn prepare_selected(
        &self,
        session_id: Uuid,
        call_id: &str,
        agent_id: &str,
        workspace_id: &str,
        prompt: String,
    ) -> Result<Run> {
        let agent = self
            .0
            .agents
            .iter()
            .find(|a| a.id == agent_id)
            .ok_or_else(|| Error::Config("unknown agent id".into()))?;
        let workspace = agent
            .workspaces
            .iter()
            .find(|w| w.id == workspace_id)
            .ok_or_else(|| Error::Config("workspace is not available for this agent".into()))?;
        self.validate_request(session_id, call_id, &prompt)?;
        if self.0.shutdown.is_cancelled() {
            return Err(Error::Closed);
        }
        let _connections = self.lock_connections()?;
        if self.0.shutdown.is_cancelled() {
            return Err(Error::Closed);
        }
        if self.lock_detached()?.contains(&session_id) {
            return Err(Error::State("session is detached".into()));
        }
        let key = request_key(session_id, call_id);
        if let Some(run) = self.0.data.find_run_by_request(&key)? {
            let bound = self.0.data.get_session(run.acp_session_id)?;
            if run.session_id != session_id
                || self.0.data.prompt(run.id)? != prompt
                || bound.agent_id != agent_id
                || bound.workspace != workspace.path
            {
                return Err(Error::State(
                    "request key reused for different content".into(),
                ));
            }
            return Ok(run);
        }
        let now = now_ms();
        let acp = AcpSession {
            id: Uuid::new_v4(),
            session_id,
            remote_session_id: None,
            state: AcpSessionState::Opening,
            workspace: workspace.path.clone(),
            agent_id: agent.id.clone(),
            created_at_ms: now,
            updated_at_ms: now,
            last_activity_ms: now,
            error: None,
        };
        let run = self.0.data.change(|d, events| {
            require_active(d, session_id)?;
            if d.acp_sessions
                .iter()
                .filter(|s| !s.state.is_terminal())
                .count()
                >= self.0.limits.max_acp_sessions
            {
                return Err(Error::Busy("ACP sessions"));
            }
            if d.runs.iter().filter(|r| !r.state.is_terminal()).count() >= self.0.limits.max_runs {
                return Err(Error::Busy("execution rounds"));
            }
            let run = prepare_input(d, &acp, &key, &prompt)?;
            events.push(SessionEvent::MessageRecorded(
                d.messages.last().expect("prepared input").clone(),
            ));
            events.push(SessionEvent::RunChanged {
                session_id,
                run: run.clone(),
            });
            super::state::emit_owner(d, session_id, events);
            d.acp_sessions.push(acp.clone());
            events.push(SessionEvent::AcpSessionChanged(acp.clone()));
            d.runs.push(run.clone());
            Ok(run)
        })?;
        self.emit(SessionEvent::AcpSessionChanged(acp));
        self.emit(SessionEvent::RunChanged {
            session_id,
            run: run.clone(),
        });
        Ok(run)
    }

    #[cfg(test)]
    pub(crate) fn prepare_start(
        &self,
        session_id: Uuid,
        call_id: &str,
        prompt: String,
    ) -> Result<Run> {
        let agent = &self.0.agents[0];
        self.prepare_selected(
            session_id,
            call_id,
            &agent.id,
            &agent.workspaces[0].id,
            prompt,
        )
    }

    pub(crate) fn prepare_continue(
        &self,
        session_id: Uuid,
        call_id: &str,
        acp_session_id: Uuid,
        prompt: String,
    ) -> Result<Run> {
        self.validate_request(session_id, call_id, &prompt)?;
        if self.0.shutdown.is_cancelled() {
            return Err(Error::Closed);
        }
        let _connections = self.lock_connections()?;
        if self.0.shutdown.is_cancelled() {
            return Err(Error::Closed);
        }
        if self.lock_detached()?.contains(&session_id) {
            return Err(Error::State("session is detached".into()));
        }
        let key = request_key(session_id, call_id);
        if let Some(run) = self.0.data.find_run_by_request(&key)? {
            if run.session_id != session_id
                || run.acp_session_id != acp_session_id
                || self.0.data.prompt(run.id)? != prompt
            {
                return Err(Error::State(
                    "request key reused for different execution".into(),
                ));
            }
            return Ok(run);
        }
        let acp = self.0.data.get_session(acp_session_id)?;
        self.require_owner(&acp, session_id)?;
        if !acp.state.accepts_runs() {
            return Err(Error::State(
                "ACP session does not accept new rounds".into(),
            ));
        }
        let run = self.0.data.change(|d, events| {
            require_active(d, session_id)?;
            if d.runs.iter().filter(|r| !r.state.is_terminal()).count() >= self.0.limits.max_runs {
                return Err(Error::Busy("execution rounds"));
            }
            let run = prepare_input(d, &acp, &key, &prompt)?;
            events.push(SessionEvent::MessageRecorded(
                d.messages.last().expect("prepared input").clone(),
            ));
            events.push(SessionEvent::RunChanged {
                session_id,
                run: run.clone(),
            });
            super::state::emit_owner(d, session_id, events);
            d.runs.push(run.clone());
            Ok(run)
        })?;
        self.emit(SessionEvent::RunChanged {
            session_id,
            run: run.clone(),
        });
        Ok(run)
    }

    /// Schedule model-prepared work after host message acknowledgement.
    /// Calling it again never repeats an accepted execution.
    fn launch(&self, session_id: Uuid, run_id: Uuid) -> Result<Run> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::State("launch requires a Tokio runtime".into()))?;
        if self.0.shutdown.is_cancelled() {
            return Err(Error::Closed);
        }
        let mut connections = self.lock_connections()?;
        if self.0.shutdown.is_cancelled() {
            return Err(Error::Closed);
        }
        let run = self.0.data.get_run(run_id)?;
        let acp = self.0.data.get_session(run.acp_session_id)?;
        self.require_owner(&acp, session_id)?;
        if self.session(session_id)?.state != super::super::domain::SessionState::Active
            || self.lock_detached()?.contains(&session_id)
        {
            return Err(Error::State("session is not active".into()));
        }
        if run.state != RunState::Prepared {
            return Ok(run);
        }
        if !acp.state.accepts_runs() {
            return Err(Error::State("ACP connection is unavailable".into()));
        }
        let queued = QueuedRun {
            id: run.id,
            prompt: self.0.data.prompt(run.id)?,
        };
        let mut spawn = None;
        if let Some(control) = connections.get_mut(&acp.id) {
            if control.closing || control.detached {
                return Err(Error::State("ACP session is closing".into()));
            }
            control.queue.push_back(queued);
        } else {
            if acp.state != AcpSessionState::Opening {
                return Err(Error::State("ACP session is not connected".into()));
            }
            let (input_tx, input_rx) = mpsc::unbounded_channel();
            let (event_tx, event_rx) = mpsc::channel(64);
            connections.insert(
                acp.id,
                SessionControl {
                    session_id,
                    input: input_tx,
                    active_run: None,
                    queue: VecDeque::from([queued]),
                    detached: false,
                    closing: false,
                },
            );
            spawn = Some((input_rx, event_tx, event_rx));
        }
        let run = self
            .0
            .data
            .update_run(run_id, |r| r.state = RunState::Queued)?;
        drop(connections);
        self.emit(SessionEvent::RunChanged {
            session_id,
            run: run.clone(),
        });
        if let Some((input_rx, event_tx, event_rx)) = spawn {
            let manager = self.clone();
            let executor = self
                .0
                .agents
                .iter()
                .find(|a| a.id == acp.agent_id)
                .ok_or_else(|| Error::Config("session agent binding is unavailable".into()))?
                .executor
                .clone();
            let cancel = self.0.shutdown.child_token();
            runtime.spawn(async move {
                manager
                    .supervise(acp, input_rx, event_tx, event_rx, executor, cancel)
                    .await;
            });
        } else {
            self.dispatch(acp.id);
        }
        Ok(run)
    }

    /// Read an ACP context, its runs and pending questions without controlling it.
    pub fn status(&self, session_id: Uuid, acp_session_id: Uuid) -> Result<AcpStatus> {
        let session = self.0.data.get_session(acp_session_id)?;
        self.require_owner(&session, session_id)?;
        self.status_snapshot(acp_session_id)
    }

    /// Answering is a single transition. Repeated, expired and cross-conversation
    /// answers fail before anything reaches the original ACP request.
    pub(crate) fn answer(
        &self,
        session_id: Uuid,
        question_id: Uuid,
        answer: Answer,
    ) -> Result<Question> {
        if self.0.shutdown.is_cancelled() {
            return Err(Error::Closed);
        }
        if self.lock_detached()?.contains(&session_id) {
            return Err(Error::State("conversation has ended".into()));
        }
        let mut questions = self.lock_questions()?;
        let entry = questions
            .get(&question_id)
            .cloned()
            .ok_or_else(|| Error::State("question is not pending".into()))?;
        if entry.session_id != session_id {
            return Err(Error::State(
                "question belongs to another conversation".into(),
            ));
        }
        if entry.expires_at_ms <= now_ms() {
            drop(questions);
            self.expire_question(question_id);
            return Err(Error::State("question has expired".into()));
        }
        let question = self.0.data.get_question(question_id)?;
        question.validate_answer(&answer)?;
        let state = answer.state();
        if let Err(error) = self.send_input(
            entry.acp_session_id,
            ExecutionInput::Answer {
                question_id,
                answer: answer.clone(),
            },
        ) {
            if matches!(error, Error::Closed) {
                questions.remove(&question_id);
                let _ = self.0.data.update_question(question_id, |question| {
                    question.resolve(QuestionState::Cancelled, None);
                });
                drop(questions);
                self.emit(SessionEvent::QuestionResolved {
                    session_id,
                    run_id: entry.run_id,
                    question_id,
                    state: QuestionState::Cancelled,
                });
            }
            return Err(error);
        }
        questions.remove(&question_id);
        let updated = self.0.data.update_question(question_id, |question| {
            question.resolve(state, Some(answer));
        })?;
        drop(questions);
        self.emit(SessionEvent::QuestionResolved {
            session_id,
            run_id: entry.run_id,
            question_id,
            state,
        });
        Ok(updated)
    }

    /// Cancel the active round, or the oldest queued round if nothing is running.
    /// The ACP session stays open when cancellation is confirmed later.
    pub(crate) fn cancel_run(&self, owner_id: Uuid, session_id: Uuid) -> Result<Run> {
        let session = self.0.data.get_session(session_id)?;
        self.require_owner(&session, owner_id)?;
        let prepared = self
            .0
            .data
            .list_runs(session_id)?
            .into_iter()
            .find(|r| r.state == RunState::Prepared);
        if let Some(run) = prepared.filter(|_| {
            self.lock_connections()
                .map(|s| {
                    s.get(&session_id)
                        .is_none_or(|c| c.active_run.is_none() && c.queue.is_empty())
                })
                .unwrap_or(false)
        }) {
            let run = self.0.data.update_run(run.id, |r| {
                r.cancel_before_start("cancelled_before_launch");
            })?;
            self.emit(SessionEvent::RunChanged {
                session_id: owner_id,
                run: run.clone(),
            });
            return Ok(run);
        }
        let (active, queued) = {
            let mut sessions = self.lock_connections()?;
            let control = sessions
                .get_mut(&session_id)
                .ok_or_else(|| Error::State("ACP session is not connected".into()))?;
            if let Some(run_id) = control.active_run {
                if control
                    .input
                    .send(ExecutionInput::CancelRun { run_id })
                    .is_err()
                {
                    return Err(Error::Closed);
                }
                (Some(run_id), None)
            } else if let Some(queued) = control.queue.pop_front() {
                (None, Some(queued))
            } else {
                (None, None)
            }
        };
        if let Some(run_id) = active {
            self.cancel_questions_for_run(run_id, QuestionState::Cancelled, true);
            return self.0.data.get_run(run_id);
        }
        if let Some(queued) = queued {
            let run = self.0.data.update_run(queued.id, |run| {
                run.cancel_before_start("cancelled_before_execution");
            })?;
            self.emit(SessionEvent::RunChanged {
                session_id: owner_id,
                run: run.clone(),
            });
            return Ok(run);
        }
        Err(Error::State("session has no active run".into()))
    }

    /// Explicitly close the session. Pending questions are cancelled; an in-flight
    /// round is cancelled before the connection is released.
    pub(crate) fn close(&self, owner_id: Uuid, session_id: Uuid) -> Result<AcpSession> {
        let session = self.0.data.get_session(session_id)?;
        self.require_owner(&session, owner_id)?;
        self.cancel_prepared(Some(owner_id), Some(session_id))?;
        if session.state.is_terminal() {
            return Ok(session);
        }
        let input = {
            let mut sessions = self.lock_connections()?;
            sessions.get_mut(&session_id).map(|control| {
                control.closing = true;
                control.input.clone()
            })
        };
        let Some(input) = input else {
            let record = self.0.data.update_session(session_id, |session| {
                session.closed();
            })?;
            self.cancel_questions_for_session(session_id, QuestionState::Cancelled, false);
            self.emit(SessionEvent::AcpSessionChanged(record.clone()));
            return Ok(record);
        };
        let record = self.0.data.update_session(session_id, |session| {
            session.request_close();
        })?;
        let _ = input.send(ExecutionInput::Close);
        self.cancel_questions_for_session(session_id, QuestionState::Cancelled, true);
        self.emit(SessionEvent::AcpSessionChanged(record.clone()));
        Ok(record)
    }

    /// Archive the business session at runtime. No new rounds
    /// are accepted; pending questions and queued rounds are cancelled; the active
    /// round may finish before its ACP session is released.
    fn detach_session(&self, session_id: Uuid) {
        let _ = self.cancel_prepared(Some(session_id), None);
        let Ok(mut detached) = self.lock_detached() else {
            return;
        };
        if !detached.insert(session_id) {
            return;
        }
        drop(detached);
        let session_ids = {
            let Ok(mut sessions) = self.lock_connections() else {
                return;
            };
            let mut ids = Vec::new();
            for (id, control) in sessions.iter_mut() {
                if control.session_id == session_id {
                    control.detached = true;
                    ids.push(*id);
                }
            }
            ids
        };
        for session_id in session_ids {
            self.cancel_questions_for_session(session_id, QuestionState::Cancelled, true);
            self.close_if_idle(session_id);
        }
    }

    pub(crate) fn cancel_pending_questions(&self, session_id: Uuid) {
        let ids: Vec<_> = self
            .lock_questions()
            .map(|q| {
                q.iter()
                    .filter(|(_, v)| v.session_id == session_id)
                    .map(|(id, _)| *id)
                    .collect()
            })
            .unwrap_or_default();
        for id in ids {
            self.resolve_question(id, QuestionState::Cancelled, None, true);
        }
    }
    fn cancel_prepared(&self, owner: Option<Uuid>, acp: Option<Uuid>) -> Result<()> {
        let connections = self.lock_connections()?;
        self.0.data.change(|d, events| {
            for r in &mut d.runs {
                if r.state == RunState::Prepared
                    && owner.is_none_or(|id| id == r.session_id)
                    && acp.is_none_or(|id| id == r.acp_session_id)
                {
                    r.cancel_before_start("not_launched");
                    events.push(SessionEvent::RunChanged {
                        session_id: r.session_id,
                        run: r.clone(),
                    });
                }
            }
            for s in &mut d.acp_sessions {
                if owner.is_none_or(|id| id == s.session_id)
                    && acp.is_none_or(|id| id == s.id)
                    && s.state == AcpSessionState::Opening
                    && s.remote_session_id.is_none()
                    && !connections.contains_key(&s.id)
                    && !d
                        .runs
                        .iter()
                        .any(|r| r.acp_session_id == s.id && !r.state.is_terminal())
                {
                    s.closed();
                    events.push(SessionEvent::AcpSessionChanged(s.clone()));
                }
            }
            Ok(())
        })
    }

    pub async fn shutdown(&self, timeout: Duration) -> Result<()> {
        self.0.shutdown.cancel();
        self.cancel_prepared(None, None)?;
        tokio::time::timeout(timeout, async {
            loop {
                let notified = self.0.idle.notified();
                tokio::pin!(notified);
                // Register before inspecting the condition to avoid lost wakes.
                notified.as_mut().enable();
                if self.lock_connections()?.is_empty() {
                    return Ok(());
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| Error::Timeout("execution shutdown"))?
    }

    pub(crate) fn request_shutdown(&self) {
        self.0.shutdown.cancel();
        let _ = self.cancel_prepared(None, None);
    }

    async fn supervise(
        self,
        session: AcpSession,
        inputs: mpsc::UnboundedReceiver<ExecutionInput>,
        event_tx: mpsc::Sender<ExecutionEvent>,
        mut event_rx: mpsc::Receiver<ExecutionEvent>,
        executor: Arc<dyn Executor>,
        cancel: CancellationToken,
    ) {
        let session_id = session.id;
        let mut future = Box::pin(executor.run_session(session, inputs, event_tx, cancel));
        let mut record_failed = false;
        let mut result = loop {
            tokio::select! {
                result = &mut future => break result,
                event = event_rx.recv() => match event {
                    Some(event) => {
                        if let Err(error) = self.handle_event(session_id, event) {
                            record_failed=true;
                            break Err(error);
                        }
                    }
                    None => break (&mut future).await,
                },
            }
        };
        drop(future);
        if !record_failed {
            while let Ok(event) = event_rx.try_recv() {
                if let Err(error) = self.handle_event(session_id, event) {
                    result = Err(error);
                    break;
                }
            }
        }
        if let Err(error) = self.finalize_session(session_id, result) {
            self.emit_error(None, Some(session_id), None, error.to_string());
        }
    }

    fn handle_event(&self, acp_session_id: Uuid, event: ExecutionEvent) -> Result<()> {
        let session_id = self.0.data.get_session(acp_session_id)?.session_id;
        let check_run = |id| -> Result<Run> {
            let run = self.0.data.get_run(id)?;
            if run.acp_session_id != acp_session_id {
                return Err(Error::Protocol(
                    "event belongs to another ACP session".into(),
                ));
            }
            Ok(run)
        };
        match event {
            ExecutionEvent::CloseFailed { error } => {
                let record = self.0.data.update_session(acp_session_id, |s| {
                    s.request_close();
                    s.error = Some(error.chars().take(8192).collect());
                })?;
                self.emit(SessionEvent::AcpSessionChanged(record));
            }
            ExecutionEvent::Opened { remote_session_id } => {
                let record = self.0.data.update_session(acp_session_id, |s| {
                    s.opened(remote_session_id);
                })?;
                self.emit(SessionEvent::AcpSessionChanged(record));
                let (closing, detached) = self
                    .lock_connections()?
                    .get(&acp_session_id)
                    .map(|s| (s.closing, s.detached))
                    .unwrap_or((false, false));
                if closing {
                    let _ = self.send_input(acp_session_id, ExecutionInput::Close);
                } else {
                    self.dispatch(acp_session_id);
                    if detached {
                        self.close_if_idle(acp_session_id);
                    }
                }
            }
            ExecutionEvent::Update { run_id, update } => {
                check_run(run_id)?;
                self.emit(SessionEvent::Progress {
                    session_id,
                    run_id,
                    update,
                });
            }
            ExecutionEvent::Message {
                run_id,
                source_message_id,
                role,
                content,
            } => {
                if run_id
                    .map(check_run)
                    .transpose()?
                    .is_none_or(|r| r.state == RunState::Running)
                {
                    self.0.data.append_acp(
                        acp_session_id,
                        run_id,
                        source_message_id,
                        role,
                        content,
                    )?;
                }
            }
            ExecutionEvent::ToolCall {
                run_id,
                source_call_id,
                name,
                title,
                kind,
                state,
                input,
                output,
                content,
                locations,
            } => {
                if run_id
                    .map(check_run)
                    .transpose()?
                    .is_none_or(|r| r.state == RunState::Running)
                {
                    self.0.data.apply_tool(
                        acp_session_id,
                        run_id,
                        source_call_id,
                        name,
                        title,
                        kind,
                        state,
                        input,
                        output,
                        content,
                        locations,
                    )?;
                }
            }
            ExecutionEvent::RunFinished { run_id, outcome } => {
                if check_run(run_id)?.state != RunState::Running {
                    return Ok(());
                }
                let run = self.0.data.update_run(run_id, |r| {
                    r.finish(outcome);
                })?;
                self.0
                    .data
                    .finish_messages(run_id, run.state != RunState::Completed)?;
                self.emit(SessionEvent::RunChanged { session_id, run });
                self.cancel_questions_for_run(run_id, QuestionState::Cancelled, true);
                self.after_run(acp_session_id, run_id);
            }
            ExecutionEvent::QuestionAsked { question } => {
                self.register_question(acp_session_id, session_id, question)?
            }
            ExecutionEvent::QuestionResolved { question_id, state } => {
                let q = self.0.data.get_question(question_id)?;
                if q.acp_session_id != acp_session_id {
                    return Err(Error::Protocol(
                        "question belongs to another ACP session".into(),
                    ));
                }
                self.resolve_question(question_id, state, None, false);
            }
        }
        Ok(())
    }

    fn register_question(
        &self,
        acp_session_id: Uuid,
        session_id: Uuid,
        draft: QuestionDraft,
    ) -> Result<()> {
        let session = self.0.data.get_session(acp_session_id)?;
        let run = self.0.data.get_run(draft.run_id)?;
        if !session.state.accepts_runs()
            || run.acp_session_id != acp_session_id
            || run.state != RunState::Running
        {
            let _ = self.send_input(
                acp_session_id,
                ExecutionInput::Answer {
                    question_id: draft.id,
                    answer: Answer::Cancel,
                },
            );
            return Ok(());
        }
        let now = now_ms();
        let expires_at_ms = now.saturating_add(self.question_timeout_ms());
        let tool_call_id = self
            .0
            .data
            .source_tool(draft.run_id, draft.source_tool_call_id.as_deref())?;
        let question = Question {
            id: draft.id,
            tool_call_id,
            updated_at_ms: now,
            run_id: draft.run_id,
            session_id,
            acp_session_id,
            kind: draft.kind,
            state: QuestionState::Pending,
            answer: None,
            created_at_ms: now,
            expires_at_ms,
            resolved_at_ms: None,
        };
        self.0.data.insert_question(&question)?;
        let detached = self
            .lock_connections()
            .ok()
            .and_then(|sessions| {
                sessions
                    .get(&acp_session_id)
                    .map(|control| control.detached)
            })
            .unwrap_or(false);
        self.lock_questions()?.insert(
            question.id,
            PendingQuestion {
                session_id,
                acp_session_id,
                run_id: question.run_id,
                expires_at_ms,
            },
        );
        self.emit(SessionEvent::QuestionAsked {
            session_id,
            question: question.clone(),
        });
        if detached {
            self.resolve_question(question.id, QuestionState::Cancelled, None, true);
        } else {
            self.schedule_expiry(question.id, expires_at_ms);
        }
        Ok(())
    }

    fn resolve_question(
        &self,
        question_id: Uuid,
        state: QuestionState,
        answer: Option<Answer>,
        notify_agent: bool,
    ) -> Option<Question> {
        let (entry, question) = {
            let mut questions = self.lock_questions().ok()?;
            let entry = questions.get(&question_id).cloned()?;
            let mut current = self.0.data.get_question(question_id).ok()?;
            if !current.resolve(state, answer.clone()) {
                questions.remove(&question_id);
                return None;
            }
            let updated = self
                .0
                .data
                .update_question(question_id, |question| {
                    question.resolve(state, answer);
                })
                .ok()?;
            questions.remove(&question_id);
            (entry, updated)
        };
        if notify_agent {
            let _ = self.send_input(
                entry.acp_session_id,
                ExecutionInput::Answer {
                    question_id,
                    answer: Answer::Cancel,
                },
            );
        }
        self.emit(SessionEvent::QuestionResolved {
            session_id: entry.session_id,
            run_id: entry.run_id,
            question_id,
            state,
        });
        Some(question)
    }

    fn expire_question(&self, question_id: Uuid) {
        let expired = self
            .lock_questions()
            .ok()
            .and_then(|questions| {
                questions
                    .get(&question_id)
                    .map(|entry| entry.expires_at_ms <= now_ms())
            })
            .unwrap_or(false);
        if expired {
            self.resolve_question(question_id, QuestionState::Expired, None, true);
        }
    }

    fn schedule_expiry(&self, question_id: Uuid, expires_at_ms: u64) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let weak = Arc::downgrade(&self.0);
        let delay = expires_at_ms.saturating_sub(now_ms());
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            if let Some(inner) = weak.upgrade() {
                SessionManager(inner).expire_question(question_id);
            }
        });
    }

    fn cancel_questions_for_run(&self, run_id: Uuid, state: QuestionState, notify_agent: bool) {
        let ids: Vec<Uuid> = self
            .lock_questions()
            .map(|questions| {
                questions
                    .iter()
                    .filter(|(_, entry)| entry.run_id == run_id)
                    .map(|(id, _)| *id)
                    .collect()
            })
            .unwrap_or_default();
        for id in ids {
            self.resolve_question(id, state, None, notify_agent);
        }
    }

    fn cancel_questions_for_session(
        &self,
        session_id: Uuid,
        state: QuestionState,
        notify_agent: bool,
    ) {
        let ids: Vec<Uuid> = self
            .lock_questions()
            .map(|questions| {
                questions
                    .iter()
                    .filter(|(_, entry)| entry.acp_session_id == session_id)
                    .map(|(id, _)| *id)
                    .collect()
            })
            .unwrap_or_default();
        for id in ids {
            self.resolve_question(id, state, None, notify_agent);
        }
    }

    fn dispatch(&self, session_id: Uuid) {
        let mut scheduled = None;
        let mut send_failed = None;
        let mut store_failed = None;
        let mut skipped = false;
        {
            let Ok(mut sessions) = self.lock_connections() else {
                return;
            };
            let Some(control) = sessions.get_mut(&session_id) else {
                return;
            };
            if control.active_run.is_some() || control.closing || control.queue.is_empty() {
                return;
            }
            let queued = control.queue.pop_front().expect("checked queue");
            let queued_id = queued.id;
            match self.0.data.update_run(queued.id, |run| {
                run.mark_running();
            }) {
                Ok(run) if run.state == RunState::Running => {
                    if control
                        .input
                        .send(ExecutionInput::Prompt {
                            run_id: run.id,
                            prompt: queued.prompt,
                        })
                        .is_err()
                    {
                        send_failed =
                            Some((queued_id, "ACP session input channel closed".to_owned()));
                    } else {
                        control.active_run = Some(run.id);
                        scheduled = Some(run);
                    }
                }
                Ok(_) => skipped = true,
                Err(error) => {
                    control.queue.push_front(queued);
                    store_failed = Some((queued_id, error.to_string()));
                }
            }
        }
        if let Some(run) = scheduled {
            let session_id = self
                .0
                .data
                .get_session(session_id)
                .map(|session| session.session_id);
            if let Ok(session_id) = session_id {
                self.emit(SessionEvent::RunChanged { session_id, run });
            }
        }
        if skipped {
            self.dispatch(session_id);
            return;
        }
        if let Some((run_id, message)) = store_failed {
            self.emit_error(None, Some(session_id), Some(run_id), message);
            return;
        }
        if let Some((run_id, message)) = send_failed {
            if let Ok(run) = self.0.data.update_run(run_id, |run| {
                run.interrupt(message.clone());
            }) && let Ok(session) = self.0.data.get_session(session_id)
            {
                self.emit(SessionEvent::RunChanged {
                    session_id: session.session_id,
                    run,
                });
            }
            self.emit_error(None, Some(session_id), Some(run_id), message);
        }
    }

    fn after_run(&self, session_id: Uuid, run_id: Uuid) {
        let (detached, closing) = {
            let Ok(mut sessions) = self.lock_connections() else {
                return;
            };
            let Some(control) = sessions.get_mut(&session_id) else {
                return;
            };
            if control.active_run == Some(run_id) {
                control.active_run = None;
            }
            (control.detached, control.closing)
        };
        if detached {
            // Rounds accepted before the conversation ended may still run; the
            // session is released once that queue drains.
            self.dispatch(session_id);
            self.close_if_idle(session_id);
        } else if !closing {
            self.dispatch(session_id);
        }
    }

    fn close_if_idle(&self, session_id: Uuid) {
        let (record, send_failed) = {
            let Ok(mut sessions) = self.lock_connections() else {
                return;
            };
            let Some(control) = sessions.get_mut(&session_id) else {
                return;
            };
            if !control.detached
                || control.active_run.is_some()
                || control.closing
                || !control.queue.is_empty()
            {
                return;
            }
            let record = match self.0.data.update_session(session_id, |session| {
                session.request_close();
            }) {
                Ok(record) => record,
                Err(error) => {
                    self.emit_error(
                        Some(control.session_id),
                        Some(session_id),
                        None,
                        error.to_string(),
                    );
                    return;
                }
            };
            control.closing = true;
            let failed = control.input.send(ExecutionInput::Close).is_err();
            (record, failed)
        };
        self.emit(SessionEvent::AcpSessionChanged(record));
        let _ = send_failed; // The supervisor confirms cleanup, even if the input channel closed.
    }

    fn finalize_session(&self, session_id: Uuid, result: Result<()>) -> Result<()> {
        let control = {
            let mut sessions = self.lock_connections()?;
            sessions.remove(&session_id)
        };
        let Some(control) = control else {
            self.0.idle.notify_waiters();
            return Ok(());
        };
        let outcome = self.finalize_control(session_id, control, result);
        self.0.idle.notify_waiters();
        outcome
    }

    fn finalize_control(
        &self,
        session_id: Uuid,
        control: SessionControl,
        result: Result<()>,
    ) -> Result<()> {
        let session = self.0.data.get_session(session_id)?;
        self.cancel_questions_for_session(session_id, QuestionState::Cancelled, false);
        if let Some(run_id) = control.active_run {
            let message = match &result {
                Ok(()) => "ACP executor exited while a run was active".to_owned(),
                Err(error) => error.to_string(),
            };
            if let Ok(run) = self.0.data.update_run(run_id, |run| {
                run.interrupt(message);
            }) {
                self.emit(SessionEvent::RunChanged {
                    session_id: session.session_id,
                    run,
                });
            }
        }
        for queued in control.queue {
            let updated = match &result {
                Ok(()) => self.0.data.update_run(queued.id, |run| {
                    run.cancel_before_start("session_closed_before_execution");
                }),
                Err(_) => self.0.data.update_run(queued.id, |run| {
                    run.interrupt("ACP session closed before the run started".into());
                }),
            };
            if let Ok(run) = updated {
                self.emit(SessionEvent::RunChanged {
                    session_id: session.session_id,
                    run,
                });
            }
        }
        for mut run in self.0.data.list_runs(session_id)? {
            if !run.state.is_terminal() {
                run = self.0.data.update_run(run.id, |r| match &result {
                    Err(error) => {
                        r.interrupt(error.to_string());
                    }
                    Ok(()) => {
                        r.cancel_before_start("session_closed_before_execution");
                    }
                })?;
                self.emit(SessionEvent::RunChanged {
                    session_id: session.session_id,
                    run: run.clone(),
                });
            }
            self.0
                .data
                .finish_messages(run.id, run.state != RunState::Completed)?;
        }
        let record = match &result {
            Ok(()) => self.0.data.update_session(session_id, |session| {
                session.closed();
            })?,
            Err(error) => self.0.data.update_session(session_id, |session| {
                session.disconnected(error.to_string());
            })?,
        };
        self.emit(SessionEvent::AcpSessionChanged(record));
        Ok(())
    }

    fn send_input(&self, session_id: Uuid, input: ExecutionInput) -> Result<()> {
        let sessions = self.lock_connections()?;
        let control = sessions
            .get(&session_id)
            .ok_or_else(|| Error::State("ACP session is not connected".into()))?;
        control.input.send(input).map_err(|_| Error::Closed)
    }

    fn status_snapshot(&self, session_id: Uuid) -> Result<AcpStatus> {
        let session = self.0.data.get_session(session_id)?;
        let runs = self.0.data.list_runs(session_id)?;
        let run_ids: HashSet<Uuid> = runs.iter().map(|run| run.id).collect();
        let pending_questions = self
            .0
            .data
            .list_pending_questions()?
            .into_iter()
            .filter(|question| run_ids.contains(&question.run_id))
            .collect();
        Ok(AcpStatus {
            session,
            runs,
            pending_questions,
        })
    }

    fn validate_request(&self, session_id: Uuid, call_id: &str, prompt: &str) -> Result<()> {
        if session_id.is_nil() {
            return Err(Error::Config("conversation id must not be nil".into()));
        }
        if call_id.trim().is_empty() || call_id.len() > 256 {
            return Err(Error::Config("invalid task call id".into()));
        }
        if prompt.trim().is_empty() || prompt.len() > 64 * 1024 {
            return Err(Error::Config("invalid task prompt".into()));
        }
        Ok(())
    }

    fn require_owner(&self, session: &AcpSession, session_id: Uuid) -> Result<()> {
        if session.session_id != session_id {
            return Err(Error::State(
                "ACP session belongs to another conversation".into(),
            ));
        }
        Ok(())
    }

    fn question_timeout_ms(&self) -> u64 {
        u64::try_from(self.0.limits.question_timeout.as_millis()).unwrap_or(u64::MAX)
    }

    fn lock_connections(&self) -> Result<MutexGuard<'_, HashMap<Uuid, SessionControl>>> {
        self.0
            .connections
            .lock()
            .map_err(|_| Error::State("ACP connections lock poisoned".into()))
    }

    fn lock_questions(&self) -> Result<MutexGuard<'_, HashMap<Uuid, PendingQuestion>>> {
        self.0
            .questions
            .lock()
            .map_err(|_| Error::State("pending questions lock poisoned".into()))
    }

    fn lock_detached(&self) -> Result<MutexGuard<'_, HashSet<Uuid>>> {
        self.0
            .detached
            .lock()
            .map_err(|_| Error::State("detached conversations lock poisoned".into()))
    }

    fn emit(&self, event: SessionEvent) {
        let _ = self.0.events.send(event);
    }

    fn emit_error(
        &self,
        session_id: Option<Uuid>,
        acp_session_id: Option<Uuid>,
        run_id: Option<Uuid>,
        message: impl Into<String>,
    ) {
        self.emit(SessionEvent::Error {
            session_id,
            acp_session_id,
            run_id,
            message: message.into(),
        });
    }
}

fn request_key(session_id: Uuid, call_id: &str) -> String {
    format!("{session_id}:{call_id}")
}

fn prepare_input(d: &mut SessionData, acp: &AcpSession, key: &str, prompt: &str) -> Result<Run> {
    let id = Uuid::new_v4();
    let now = now_ms();
    let source = d.messages.iter().rev().find(|message| {
        message.session_id==acp.session_id && matches!(&message.content,
            MessageContent::Model(ModelMessage::Assistant(assistant))
                if assistant.tool_calls().any(|call|key==format!("{}:{}",acp.session_id,call.call_id)))
    }).map(|message|message.source.clone()).unwrap_or(MessageSource::UserText);
    let message = push_message(
        d,
        acp.session_id,
        Some(acp.id),
        Some(id),
        None,
        MessageRole::User,
        source,
        MessageContent::Blocks(vec![serde_json::json!({"type":"text","text":prompt})]),
        MessageState::Complete,
    )?;
    Ok(Run {
        id,
        session_id: acp.session_id,
        input_message_id: message.id,
        acp_session_id: acp.id,
        request_key: key.to_owned(),
        state: RunState::Prepared,
        summary: String::new(),
        error: None,
        stop_reason: None,
        created_at_ms: now,
        updated_at_ms: now,
        started_at_ms: None,
        finished_at_ms: None,
    })
}
