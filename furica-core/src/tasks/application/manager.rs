//! TaskManager coordinates ACP sessions, serial rounds and agent questions.
//!
//! It owns execution facts and conversation bindings. Base-model history stays in
//! `orchestration`; TaskManager only publishes bounded events for that layer.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use tokio::sync::{Notify, broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::super::{
    domain::{
        AcpSessionRecord, AcpSessionState, QuestionState, TaskAnswer, TaskEvent, TaskQuestion,
        TaskQuestionDraft, TaskRun, TaskRunState, TaskStatus, now_ms,
    },
    infrastructure::sqlite::TaskStore,
    ports::{ExecutionEvent, SessionInput, TaskExecutor},
};
use crate::{Error, Result};

#[derive(Clone, Debug)]
pub struct TaskLimits {
    /// Concurrent ACP sessions, each backed by one agent process.
    pub max_sessions: usize,
    /// Queued plus running rounds across all sessions.
    pub max_runs: usize,
    /// Independent deadline for each pending agent question.
    pub question_timeout: Duration,
}

impl Default for TaskLimits {
    fn default() -> Self {
        Self {
            max_sessions: 4,
            max_runs: 32,
            question_timeout: Duration::from_secs(300),
        }
    }
}

#[derive(Clone)]
pub struct TaskManager(Arc<Inner>);

struct Inner {
    store: Arc<TaskStore>,
    executor: Arc<dyn TaskExecutor>,
    workspace: PathBuf,
    limits: TaskLimits,
    sessions: Mutex<HashMap<Uuid, SessionControl>>,
    questions: Mutex<HashMap<Uuid, PendingQuestion>>,
    detached: Mutex<HashSet<Uuid>>,
    shutdown: CancellationToken,
    idle: Notify,
    events: broadcast::Sender<TaskEvent>,
}

struct SessionControl {
    conversation_id: Uuid,
    input: mpsc::UnboundedSender<SessionInput>,
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
    conversation_id: Uuid,
    session_id: Uuid,
    run_id: Uuid,
    expires_at_ms: u64,
}

impl TaskManager {
    pub fn new(
        store: Arc<TaskStore>,
        executor: Arc<dyn TaskExecutor>,
        workspace: PathBuf,
        limits: TaskLimits,
    ) -> Result<Self> {
        if !workspace.is_absolute() || !workspace.is_dir() {
            return Err(Error::Config(
                "workspace must be an existing absolute directory".into(),
            ));
        }
        if limits.max_sessions == 0
            || limits.max_sessions > 64
            || limits.max_runs == 0
            || limits.max_runs > 1024
            || limits.question_timeout.is_zero()
        {
            return Err(Error::Config("invalid task limits".into()));
        }
        Self::recover(&store)?;
        let (events, _) = broadcast::channel(128);
        Ok(Self(Arc::new(Inner {
            store,
            executor,
            workspace: std::fs::canonicalize(workspace)?,
            limits,
            sessions: Mutex::new(HashMap::new()),
            questions: Mutex::new(HashMap::new()),
            detached: Mutex::new(HashSet::new()),
            shutdown: CancellationToken::new(),
            idle: Notify::new(),
            events,
        })))
    }

    /// Interrupted rounds are never replayed automatically. Pending response handles
    /// disappeared with the previous process, so their questions are cancelled.
    fn recover(store: &TaskStore) -> Result<()> {
        for session in store.list_sessions()? {
            if !session.state.is_terminal() {
                store.update_session(session.id, |session| {
                    session
                        .disconnected("core restarted; ACP session is no longer connected".into());
                })?;
            }
        }
        for run in store.list_all_runs()? {
            if !run.state.is_terminal() {
                store.update_run(run.id, |run| {
                    run.interrupt(
                        "core restarted; execution effects require reconciliation".into(),
                    );
                })?;
            }
        }
        for question in store.list_all_questions()? {
            if question.state == QuestionState::Pending {
                store.update_question(question.id, |question| {
                    question.resolve(QuestionState::Cancelled, None);
                })?;
            }
        }
        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TaskEvent> {
        self.0.events.subscribe()
    }

    pub fn session(&self, id: Uuid) -> Result<AcpSessionRecord> {
        self.0.store.get_session(id)
    }

    pub fn run(&self, id: Uuid) -> Result<TaskRun> {
        self.0.store.get_run(id)
    }

    pub fn question(&self, id: Uuid) -> Result<TaskQuestion> {
        self.0.store.get_question(id)
    }

    pub fn recent_runs(&self, limit: usize) -> Result<Vec<TaskRun>> {
        self.0.store.recent_runs(limit)
    }

    pub fn recent_runs_for_conversation(
        &self,
        conversation_id: Uuid,
        limit: usize,
    ) -> Result<Vec<TaskRun>> {
        self.0
            .store
            .recent_runs_for_conversation(conversation_id, limit)
    }

    /// Pending questions keep only their owning conversation alive. The manager
    /// releases this hold when an answer, cancellation or deadline resolves them.
    pub fn has_pending_questions(&self, conversation_id: Uuid) -> Result<bool> {
        Ok(self
            .lock_questions()?
            .values()
            .any(|question| question.conversation_id == conversation_id))
    }

    /// Create one ACP session and queue its first round. The first round is also
    /// the idempotent unit for a retried `start_task` tool call.
    pub fn start(&self, conversation_id: Uuid, call_id: &str, prompt: String) -> Result<TaskRun> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::State("task submission requires a Tokio runtime".into()))?;
        self.validate_request(conversation_id, call_id, &prompt)?;
        if self.0.shutdown.is_cancelled() {
            return Err(Error::Closed);
        }
        let request_key = request_key(conversation_id, call_id);
        if let Some(run) = self.0.store.find_run_by_request(&request_key)? {
            if run.prompt != prompt {
                return Err(Error::State(
                    "request key reused for different content".into(),
                ));
            }
            return Ok(run);
        }
        if self.lock_detached()?.contains(&conversation_id) {
            return Err(Error::State("conversation has ended".into()));
        }
        let now = now_ms();
        let session = AcpSessionRecord {
            id: Uuid::new_v4(),
            conversation_id,
            remote_session_id: None,
            state: AcpSessionState::Opening,
            workspace: self.0.workspace.clone(),
            created_at_ms: now,
            last_activity_ms: now,
            error: None,
        };
        let run = TaskRun {
            id: Uuid::new_v4(),
            acp_session_id: session.id,
            request_key,
            prompt: prompt.clone(),
            state: TaskRunState::Queued,
            summary: String::new(),
            error: None,
            stop_reason: None,
            created_at_ms: now,
            started_at_ms: None,
            finished_at_ms: None,
        };
        let (input_tx, input_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::channel(64);
        let cancel = self.0.shutdown.child_token();
        {
            let mut sessions = self.lock_sessions()?;
            if sessions.len() >= self.0.limits.max_sessions {
                return Err(Error::Busy("ACP sessions"));
            }
            if active_runs(&sessions) >= self.0.limits.max_runs {
                return Err(Error::Busy("task runs"));
            }
            self.0.store.insert_session(&session)?;
            if let Err(error) = self.0.store.insert_run(&run) {
                let message = error.to_string();
                let _ = self.0.store.update_session(session.id, |session| {
                    session.disconnected(message);
                });
                return Err(error);
            }
            sessions.insert(
                session.id,
                SessionControl {
                    conversation_id,
                    input: input_tx,
                    active_run: None,
                    queue: VecDeque::from([QueuedRun {
                        id: run.id,
                        prompt: run.prompt.clone(),
                    }]),
                    detached: false,
                    closing: false,
                },
            );
        }
        self.emit(TaskEvent::SessionChanged(session.clone()));
        self.emit(TaskEvent::RunChanged {
            conversation_id,
            run: run.clone(),
        });
        let manager = self.clone();
        let executor = self.0.executor.clone();
        runtime.spawn(async move {
            manager
                .supervise(session, input_rx, event_tx, event_rx, executor, cancel)
                .await;
        });
        Ok(run)
    }

    /// Queue the next round on an existing ACP session. Rounds on one session are
    /// always serial; different sessions are independent.
    pub fn continue_task(
        &self,
        conversation_id: Uuid,
        call_id: &str,
        session_id: Uuid,
        prompt: String,
    ) -> Result<TaskRun> {
        self.validate_request(conversation_id, call_id, &prompt)?;
        if self.0.shutdown.is_cancelled() {
            return Err(Error::Closed);
        }
        let request_key = request_key(conversation_id, call_id);
        if let Some(run) = self.0.store.find_run_by_request(&request_key)? {
            if run.prompt != prompt {
                return Err(Error::State(
                    "request key reused for different content".into(),
                ));
            }
            if run.acp_session_id != session_id {
                return Err(Error::State(
                    "request key belongs to another ACP session".into(),
                ));
            }
            return Ok(run);
        }
        if self.lock_detached()?.contains(&conversation_id) {
            return Err(Error::State("conversation has ended".into()));
        }
        let session = self.0.store.get_session(session_id)?;
        self.require_owner(&session, conversation_id)?;
        if !session.state.accepts_runs() {
            return Err(Error::State(format!(
                "ACP session does not accept new rounds: {:?}",
                session.state
            )));
        }
        let now = now_ms();
        let run = TaskRun {
            id: Uuid::new_v4(),
            acp_session_id: session_id,
            request_key,
            prompt: prompt.clone(),
            state: TaskRunState::Queued,
            summary: String::new(),
            error: None,
            stop_reason: None,
            created_at_ms: now,
            started_at_ms: None,
            finished_at_ms: None,
        };
        {
            let mut sessions = self.lock_sessions()?;
            if active_runs(&sessions) >= self.0.limits.max_runs {
                return Err(Error::Busy("task runs"));
            }
            let control = sessions
                .get_mut(&session_id)
                .ok_or_else(|| Error::State("ACP session is not connected".into()))?;
            if control.detached {
                return Err(Error::State("conversation has ended".into()));
            }
            if control.closing {
                return Err(Error::State("ACP session is closing".into()));
            }
            self.0.store.insert_run(&run)?;
            control.queue.push_back(QueuedRun {
                id: run.id,
                prompt: run.prompt.clone(),
            });
        }
        self.emit(TaskEvent::RunChanged {
            conversation_id,
            run: run.clone(),
        });
        self.dispatch(session_id);
        Ok(run)
    }

    pub fn status(&self, conversation_id: Uuid, session_id: Uuid) -> Result<TaskStatus> {
        let session = self.0.store.get_session(session_id)?;
        self.require_owner(&session, conversation_id)?;
        self.status_snapshot(session_id)
    }

    /// Answering is a single transition. Repeated, expired and cross-conversation
    /// answers fail before anything reaches the original ACP request.
    pub fn answer(
        &self,
        conversation_id: Uuid,
        question_id: Uuid,
        answer: TaskAnswer,
    ) -> Result<TaskQuestion> {
        if self.0.shutdown.is_cancelled() {
            return Err(Error::Closed);
        }
        if self.lock_detached()?.contains(&conversation_id) {
            return Err(Error::State("conversation has ended".into()));
        }
        let mut questions = self.lock_questions()?;
        let entry = questions
            .get(&question_id)
            .cloned()
            .ok_or_else(|| Error::State("question is not pending".into()))?;
        if entry.conversation_id != conversation_id {
            return Err(Error::State(
                "question belongs to another conversation".into(),
            ));
        }
        if entry.expires_at_ms <= now_ms() {
            drop(questions);
            self.expire_question(question_id);
            return Err(Error::State("question has expired".into()));
        }
        let question = self.0.store.get_question(question_id)?;
        question.validate_answer(&answer)?;
        let state = answer.state();
        if let Err(error) = self.send_input(
            entry.session_id,
            SessionInput::Answer {
                question_id,
                answer: answer.clone(),
            },
        ) {
            if matches!(error, Error::Closed) {
                questions.remove(&question_id);
                let _ = self.0.store.update_question(question_id, |question| {
                    question.resolve(QuestionState::Cancelled, None);
                });
                drop(questions);
                self.emit(TaskEvent::QuestionResolved {
                    conversation_id,
                    run_id: entry.run_id,
                    question_id,
                    state: QuestionState::Cancelled,
                });
            }
            return Err(error);
        }
        questions.remove(&question_id);
        let updated = self.0.store.update_question(question_id, |question| {
            question.resolve(state, Some(answer));
        })?;
        drop(questions);
        self.emit(TaskEvent::QuestionResolved {
            conversation_id,
            run_id: entry.run_id,
            question_id,
            state,
        });
        Ok(updated)
    }

    /// Cancel the active round, or the oldest queued round if nothing is running.
    /// The ACP session stays open when cancellation is confirmed later.
    pub fn cancel_run(&self, conversation_id: Uuid, session_id: Uuid) -> Result<TaskRun> {
        let session = self.0.store.get_session(session_id)?;
        self.require_owner(&session, conversation_id)?;
        let (active, queued) = {
            let mut sessions = self.lock_sessions()?;
            let control = sessions
                .get_mut(&session_id)
                .ok_or_else(|| Error::State("ACP session is not connected".into()))?;
            if let Some(run_id) = control.active_run {
                if control
                    .input
                    .send(SessionInput::CancelRun { run_id })
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
            return self.0.store.get_run(run_id);
        }
        if let Some(queued) = queued {
            let run = self.0.store.update_run(queued.id, |run| {
                run.cancel_before_start("cancelled_before_execution");
            })?;
            self.emit(TaskEvent::RunChanged {
                conversation_id,
                run: run.clone(),
            });
            return Ok(run);
        }
        Err(Error::State("session has no active run".into()))
    }

    /// Explicitly close the session. Pending questions are cancelled; an in-flight
    /// round is cancelled before the connection is released.
    pub fn close(&self, conversation_id: Uuid, session_id: Uuid) -> Result<AcpSessionRecord> {
        let session = self.0.store.get_session(session_id)?;
        self.require_owner(&session, conversation_id)?;
        if session.state == AcpSessionState::Closed {
            return Ok(session);
        }
        let input = {
            let mut sessions = self.lock_sessions()?;
            sessions.get_mut(&session_id).map(|control| {
                control.closing = true;
                control.input.clone()
            })
        };
        let Some(input) = input else {
            let record = self.0.store.update_session(session_id, |session| {
                session.closed();
            })?;
            self.cancel_questions_for_session(session_id, QuestionState::Cancelled, false);
            self.emit(TaskEvent::SessionChanged(record.clone()));
            return Ok(record);
        };
        let record = self.0.store.update_session(session_id, |session| {
            session.request_close();
        })?;
        if input.send(SessionInput::Close).is_err() {
            if let Ok(mut sessions) = self.lock_sessions() {
                sessions.remove(&session_id);
            }
            let record = self.0.store.update_session(session_id, |session| {
                session.closed();
            })?;
            self.cancel_questions_for_session(session_id, QuestionState::Cancelled, false);
            self.emit(TaskEvent::SessionChanged(record.clone()));
            return Ok(record);
        }
        self.cancel_questions_for_session(session_id, QuestionState::Cancelled, true);
        self.emit(TaskEvent::SessionChanged(record.clone()));
        Ok(record)
    }

    /// Called by orchestration when the base-model conversation ends. No new rounds
    /// are accepted; pending questions and queued rounds are cancelled; the active
    /// round may finish before its ACP session is released.
    pub fn detach_conversation(&self, conversation_id: Uuid) {
        let Ok(mut detached) = self.lock_detached() else {
            return;
        };
        if !detached.insert(conversation_id) {
            return;
        }
        let session_ids = {
            let Ok(mut sessions) = self.lock_sessions() else {
                return;
            };
            let mut ids = Vec::new();
            for (id, control) in sessions.iter_mut() {
                if control.conversation_id == conversation_id {
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

    pub async fn shutdown(&self, timeout: Duration) -> Result<()> {
        self.0.shutdown.cancel();
        tokio::time::timeout(timeout, async {
            loop {
                let notified = self.0.idle.notified();
                tokio::pin!(notified);
                // Register before inspecting the condition to avoid lost wakes.
                notified.as_mut().enable();
                if self.lock_sessions()?.is_empty() {
                    return Ok(());
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| Error::Timeout("task shutdown"))?
    }

    pub(crate) fn request_shutdown(&self) {
        self.0.shutdown.cancel();
    }

    async fn supervise(
        self,
        session: AcpSessionRecord,
        inputs: mpsc::UnboundedReceiver<SessionInput>,
        event_tx: mpsc::Sender<ExecutionEvent>,
        mut event_rx: mpsc::Receiver<ExecutionEvent>,
        executor: Arc<dyn TaskExecutor>,
        cancel: CancellationToken,
    ) {
        let session_id = session.id;
        let mut future = Box::pin(executor.run_session(session, inputs, event_tx, cancel));
        let result = loop {
            tokio::select! {
                result = &mut future => break result,
                event = event_rx.recv() => match event {
                    Some(event) => {
                        if let Err(error) = self.handle_event(session_id, event) {
                            self.emit_error(None, Some(session_id), None, error.to_string());
                        }
                    }
                    None => break (&mut future).await,
                },
            }
        };
        while let Ok(event) = event_rx.try_recv() {
            if let Err(error) = self.handle_event(session_id, event) {
                self.emit_error(None, Some(session_id), None, error.to_string());
            }
        }
        if let Err(error) = self.finalize_session(session_id, result) {
            self.emit_error(None, Some(session_id), None, error.to_string());
        }
    }

    fn handle_event(&self, session_id: Uuid, event: ExecutionEvent) -> Result<()> {
        let conversation_id = self.0.store.get_session(session_id)?.conversation_id;
        match event {
            ExecutionEvent::Opened { remote_session_id } => {
                let record = self.0.store.update_session(session_id, |session| {
                    session.opened(remote_session_id);
                })?;
                self.emit(TaskEvent::SessionChanged(record));
                let (closing, detached) = self
                    .lock_sessions()
                    .ok()
                    .and_then(|sessions| {
                        sessions
                            .get(&session_id)
                            .map(|control| (control.closing, control.detached))
                    })
                    .unwrap_or((false, false));
                if closing {
                    let _ = self.send_input(session_id, SessionInput::Close);
                } else {
                    // A detached conversation may still have rounds accepted
                    // before it ended; run them, then release the session.
                    self.dispatch(session_id);
                    if detached {
                        self.close_if_idle(session_id);
                    }
                }
            }
            ExecutionEvent::Update { run_id, update } => {
                self.emit(TaskEvent::Progress {
                    conversation_id,
                    run_id,
                    update,
                });
            }
            ExecutionEvent::RunFinished { run_id, outcome } => {
                let existing = self.0.store.get_run(run_id)?;
                if existing.acp_session_id != session_id {
                    return Err(Error::Protocol(
                        "run result came from another ACP session".into(),
                    ));
                }
                let run = self.0.store.update_run(run_id, |run| {
                    run.finish(outcome);
                })?;
                self.emit(TaskEvent::RunChanged {
                    conversation_id,
                    run,
                });
                // A well-behaved agent resolves questions before ending the round.
                // Cancelling leftovers keeps late handles from outliving the run.
                self.cancel_questions_for_run(run_id, QuestionState::Cancelled, true);
                self.after_run(session_id, run_id);
            }
            ExecutionEvent::QuestionAsked { question } => {
                self.register_question(session_id, conversation_id, question)?;
            }
            ExecutionEvent::QuestionResolved { question_id, state } => {
                self.resolve_question(question_id, state, None, false);
            }
        }
        Ok(())
    }

    fn register_question(
        &self,
        session_id: Uuid,
        conversation_id: Uuid,
        draft: TaskQuestionDraft,
    ) -> Result<()> {
        let session = self.0.store.get_session(session_id)?;
        let run = self.0.store.get_run(draft.run_id)?;
        if !session.state.accepts_runs()
            || run.acp_session_id != session_id
            || run.state != TaskRunState::Running
        {
            let _ = self.send_input(
                session_id,
                SessionInput::Answer {
                    question_id: draft.id,
                    answer: TaskAnswer::Cancel,
                },
            );
            return Ok(());
        }
        let now = now_ms();
        let expires_at_ms = now.saturating_add(self.question_timeout_ms());
        let question = TaskQuestion {
            id: draft.id,
            run_id: draft.run_id,
            conversation_id,
            acp_session_id: session_id,
            kind: draft.kind,
            state: QuestionState::Pending,
            answer: None,
            created_at_ms: now,
            expires_at_ms,
            resolved_at_ms: None,
        };
        self.0.store.insert_question(&question)?;
        let detached = self
            .lock_sessions()
            .ok()
            .and_then(|sessions| sessions.get(&session_id).map(|control| control.detached))
            .unwrap_or(false);
        self.lock_questions()?.insert(
            question.id,
            PendingQuestion {
                conversation_id,
                session_id,
                run_id: question.run_id,
                expires_at_ms,
            },
        );
        self.emit(TaskEvent::QuestionAsked {
            conversation_id,
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
        answer: Option<TaskAnswer>,
        notify_agent: bool,
    ) -> Option<TaskQuestion> {
        let (entry, question) = {
            let mut questions = self.lock_questions().ok()?;
            let entry = questions.get(&question_id).cloned()?;
            let mut current = self.0.store.get_question(question_id).ok()?;
            if !current.resolve(state, answer.clone()) {
                questions.remove(&question_id);
                return None;
            }
            let updated = self
                .0
                .store
                .update_question(question_id, |question| {
                    question.resolve(state, answer);
                })
                .ok()?;
            questions.remove(&question_id);
            (entry, updated)
        };
        if notify_agent {
            let _ = self.send_input(
                entry.session_id,
                SessionInput::Answer {
                    question_id,
                    answer: TaskAnswer::Cancel,
                },
            );
        }
        self.emit(TaskEvent::QuestionResolved {
            conversation_id: entry.conversation_id,
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
                TaskManager(inner).expire_question(question_id);
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
                    .filter(|(_, entry)| entry.session_id == session_id)
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
            let Ok(mut sessions) = self.lock_sessions() else {
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
            match self.0.store.update_run(queued.id, |run| {
                run.mark_running();
            }) {
                Ok(run) if run.state == TaskRunState::Running => {
                    if control
                        .input
                        .send(SessionInput::Prompt {
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
            let conversation_id = self
                .0
                .store
                .get_session(session_id)
                .map(|session| session.conversation_id);
            if let Ok(conversation_id) = conversation_id {
                self.emit(TaskEvent::RunChanged {
                    conversation_id,
                    run,
                });
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
            if let Ok(run) = self.0.store.update_run(run_id, |run| {
                run.interrupt(message.clone());
            }) && let Ok(session) = self.0.store.get_session(session_id)
            {
                self.emit(TaskEvent::RunChanged {
                    conversation_id: session.conversation_id,
                    run,
                });
            }
            self.emit_error(None, Some(session_id), Some(run_id), message);
        }
    }

    fn after_run(&self, session_id: Uuid, run_id: Uuid) {
        let (detached, closing) = {
            let Ok(mut sessions) = self.lock_sessions() else {
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
            let Ok(mut sessions) = self.lock_sessions() else {
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
            let record = match self.0.store.update_session(session_id, |session| {
                session.request_close();
            }) {
                Ok(record) => record,
                Err(error) => {
                    self.emit_error(
                        Some(control.conversation_id),
                        Some(session_id),
                        None,
                        error.to_string(),
                    );
                    return;
                }
            };
            control.closing = true;
            let failed = control.input.send(SessionInput::Close).is_err();
            (record, failed)
        };
        self.emit(TaskEvent::SessionChanged(record));
        if send_failed {
            if let Ok(mut sessions) = self.lock_sessions() {
                sessions.remove(&session_id);
            }
            if let Ok(record) = self.0.store.update_session(session_id, |session| {
                session.closed();
            }) {
                self.cancel_questions_for_session(session_id, QuestionState::Cancelled, false);
                self.emit(TaskEvent::SessionChanged(record));
            }
        }
    }

    fn finalize_session(&self, session_id: Uuid, result: Result<()>) -> Result<()> {
        let control = {
            let mut sessions = self.lock_sessions()?;
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
        let session = self.0.store.get_session(session_id)?;
        self.cancel_questions_for_session(session_id, QuestionState::Cancelled, false);
        if let Some(run_id) = control.active_run {
            let message = match &result {
                Ok(()) => "ACP executor exited while a run was active".to_owned(),
                Err(error) => error.to_string(),
            };
            if let Ok(run) = self.0.store.update_run(run_id, |run| {
                run.interrupt(message);
            }) {
                self.emit(TaskEvent::RunChanged {
                    conversation_id: session.conversation_id,
                    run,
                });
            }
        }
        for queued in control.queue {
            let updated = match &result {
                Ok(()) => self.0.store.update_run(queued.id, |run| {
                    run.cancel_before_start("session_closed_before_execution");
                }),
                Err(_) => self.0.store.update_run(queued.id, |run| {
                    run.interrupt("ACP session closed before the run started".into());
                }),
            };
            if let Ok(run) = updated {
                self.emit(TaskEvent::RunChanged {
                    conversation_id: session.conversation_id,
                    run,
                });
            }
        }
        let record = match &result {
            Ok(()) => self.0.store.update_session(session_id, |session| {
                session.closed();
            })?,
            Err(error) => self.0.store.update_session(session_id, |session| {
                session.disconnected(error.to_string());
            })?,
        };
        self.emit(TaskEvent::SessionChanged(record));
        Ok(())
    }

    fn send_input(&self, session_id: Uuid, input: SessionInput) -> Result<()> {
        let sessions = self.lock_sessions()?;
        let control = sessions
            .get(&session_id)
            .ok_or_else(|| Error::State("ACP session is not connected".into()))?;
        control.input.send(input).map_err(|_| Error::Closed)
    }

    fn status_snapshot(&self, session_id: Uuid) -> Result<TaskStatus> {
        let session = self.0.store.get_session(session_id)?;
        let runs = self.0.store.list_runs(session_id)?;
        let run_ids: HashSet<Uuid> = runs.iter().map(|run| run.id).collect();
        let pending_questions = self
            .0
            .store
            .list_pending_questions()?
            .into_iter()
            .filter(|question| run_ids.contains(&question.run_id))
            .collect();
        Ok(TaskStatus {
            session,
            runs,
            pending_questions,
        })
    }

    fn validate_request(&self, conversation_id: Uuid, call_id: &str, prompt: &str) -> Result<()> {
        if conversation_id.is_nil() {
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

    fn require_owner(&self, session: &AcpSessionRecord, conversation_id: Uuid) -> Result<()> {
        if session.conversation_id != conversation_id {
            return Err(Error::State(
                "ACP session belongs to another conversation".into(),
            ));
        }
        Ok(())
    }

    fn question_timeout_ms(&self) -> u64 {
        u64::try_from(self.0.limits.question_timeout.as_millis()).unwrap_or(u64::MAX)
    }

    fn lock_sessions(&self) -> Result<MutexGuard<'_, HashMap<Uuid, SessionControl>>> {
        self.0
            .sessions
            .lock()
            .map_err(|_| Error::State("task sessions lock poisoned".into()))
    }

    fn lock_questions(&self) -> Result<MutexGuard<'_, HashMap<Uuid, PendingQuestion>>> {
        self.0
            .questions
            .lock()
            .map_err(|_| Error::State("task questions lock poisoned".into()))
    }

    fn lock_detached(&self) -> Result<MutexGuard<'_, HashSet<Uuid>>> {
        self.0
            .detached
            .lock()
            .map_err(|_| Error::State("detached conversations lock poisoned".into()))
    }

    fn emit(&self, event: TaskEvent) {
        let _ = self.0.events.send(event);
    }

    fn emit_error(
        &self,
        conversation_id: Option<Uuid>,
        session_id: Option<Uuid>,
        run_id: Option<Uuid>,
        message: impl Into<String>,
    ) {
        self.emit(TaskEvent::Error {
            conversation_id,
            session_id,
            run_id,
            message: message.into(),
        });
    }
}

fn request_key(conversation_id: Uuid, call_id: &str) -> String {
    format!("{conversation_id}:{call_id}")
}

fn active_runs(sessions: &HashMap<Uuid, SessionControl>) -> usize {
    sessions
        .values()
        .map(|control| usize::from(control.active_run.is_some()) + control.queue.len())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::path::Path;

    struct NoopExecutor;

    #[async_trait]
    impl TaskExecutor for NoopExecutor {
        async fn run_session(
            &self,
            _session: AcpSessionRecord,
            _inputs: mpsc::UnboundedReceiver<SessionInput>,
            _events: mpsc::Sender<ExecutionEvent>,
            _cancel: CancellationToken,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn recovery_interrupts_runs_and_cancels_questions() {
        let store = Arc::new(TaskStore::in_memory().unwrap());
        let now = now_ms();
        let session = AcpSessionRecord {
            id: Uuid::new_v4(),
            conversation_id: Uuid::new_v4(),
            remote_session_id: Some("remote".into()),
            state: AcpSessionState::Ready,
            workspace: Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf(),
            created_at_ms: now,
            last_activity_ms: now,
            error: None,
        };
        store.insert_session(&session).unwrap();
        let run = TaskRun {
            id: Uuid::new_v4(),
            acp_session_id: session.id,
            request_key: "recovery".into(),
            prompt: "unfinished".into(),
            state: TaskRunState::Running,
            summary: String::new(),
            error: None,
            stop_reason: None,
            created_at_ms: now,
            started_at_ms: Some(now),
            finished_at_ms: None,
        };
        store.insert_run(&run).unwrap();
        let question = TaskQuestion {
            id: Uuid::new_v4(),
            run_id: run.id,
            conversation_id: session.conversation_id,
            acp_session_id: session.id,
            kind: crate::tasks::domain::TaskQuestionKind::Elicitation {
                message: "still waiting".into(),
                schema: serde_json::Value::Null,
            },
            state: QuestionState::Pending,
            answer: None,
            created_at_ms: now,
            expires_at_ms: now + 60_000,
            resolved_at_ms: None,
        };
        store.insert_question(&question).unwrap();

        let manager = TaskManager::new(
            store,
            Arc::new(NoopExecutor),
            Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf(),
            TaskLimits::default(),
        )
        .unwrap();

        assert_eq!(
            manager.session(session.id).unwrap().state,
            AcpSessionState::Disconnected
        );
        assert_eq!(
            manager.run(run.id).unwrap().state,
            TaskRunState::Interrupted
        );
        assert_eq!(
            manager.question(question.id).unwrap().state,
            QuestionState::Cancelled
        );
        manager.shutdown(Duration::from_secs(1)).await.unwrap();
    }
}
