//! Task use cases: idempotent submission, scheduling, cancellation and recovery.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::sync::{Notify, Semaphore, broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    adapters::sqlite::TaskStore,
    domain::{TaskEvent, TaskOutcome, TaskRecord, TaskState},
    ports::{ExecutionEvent, TaskExecutor},
};
use crate::{Error, Result};

#[derive(Clone, Debug)]
pub struct TaskLimits {
    pub max_concurrent: usize,
    pub max_pending: usize,
    pub execution_timeout: Duration,
}

impl Default for TaskLimits {
    fn default() -> Self {
        Self {
            max_concurrent: 1,
            max_pending: 32,
            execution_timeout: Duration::from_secs(600),
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
    slots: Arc<Semaphore>,
    controls: Mutex<HashMap<Uuid, CancellationToken>>,
    shutdown: CancellationToken,
    idle: Notify,
    events: broadcast::Sender<TaskEvent>,
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
        if limits.max_concurrent == 0
            || limits.max_pending < limits.max_concurrent
            || limits.max_pending > 1024
            || limits.execution_timeout.is_zero()
        {
            return Err(Error::Config("invalid task limits".into()));
        }
        for record in store.list()? {
            if !record.state.is_terminal() {
                store.update(record.id, TaskRecord::recover)?;
            }
        }
        let (events, _) = broadcast::channel(128);
        Ok(Self(Arc::new(Inner {
            store,
            executor,
            workspace: std::fs::canonicalize(workspace)?,
            slots: Arc::new(Semaphore::new(limits.max_concurrent)),
            limits,
            controls: Mutex::new(HashMap::new()),
            shutdown: CancellationToken::new(),
            idle: Notify::new(),
            events,
        })))
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TaskEvent> {
        self.0.events.subscribe()
    }
    pub fn get(&self, id: Uuid) -> Result<TaskRecord> {
        self.0.store.get(id)
    }
    pub fn list(&self) -> Result<Vec<TaskRecord>> {
        self.0.store.list()
    }

    pub fn recent(&self, limit: usize) -> Result<Vec<TaskRecord>> {
        self.0.store.recent(limit)
    }

    /// Idempotency is scoped by the host's stable request key. Reusing a key for
    /// different content is an error; interrupted tasks are never auto-replayed.
    pub fn submit(&self, request_key: String, prompt: String) -> Result<TaskRecord> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::State("task submission requires a Tokio runtime".into()))?;
        if self.0.shutdown.is_cancelled() {
            return Err(Error::Closed);
        }
        if request_key.is_empty()
            || request_key.len() > 512
            || prompt.trim().is_empty()
            || prompt.len() > 64 * 1024
        {
            return Err(Error::Config("invalid task request".into()));
        }
        let mut controls = self
            .0
            .controls
            .lock()
            .map_err(|_| Error::State("task lock poisoned".into()))?;
        if self.0.shutdown.is_cancelled() {
            return Err(Error::Closed);
        }
        if let Some(record) = self.0.store.find_request(&request_key)? {
            if record.prompt != prompt {
                return Err(Error::State(
                    "request key reused for different content".into(),
                ));
            }
            return Ok(record);
        }
        if controls.len() >= self.0.limits.max_pending {
            return Err(Error::Busy("pending tasks"));
        }
        let record = TaskRecord {
            id: Uuid::new_v4(),
            request_key,
            prompt,
            workspace: self.0.workspace.clone(),
            state: TaskState::Queued,
            session_id: None,
            summary: String::new(),
            error: None,
            stop_reason: None,
            created_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };
        self.0.store.insert(&record)?;
        let cancel = self.0.shutdown.child_token();
        controls.insert(record.id, cancel.clone());
        let _ = self.0.events.send(TaskEvent::Changed(record.clone()));
        let manager = self.clone();
        let task = record.clone();
        runtime.spawn(async move {
            let id = task.id;
            if let Err(error) = manager.run(task, cancel).await {
                let _ = manager.0.store.update(id, |record| {
                    record.interrupt(error.to_string());
                });
                let _ = manager.0.events.send(TaskEvent::Error {
                    task_id: id,
                    message: error.to_string(),
                });
            }
            if let Ok(mut controls) = manager.0.controls.lock() {
                controls.remove(&id);
            }
            manager.0.idle.notify_waiters();
        });
        Ok(record)
    }

    pub fn cancel(&self, id: Uuid) -> Result<TaskRecord> {
        let controls = self
            .0
            .controls
            .lock()
            .map_err(|_| Error::State("task lock poisoned".into()))?;
        let mut changed = false;
        let record = self.0.store.update(id, |record| {
            changed = record.request_cancellation();
        })?;
        if let Some(cancel) = controls.get(&id) {
            cancel.cancel();
        }
        if changed {
            let _ = self.0.events.send(TaskEvent::Changed(record.clone()));
        }
        Ok(record)
    }

    pub async fn shutdown(&self, timeout: Duration) -> Result<()> {
        self.0.shutdown.cancel();
        tokio::time::timeout(timeout, async {
            loop {
                let notified = self.0.idle.notified();
                // Register before inspecting the condition to avoid lost wakes.
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self
                    .0
                    .controls
                    .lock()
                    .map_err(|_| Error::State("task lock poisoned".into()))?
                    .is_empty()
                {
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

    async fn run(&self, task: TaskRecord, cancel: CancellationToken) -> Result<()> {
        let permit = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                self.finish(task.id, Ok(TaskOutcome { state: TaskState::Cancelled,
                    summary: String::new(), stop_reason: "cancelled_before_execution".into() }))?;
                return Ok(());
            }
            permit = self.0.slots.clone().acquire_owned() => permit.map_err(|_| Error::Closed)?,
        };
        if cancel.is_cancelled() {
            self.finish(
                task.id,
                Ok(TaskOutcome {
                    state: TaskState::Cancelled,
                    summary: String::new(),
                    stop_reason: "cancelled_before_execution".into(),
                }),
            )?;
            return Ok(());
        }
        let record = self.0.store.update(task.id, TaskRecord::mark_running)?;
        let _ = self.0.events.send(TaskEvent::Changed(record));
        let (tx, mut rx) = mpsc::channel(64);
        let future = tokio::time::timeout(
            self.0.limits.execution_timeout,
            self.0.executor.execute(task.clone(), cancel, tx),
        );
        tokio::pin!(future);
        let outcome = loop {
            tokio::select! {
                result = &mut future => break result.unwrap_or(Err(Error::Timeout("task execution"))),
                Some(event) = rx.recv() => self.progress(task.id, event)?,
            }
        };
        while let Ok(event) = rx.try_recv() {
            self.progress(task.id, event)?;
        }
        self.finish(task.id, outcome)?;
        drop(permit);
        Ok(())
    }

    fn progress(&self, id: Uuid, event: ExecutionEvent) -> Result<()> {
        match event {
            ExecutionEvent::SessionCreated(session_id) => {
                let record = self
                    .0
                    .store
                    .update(id, |record| record.session_id = Some(session_id))?;
                let _ = self.0.events.send(TaskEvent::Changed(record));
            }
            ExecutionEvent::Update(update) => {
                let _ = self.0.events.send(TaskEvent::Progress {
                    task_id: id,
                    update,
                });
            }
        }
        Ok(())
    }

    fn finish(&self, id: Uuid, outcome: Result<TaskOutcome>) -> Result<()> {
        let record = self.0.store.update(id, |record| record.finish(outcome))?;
        let _ = self.0.events.send(TaskEvent::Changed(record));
        Ok(())
    }
}
