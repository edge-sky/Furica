//! SQLite snapshots for ACP sessions, execution rounds and questions.
//!
//! Lifecycle and recovery policy belong to `TaskManager`; this layer only stores
//! bounded snapshots and indexed lookup columns. Conversation history is never
//! persisted here.

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::{
    Error, Result,
    tasks::domain::{AcpSessionRecord, AcpSessionState, TaskQuestion, TaskRun, TaskRunState},
};

/// One TaskManager must own a database at a time; this is not a multi-process queue.
pub struct TaskStore {
    connection: Mutex<Connection>,
}

impl TaskStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_connection(Connection::open(path)?)
    }

    pub fn in_memory() -> Result<Self> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self> {
        connection.busy_timeout(std::time::Duration::from_secs(2))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS acp_sessions (
                 id TEXT PRIMARY KEY,
                 conversation_id TEXT NOT NULL,
                 state TEXT NOT NULL,
                 record TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS acp_sessions_conversation
                 ON acp_sessions(conversation_id);
             CREATE TABLE IF NOT EXISTS task_runs (
                 id TEXT PRIMARY KEY,
                 session_id TEXT NOT NULL,
                 request_key TEXT NOT NULL UNIQUE,
                 state TEXT NOT NULL,
                 record TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS task_runs_session ON task_runs(session_id);
             CREATE TABLE IF NOT EXISTS task_questions (
                 id TEXT PRIMARY KEY,
                 run_id TEXT NOT NULL,
                 state TEXT NOT NULL,
                 record TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS task_questions_run ON task_questions(run_id);
             CREATE INDEX IF NOT EXISTS task_questions_state ON task_questions(state);",
        )?;
        let store = Self {
            connection: Mutex::new(connection),
        };
        store.migrate_legacy_tasks()?;
        Ok(store)
    }

    /// The previous schema kept one `TaskRecord` per one-shot execution. Preserve
    /// that history as closed sessions with interrupted or terminal runs; old rows
    /// have no conversation binding and remain read-only for tool calls.
    fn migrate_legacy_tasks(&self) -> Result<()> {
        let exists: bool = self.lock()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='tasks')",
            [],
            |row| row.get(0),
        )?;
        if !exists {
            return Ok(());
        }
        let records: Vec<String> = {
            let connection = self.lock()?;
            let mut statement = connection.prepare("SELECT record FROM tasks ORDER BY rowid")?;
            let records = statement.query_map([], |row| row.get::<_, String>(0))?;
            records.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for json in records {
            let Ok(legacy) = serde_json::from_str::<LegacyTask>(&json) else {
                continue;
            };
            if self.find_run_by_request(&legacy.request_key)?.is_some()
                || self.get_run(legacy.id).is_ok()
            {
                continue;
            }
            let created_at_ms = legacy.created_at_ms;
            let session = AcpSessionRecord {
                id: Uuid::new_v4(),
                conversation_id: Uuid::nil(),
                remote_session_id: legacy.session_id,
                state: AcpSessionState::Closed,
                workspace: legacy.workspace,
                created_at_ms,
                last_activity_ms: created_at_ms,
                error: None,
            };
            let (state, error) = legacy.state.into_run(legacy.error);
            let run = TaskRun {
                id: legacy.id,
                acp_session_id: session.id,
                request_key: legacy.request_key,
                prompt: legacy.prompt,
                state,
                summary: legacy.summary,
                error,
                stop_reason: legacy.stop_reason,
                created_at_ms,
                started_at_ms: None,
                finished_at_ms: Some(created_at_ms),
            };
            self.insert_session(&session)?;
            self.insert_run(&run)?;
        }
        Ok(())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| Error::State("task store lock poisoned".into()))
    }

    fn load<T: DeserializeOwned>(&self, table: &str, id: Uuid) -> Result<T> {
        let json: Option<String> = self
            .lock()?
            .query_row(
                &format!("SELECT record FROM {table} WHERE id=?1"),
                [id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        serde_json::from_str(&json.ok_or_else(|| Error::TaskNotFound(id.to_string()))?)
            .map_err(Into::into)
    }

    fn list_records<T: DeserializeOwned>(&self, sql: &str) -> Result<Vec<T>> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(sql)?;
        let records = statement.query_map([], |row| row.get::<_, String>(0))?;
        records
            .map(|json| serde_json::from_str(&json?).map_err(Into::into))
            .collect()
    }

    fn list_records_with<T: DeserializeOwned>(
        &self,
        sql: &str,
        argument: impl rusqlite::ToSql,
    ) -> Result<Vec<T>> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(sql)?;
        let records = statement.query_map([argument], |row| row.get::<_, String>(0))?;
        records
            .map(|json| serde_json::from_str(&json?).map_err(Into::into))
            .collect()
    }

    pub fn get_session(&self, id: Uuid) -> Result<AcpSessionRecord> {
        self.load("acp_sessions", id)
    }

    pub fn get_run(&self, id: Uuid) -> Result<TaskRun> {
        self.load("task_runs", id)
    }

    pub fn get_question(&self, id: Uuid) -> Result<TaskQuestion> {
        self.load("task_questions", id)
    }

    pub fn find_run_by_request(&self, request_key: &str) -> Result<Option<TaskRun>> {
        let json: Option<String> = self
            .lock()?
            .query_row(
                "SELECT record FROM task_runs WHERE request_key=?1",
                [request_key],
                |row| row.get(0),
            )
            .optional()?;
        json.map(|json| serde_json::from_str(&json).map_err(Into::into))
            .transpose()
    }

    pub fn list_sessions(&self) -> Result<Vec<AcpSessionRecord>> {
        self.list_records("SELECT record FROM acp_sessions ORDER BY rowid")
    }

    pub fn list_runs(&self, session_id: Uuid) -> Result<Vec<TaskRun>> {
        self.list_records_with(
            "SELECT record FROM task_runs WHERE session_id=?1 ORDER BY rowid",
            session_id.to_string(),
        )
    }

    pub fn list_all_runs(&self) -> Result<Vec<TaskRun>> {
        self.list_records("SELECT record FROM task_runs ORDER BY rowid")
    }

    pub fn recent_runs(&self, limit: usize) -> Result<Vec<TaskRun>> {
        if limit == 0 || limit > 1000 {
            return Err(Error::Config("invalid task snapshot limit".into()));
        }
        let connection = self.lock()?;
        let mut statement =
            connection.prepare("SELECT record FROM task_runs ORDER BY rowid DESC LIMIT ?1")?;
        let records = statement.query_map([limit as u32], |row| row.get::<_, String>(0))?;
        records
            .map(|json| serde_json::from_str(&json?).map_err(Into::into))
            .collect()
    }

    /// Filter by ownership before limiting, so other conversations cannot hide
    /// or disclose the current conversation's history.
    pub fn recent_runs_for_conversation(
        &self,
        conversation_id: Uuid,
        limit: usize,
    ) -> Result<Vec<TaskRun>> {
        if limit == 0 || limit > 1000 {
            return Err(Error::Config("invalid task snapshot limit".into()));
        }
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            "SELECT r.record FROM task_runs r
             JOIN acp_sessions s ON s.id = r.session_id
             WHERE s.conversation_id = ?1
             ORDER BY r.rowid DESC LIMIT ?2",
        )?;
        let records = statement
            .query_map(params![conversation_id.to_string(), limit as u32], |row| {
                row.get::<_, String>(0)
            })?;
        records
            .map(|json| serde_json::from_str(&json?).map_err(Into::into))
            .collect()
    }

    pub fn list_all_questions(&self) -> Result<Vec<TaskQuestion>> {
        self.list_records("SELECT record FROM task_questions ORDER BY rowid")
    }

    pub fn list_pending_questions(&self) -> Result<Vec<TaskQuestion>> {
        self.list_records("SELECT record FROM task_questions WHERE state='pending' ORDER BY rowid")
    }

    pub(crate) fn insert_session(&self, record: &AcpSessionRecord) -> Result<()> {
        self.lock()?.execute(
            "INSERT INTO acp_sessions(id,conversation_id,state,record) VALUES(?1,?2,?3,?4)",
            params![
                record.id.to_string(),
                record.conversation_id.to_string(),
                state_text(record.state)?,
                serde_json::to_string(record)?
            ],
        )?;
        Ok(())
    }

    pub(crate) fn update_session(
        &self,
        id: Uuid,
        f: impl FnOnce(&mut AcpSessionRecord),
    ) -> Result<AcpSessionRecord> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        let json: String = transaction
            .query_row(
                "SELECT record FROM acp_sessions WHERE id=?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| Error::TaskNotFound(id.to_string()))?;
        let mut record: AcpSessionRecord = serde_json::from_str(&json)?;
        f(&mut record);
        transaction.execute(
            "UPDATE acp_sessions SET state=?2, record=?3 WHERE id=?1",
            params![
                id.to_string(),
                state_text(record.state)?,
                serde_json::to_string(&record)?
            ],
        )?;
        transaction.commit()?;
        Ok(record)
    }

    pub(crate) fn insert_run(&self, record: &TaskRun) -> Result<()> {
        self.lock()?.execute(
            "INSERT INTO task_runs(id,session_id,request_key,state,record) VALUES(?1,?2,?3,?4,?5)",
            params![
                record.id.to_string(),
                record.acp_session_id.to_string(),
                record.request_key,
                state_text(record.state)?,
                serde_json::to_string(record)?
            ],
        )?;
        Ok(())
    }

    pub(crate) fn update_run(&self, id: Uuid, f: impl FnOnce(&mut TaskRun)) -> Result<TaskRun> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        let json: String = transaction
            .query_row(
                "SELECT record FROM task_runs WHERE id=?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| Error::TaskNotFound(id.to_string()))?;
        let mut record: TaskRun = serde_json::from_str(&json)?;
        f(&mut record);
        transaction.execute(
            "UPDATE task_runs SET state=?2, record=?3 WHERE id=?1",
            params![
                id.to_string(),
                state_text(record.state)?,
                serde_json::to_string(&record)?
            ],
        )?;
        transaction.commit()?;
        Ok(record)
    }

    pub(crate) fn insert_question(&self, record: &TaskQuestion) -> Result<()> {
        self.lock()?.execute(
            "INSERT INTO task_questions(id,run_id,state,record) VALUES(?1,?2,?3,?4)",
            params![
                record.id.to_string(),
                record.run_id.to_string(),
                state_text(record.state)?,
                serde_json::to_string(record)?
            ],
        )?;
        Ok(())
    }

    pub(crate) fn update_question(
        &self,
        id: Uuid,
        f: impl FnOnce(&mut TaskQuestion),
    ) -> Result<TaskQuestion> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        let json: String = transaction
            .query_row(
                "SELECT record FROM task_questions WHERE id=?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| Error::TaskNotFound(id.to_string()))?;
        let mut record: TaskQuestion = serde_json::from_str(&json)?;
        f(&mut record);
        transaction.execute(
            "UPDATE task_questions SET state=?2, record=?3 WHERE id=?1",
            params![
                id.to_string(),
                state_text(record.state)?,
                serde_json::to_string(&record)?
            ],
        )?;
        transaction.commit()?;
        Ok(record)
    }
}

#[derive(Deserialize)]
struct LegacyTask {
    id: Uuid,
    request_key: String,
    prompt: String,
    workspace: PathBuf,
    state: LegacyState,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    summary: String,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    stop_reason: Option<String>,
    #[serde(default)]
    created_at_ms: u64,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LegacyState {
    Queued,
    Running,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

impl LegacyState {
    fn into_run(self, error: Option<String>) -> (TaskRunState, Option<String>) {
        match self {
            Self::Completed => (TaskRunState::Completed, error),
            Self::Failed => (TaskRunState::Failed, error),
            Self::Cancelled => (TaskRunState::Cancelled, error),
            Self::Interrupted | Self::Queued | Self::Running | Self::Cancelling => (
                TaskRunState::Interrupted,
                Some(error.unwrap_or_else(|| {
                    "legacy task was not completed before the core restarted".into()
                })),
            ),
        }
    }
}

fn state_text(state: impl Serialize) -> Result<String> {
    serde_json::to_value(state)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| Error::State("task state is not a string".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_tasks_migrate_once_as_read_only_history() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE tasks (
                     id TEXT PRIMARY KEY,
                     request_key TEXT NOT NULL UNIQUE,
                     record TEXT NOT NULL
                 );",
            )
            .unwrap();
        let record = serde_json::json!({
            "id": Uuid::new_v4(),
            "request_key": "legacy:1",
            "prompt": "old prompt",
            "workspace": "/tmp",
            "state": "running",
            "session_id": "remote-old",
            "summary": "partial",
            "error": null,
            "stop_reason": null,
            "created_at_ms": 1
        });
        connection
            .execute(
                "INSERT INTO tasks(id,request_key,record) VALUES(?1,?2,?3)",
                params![
                    record["id"].as_str().unwrap(),
                    "legacy:1",
                    serde_json::to_string(&record).unwrap()
                ],
            )
            .unwrap();

        let store = TaskStore::from_connection(connection).unwrap();
        let run = store.find_run_by_request("legacy:1").unwrap().unwrap();
        assert_eq!(run.state, TaskRunState::Interrupted);
        let sessions = store.list_sessions().unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].state, AcpSessionState::Closed);
        assert_eq!(sessions[0].conversation_id, Uuid::nil());

        store.migrate_legacy_tasks().unwrap();
        assert_eq!(store.list_sessions().unwrap().len(), 1);
    }
}
