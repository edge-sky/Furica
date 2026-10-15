//! SQLite snapshot operations. Lifecycle and recovery policy belong to tasks.

use std::{path::Path, sync::Mutex};

use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

use crate::{Error, Result, tasks::domain::TaskRecord};

/// Small task snapshots only. Raw audio and API credentials are never persisted.
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
             CREATE TABLE IF NOT EXISTS tasks (
                 id TEXT PRIMARY KEY,
                 request_key TEXT NOT NULL UNIQUE,
                 record TEXT NOT NULL
             );",
        )?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| Error::State("task store lock poisoned".into()))
    }

    pub fn get(&self, id: Uuid) -> Result<TaskRecord> {
        let json: Option<String> = self
            .lock()?
            .query_row(
                "SELECT record FROM tasks WHERE id=?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        serde_json::from_str(&json.ok_or_else(|| Error::TaskNotFound(id.to_string()))?)
            .map_err(Into::into)
    }

    pub fn find_request(&self, key: &str) -> Result<Option<TaskRecord>> {
        let json: Option<String> = self
            .lock()?
            .query_row(
                "SELECT record FROM tasks WHERE request_key=?1",
                [key],
                |row| row.get(0),
            )
            .optional()?;
        json.map(|json| serde_json::from_str(&json).map_err(Into::into))
            .transpose()
    }

    pub fn list(&self) -> Result<Vec<TaskRecord>> {
        let connection = self.lock()?;
        let mut statement = connection.prepare("SELECT record FROM tasks ORDER BY rowid")?;
        let records = statement.query_map([], |row| row.get::<_, String>(0))?;
        records
            .map(|json| serde_json::from_str(&json?).map_err(Into::into))
            .collect()
    }

    pub fn recent(&self, limit: usize) -> Result<Vec<TaskRecord>> {
        if limit == 0 || limit > 1000 {
            return Err(Error::Config("invalid task snapshot limit".into()));
        }
        let connection = self.lock()?;
        let mut statement =
            connection.prepare("SELECT record FROM tasks ORDER BY rowid DESC LIMIT ?1")?;
        let records = statement.query_map([limit as u32], |row| row.get::<_, String>(0))?;
        records
            .map(|json| serde_json::from_str(&json?).map_err(Into::into))
            .collect()
    }

    pub(crate) fn insert(&self, record: &TaskRecord) -> Result<()> {
        self.lock()?.execute(
            "INSERT INTO tasks(id,request_key,record) VALUES(?1,?2,?3)",
            params![
                record.id.to_string(),
                record.request_key,
                serde_json::to_string(record)?
            ],
        )?;
        Ok(())
    }

    pub(crate) fn update(&self, id: Uuid, f: impl FnOnce(&mut TaskRecord)) -> Result<TaskRecord> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        let json: String = transaction
            .query_row(
                "SELECT record FROM tasks WHERE id=?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| Error::TaskNotFound(id.to_string()))?;
        let mut record: TaskRecord = serde_json::from_str(&json)?;
        f(&mut record);
        transaction.execute(
            "UPDATE tasks SET record=?2 WHERE id=?1",
            params![id.to_string(), serde_json::to_string(&record)?],
        )?;
        transaction.commit()?;
        Ok(record)
    }
}
