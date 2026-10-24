//! Host-owned record organization. Core messages are persisted atomically and queried here.
use crate::{Error, Result};
use furica_core::sessions::{Session, SessionData, SessionEvent};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Clone)]
pub struct SessionStore {
    path: PathBuf,
    max_context_bytes: u64,
}

pub(super) fn create_tables(db: &Connection) -> Result<()> {
    for table in [
        "sessions",
        "messages",
        "acp_sessions",
        "runs",
        "tool_calls",
        "questions",
    ] {
        db.execute_batch(&format!(
            "CREATE TABLE {table}(id TEXT PRIMARY KEY NOT NULL, session_id TEXT NOT NULL,
             sequence INTEGER NOT NULL, document TEXT NOT NULL);
             CREATE INDEX {table}_owner ON {table}(session_id, sequence);"
        ))
        .map_err(|_| Error::storage())?;
    }
    db.execute_batch("CREATE UNIQUE INDEX message_sequence ON messages(session_id,sequence);
        CREATE TABLE received_messages(stream TEXT PRIMARY KEY NOT NULL, sequence INTEGER NOT NULL, document TEXT NOT NULL);")
        .map_err(|_| Error::storage())
}

impl SessionStore {
    pub fn new(path: impl AsRef<Path>, max_context_bytes: u64) -> Self {
        Self {
            path: path.as_ref().into(),
            max_context_bytes,
        }
    }
    /// Only the newest receipt per runtime is retained. Repeated delivery must match exactly.
    pub fn receive(&self, stream: Uuid, sequence: u64, events: &[SessionEvent]) -> Result<()> {
        let mut db = super::connection(&self.path, false)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| Error::storage())?;
        let payload = serde_json::to_string(events).map_err(|_| Error::storage())?;
        let sequence = i64::try_from(sequence).map_err(|_| Error::storage())?;
        let previous: Option<(i64, String)> = tx
            .query_row(
                "SELECT sequence,document FROM received_messages WHERE stream=?1",
                [stream.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(|_| Error::storage())?;
        if let Some((last, content)) = &previous
            && sequence == *last
            && *content == payload
        {
            return Ok(());
        }
        if Some(sequence) != previous.map_or(Some(1), |(last, _)| last.checked_add(1)) {
            return Err(Error::new(
                409,
                "invalid_delivery",
                "message delivery is conflicting or out of order",
            ));
        }
        for event in events {
            write_event(&tx, event)?;
        }
        tx.execute("INSERT INTO received_messages(stream,sequence,document) VALUES(?1,?2,?3)
            ON CONFLICT(stream) DO UPDATE SET sequence=excluded.sequence,document=excluded.document",
            params![stream.to_string(),sequence,payload]).map_err(|_| Error::storage())?;
        tx.commit().map_err(|_| Error::storage())
    }
    pub fn create_session(&self) -> Result<Session> {
        let session = Session::new();
        let db = super::connection(&self.path, false)?;
        write_record(&db, "sessions", session.id, session.id, 0, &session)?;
        Ok(session)
    }
    pub fn session(&self, id: Uuid) -> Result<Session> {
        let db = super::connection(&self.path, false)?;
        read_session(&db, id)
    }
    /// Load just the selected conversation, never the whole host history.
    pub fn context(&self, id: Uuid) -> Result<SessionData> {
        let mut db = super::connection(&self.path, false)?;
        let tx = db.transaction().map_err(|_| Error::storage())?;
        let session = read_session(&tx, id)?;
        let mut bytes = serde_json::to_vec(&session)
            .map_err(|_| Error::storage())?
            .len() as u64;
        for table in [
            "messages",
            "acp_sessions",
            "runs",
            "tool_calls",
            "questions",
        ] {
            let n: u64 = tx.query_row(&format!("SELECT coalesce(sum(length(CAST(document AS BLOB))),0) FROM {table} WHERE session_id=?1"), [id.to_string()], |r|r.get(0)).map_err(|_| Error::storage())?;
            bytes = bytes.checked_add(n).ok_or_else(Error::storage)?;
        }
        if bytes > self.max_context_bytes {
            return Err(Error::new(
                413,
                "context_too_large",
                "conversation context exceeds its configured limit",
            ));
        }
        let data = SessionData {
            sessions: vec![session],
            messages: read_records(&tx, "messages", id)?,
            acp_sessions: read_records(&tx, "acp_sessions", id)?,
            runs: read_records(&tx, "runs", id)?,
            tool_calls: read_records(&tx, "tool_calls", id)?,
            questions: read_records(&tx, "questions", id)?,
        };
        data.validate().map_err(|_| Error::storage())?;
        Ok(data)
    }
    pub fn session_ids(&self) -> Result<Vec<Uuid>> {
        let db = super::connection(&self.path, false)?;
        let mut stmt = db
            .prepare("SELECT id FROM sessions ORDER BY rowid")
            .map_err(|_| Error::storage())?;
        stmt.query_map([], |r| r.get::<_, String>(0))
            .map_err(|_| Error::storage())?
            .map(|v| {
                Uuid::parse_str(&v.map_err(|_| Error::storage())?).map_err(|_| Error::storage())
            })
            .collect()
    }
    /// One transaction supplies both the page and its total.
    pub fn page(
        &self,
        owner: Option<Uuid>,
        resource: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Value> {
        let table = match resource {
            "sessions" => "sessions",
            "messages" => "messages",
            "acp-sessions" => "acp_sessions",
            "runs" => "runs",
            "tool-calls" => "tool_calls",
            "questions" => "questions",
            _ => return Err(Error::new(404, "not_found", "resource does not exist")),
        };
        let mut db = super::connection(&self.path, false)?;
        let tx = db.transaction().map_err(|_| Error::storage())?;
        if let Some(id) = owner {
            read_session(&tx, id)?;
        }
        let owner = owner.map(|id| id.to_string());
        let filter = if owner.is_some() {
            "session_id=?1"
        } else {
            "?1 IS NULL"
        };
        let total: u64 = tx
            .query_row(
                &format!("SELECT count(*) FROM {table} WHERE {filter}"),
                [&owner],
                |r| r.get(0),
            )
            .map_err(|_| Error::storage())?;
        let offset_sql = i64::try_from(offset).unwrap_or(i64::MAX);
        let limit_sql = i64::try_from(limit).map_err(|_| Error::config())?;
        let mut stmt = tx.prepare(&format!("SELECT document FROM {table} WHERE {filter} ORDER BY sequence,rowid LIMIT ?2 OFFSET ?3")).map_err(|_| Error::storage())?;
        let items: Vec<Value> = stmt
            .query_map(params![owner, limit_sql, offset_sql], |r| {
                r.get::<_, String>(0)
            })
            .map_err(|_| Error::storage())?
            .map(|s| {
                serde_json::from_str(&s.map_err(|_| Error::storage())?)
                    .map_err(|_| Error::storage())
            })
            .collect::<Result<_>>()?;
        Ok(json!({"items":items,"offset":offset,"limit":limit,"total":total}))
    }
    pub fn finish_delivery(&self, stream: Uuid) -> Result<()> {
        super::connection(&self.path, false)?
            .execute(
                "DELETE FROM received_messages WHERE stream=?1",
                [stream.to_string()],
            )
            .map_err(|_| Error::storage())?;
        Ok(())
    }
}

fn read_session(db: &Connection, id: Uuid) -> Result<Session> {
    let value: Option<String> = db
        .query_row(
            "SELECT document FROM sessions WHERE id=?1",
            [id.to_string()],
            |r| r.get(0),
        )
        .optional()
        .map_err(|_| Error::storage())?;
    serde_json::from_str(
        &value.ok_or_else(|| Error::new(404, "not_found", "session does not exist"))?,
    )
    .map_err(|_| Error::storage())
}
fn read_records<T: DeserializeOwned>(db: &Connection, table: &str, id: Uuid) -> Result<Vec<T>> {
    let mut stmt = db
        .prepare(&format!(
            "SELECT document FROM {table} WHERE session_id=?1 ORDER BY sequence,rowid"
        ))
        .map_err(|_| Error::storage())?;
    stmt.query_map([id.to_string()], |r| r.get::<_, String>(0))
        .map_err(|_| Error::storage())?
        .map(|s| {
            serde_json::from_str(&s.map_err(|_| Error::storage())?).map_err(|_| Error::storage())
        })
        .collect()
}
fn write_record(
    db: &Connection,
    table: &str,
    id: Uuid,
    owner: Uuid,
    sequence: u64,
    record: &impl Serialize,
) -> Result<()> {
    let document = serde_json::to_string(record).map_err(|_| Error::storage())?;
    let sequence = i64::try_from(sequence).map_err(|_| Error::storage())?;
    db.execute(&format!("INSERT INTO {table}(id,session_id,sequence,document) VALUES(?1,?2,?3,?4)
        ON CONFLICT(id) DO UPDATE SET session_id=excluded.session_id,sequence=excluded.sequence,document=excluded.document"),
        params![id.to_string(),owner.to_string(),sequence,document]).map_err(|_| Error::storage())?;
    Ok(())
}
fn write_event(db: &Connection, event: &SessionEvent) -> Result<()> {
    match event {
        SessionEvent::SessionChanged(v) => write_record(db, "sessions", v.id, v.id, 0, v),
        SessionEvent::MessageRecorded(v) => {
            write_record(db, "messages", v.id, v.session_id, v.sequence, v)
        }
        SessionEvent::AcpSessionChanged(v) => {
            write_record(db, "acp_sessions", v.id, v.session_id, 0, v)
        }
        SessionEvent::RunChanged { run: v, .. } => {
            write_record(db, "runs", v.id, v.session_id, 0, v)
        }
        SessionEvent::ToolCallRecorded(v) => {
            write_record(db, "tool_calls", v.id, v.session_id, 0, v)
        }
        SessionEvent::QuestionChanged(v) => write_record(db, "questions", v.id, v.session_id, 0, v),
        _ => Err(Error::new(
            400,
            "invalid_delivery",
            "transient notifications cannot be persisted as business messages",
        )),
    }
}
