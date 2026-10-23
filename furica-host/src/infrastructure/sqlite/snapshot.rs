use crate::{Error, Result};
use furica_core::sessions::{SessionData, SessionManager};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use std::{
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

pub struct SnapshotStore {
    path: PathBuf,
    max_bytes: u64,
    serial: Arc<Mutex<()>>,
}

impl SnapshotStore {
    /// The database schema is initialized by ConfigStore::open during bootstrap.
    pub fn new(path: impl Into<PathBuf>, max_bytes: u64) -> Self {
        Self {
            path: path.into(),
            max_bytes,
            serial: Arc::new(Mutex::new(())),
        }
    }

    pub fn load(&self) -> Result<SessionData> {
        let mut db = super::connection(&self.path, false)?;
        let tx = db.transaction().map_err(|_| Error::storage())?;
        Ok(read_snapshot(&tx, self.max_bytes)?.unwrap_or_default())
    }

    /// Import only when no database snapshot exists. Never overwrite or delete
    /// the legacy file, and never fall back to it after a database read error.
    pub fn load_or_import(&self, legacy: &Path) -> Result<SessionData> {
        let mut db = super::connection(&self.path, false)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| Error::storage())?;
        if let Some(data) = read_snapshot(&tx, self.max_bytes)? {
            tx.commit().map_err(|_| Error::storage())?;
            return Ok(data);
        }
        let data = match std::fs::File::open(legacy) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(self.max_bytes.saturating_add(1))
                    .read_to_end(&mut bytes)
                    .map_err(|_| Error::storage())?;
                if bytes.len() as u64 > self.max_bytes {
                    return Err(Error::storage());
                }
                let data: SessionData =
                    serde_json::from_slice(&bytes).map_err(|_| Error::storage())?;
                data.validate().map_err(|_| Error::storage())?;
                data
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => SessionData::default(),
            Err(_) => return Err(Error::storage()),
        };
        write_snapshot(&tx, &data, self.max_bytes)?;
        tx.commit().map_err(|_| Error::storage())?;
        Ok(data)
    }

    /// Serialize saves before capturing the snapshot, and acknowledge only the
    /// exact committed data. The worker owns the guard through commit and
    /// acknowledgement even if its caller is cancelled.
    pub async fn save_manager(&self, manager: &SessionManager) -> Result<()> {
        let guard = self.serial.clone().lock_owned().await;
        let manager = manager.clone();
        let path = self.path.clone();
        let limit = self.max_bytes;
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let snapshot = manager.snapshot()?;
            let mut db = super::connection(&path, false)?;
            let tx = db.transaction().map_err(|_| Error::storage())?;
            write_snapshot(&tx, &snapshot, limit)?;
            tx.commit().map_err(|_| Error::storage())?;
            // A failure here does not undo the already committed transaction.
            manager.acknowledge_saved(&snapshot).map_err(|_| {
                Error::new(
                    503,
                    "snapshot_acknowledgement_failed",
                    "snapshot committed but execution acknowledgement failed",
                )
            })
        })
        .await
        .map_err(|_| Error::storage())?
    }
}

fn read_snapshot(db: &Connection, limit: u64) -> Result<Option<SessionData>> {
    // Check byte length before loading the full document into process memory.
    let length: Option<u64> = db
        .query_row(
            "SELECT length(CAST(document AS BLOB)) FROM session_snapshot WHERE id=1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| Error::storage())?;
    let Some(length) = length else {
        return Ok(None);
    };
    if length > limit {
        return Err(Error::storage());
    }
    let document: String = db
        .query_row(
            "SELECT document FROM session_snapshot WHERE id=1",
            [],
            |row| row.get(0),
        )
        .map_err(|_| Error::storage())?;
    let data: SessionData = serde_json::from_str(&document).map_err(|_| Error::storage())?;
    data.validate().map_err(|_| Error::storage())?;
    Ok(Some(data))
}

fn write_snapshot(db: &Connection, snapshot: &SessionData, limit: u64) -> Result<()> {
    let document = serde_json::to_string(snapshot).map_err(|_| Error::storage())?;
    if document.len() as u64 > limit {
        return Err(Error::storage());
    }
    db.execute(
        "INSERT INTO session_snapshot(id,document) VALUES(1,?1)
         ON CONFLICT(id) DO UPDATE SET document=excluded.document",
        [document],
    )
    .map_err(|_| Error::storage())?;
    Ok(())
}
