use crate::{Error, Result};
use furica_core::sessions::{SessionData, SessionManager};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct SnapshotStore {
    path: PathBuf,
    max_bytes: u64,
    serial: Mutex<()>,
}

impl SnapshotStore {
    pub fn new(path: impl Into<PathBuf>, max_bytes: u64) -> Self {
        Self {
            path: path.into(),
            max_bytes,
            serial: Mutex::new(()),
        }
    }
    pub fn load(&self) -> Result<SessionData> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(SessionData::default()),
            Err(_) => return Err(Error::storage()),
        };
        let mut bytes = Vec::new();
        file.take(self.max_bytes + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::storage())?;
        if bytes.len() as u64 > self.max_bytes {
            return Err(Error::storage());
        }
        let data: SessionData = serde_json::from_slice(&bytes).map_err(|_| Error::storage())?;
        data.validate().map_err(|_| Error::storage())?;
        Ok(data)
    }
    /// The lock precedes snapshot capture, so an older capture cannot replace a newer save.
    pub async fn save_manager(&self, manager: &SessionManager) -> Result<()> {
        let _serial = self.serial.lock().await;
        let snapshot = manager.snapshot()?;
        let path = self.path.clone();
        let limit = self.max_bytes;
        let snapshot = tokio::task::spawn_blocking(move || {
            atomic_save(&path, &snapshot, limit)?;
            Ok::<_, Error>(snapshot)
        })
        .await
        .map_err(|_| Error::storage())??;
        manager.acknowledge_saved(&snapshot)?;
        Ok(())
    }
}

fn atomic_save(path: &Path, snapshot: &SessionData, limit: u64) -> Result<()> {
    let parent = path.parent().ok_or_else(Error::storage)?;
    let temp = parent.join(format!(".sessions-{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&temp).map_err(|_| Error::storage())?;
        struct LimitedWriter {
            file: File,
            remaining: u64,
        }
        impl Write for LimitedWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() as u64 > self.remaining {
                    return Err(std::io::Error::other("snapshot size limit"));
                }
                let written = self.file.write(bytes)?;
                self.remaining -= written as u64;
                Ok(written)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                self.file.flush()
            }
        }
        let mut writer = LimitedWriter {
            file,
            remaining: limit,
        };
        serde_json::to_writer(&mut writer, snapshot).map_err(|_| Error::storage())?;
        writer.file.sync_all().map_err(|_| Error::storage())?;
        fs::rename(&temp, path).map_err(|_| Error::storage())?;
        File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|_| Error::storage())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}
