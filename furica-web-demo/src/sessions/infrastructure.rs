//! Atomic local snapshots; Core owns validation and recovery semantics.
use furica_core::{Error, Result, sessions::SessionData};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

pub struct SnapshotStore {
    pub(super) path: PathBuf,
}

impl SnapshotStore {
    pub fn configured() -> Self {
        let root = std::env::var_os("FURICA_DEMO_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| Path::new(crate::bootstrap::HOST_ROOT).join("runtime"));
        Self {
            path: root.join("sessions.json"),
        }
    }

    pub fn load(&self) -> Result<SessionData> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(SessionData::default());
            }
            Err(error) => return Err(error.into()),
        };
        let mut bytes = Vec::new();
        file.take(64 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(Error::Config("session snapshot exceeds 64 MiB".into()));
        }
        let data: SessionData = serde_json::from_slice(&bytes).map_err(|_| {
            Error::Config(
                "invalid sessions.json; keep the file and restore a valid snapshot".into(),
            )
        })?;
        data.validate()?;
        Ok(data)
    }

    pub fn save(&self, data: &SessionData) -> Result<()> {
        data.validate()?;
        let parent = self
            .path
            .parent()
            .ok_or_else(|| Error::Config("snapshot directory missing".into()))?;
        fs::create_dir_all(parent)?;
        let temporary = parent.join(format!(".sessions-{}.tmp", Uuid::new_v4()));
        let result = (|| -> Result<()> {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temporary)?;
            serde_json::to_writer(&mut file, data)?;
            file.flush()?;
            if file.metadata()?.len() > 64 * 1024 * 1024 {
                return Err(Error::Config("session snapshot exceeds 64 MiB".into()));
            }
            file.sync_all()?;
            drop(file);
            fs::rename(&temporary, &self.path)?;
            #[cfg(unix)]
            File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }
}
