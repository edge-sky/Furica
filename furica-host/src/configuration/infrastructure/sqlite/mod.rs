use crate::{Error, Result, configuration::application::HostConfig};
use rusqlite::{Connection, OptionalExtension};
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct ConfigStore {
    path: PathBuf,
}

impl ConfigStore {
    pub fn open(
        path: impl AsRef<Path>,
        initial: Option<&HostConfig>,
    ) -> Result<(Self, HostConfig)> {
        let store = Self {
            path: path.as_ref().into(),
        };
        let mut db = store.connection()?;
        let tx = db.transaction().map_err(|_| Error::storage())?;
        let version: u32 = tx
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(|_| Error::storage())?;
        if version > 1 {
            return Err(Error::new(
                503,
                "unsupported_database",
                "database version is newer than this host",
            ));
        }
        if version == 0 {
            tx.execute_batch("CREATE TABLE host_configuration(id INTEGER PRIMARY KEY CHECK(id=1), document TEXT NOT NULL); PRAGMA user_version=1;").map_err(|_| Error::storage())?;
        }
        let document: Option<String> = tx
            .query_row(
                "SELECT document FROM host_configuration WHERE id=1",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(|_| Error::storage())?;
        let config = match (document, initial) {
            (Some(_), Some(_)) => {
                return Err(Error::new(
                    409,
                    "already_initialized",
                    "initialization cannot overwrite existing configuration",
                ));
            }
            (Some(document), None) => {
                serde_json::from_str::<HostConfig>(&document).map_err(|_| Error::config())?
            }
            (None, Some(config)) => {
                config.validate()?;
                tx.execute(
                    "INSERT INTO host_configuration(id,document) VALUES(1,?1)",
                    [serde_json::to_string(config).map_err(|_| Error::config())?],
                )
                .map_err(|_| Error::storage())?;
                config.clone()
            }
            (None, None) => {
                return Err(Error::new(
                    400,
                    "initialization_required",
                    "first startup requires an initialization configuration",
                ));
            }
        };
        config.validate()?;
        tx.commit().map_err(|_| Error::storage())?;
        Ok((store, config))
    }
    fn connection(&self) -> Result<Connection> {
        let db = Connection::open(&self.path).map_err(|_| Error::storage())?;
        db.busy_timeout(std::time::Duration::from_secs(2))
            .map_err(|_| Error::storage())?;
        db.execute_batch("PRAGMA synchronous=FULL;")
            .map_err(|_| Error::storage())?;
        Ok(db)
    }
    pub fn read(&self) -> Result<HostConfig> {
        let document: String = self
            .connection()?
            .query_row(
                "SELECT document FROM host_configuration WHERE id=1",
                [],
                |r| r.get(0),
            )
            .map_err(|_| Error::storage())?;
        let config: HostConfig = serde_json::from_str(&document).map_err(|_| Error::config())?;
        config.validate()?;
        Ok(config)
    }
    pub fn save(&self, config: &HostConfig) -> Result<()> {
        config.validate()?;
        let mut db = self.connection()?;
        let tx = db.transaction().map_err(|_| Error::storage())?;
        let changed = tx
            .execute(
                "UPDATE host_configuration SET document=?1 WHERE id=1",
                [serde_json::to_string(config).map_err(|_| Error::config())?],
            )
            .map_err(|_| Error::storage())?;
        if changed != 1 {
            return Err(Error::storage());
        }
        tx.commit().map_err(|_| Error::storage())
    }
}
