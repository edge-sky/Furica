use crate::{Error, Result, configuration::application::HostConfig};
use rusqlite::{Connection, TransactionBehavior};
use serde_json::{Map, Value};
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
        if version != 0 && version != 4 {
            return Err(Error::new(
                503,
                "unsupported_database",
                "database schema is incompatible with this host",
            ));
        }
        if version == 0 {
            // Never reinterpret an unversioned pre-existing database as an empty one.
            let tables: u64 = tx.query_row("SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'", [], |r| r.get(0)).map_err(|_| Error::storage())?;
            if tables != 0 {
                return Err(Error::storage());
            }
            create_table(&tx)?;
            super::sessions::create_tables(&tx)?;
            tx.execute_batch("PRAGMA user_version=4;")
                .map_err(|_| Error::storage())?;
        }
        let initialized: bool = tx
            .query_row("SELECT EXISTS(SELECT 1 FROM host_configuration)", [], |r| {
                r.get(0)
            })
            .map_err(|_| Error::storage())?;
        // Retire the old environment reference without reading it. Generate once,
        // persist in this transaction, and let clients consume the access-token file.
        if initialized {
            let old_reference: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM host_configuration WHERE key='http.token_env')",
                    [],
                    |row| row.get(0),
                )
                .map_err(|_| Error::storage())?;
            let direct_token: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM host_configuration WHERE key='http.token')",
                    [],
                    |row| row.get(0),
                )
                .map_err(|_| Error::storage())?;
            if old_reference && !direct_token {
                let token = crate::configuration::application::new_access_token();
                tx.execute(
                    "INSERT INTO host_configuration(key,value) VALUES('http.token',?1)",
                    [serde_json::to_string(&token).map_err(|_| Error::config())?],
                )
                .map_err(|_| Error::storage())?;
            }
            tx.execute("DELETE FROM host_configuration WHERE key IN ('http.token_env','model.api_key_env','speech.stt.api_key_env','speech.tts.api_key_env')", []).map_err(|_| Error::storage())?;
        }
        let config = match (initialized, initial) {
            (true, Some(_)) => {
                return Err(Error::new(
                    409,
                    "already_initialized",
                    "initialization cannot overwrite existing configuration",
                ));
            }
            (true, None) => read_config(&tx)?,
            (false, Some(config)) => {
                config.validate()?;
                write_config(&tx, config)?;
                config.clone()
            }
            (false, None) => {
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
        super::connection(&self.path, true)
    }
    pub fn read(&self) -> Result<HostConfig> {
        read_config(&self.connection()?)
    }
    /// Merge masked credentials and replace configuration in one serialized transaction.
    pub fn save_update(&self, value: Value) -> Result<()> {
        let mut db = self.connection()?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| Error::storage())?;
        let config =
            crate::configuration::application::credentials::update(value, &read_config(&tx)?)?;
        tx.execute("DELETE FROM host_configuration", [])
            .map_err(|_| Error::storage())?;
        write_config(&tx, &config)?;
        tx.commit().map_err(|_| Error::storage())
    }
    pub fn save(&self, config: &HostConfig) -> Result<()> {
        config.validate()?;
        let mut db = self.connection()?;
        let tx = db.transaction().map_err(|_| Error::storage())?;
        // Replace the complete configuration, including obsolete protocol fields.
        tx.execute("DELETE FROM host_configuration", [])
            .map_err(|_| Error::storage())?;
        write_config(&tx, config)?;
        tx.commit().map_err(|_| Error::storage())
    }
}

fn create_table(db: &Connection) -> Result<()> {
    db.execute_batch(
        "CREATE TABLE host_configuration(key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);",
    )
    .map_err(|_| Error::storage())
}

fn write_config(db: &Connection, config: &HostConfig) -> Result<()> {
    fn write_value(db: &Connection, key: &str, value: &Value) -> Result<()> {
        if let Value::Object(fields) = value {
            for (field, value) in fields {
                let path = if key.is_empty() {
                    field.clone()
                } else {
                    format!("{key}.{field}")
                };
                write_value(db, &path, value)?;
            }
        } else {
            db.execute(
                "INSERT INTO host_configuration(key,value) VALUES(?1,?2)",
                rusqlite::params![key, value.to_string()],
            )
            .map_err(|_| Error::storage())?;
        }
        Ok(())
    }
    write_value(
        db,
        "",
        &serde_json::to_value(config).map_err(|_| Error::config())?,
    )
}

fn read_config(db: &Connection) -> Result<HostConfig> {
    let mut statement = db
        .prepare("SELECT key,value FROM host_configuration ORDER BY key")
        .map_err(|_| Error::storage())?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|_| Error::storage())?;
    let mut document = Value::Object(Map::new());
    for row in rows {
        let (key, value) = row.map_err(|_| Error::storage())?;
        // Old databases may still contain this retired field. Reads leave it intact;
        // the next complete save removes it with the other obsolete fields.
        if key == "instructions"
            || matches!(
                key.as_str(),
                "http.token_env"
                    | "model.api_key_env"
                    | "speech.stt.api_key_env"
                    | "speech.tts.api_key_env"
            )
        {
            continue;
        }
        let mut path = key.split('.').peekable();
        let mut current = &mut document;
        while let Some(field) = path.next() {
            if field.is_empty() {
                return Err(Error::config());
            }
            let object = current.as_object_mut().ok_or_else(Error::config)?;
            if path.peek().is_none() {
                let value: Value = serde_json::from_str(&value).map_err(|_| Error::config())?;
                if value.is_object() || object.insert(field.into(), value).is_some() {
                    return Err(Error::config());
                }
                break;
            }
            current = object
                .entry(field)
                .or_insert_with(|| Value::Object(Map::new()));
        }
    }
    // A generated startup token must be committed, never invented by a read.
    if document.pointer("/http/token").is_none() {
        return Err(Error::config());
    }
    // Serde applies the existing typed defaults only to missing fields.
    let config: HostConfig = serde_json::from_value(document).map_err(|_| Error::config())?;
    config.validate()?;
    Ok(config)
}
