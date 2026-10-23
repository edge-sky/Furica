use crate::{Error, Result, configuration::application::HostConfig};
use rusqlite::{Connection, OptionalExtension};
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
        if version > 3 {
            return Err(Error::new(
                503,
                "unsupported_database",
                "database version is newer than this host",
            ));
        }
        if version == 1 {
            let document: Option<String> = tx
                .query_row(
                    "SELECT document FROM host_configuration WHERE id=1",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|_| Error::storage())?;
            tx.execute_batch("ALTER TABLE host_configuration RENAME TO legacy_configuration;")
                .map_err(|_| Error::storage())?;
            create_table(&tx)?;
            if let Some(document) = document {
                let config: HostConfig =
                    serde_json::from_str(&document).map_err(|_| Error::config())?;
                config.validate()?;
                write_config(&tx, &config)?;
            }
            tx.execute_batch("DROP TABLE legacy_configuration;")
                .map_err(|_| Error::storage())?;
        } else if version == 0 {
            create_table(&tx)?;
        }
        if version < 3 {
            tx.execute_batch(
                "CREATE TABLE session_snapshot(
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    document TEXT NOT NULL
                 );
                 PRAGMA user_version=3;",
            )
            .map_err(|_| Error::storage())?;
        }
        let initialized: bool = tx
            .query_row("SELECT EXISTS(SELECT 1 FROM host_configuration)", [], |r| {
                r.get(0)
            })
            .map_err(|_| Error::storage())?;
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
        "CREATE TABLE host_configuration(key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);
         PRAGMA user_version=2;",
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
    // Serde applies the existing typed defaults only to missing fields.
    let config: HostConfig = serde_json::from_value(document).map_err(|_| Error::config())?;
    config.validate()?;
    Ok(config)
}
