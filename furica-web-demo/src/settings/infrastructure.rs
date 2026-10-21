//! SQLite owns atomic replacement and secret preservation; no legacy import.
use super::Config;
use furica_core::{Error, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use std::{fs, path::PathBuf, time::Duration};

#[derive(Clone)]
pub struct SettingsStore {
    path: PathBuf,
}

fn database_error(_: rusqlite::Error) -> Error {
    Error::Config("无法读写设置数据库，请检查目录权限、磁盘空间或数据库占用".into())
}

impl SettingsStore {
    pub fn configured() -> Self {
        let root = std::env::var_os("FURICA_DEMO_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(crate::bootstrap::HOST_ROOT).join("runtime"));
        Self {
            path: root.join("settings.sqlite3"),
        }
    }

    fn open(&self) -> Result<Connection> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| Error::Config("设置目录无效".into()))?;
        fs::create_dir_all(parent)?;
        let mut options = fs::OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
            options.mode(0o600);
            let file = options.open(&self.path)?;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        #[cfg(not(unix))]
        options.open(&self.path)?;
        let connection = Connection::open(&self.path).map_err(database_error)?;
        connection
            .busy_timeout(Duration::from_secs(3))
            .map_err(database_error)?;
        // DELETE journaling keeps sidecars short-lived; secure_delete removes replaced keys.
        connection.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA secure_delete=ON; CREATE TABLE IF NOT EXISTS settings (id INTEGER PRIMARY KEY CHECK (id = 1), document TEXT NOT NULL);").map_err(database_error)?;
        Ok(connection)
    }

    fn read_config(connection: &Connection) -> Result<Config> {
        let document: Option<String> = connection
            .query_row("SELECT document FROM settings WHERE id=1", [], |row| {
                row.get(0)
            })
            .optional()
            .map_err(database_error)?;
        match document {
            Some(document) => serde_json::from_str(&document)
                .map_err(|_| Error::Config("设置数据库内容无效；请保留数据库并修复".into())),
            None => Ok(Config::default()),
        }
    }

    pub fn load(&self) -> Result<Config> {
        Self::read_config(&self.open()?)
    }

    pub fn save(&self, mut value: Value) -> Result<Config> {
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(database_error)?;
        let previous = Self::read_config(&transaction)?;
        if value.get("stt").is_none()
            && let Some(object) = value.as_object_mut()
        {
            object.insert("stt".into(), serde_json::to_value(&previous.stt)?);
        }
        // Blank or absent keys retain the stored secret; null explicitly clears it.
        for (name, key) in [
            ("stt", &previous.stt.api_key),
            ("tts", &previous.tts.api_key),
            ("text_model", &previous.text_model.api_key),
        ] {
            let section = value
                .get_mut(name)
                .and_then(Value::as_object_mut)
                .ok_or_else(|| Error::Config(format!("缺少 {name} 设置")))?;
            match section.get("api_key") {
                None => {
                    section.insert("api_key".into(), Value::String(key.clone()));
                }
                Some(Value::String(text)) if text.is_empty() => {
                    section.insert("api_key".into(), Value::String(key.clone()));
                }
                Some(Value::Null) => {
                    section.insert("api_key".into(), Value::String(String::new()));
                }
                _ => (),
            }
        }
        let config: Config = serde_json::from_value(value)
            .map_err(|_| Error::Config("设置格式或字段无效".into()))?;
        config.validate()?;
        let document = serde_json::to_string(&config)?;
        if document.len() > 64 * 1024 {
            return Err(Error::Config("设置不能超过 64 KiB".into()));
        }
        transaction.execute("INSERT INTO settings(id, document) VALUES(1, ?1) ON CONFLICT(id) DO UPDATE SET document=excluded.document", params![document]).map_err(database_error)?;
        transaction.commit().map_err(database_error)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn draft(key: Value) -> Value {
        json!({"stt":{"api_key":key}, "tts":{}, "text_model":{}, "acp":{}})
    }

    #[test]
    fn secrets_survive_reopen_blank_save_and_explicit_clear() {
        let root = tempfile::tempdir().unwrap();
        let store = SettingsStore {
            path: root.path().join("settings.sqlite3"),
        };
        assert!(store.load().unwrap().stt.api_key.is_empty());
        store.save(draft(json!("test-secret"))).unwrap();
        let reopened = SettingsStore {
            path: store.path.clone(),
        };
        assert_eq!(reopened.load().unwrap().stt.api_key, "test-secret");
        let saved = reopened.save(draft(json!(""))).unwrap();
        assert_eq!(saved.stt.api_key, "test-secret");
        let view = crate::settings::presentation::editable(&saved);
        assert!(!view.to_string().contains("test-secret"));
        assert!(view["stt"].get("api_key").is_none());
        assert_eq!(view["stt"]["api_key_configured"], true);
        let mut missing_key = draft(json!(""));
        missing_key["stt"]
            .as_object_mut()
            .unwrap()
            .remove("api_key");
        reopened.save(missing_key).unwrap();
        assert_eq!(reopened.load().unwrap().stt.api_key, "test-secret");
        reopened.save(draft(Value::Null)).unwrap();
        assert!(store.load().unwrap().stt.api_key.is_empty());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&store.path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn invalid_drafts_never_replace_saved_settings() {
        let root = tempfile::tempdir().unwrap();
        let store = SettingsStore {
            path: root.path().join("settings.sqlite3"),
        };
        store.save(draft(json!("original-secret"))).unwrap();
        for patch in [
            json!({"provider":"unknown"}),
            json!({"endpoint":"https://user:secret@example.com/v1"}),
            json!({"endpoint":"http://example.com/v1"}),
            json!({"api_key":"secret\nvalue"}),
            json!({"unexpected":true}),
        ] {
            let mut value = draft(json!("replacement-secret"));
            value["tts"]
                .as_object_mut()
                .unwrap()
                .extend(patch.as_object().unwrap().clone());
            let error = match store.save(value) {
                Err(error) => error,
                Ok(_) => panic!("invalid draft saved"),
            };
            assert!(!error.to_string().contains("replacement-secret"));
            assert_eq!(store.load().unwrap().stt.api_key, "original-secret");
        }
        assert!(store.save(json!({})).is_err());
    }

    #[test]
    fn saving_settings_does_not_require_stt_configuration() {
        let root = tempfile::tempdir().unwrap();
        let store = SettingsStore {
            path: root.path().join("settings.sqlite3"),
        };
        let saved = store
            .save(json!({"tts":{}, "text_model":{}, "acp":{}}))
            .unwrap();
        assert!(saved.stt.provider.is_empty());
        assert!(
            !saved
                .missing(false)
                .iter()
                .any(|field| field.starts_with("stt."))
        );
    }

    #[test]
    fn corrupt_database_is_not_overwritten() {
        let root = tempfile::tempdir().unwrap();
        let store = SettingsStore {
            path: root.path().join("settings.sqlite3"),
        };
        fs::write(&store.path, b"not a sqlite database").unwrap();
        assert!(store.load().is_err());
        assert_eq!(fs::read(&store.path).unwrap(), b"not a sqlite database");
    }
}
