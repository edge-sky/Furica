//! Settings boundary: existing configuration, use cases, SQLite, and browser projection.
mod domain;
mod infrastructure;
pub mod presentation;
pub use domain::Config;
use furica_core::{Error, Result};
pub use infrastructure::SettingsStore;
use serde_json::Value;

impl SettingsStore {
    pub async fn read(&self) -> Result<Config> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.load())
            .await
            .map_err(|_| Error::State("设置读取任务失败".into()))?
    }

    pub async fn write(&self, value: Value) -> Result<Config> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.save(value))
            .await
            .map_err(|_| Error::State("设置保存任务失败".into()))?
    }
}
