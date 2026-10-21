//! Host session use cases reuse Core records and keep storage in infrastructure.
mod infrastructure;
use furica_core::{
    Error, Result,
    sessions::{SessionData, SessionManager},
};
pub use infrastructure::SnapshotStore;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

pub fn snapshot(data: SessionData) -> Value {
    json!({"type":"session_snapshot", "data":data})
}

pub async fn save(
    manager: &SessionManager,
    store: &Arc<SnapshotStore>,
    acknowledge: bool,
) -> Result<()> {
    let data = manager.snapshot()?;
    let saved = data.clone();
    let store = store.clone();
    tokio::task::spawn_blocking(move || store.save(&saved))
        .await
        .map_err(|_| Error::State("session save worker failed".into()))??;
    if acknowledge {
        manager.acknowledge_saved(&data)?;
    }
    Ok(())
}

/// One independent writer: never cancelled by an incoming WebSocket message.
/// A failed save is retried and never acknowledges prepared work.
pub async fn autosave(
    manager: SessionManager,
    store: Arc<SnapshotStore>,
    mut changes: watch::Receiver<u64>,
    stop: CancellationToken,
    events: mpsc::Sender<Value>,
) {
    let mut retry = false;
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(1)), if retry => (),
            changed = changes.changed(), if !retry => { if changed.is_err() { break; } },
        }
        match save(&manager, &store, true).await {
            Ok(()) => {
                if retry {
                    let _ = events.try_send(json!({"type":"save_recovered"}));
                }
                retry = false;
            }
            Err(error) => {
                if !retry {
                    let _ = events.try_send(json!({"type":"save_error", "message":format!(
                        "会话保存失败；待执行的工作尚未启动，正在重试：{error}"
                    )}));
                }
                retry = true;
            }
        }
    }
}

#[cfg(test)]
mod tests;
