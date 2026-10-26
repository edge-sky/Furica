mod configuration;
mod delivery;
mod interaction;
mod lifecycle;
mod sessions;
mod voice;
mod voice_lease;

use crate::{
    Error, Result,
    configuration::application::HostConfig,
    infrastructure::{
        components::{ComponentManager, Components},
        sqlite::{ConfigStore, SessionStore},
    },
};
use furica_core::{
    orchestration::{Core, CoreEvent, CoreHandle, CoreState},
    playback::{PlaybackChunk, PlaybackState},
    sessions::{SessionData, SessionManager},
};
use serde_json::{Value, json};
use sessions::switching_allowed;
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, broadcast, mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use voice_lease::VoiceLease;

type Delivery = Arc<StdMutex<Option<(Uuid, mpsc::Sender<PlaybackChunk>, CancellationToken)>>>;

pub struct Host {
    state: Mutex<HostState>,
    components: ComponentManager,
    config_store: ConfigStore,
    updating: AtomicBool,
    store: Arc<SessionStore>,
    receiver_serial: Arc<Mutex<()>>,
    delivery: Delivery,
    events: broadcast::Sender<Value>,
    storage_healthy: Arc<AtomicBool>,
    stopped: AtomicBool,
    lifetime: CancellationToken,
}

struct HostState {
    directory_lock: Option<rusqlite::Connection>,
    manager: SessionManager,
    selected: Option<Uuid>,
    runtime: Option<Runtime>,
    voice: Option<VoiceLease>,
    capture_sequence: u64,
    receiver: Option<(CancellationToken, JoinHandle<()>)>,
    ready: bool,
}

struct Runtime {
    handle: CoreHandle,
    playback: watch::Receiver<PlaybackState>,
    runner: JoinHandle<furica_core::Result<()>>,
    monitor: JoinHandle<()>,
    cancel: CancellationToken,
}

impl Host {
    /// Assemble application state using a database initialized by ConfigStore::open.
    pub async fn new(components: Components, database_path: PathBuf) -> Result<Arc<Self>> {
        components.validate()?;
        let config = &components.config;
        let config_store = ConfigStore::at(database_path.clone());
        let store = Arc::new(SessionStore::new(
            database_path,
            config.limits.context_max_bytes,
        ));
        let receiver_serial = Arc::new(Mutex::new(()));
        let ids = tokio::task::spawn_blocking({
            let store = store.clone();
            move || store.session_ids()
        })
        .await
        .map_err(|_| Error::storage())??;
        // Normalize host-loaded contexts before serving requests. Old work is never replayed.
        for id in ids {
            let data = tokio::task::spawn_blocking({
                let store = store.clone();
                move || store.context(id)
            })
            .await
            .map_err(|_| Error::storage())??;
            let manager = SessionManager::with_agents(
                data,
                components.agents.clone(),
                config.session_limits(),
            )?;
            delivery::receive(&manager, store.clone(), receiver_serial.clone()).await?;
            tokio::task::spawn_blocking({
                let store = store.clone();
                let stream = manager.delivery_id();
                move || store.finish_delivery(stream)
            })
            .await
            .map_err(|_| Error::storage())??;
        }
        let manager = SessionManager::with_agents(
            SessionData::default(),
            components.agents.clone(),
            config.session_limits(),
        )?;
        let (events, _) = broadcast::channel(config.limits.channel_capacity);
        let host = Arc::new(Self {
            components: ComponentManager::new(components),
            config_store,
            updating: AtomicBool::new(false),
            store,
            receiver_serial,
            delivery: Arc::new(StdMutex::new(None)),
            events,
            storage_healthy: Arc::new(AtomicBool::new(true)),
            stopped: AtomicBool::new(false),
            lifetime: CancellationToken::new(),
            state: Mutex::new(HostState {
                directory_lock: None,
                manager,
                selected: None,
                runtime: None,
                voice: None,
                capture_sequence: 0,
                receiver: None,
                ready: true,
            }),
        });
        {
            let mut state = host.state.lock().await;
            host.start_receiver(&mut state);
        }
        let weak = Arc::downgrade(&host);
        let lifetime = host.lifetime.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(100));
            loop {
                tokio::select! { biased; _ = lifetime.cancelled() => break, _ = tick.tick() => () }
                let Some(host) = weak.upgrade() else {
                    break;
                };
                let mut state = host.state.lock().await;
                let components = host.components.current();
                let reason = state.voice.as_ref().and_then(|lease| {
                    if lease.cancelled.is_cancelled() {
                        Some("delivery_failed")
                    } else if lease.last_seen.elapsed()
                        >= Duration::from_millis(components.config.limits.heartbeat_timeout_ms)
                    {
                        Some("heartbeat_timeout")
                    } else if lease.retry_expired(Duration::from_millis(
                        components.config.limits.upload_retry_window_ms,
                    )) {
                        Some("upload_retry_exhausted")
                    } else {
                        None
                    }
                });
                if let Some(reason) = reason {
                    let _ = host.close_voice_locked(&mut state, reason).await;
                }
                if state
                    .runtime
                    .as_ref()
                    .is_some_and(|r| r.runner.is_finished())
                {
                    state.ready = false;
                    if state.voice.is_some() {
                        let _ = host.close_voice_locked(&mut state, "runtime_failed").await;
                    }
                }
            }
        });
        Ok(host)
    }

    pub(crate) async fn hold_directory_lock(&self, lock: rusqlite::Connection) {
        self.state.lock().await.directory_lock = Some(lock);
    }

    /// The configuration currently applied to capability instances (not pending SQL edits).
    pub fn config(&self) -> HostConfig {
        self.components.current().config.clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }
    fn emit(&self, event: Value) {
        let _ = self.events.send(event);
    }
    fn accepting(&self) -> Result<()> {
        if self.updating.load(Ordering::Acquire) || self.stopped.load(Ordering::Acquire) {
            return Err(Error::new(
                503,
                "host_not_ready",
                "host is updating or stopped",
            ));
        }
        Ok(())
    }

    fn usable(&self, state: &HostState) -> Result<()> {
        self.accepting()?;
        if !state.ready || !self.storage_healthy.load(Ordering::Acquire) {
            return Err(Error::new(
                503,
                "host_not_ready",
                "host runtime or persistent storage is unavailable",
            ));
        }
        Ok(())
    }
    pub async fn health(&self) -> Value {
        if self.updating.load(Ordering::Acquire) {
            return json!({"ready":false,"updating":true});
        }
        let state = self.state.lock().await;
        let updating = self.updating.load(Ordering::Acquire);
        let components = self.components.current();
        let playback = state
            .runtime
            .as_ref()
            .map(|r| *r.playback.borrow())
            .unwrap_or(PlaybackState {
                generation: 0,
                active: false,
            });
        json!({"updating":updating,"ready":!updating && state.ready && !self.stopped.load(Ordering::Acquire) && self.storage_healthy.load(Ordering::Acquire),
            "storage":if self.storage_healthy.load(Ordering::Acquire) {"healthy"} else {"failed"},
            "interaction_state":state.runtime.as_ref().map(|r| format!("{:?}",r.handle.state())).unwrap_or("Standby".into()),
            "active_session_id":state.selected,"voice_available":components.voice_available,
            "text_available":components.backend.is_some(), "acp_available":!components.agents.is_empty(),
            "voice_channel_id":state.voice.as_ref().map(|v| v.id),"playback_generation":playback.generation,"playback_active":playback.active,
            "sample_rate":16000,"channels":1,"encoding":"pcm_s16le","max_batch_samples":3200,
            "heartbeat_interval_ms":5000,"heartbeat_timeout_ms":components.config.limits.heartbeat_timeout_ms,
            "upload_retry_limit":components.config.limits.upload_retry_limit,"upload_retry_window_ms":components.config.limits.upload_retry_window_ms})
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        self.lifetime.cancel();
        let state = self.state.get_mut();
        if let Some((cancel, task)) = state.receiver.take() {
            cancel.cancel();
            task.abort();
        }
        if let Some(runtime) = state.runtime.take() {
            runtime.cancel.cancel();
            runtime.monitor.abort();
            runtime.runner.abort();
        }
        if let Some(voice) = state.voice.take() {
            voice.cancelled.cancel();
        }
    }
}
