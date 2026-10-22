use crate::{
    Error, Result,
    configuration::application::HostConfig,
    sessions::{application::switching_allowed, infrastructure::storage::SnapshotStore},
    voice::application::VoiceLease,
};
use furica_core::{
    orchestration::{Core, CoreEvent, CoreHandle, CoreState, VoiceBackend},
    playback::{PlaybackChunk, PlaybackState},
    sessions::{Executor, SessionData, SessionManager},
};
use serde_json::{Value, json};
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

pub struct Components {
    pub backend: Arc<dyn VoiceBackend>,
    pub executor: Arc<dyn Executor>,
    pub speech_enabled: Arc<AtomicBool>,
    pub voice_available: bool,
}

pub(crate) type Delivery =
    Arc<StdMutex<Option<(Uuid, mpsc::Sender<PlaybackChunk>, CancellationToken)>>>;

pub struct Host {
    pub config: HostConfig,
    pub(crate) state: Mutex<HostState>,
    pub(crate) components: Components,
    store: Arc<SnapshotStore>,
    pub(crate) delivery: Delivery,
    events: broadcast::Sender<Value>,
    storage_healthy: Arc<AtomicBool>,
    stopped: AtomicBool,
    lifetime: CancellationToken,
}

pub(crate) struct HostState {
    pub(crate) directory_lock: Option<rusqlite::Connection>,
    pub manager: SessionManager,
    pub selected: Option<Uuid>,
    pub(crate) runtime: Option<Runtime>,
    pub voice: Option<VoiceLease>,
    pub(crate) capture_sequence: u64,
    saver: Option<(CancellationToken, JoinHandle<()>)>,
    pub(crate) ready: bool,
}

pub(crate) struct Runtime {
    pub(crate) handle: CoreHandle,
    pub(crate) playback: watch::Receiver<PlaybackState>,
    runner: JoinHandle<furica_core::Result<()>>,
    monitor: JoinHandle<()>,
    cancel: CancellationToken,
}

impl Host {
    pub async fn new(
        config: HostConfig,
        components: Components,
        data: SessionData,
        snapshot_path: PathBuf,
    ) -> Result<Arc<Self>> {
        config.validate()?;
        let manager = SessionManager::new(
            data,
            components.executor.clone(),
            config.workspace.clone(),
            config.session_limits(),
        )?;
        let store = Arc::new(SnapshotStore::new(
            snapshot_path,
            config.limits.snapshot_max_bytes,
        ));
        let (events, _) = broadcast::channel(config.limits.channel_capacity);
        let host = Arc::new(Self {
            config,
            components,
            store,
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
                saver: None,
                ready: true,
            }),
        });
        {
            let mut state = host.state.lock().await;
            host.start_saver(&mut state);
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
                let reason = state.voice.as_ref().and_then(|lease| {
                    if lease.cancelled.is_cancelled() {
                        Some("delivery_failed")
                    } else if lease.last_seen.elapsed()
                        >= Duration::from_millis(host.config.limits.heartbeat_timeout_ms)
                    {
                        Some("heartbeat_timeout")
                    } else if lease.retry_expired(Duration::from_millis(
                        host.config.limits.upload_retry_window_ms,
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

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }
    pub(crate) fn emit(&self, event: Value) {
        let _ = self.events.send(event);
    }
    pub(crate) fn usable(&self, state: &HostState) -> Result<()> {
        if self.stopped.load(Ordering::Acquire)
            || !state.ready
            || !self.storage_healthy.load(Ordering::Acquire)
        {
            return Err(Error::new(
                503,
                "host_not_ready",
                "host runtime or persistent storage is unavailable",
            ));
        }
        Ok(())
    }
    pub async fn health(&self) -> Value {
        let state = self.state.lock().await;
        let playback = state
            .runtime
            .as_ref()
            .map(|r| *r.playback.borrow())
            .unwrap_or(PlaybackState {
                generation: 0,
                active: false,
            });
        json!({"ready":state.ready && !self.stopped.load(Ordering::Acquire) && self.storage_healthy.load(Ordering::Acquire),
            "storage":if self.storage_healthy.load(Ordering::Acquire) {"healthy"} else {"failed"},
            "interaction_state":state.runtime.as_ref().map(|r| format!("{:?}",r.handle.state())).unwrap_or("Standby".into()),
            "active_session_id":state.selected,"voice_available":self.components.voice_available,
            "voice_channel_id":state.voice.as_ref().map(|v| v.id),"playback_generation":playback.generation,"playback_active":playback.active,
            "sample_rate":16000,"channels":1,"encoding":"pcm_s16le","max_batch_samples":3200,
            "heartbeat_interval_ms":5000,"heartbeat_timeout_ms":self.config.limits.heartbeat_timeout_ms,
            "upload_retry_limit":self.config.limits.upload_retry_limit,"upload_retry_window_ms":self.config.limits.upload_retry_window_ms})
    }

    pub async fn flush(&self) -> Result<()> {
        let state = self.state.lock().await;
        self.persist(&state.manager).await
    }
    pub(crate) async fn persist(&self, manager: &SessionManager) -> Result<()> {
        let result = self.store.save_manager(manager).await;
        self.storage_healthy
            .store(result.is_ok(), Ordering::Release);
        self.emit(json!({"kind":"storage","healthy":result.is_ok()}));
        result
    }
    pub(crate) fn start_saver(&self, state: &mut HostState) {
        // Subscribe first, then capture the initial snapshot in the task.
        let mut changes = state.manager.changes();
        let manager = state.manager.clone();
        let store = self.store.clone();
        let healthy = self.storage_healthy.clone();
        let events = self.events.clone();
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let task = tokio::spawn(async move {
            loop {
                changes.borrow_and_update();
                let mut success = false;
                for attempt in 0..3 {
                    let result = store.save_manager(&manager).await;
                    healthy.store(result.is_ok(), Ordering::Release);
                    let _ = events.send(json!({"kind":"storage","healthy":result.is_ok()}));
                    if result.is_ok() {
                        success = true;
                        break;
                    }
                    if attempt < 2 {
                        tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(Duration::from_millis(100 << attempt)) => () }
                    }
                }
                if !success {
                    // Remain failed until an explicit flush/new change succeeds; no unbounded retries.
                    let _ = events.send(json!({"kind":"error","code":"storage_failed","message":"snapshot was not acknowledged"}));
                }
                let _ = events.send(json!({"kind":"sessions_changed"}));
                tokio::select! { biased; _ = stop.cancelled() => return, changed = changes.changed() => if changed.is_err() {return;} }
            }
        });
        state.saver = Some((cancel, task));
    }
    pub(crate) async fn stop_saver(&self, state: &mut HostState) {
        if let Some((cancel, task)) = state.saver.take() {
            cancel.cancel();
            let _ = task.await;
        }
    }

    pub(crate) fn start_runtime(&self, state: &mut HostState, id: Uuid) -> Result<()> {
        let (handle, mut output) = Core::start(
            self.config.core_config(),
            self.components.backend.clone(),
            state.manager.clone(),
            id,
        )?;
        let mut events = handle.subscribe();
        let playback = output.playback.clone();
        let mut playback_changes = playback.clone();
        let delivery = self.delivery.clone();
        let sender = self.events.clone();
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let monitor = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => break,
                    changed = playback_changes.changed() => {
                        if changed.is_err() { break; }
                        let current = *playback_changes.borrow_and_update();
                        let channel = delivery.lock().ok().and_then(|d| d.as_ref().map(|(id,_,_)| *id));
                        let _ = sender.send(json!({"kind":"playback","channel_id":channel,"generation":current.generation,"active":current.active}));
                    },
                    event = events.recv() => {
                        let value = match event {
                            Ok(CoreEvent::State(s)) => json!({"kind":"state","session_id":id,"state":format!("{s:?}")}),
                            Ok(CoreEvent::Transcript {user,text,final_chunk}) => json!({"kind":"transcript","session_id":id,"user":user,"text":text,"final_chunk":final_chunk}),
                            Ok(CoreEvent::Execution(_)) => json!({"kind":"sessions_changed"}),
                            Ok(CoreEvent::Error(_)) => {
                                if let Ok(d) = delivery.lock() && let Some((_,_,cancel)) = d.as_ref() {cancel.cancel();}
                                json!({"kind":"error","code":"runtime_failed","message":"capability failed; query current state"})
                            },
                            Err(broadcast::error::RecvError::Lagged(_)) => json!({"kind":"resync"}),
                            Err(broadcast::error::RecvError::Closed) => break,
                            _ => continue,
                        };
                        let _ = sender.send(value);
                    },
                    chunk = output.audio.recv() => {
                        let Some(chunk) = chunk else { break; };
                        if chunk.generation != playback_changes.borrow().generation { continue; }
                        if let Ok(d) = delivery.lock() && let Some((_,tx,cancel)) = d.as_ref() && tx.try_send(chunk).is_err() {cancel.cancel();}
                    },
                }
            }
        });
        state.runtime = Some(Runtime {
            handle,
            playback,
            runner: output.runner,
            monitor,
            cancel,
        });
        state.selected = Some(id);
        Ok(())
    }

    pub(crate) async fn reset_backend(&self, runtime: &Runtime) -> Result<()> {
        // Core's explicit end_conversation cancels pending ACP questions. The
        // public unmute control also drops the backend and enters Standby, but
        // preserves those questions, so an audio-only transition uses it.
        runtime.handle.mute(false).await.map_err(Into::into)
    }

    pub(crate) async fn active(&self, runtime: &Runtime) -> Result<()> {
        if runtime.handle.state() == CoreState::Active {
            return Ok(());
        }
        let mut events = runtime.handle.subscribe();
        let activate = async {
            runtime.handle.wake(None).await?;
            if runtime.handle.state() == CoreState::Active {
                return Ok(());
            }
            loop {
                match events.recv().await {
                    Ok(CoreEvent::State(CoreState::Active)) => return Ok(()),
                    Ok(CoreEvent::Error(_))
                    | Ok(CoreEvent::State(CoreState::Standby | CoreState::Stopped)) => {
                        return Err(Error::new(
                            503,
                            "activation_failed",
                            "backend activation failed",
                        ));
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(Error::new(
                            503,
                            "activation_failed",
                            "backend activation failed",
                        ));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => match runtime.handle.state() {
                        CoreState::Active => return Ok(()),
                        CoreState::Standby | CoreState::Stopped => {
                            return Err(Error::new(
                                503,
                                "activation_failed",
                                "backend activation failed",
                            ));
                        }
                        _ => (),
                    },
                    _ => (),
                }
            }
        };
        tokio::time::timeout(
            Duration::from_millis(self.config.limits.connect_timeout_ms + 500),
            activate,
        )
        .await
        .map_err(|_| Error::new(504, "activation_timeout", "backend did not become active"))?
    }

    pub(crate) async fn select_locked(
        &self,
        state: &mut HostState,
        id: Uuid,
        explicit: bool,
    ) -> Result<()> {
        self.usable(state)?;
        let session = state.manager.session(id)?;
        if session.state != furica_core::sessions::SessionState::Active {
            return Err(Error::conflict());
        }
        if state.selected == Some(id) {
            return Ok(());
        }
        if state.selected.is_some() {
            if !explicit || state.voice.is_some() || !switching_allowed(&state.manager)? {
                return Err(Error::conflict());
            }
            // Stop inference first; it may have prepared work immediately before stopping.
            if let Some(runtime) = &state.runtime {
                runtime.handle.end_conversation().await?;
            }
            if !switching_allowed(&state.manager)? {
                return Err(Error::conflict());
            }
            state.ready = false;
            self.stop_saver(state).await;
            let shutdown = self.stop_runtime(state).await;
            let saved = self.persist(&state.manager).await;
            if shutdown.is_err() || saved.is_err() {
                state.ready = false;
                return shutdown.and(saved);
            }
            let data = state.manager.snapshot()?;
            state.manager = SessionManager::new(
                data,
                self.components.executor.clone(),
                self.config.workspace.clone(),
                self.config.session_limits(),
            )?;
            self.start_saver(state);
            state.capture_sequence = 0;
        }
        let started = self.start_runtime(state, id);
        if started.is_ok() {
            state.ready = true;
        }
        started
    }
    pub async fn select(&self, id: Uuid) -> Result<()> {
        let mut state = self.state.lock().await;
        self.select_locked(&mut state, id, true).await
    }
    pub async fn text(&self, id: Uuid, text: String) -> Result<()> {
        if text.trim().is_empty() || text.len() > 64 * 1024 {
            return Err(Error::new(
                400,
                "invalid_text",
                "text must contain 1–65536 bytes",
            ));
        }
        let mut state = self.state.lock().await;
        self.select_locked(&mut state, id, false).await?;
        let runtime = state.runtime.as_ref().ok_or_else(Error::conflict)?;
        self.active(runtime).await?;
        runtime.handle.text(text).await.map_err(Into::into)
    }
    pub async fn interrupt(&self) -> Result<()> {
        let state = self.state.lock().await;
        self.usable(&state)?;
        state
            .runtime
            .as_ref()
            .ok_or_else(Error::conflict)?
            .handle
            .interrupt()
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn stop_runtime(&self, state: &mut HostState) -> Result<()> {
        let Some(runtime) = state.runtime.take() else {
            return Ok(());
        };
        let shutdown =
            tokio::time::timeout(Duration::from_secs(12), runtime.handle.shutdown()).await;
        runtime.cancel.cancel();
        let _ = runtime.monitor.await;
        let mut runner = runtime.runner;
        let joined = tokio::time::timeout(Duration::from_secs(1), &mut runner).await;
        let runner_ok = matches!(joined, Ok(Ok(Ok(()))));
        if joined.is_err() {
            runner.abort();
            let _ = runner.await;
        }
        if !matches!(shutdown, Ok(Ok(()))) || !runner_ok {
            return Err(Error::new(
                504,
                "shutdown_failed",
                "runtime cleanup did not complete",
            ));
        }
        Ok(())
    }
    pub async fn shutdown(&self) -> Result<()> {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.lifetime.cancel();
        let mut state = self.state.lock().await;
        let voice = self.close_voice_locked(&mut state, "host_shutdown").await;
        let runtime = self.stop_runtime(&mut state).await;
        let manager = if state.runtime.is_none() {
            state
                .manager
                .shutdown(Duration::from_secs(10))
                .await
                .map_err(Into::into)
        } else {
            Ok(())
        };
        self.stop_saver(&mut state).await;
        let saved = self.persist(&state.manager).await;
        voice.and(runtime).and(manager).and(saved)
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        self.lifetime.cancel();
        let state = self.state.get_mut();
        if let Some((cancel, task)) = state.saver.take() {
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
