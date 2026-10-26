//! One application boundary for saved updates and explicit reloads.
use super::*;

impl Host {
    pub async fn update_configuration(self: &Arc<Self>, config: HostConfig) -> Result<()> {
        let host = self.clone();
        // A disconnected HTTP caller must not abandon a partially stopped runtime.
        tokio::spawn(async move { host.apply_configuration(Some(config)).await })
            .await
            .map_err(|_| {
                Error::new(
                    503,
                    "configuration_update_failed",
                    "configuration worker failed",
                )
            })?
    }

    pub async fn reload_configuration(self: &Arc<Self>) -> Result<()> {
        let host = self.clone();
        tokio::spawn(async move { host.apply_configuration(None).await })
            .await
            .map_err(|_| {
                Error::new(
                    503,
                    "configuration_update_failed",
                    "configuration worker failed",
                )
            })?
    }

    pub async fn saved_configuration(&self) -> Result<HostConfig> {
        let store = self.config_store.clone();
        tokio::task::spawn_blocking(move || store.read())
            .await
            .map_err(|_| Error::storage())?
    }

    async fn apply_configuration(&self, supplied: Option<HostConfig>) -> Result<()> {
        let _update = self.components.update_lock.lock().await;
        if self.stopped.load(Ordering::Acquire) {
            return Err(Error::new(503, "host_not_ready", "host has stopped"));
        }
        let save = supplied.is_some();
        let config = match supplied {
            Some(config) => config,
            None => self.saved_configuration().await?,
        };
        config.validate()?;
        self.check_startup_configuration(&config)?;
        let candidate = Arc::new(
            self.components
                .prepare(&config, self.config_store.directory())
                .await?,
        );
        let manager = match SessionManager::with_agents(
            SessionData::default(),
            candidate.agents.clone(),
            config.session_limits(),
        ) {
            Ok(manager) => manager,
            Err(error) => {
                let _ = candidate.shutdown().await;
                return Err(error.into());
            }
        };

        self.updating.store(true, Ordering::Release);
        let result = self
            .replace_components(candidate.clone(), manager, save)
            .await;
        self.updating.store(false, Ordering::Release);
        if result.is_err() {
            let _ = candidate.shutdown().await;
        }
        self.emit(json!({"kind":"configuration_changed","applied":result.is_ok()}));
        result
    }

    fn check_startup_configuration(&self, candidate: &HostConfig) -> Result<()> {
        let current = self.components.current();
        let active = &current.config;
        let changed = if active.http.listen != candidate.http.listen {
            Some("http.listen requires restart")
        } else if active.http.token_env != candidate.http.token_env {
            Some("http.token_env requires restart")
        } else if active.http.allowed_origins != candidate.http.allowed_origins {
            Some("http.allowed_origins requires restart")
        } else if active.workspace != candidate.workspace {
            Some("workspace requires restart")
        } else if active.limits.channel_capacity != candidate.limits.channel_capacity {
            Some("limits.channel_capacity requires restart")
        } else if active.limits.context_max_bytes != candidate.limits.context_max_bytes {
            Some("limits.context_max_bytes requires restart")
        } else {
            None
        };
        match changed {
            Some(message) => Err(Error::new(409, "restart_required", message)),
            None => Ok(()),
        }
    }

    async fn replace_components(
        &self,
        candidate: Arc<Components>,
        manager: SessionManager,
        save: bool,
    ) -> Result<()> {
        // All business admission takes this lock before using a capability instance.
        let mut state = self.state.lock().await;
        state.ready = false;
        let old = self.components.current();
        old.cancelled.cancel();
        let voice = self
            .close_voice_locked(&mut state, "configuration_updated")
            .await;
        let runtime = self.stop_runtime(&mut state).await;
        let sessions = state
            .manager
            .shutdown(Duration::from_secs(10))
            .await
            .map_err(Into::into);
        let executors = old.shutdown().await;
        self.stop_receiver(&mut state).await;
        let saved = self.receive_messages(&state.manager).await;
        // Attempt every cleanup even when an earlier operation fails. No publication on failure.
        voice.and(runtime).and(sessions).and(executors).and(saved)?;
        let stream = state.manager.delivery_id();
        self.database(move |store| store.finish_delivery(stream))
            .await?;
        if save {
            let store = self.config_store.clone();
            let resources = candidate.clone();
            tokio::task::spawn_blocking(move || store.save(&resources.config))
                .await
                .map_err(|_| Error::storage())??;
        }
        // No fallible I/O after SQL commit. Pending callers observe the new group only.
        state.manager = manager;
        state.selected = None;
        state.capture_sequence = 0;
        self.components.publish(candidate);
        self.start_receiver(&mut state);
        state.ready = true;
        Ok(())
    }
}
