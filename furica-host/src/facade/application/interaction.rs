use super::*;

impl Host {
    pub(super) async fn select_locked(
        &self,
        state: &mut HostState,
        id: Uuid,
        explicit: bool,
    ) -> Result<()> {
        self.usable(state)?;
        let session = self.database(move |store| store.session(id)).await?;
        if session.state != furica_core::sessions::SessionState::Active {
            return Err(Error::conflict());
        }
        if state.selected == Some(id) {
            return Ok(());
        }
        if state.selected.is_some() {
            if !explicit {
                return Err(Error::conflict());
            }
            self.prepare_replacement(state).await?;
            self.rebuild_sessions(state).await?;
        }
        self.stop_receiver(state).await;
        let data = self.database(move |store| store.context(id)).await?;
        state.manager = SessionManager::new(
            data,
            self.components.executor.clone(),
            self.config.workspace.clone(),
            self.config.session_limits(),
        )?;
        self.receive_messages(&state.manager).await?;
        self.start_receiver(state);
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
}
