use super::switching_allowed;
use crate::{Error, Host, Result};
use furica_core::sessions::{SessionData, SessionManager};
use uuid::Uuid;
impl Host {
    pub async fn archive(&self, id: Uuid) -> Result<furica_core::sessions::Session> {
        let mut state = self.state.lock().await;
        self.usable(&state)?;
        if state.selected == Some(id) {
            if state.voice.is_some() || !switching_allowed(&state.manager)? {
                return Err(Error::conflict());
            }
            if let Some(runtime) = &state.runtime {
                runtime.handle.end_conversation().await?;
            }
            if !switching_allowed(&state.manager)? {
                return Err(Error::conflict());
            }
            state.ready = false;
            self.stop_saver(&mut state).await;
            let stopped = self.stop_runtime(&mut state).await;
            let saved = self.persist(&state.manager).await;
            if stopped.is_err() || saved.is_err() {
                state.ready = false;
                return Err(Error::new(
                    503,
                    "archive_failed",
                    "runtime cleanup or save failed",
                ));
            }
            let data = state.manager.snapshot()?;
            state.manager = SessionManager::new(
                data,
                self.components.executor.clone(),
                self.config.workspace.clone(),
                self.config.session_limits(),
            )?;
            state.selected = None;
            self.start_saver(&mut state);
        }
        let session = state.manager.archive_session(id)?;
        self.persist(&state.manager).await?;
        state.ready = true;
        Ok(session)
    }
    pub async fn create_session(&self) -> Result<furica_core::sessions::Session> {
        let state = self.state.lock().await;
        self.usable(&state)?;
        let session = state.manager.create_session()?;
        self.persist(&state.manager).await?;
        Ok(session)
    }
    pub async fn rename_session(
        &self,
        id: Uuid,
        name: String,
    ) -> Result<furica_core::sessions::Session> {
        let state = self.state.lock().await;
        self.usable(&state)?;
        let session = state.manager.rename_session(id, name)?;
        self.persist(&state.manager).await?;
        Ok(session)
    }
    pub async fn query(&self) -> Result<(SessionData, Option<Uuid>, String)> {
        let state = self.state.lock().await;
        Ok((
            state.manager.snapshot()?,
            state.selected,
            state
                .runtime
                .as_ref()
                .map(|r| format!("{:?}", r.handle.state()))
                .unwrap_or("Standby".into()),
        ))
    }
}
