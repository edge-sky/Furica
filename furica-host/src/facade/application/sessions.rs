use crate::{Error, Host, Result};
use furica_core::sessions::{SessionData, SessionManager};
use uuid::Uuid;
impl Host {
    pub async fn archive(&self, id: Uuid) -> Result<furica_core::sessions::Session> {
        let mut state = self.state.lock().await;
        self.usable(&state)?;
        if state.selected == Some(id) {
            self.prepare_replacement(&mut state).await?;
            self.rebuild_sessions(&mut state)
                .await
                .map_err(|_| Error::new(503, "archive_failed", "runtime cleanup or save failed"))?;
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

pub(super) fn switching_allowed(manager: &SessionManager) -> Result<bool> {
    let data = manager.snapshot()?;
    Ok(!data.runs.iter().any(|r| !r.state.is_terminal())
        && !data
            .questions
            .iter()
            .any(|q| q.state == furica_core::sessions::QuestionState::Pending))
}
