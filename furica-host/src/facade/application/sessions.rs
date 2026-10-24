use crate::{Host, Result};
use furica_core::sessions::{Session, SessionData, SessionManager};
use serde_json::Value;
use uuid::Uuid;

impl Host {
    pub async fn create_session(&self) -> Result<Session> {
        let state = self.state.lock().await;
        self.usable(&state)?;
        let session = self.database(|store| store.create_session()).await?;
        self.emit(serde_json::json!({"kind":"sessions_changed"}));
        Ok(session)
    }
    pub async fn rename_session(&self, id: Uuid, name: String) -> Result<Session> {
        let state = self.state.lock().await;
        self.usable(&state)?;
        if state.selected == Some(id) {
            let session = state.manager.rename_session(id, name)?;
            self.receive_messages(&state.manager).await?;
            return Ok(session);
        }
        let manager = self.context_manager(id).await?;
        let session = manager.rename_session(id, name)?;
        self.receive_messages(&manager).await?;
        let stream = manager.delivery_id();
        self.database(move |store| store.finish_delivery(stream))
            .await?;
        Ok(session)
    }
    pub async fn archive(&self, id: Uuid) -> Result<Session> {
        let mut state = self.state.lock().await;
        self.usable(&state)?;
        if state.selected == Some(id) {
            self.prepare_replacement(&mut state).await?;
            self.rebuild_sessions(&mut state).await?;
        }
        let manager = self.context_manager(id).await?;
        let session = manager.archive_session(id)?;
        self.receive_messages(&manager).await?;
        let stream = manager.delivery_id();
        self.database(move |store| store.finish_delivery(stream))
            .await?;
        state.ready = true;
        Ok(session)
    }
    async fn context_manager(&self, id: Uuid) -> Result<SessionManager> {
        let session = self.database(move |store| store.session(id)).await?;
        // Metadata commands need only the owner; historical records remain in the database.
        Ok(SessionManager::new(
            SessionData {
                sessions: vec![session],
                ..Default::default()
            },
            self.components.executor.clone(),
            self.config.workspace.clone(),
            self.config.session_limits(),
        )?)
    }
    pub async fn session_status(&self, id: Uuid) -> Result<Value> {
        let state = self.state.lock().await;
        self.receive_messages(&state.manager).await?;
        let session = self.database(move |store| store.session(id)).await?;
        Ok(
            serde_json::json!({"session":session,"selected":state.selected==Some(id),"interaction_state":if state.selected==Some(id) {state.runtime.as_ref().map(|r|format!("{:?}",r.handle.state())).unwrap_or("Standby".into())}else{"Standby".into()}}),
        )
    }
    pub async fn page(
        &self,
        owner: Option<Uuid>,
        resource: String,
        offset: usize,
        limit: usize,
    ) -> Result<Value> {
        // A read observes all business messages already produced by the current runtime.
        self.flush().await?;
        self.database(move |store| store.page(owner, &resource, offset, limit))
            .await
    }
    /// Explicit diagnostic/export query. HTTP pagination never loads this full value.
    pub async fn query(&self) -> Result<(SessionData, Option<Uuid>, String)> {
        let state = self.state.lock().await;
        self.receive_messages(&state.manager).await?;
        let data = self
            .database(|store| {
                let mut data = SessionData::default();
                for id in store.session_ids()? {
                    let mut context = store.context(id)?;
                    data.sessions.append(&mut context.sessions);
                    data.messages.append(&mut context.messages);
                    data.acp_sessions.append(&mut context.acp_sessions);
                    data.runs.append(&mut context.runs);
                    data.tool_calls.append(&mut context.tool_calls);
                    data.questions.append(&mut context.questions);
                }
                Ok(data)
            })
            .await?;
        Ok((
            data,
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
    manager.switching_allowed().map_err(Into::into)
}
