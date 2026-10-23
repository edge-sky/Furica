use super::*;

impl Host {
    pub async fn flush(&self) -> Result<()> {
        let state = self.state.lock().await;
        self.persist(&state.manager).await
    }
    pub(super) async fn persist(&self, manager: &SessionManager) -> Result<()> {
        let result = self.store.save_manager(manager).await;
        self.storage_healthy
            .store(result.is_ok(), Ordering::Release);
        self.emit(json!({"kind":"storage","healthy":result.is_ok()}));
        result
    }
    pub(super) fn start_saver(&self, state: &mut HostState) {
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
                let mut failure = None;
                for attempt in 0..3 {
                    let result = store.save_manager(&manager).await;
                    healthy.store(result.is_ok(), Ordering::Release);
                    let _ = events.send(json!({"kind":"storage","healthy":result.is_ok()}));
                    match result {
                        Ok(()) => {
                            failure = None;
                            break;
                        }
                        Err(error) => failure = Some(error),
                    }
                    if attempt < 2 {
                        tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(Duration::from_millis(100 << attempt)) => () }
                    }
                }
                if let Some(error) = failure {
                    // Keep commit failures distinct from post-commit acknowledgement failures.
                    // Recovery waits for an explicit flush or a new change; no unbounded retries.
                    let _ = events
                        .send(json!({"kind":"error","code":error.code,"message":error.message}));
                }
                let _ = events.send(json!({"kind":"sessions_changed"}));
                tokio::select! { biased; _ = stop.cancelled() => return, changed = changes.changed() => if changed.is_err() {return;} }
            }
        });
        state.saver = Some((cancel, task));
    }
    pub(super) async fn stop_saver(&self, state: &mut HostState) {
        if let Some((cancel, task)) = state.saver.take() {
            cancel.cancel();
            let _ = task.await;
        }
    }
}
