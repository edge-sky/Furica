//! Receive Core business messages. Database commits and runtime acknowledgement stay in Host.
use super::*;

pub(super) async fn receive(
    manager: &SessionManager,
    store: Arc<SessionStore>,
    serial: Arc<Mutex<()>>,
) -> Result<()> {
    let guard = serial.lock_owned().await;
    let manager = manager.clone();
    // The worker owns serialization through commit + acknowledgement, even if its caller is cancelled.
    tokio::task::spawn_blocking(move || {
        let _guard = guard;
        while let Some((sequence, messages)) = manager.pending_messages()? {
            store.receive(manager.delivery_id(), sequence, &messages)?;
            manager.acknowledge_messages(sequence).map_err(|_| {
                Error::new(
                    503,
                    "message_acknowledgement_failed",
                    "received messages could not be acknowledged by the runtime",
                )
            })?;
        }
        Ok(())
    })
    .await
    .map_err(|_| Error::storage())?
}

impl Host {
    pub async fn flush(&self) -> Result<()> {
        let state = self.state.lock().await;
        self.receive_messages(&state.manager).await
    }
    pub(super) async fn database<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&SessionStore) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || operation(&store))
            .await
            .map_err(|_| Error::storage())?
    }
    pub(super) async fn receive_messages(&self, manager: &SessionManager) -> Result<()> {
        let result = receive(manager, self.store.clone(), self.receiver_serial.clone()).await;
        self.storage_healthy
            .store(result.is_ok(), Ordering::Release);
        self.emit(json!({"kind":"storage","healthy":result.is_ok()}));
        result
    }
    pub(super) fn start_receiver(&self, state: &mut HostState) {
        let mut available = state.manager.messages_available();
        let manager = state.manager.clone();
        let store = self.store.clone();
        let serial = self.receiver_serial.clone();
        let healthy = self.storage_healthy.clone();
        let events = self.events.clone();
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let task = tokio::spawn(async move {
            loop {
                available.borrow_and_update();
                let mut failure = None;
                for attempt in 0..3 {
                    let result = receive(&manager, store.clone(), serial.clone()).await;
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
                        tokio::select! { _=stop.cancelled()=>return, _=tokio::time::sleep(Duration::from_millis(100 << attempt))=>() }
                    }
                }
                if let Some(error) = failure {
                    let _ = events
                        .send(json!({"kind":"error","code":error.code,"message":error.message}));
                } else {
                    let _ = events.send(json!({"kind":"sessions_changed"}));
                }
                tokio::select! { biased; _=stop.cancelled()=>return, changed=available.changed()=>if changed.is_err(){return;} }
            }
        });
        state.receiver = Some((cancel, task));
    }
    pub(super) async fn stop_receiver(&self, state: &mut HostState) {
        if let Some((cancel, task)) = state.receiver.take() {
            cancel.cancel();
            let _ = task.await;
        }
    }
}
