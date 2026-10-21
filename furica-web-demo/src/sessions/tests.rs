use super::infrastructure::SnapshotStore;
use super::*;
use furica_core::sessions::Session;
use std::fs;
use uuid::Uuid;

#[test]
fn atomic_snapshots_round_trip_and_invalid_data_preserves_last_save() {
    let dir = tempfile::tempdir().unwrap();
    let store = SnapshotStore {
        path: dir.path().join("sessions.json"),
    };
    assert!(store.load().unwrap().sessions.is_empty());
    let data = SessionData {
        sessions: vec![Session::new()],
        ..Default::default()
    };
    store.save(&data).unwrap();
    assert_eq!(store.load().unwrap().sessions[0].id, data.sessions[0].id);
    let mut invalid = data.clone();
    invalid.sessions.push(data.sessions[0].clone());
    assert!(store.save(&invalid).is_err());
    assert_eq!(store.load().unwrap().sessions.len(), 1);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[test]
fn corrupt_snapshots_are_reported_instead_of_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let store = SnapshotStore {
        path: dir.path().join("sessions.json"),
    };
    fs::write(&store.path, "broken").unwrap();
    assert!(store.load().is_err());
    assert_eq!(fs::read_to_string(&store.path).unwrap(), "broken");
}

struct ModelTools;

#[async_trait::async_trait]
impl furica_core::orchestration::VoiceBackend for ModelTools {
    fn sample_rate(&self) -> u32 {
        16_000
    }
    async fn connect(
        &self,
        _: SessionManager,
        _: Uuid,
    ) -> Result<furica_core::orchestration::VoiceSession> {
        use furica_core::orchestration::{VoiceCommand, VoiceEvent, VoiceSession};
        let (session, mut channels) = VoiceSession::channels(32)?;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = channels.cancelled.cancelled() => break,
                    command = channels.commands.recv() => match command {
                        Some(VoiceCommand::Text(prompt)) => {
                            let _ = channels.events.send(VoiceEvent::ToolCall {
                                call_id: "model-prepared-work".into(), name: "prepare_run".into(), arguments: json!({"prompt":prompt}),
                            }).await;
                        }
                        Some(VoiceCommand::ToolResult { .. }) => { let _ = channels.events.send(VoiceEvent::ResponseFinished).await; }
                        None => break,
                        _ => (),
                    }
                }
            }
        });
        Ok(session)
    }
}

struct SavedExecutor {
    store: Arc<SnapshotStore>,
    observed: mpsc::UnboundedSender<bool>,
}

#[async_trait::async_trait]
impl furica_core::sessions::Executor for SavedExecutor {
    fn agent_id(&self) -> &str {
        "test-agent"
    }
    async fn run_session(
        &self,
        _: furica_core::sessions::AcpSession,
        mut inputs: mpsc::UnboundedReceiver<furica_core::sessions::ExecutionInput>,
        events: mpsc::Sender<furica_core::sessions::ExecutionEvent>,
        cancel: CancellationToken,
    ) -> Result<()> {
        use furica_core::sessions::{ExecutionEvent, ExecutionInput, RunOutcome, RunState};
        events
            .send(ExecutionEvent::Opened {
                remote_session_id: "test-remote".into(),
            })
            .await
            .map_err(|_| Error::Closed)?;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                input = inputs.recv() => match input {
                    Some(ExecutionInput::Prompt { run_id, .. }) => {
                        let saved = self.store.load()?;
                        let persisted = saved.runs.iter().any(|r| r.id == run_id && r.state == RunState::Prepared);
                        let _ = self.observed.send(persisted);
                        events.send(ExecutionEvent::RunFinished { run_id, outcome: RunOutcome {
                            state: RunState::Completed, summary: "done".into(), stop_reason: Some("end_turn".into()), error: None,
                        }}).await.map_err(|_| Error::Closed)?;
                    }
                    Some(ExecutionInput::Close) | None => break,
                    _ => (),
                }
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn model_prepared_work_waits_for_successful_save_and_is_not_replayed() {
    use furica_core::{
        config::CoreConfig,
        orchestration::{Core, CoreState},
        sessions::{RunState, SessionLimits},
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SnapshotStore {
        path: dir.path().join("sessions.json"),
    });
    let (tx, mut observed) = mpsc::unbounded_channel();
    let manager = SessionManager::new(
        SessionData::default(),
        Arc::new(SavedExecutor {
            store: store.clone(),
            observed: tx,
        }),
        dir.path().into(),
        SessionLimits::default(),
    )
    .unwrap();
    let business = manager.create_session().unwrap();
    let (core, output) = Core::start(
        CoreConfig {
            sample_rate: 16_000,
            ..Default::default()
        },
        Arc::new(ModelTools),
        manager.clone(),
        business.id,
    )
    .unwrap();
    core.wake(None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while core.state() != CoreState::Active {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    core.text("prepare only after saving".into()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while manager.snapshot().unwrap().runs.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        manager.snapshot().unwrap().runs[0].state,
        RunState::Prepared
    );
    assert!(observed.try_recv().is_err());
    let blocker = dir.path().join("not-a-directory");
    fs::write(&blocker, "occupied").unwrap();
    let broken_store = Arc::new(SnapshotStore {
        path: blocker.join("sessions.json"),
    });
    assert!(save(&manager, &broken_store, true).await.is_err());
    assert_eq!(
        manager.snapshot().unwrap().runs[0].state,
        RunState::Prepared
    );
    assert!(observed.try_recv().is_err());
    save(&manager, &store, true).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), observed.recv())
            .await
            .unwrap()
            .unwrap()
    );
    manager.acknowledge_saved(&store.load().unwrap()).unwrap();
    core.shutdown().await.unwrap();
    output.runner.await.unwrap().unwrap();
    assert!(observed.try_recv().is_err());
    let (tx, mut restarted) = mpsc::unbounded_channel();
    let restored = SessionManager::new(
        store.load().unwrap(),
        Arc::new(SavedExecutor {
            store: store.clone(),
            observed: tx,
        }),
        dir.path().into(),
        SessionLimits::default(),
    )
    .unwrap();
    assert_eq!(
        restored.snapshot().unwrap().runs[0].state,
        RunState::Interrupted
    );
    assert_eq!(restored.snapshot().unwrap().sessions[0].id, business.id);
    restored
        .acknowledge_saved(&restored.snapshot().unwrap())
        .unwrap();
    assert!(restarted.try_recv().is_err());
    restored.shutdown(Duration::from_secs(1)).await.unwrap();
}
