use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use furica_core::{
    Result,
    audio::AudioFrame,
    config::CoreConfig,
    runtime::{Core, CoreHandle, CoreState},
    store::TaskStore,
    task::{
        ExecutionEvent, TaskExecutor, TaskLimits, TaskManager, TaskOutcome, TaskRecord, TaskState,
    },
    voice::{
        PlaybackPosition, VoiceBackend, VoiceChannels, VoiceCommand, VoiceEvent, VoiceSession,
    },
};
use serde_json::json;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

struct ControlledExecutor {
    permits: Semaphore,
    calls: AtomicUsize,
}

impl ControlledExecutor {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            permits: Semaphore::new(0),
            calls: AtomicUsize::new(0),
        })
    }
    fn finish_one(&self) {
        self.permits.add_permits(1);
    }
}

#[async_trait]
impl TaskExecutor for ControlledExecutor {
    async fn execute(
        &self,
        _: TaskRecord,
        cancel: CancellationToken,
        _: mpsc::Sender<ExecutionEvent>,
    ) -> Result<TaskOutcome> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Ok(TaskOutcome { state: TaskState::Cancelled, summary: String::new(), stop_reason: "cancelled".into() }),
            permit = self.permits.acquire() => {
                permit.unwrap().forget();
                Ok(TaskOutcome { state: TaskState::Completed, summary: "verified fixture output".into(), stop_reason: "end_turn".into() })
            }
        }
    }
}

struct ControlledVoice {
    connected: mpsc::Sender<VoiceChannels>,
    ready: Arc<Semaphore>,
}

#[async_trait]
impl VoiceBackend for ControlledVoice {
    fn sample_rate(&self) -> u32 {
        24_000
    }
    async fn connect(&self) -> Result<VoiceSession> {
        self.ready.acquire().await.unwrap().forget();
        let (session, channels) = VoiceSession::channels(128)?;
        self.connected.send(channels).await.unwrap();
        Ok(session)
    }
}

fn manager(
    store: Arc<TaskStore>,
    executor: Arc<dyn TaskExecutor>,
    workspace: &tempfile::TempDir,
) -> TaskManager {
    TaskManager::new(
        store,
        executor,
        workspace.path().to_owned(),
        TaskLimits::default(),
    )
    .unwrap()
}

fn frame(sequence: u64) -> AudioFrame {
    AudioFrame {
        sequence,
        sample_rate: 24_000,
        samples: vec![sequence as i16; 240],
    }
}

async fn state(handle: &CoreHandle, expected: CoreState) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while handle.state() != expected {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

async fn task_state(tasks: &TaskManager, id: Uuid, expected: TaskState) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while tasks.get(id).unwrap().state != expected {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

fn voice(ready: usize) -> (Arc<ControlledVoice>, mpsc::Receiver<VoiceChannels>) {
    let (connected, rx) = mpsc::channel(4);
    (
        Arc::new(ControlledVoice {
            connected,
            ready: Arc::new(Semaphore::new(ready)),
        }),
        rx,
    )
}

#[tokio::test]
async fn queued_standby_audio_is_not_uploaded_and_connecting_audio_is_ordered() {
    let workspace = tempfile::tempdir().unwrap();
    let tasks = manager(
        Arc::new(TaskStore::in_memory().unwrap()),
        ControlledExecutor::new(),
        &workspace,
    );
    let (backend, mut connections) = voice(0);
    let (handle, output) = Core::start(CoreConfig::default(), backend.clone(), tasks).unwrap();
    // No scheduling yield: the wake command must exclude even unconsumed ingress.
    handle.audio(frame(1)).unwrap();
    handle.audio(frame(2)).unwrap();
    handle.wake(None).await.unwrap();
    handle.audio(frame(3)).unwrap();
    handle.audio(frame(4)).unwrap();
    backend.ready.add_permits(1);
    let mut channels = connections.recv().await.unwrap();
    state(&handle, CoreState::Active).await;
    let first = channels.commands.recv().await.unwrap();
    let second = channels.commands.recv().await.unwrap();
    assert!(matches!(first, VoiceCommand::Audio(ref frame) if frame.sequence == 3));
    assert!(matches!(second, VoiceCommand::Audio(ref frame) if frame.sequence == 4));
    assert!(channels.commands.try_recv().is_err());
    handle.shutdown().await.unwrap();
    output.runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn explicit_wake_replays_only_requested_frames() {
    let workspace = tempfile::tempdir().unwrap();
    let tasks = manager(
        Arc::new(TaskStore::in_memory().unwrap()),
        ControlledExecutor::new(),
        &workspace,
    );
    let (backend, mut connections) = voice(1);
    let (handle, output) = Core::start(CoreConfig::default(), backend, tasks).unwrap();
    handle.audio(frame(10)).unwrap();
    handle.audio(frame(11)).unwrap();
    handle.wake(Some(11)).await.unwrap();
    let mut channels = connections.recv().await.unwrap();
    assert!(
        matches!(channels.commands.recv().await.unwrap(), VoiceCommand::Audio(frame) if frame.sequence == 11)
    );
    assert!(channels.commands.try_recv().is_err());
    handle.shutdown().await.unwrap();
    output.runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn ending_conversation_leaves_acp_task_running() {
    let workspace = tempfile::tempdir().unwrap();
    let executor = ControlledExecutor::new();
    let tasks = manager(
        Arc::new(TaskStore::in_memory().unwrap()),
        executor.clone(),
        &workspace,
    );
    let (backend, mut connections) = voice(1);
    let (handle, output) = Core::start(CoreConfig::default(), backend, tasks.clone()).unwrap();
    handle.wake(None).await.unwrap();
    let mut channels = connections.recv().await.unwrap();
    state(&handle, CoreState::Active).await;
    channels
        .events
        .send(VoiceEvent::ToolCall {
            call_id: "call1".into(),
            name: "start_task".into(),
            arguments: json!({"prompt":"perform a real task"}),
        })
        .await
        .unwrap();
    let result = channels.commands.recv().await.unwrap();
    let id = match result {
        VoiceCommand::ToolResult { result, .. } => {
            assert_eq!(result["accepted"], true);
            Uuid::parse_str(result["task_id"].as_str().unwrap()).unwrap()
        }
        _ => panic!("expected tool acceptance"),
    };
    task_state(&tasks, id, TaskState::Running).await;
    handle.end_conversation().await.unwrap();
    assert_eq!(tasks.get(id).unwrap().state, TaskState::Running);
    executor.finish_one();
    task_state(&tasks, id, TaskState::Completed).await;
    assert_eq!(tasks.get(id).unwrap().summary, "verified fixture output");
    handle.shutdown().await.unwrap();
    output.runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn task_completion_waits_for_user_and_current_response() {
    let workspace = tempfile::tempdir().unwrap();
    let executor = ControlledExecutor::new();
    let tasks = manager(
        Arc::new(TaskStore::in_memory().unwrap()),
        executor.clone(),
        &workspace,
    );
    let (backend, mut connections) = voice(1);
    let (handle, output) = Core::start(CoreConfig::default(), backend, tasks.clone()).unwrap();
    handle.wake(None).await.unwrap();
    let mut channels = connections.recv().await.unwrap();
    state(&handle, CoreState::Active).await;
    channels
        .events
        .send(VoiceEvent::SpeechStarted)
        .await
        .unwrap();
    assert!(matches!(
        channels.commands.recv().await.unwrap(),
        VoiceCommand::Interrupt { .. }
    ));
    let task = tasks.submit("host-request".into(), "task".into()).unwrap();
    task_state(&tasks, task.id, TaskState::Running).await;
    executor.finish_one();
    task_state(&tasks, task.id, TaskState::Completed).await;
    assert!(channels.commands.try_recv().is_err());
    channels
        .events
        .send(VoiceEvent::SpeechStopped)
        .await
        .unwrap();
    channels
        .events
        .send(VoiceEvent::ResponseFinished)
        .await
        .unwrap();
    assert!(
        matches!(channels.commands.recv().await.unwrap(), VoiceCommand::TaskUpdate(record) if record["state"] == "completed")
    );
    handle.shutdown().await.unwrap();
    output.runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn barge_in_invalidates_playback_and_discards_late_audio() {
    let workspace = tempfile::tempdir().unwrap();
    let tasks = manager(
        Arc::new(TaskStore::in_memory().unwrap()),
        ControlledExecutor::new(),
        &workspace,
    );
    let (backend, mut connections) = voice(1);
    let (handle, mut output) = Core::start(CoreConfig::default(), backend, tasks).unwrap();
    handle.wake(None).await.unwrap();
    let mut channels = connections.recv().await.unwrap();
    state(&handle, CoreState::Active).await;
    channels
        .events
        .send(VoiceEvent::Audio {
            item_id: "old".into(),
            content_index: 0,
            sample_rate: 24_000,
            samples: vec![1; 2400],
        })
        .await
        .unwrap();
    let first = output.audio.recv().await.unwrap();
    handle
        .report_playback(
            first.generation,
            PlaybackPosition {
                item_id: "old".into(),
                content_index: 0,
                played_ms: 50,
            },
        )
        .await
        .unwrap();
    channels
        .events
        .send(VoiceEvent::SpeechStarted)
        .await
        .unwrap();
    let interrupt = channels.commands.recv().await.unwrap();
    assert!(
        matches!(interrupt, VoiceCommand::Interrupt { playback: Some(position), cancel_response: false } if position.played_ms == 50)
    );
    assert_ne!(output.playback.borrow().generation, first.generation);
    channels
        .events
        .send(VoiceEvent::Audio {
            item_id: "old".into(),
            content_index: 0,
            sample_rate: 24_000,
            samples: vec![2; 240],
        })
        .await
        .unwrap();
    channels
        .events
        .send(VoiceEvent::Audio {
            item_id: "new".into(),
            content_index: 0,
            sample_rate: 24_000,
            samples: vec![3; 240],
        })
        .await
        .unwrap();
    assert_eq!(output.audio.recv().await.unwrap().position.item_id, "new");
    assert!(output.audio.try_recv().is_err());
    handle.shutdown().await.unwrap();
    output.runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn mute_cancels_connecting_and_cannot_be_woken() {
    let workspace = tempfile::tempdir().unwrap();
    let tasks = manager(
        Arc::new(TaskStore::in_memory().unwrap()),
        ControlledExecutor::new(),
        &workspace,
    );
    let (backend, mut connections) = voice(0);
    let (handle, output) = Core::start(CoreConfig::default(), backend.clone(), tasks).unwrap();
    handle.wake(None).await.unwrap();
    assert_eq!(handle.state(), CoreState::Connecting);
    handle.mute(true).await.unwrap();
    backend.ready.add_permits(1);
    assert!(handle.wake(None).await.is_err());
    handle.audio(frame(1)).unwrap();
    assert!(handle.audio(frame(1)).is_err());
    assert_eq!(handle.state(), CoreState::Muted);
    assert!(connections.try_recv().is_err());
    handle.shutdown().await.unwrap();
    output.runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn duplicate_submission_executes_once_and_rejects_changed_content() {
    let workspace = tempfile::tempdir().unwrap();
    let executor = ControlledExecutor::new();
    let tasks = manager(
        Arc::new(TaskStore::in_memory().unwrap()),
        executor.clone(),
        &workspace,
    );
    let first = tasks.submit("same-key".into(), "same-task".into()).unwrap();
    let second = tasks.submit("same-key".into(), "same-task".into()).unwrap();
    assert_eq!(first.id, second.id);
    assert!(
        tasks
            .submit("same-key".into(), "changed-task".into())
            .is_err()
    );
    task_state(&tasks, first.id, TaskState::Running).await;
    executor.finish_one();
    task_state(&tasks, first.id, TaskState::Completed).await;
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    tasks.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn cancelling_queued_task_does_not_start_executor() {
    let workspace = tempfile::tempdir().unwrap();
    let executor = ControlledExecutor::new();
    let tasks = manager(
        Arc::new(TaskStore::in_memory().unwrap()),
        executor.clone(),
        &workspace,
    );
    let first = tasks.submit("first".into(), "first".into()).unwrap();
    task_state(&tasks, first.id, TaskState::Running).await;
    let second = tasks.submit("second".into(), "second".into()).unwrap();
    tasks.cancel(second.id).unwrap();
    task_state(&tasks, second.id, TaskState::Cancelled).await;
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    tasks.cancel(first.id).unwrap();
    task_state(&tasks, first.id, TaskState::Cancelled).await;
    tasks.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn restart_marks_unfinished_tasks_interrupted_without_reexecution() {
    let workspace = tempfile::tempdir().unwrap();
    let db_path = workspace.path().join("tasks.sqlite3");
    let executor = ControlledExecutor::new();
    let store = Arc::new(TaskStore::open(&db_path).unwrap());
    let tasks = manager(store, executor, &workspace);
    let task = tasks
        .submit("persisted-key".into(), "persisted prompt".into())
        .unwrap();
    tasks.shutdown(Duration::from_secs(1)).await.unwrap();
    drop(tasks);
    // Simulate the durable snapshot left by a crash, without racing a worker.
    let mut unfinished = task.clone();
    unfinished.state = TaskState::Running;
    rusqlite::Connection::open(&db_path)
        .unwrap()
        .execute(
            "UPDATE tasks SET record=?1 WHERE id=?2",
            rusqlite::params![
                serde_json::to_string(&unfinished).unwrap(),
                task.id.to_string()
            ],
        )
        .unwrap();
    let executor = ControlledExecutor::new();
    let recovered = manager(
        Arc::new(TaskStore::open(db_path).unwrap()),
        executor.clone(),
        &workspace,
    );
    let record = recovered.get(task.id).unwrap();
    assert_eq!(record.state, TaskState::Interrupted);
    assert_eq!(record.prompt, "persisted prompt");
    assert_eq!(
        recovered
            .submit("persisted-key".into(), "persisted prompt".into())
            .unwrap()
            .id,
        task.id
    );
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    recovered.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn silence_returns_to_standby_even_with_continuous_capture_frames() {
    let workspace = tempfile::tempdir().unwrap();
    let tasks = manager(
        Arc::new(TaskStore::in_memory().unwrap()),
        ControlledExecutor::new(),
        &workspace,
    );
    let (backend, mut connections) = voice(1);
    let config = CoreConfig {
        idle_timeout: Duration::from_millis(20),
        ..CoreConfig::default()
    };
    let (handle, output) = Core::start(config, backend, tasks).unwrap();
    handle.wake(None).await.unwrap();
    let _channels = connections.recv().await.unwrap();
    state(&handle, CoreState::Active).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        for sequence in 1.. {
            if handle.state() == CoreState::Standby {
                break;
            }
            handle.audio(frame(sequence)).unwrap();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    handle.shutdown().await.unwrap();
    output.runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn connection_buffer_overflow_reports_error_and_returns_to_standby() {
    let workspace = tempfile::tempdir().unwrap();
    let tasks = manager(
        Arc::new(TaskStore::in_memory().unwrap()),
        ControlledExecutor::new(),
        &workspace,
    );
    let (backend, _) = voice(0);
    let config = CoreConfig {
        pre_roll: Duration::ZERO,
        connect_buffer: Duration::from_millis(20),
        ..CoreConfig::default()
    };
    let (handle, output) = Core::start(config, backend, tasks).unwrap();
    let mut events = handle.subscribe();
    handle.wake(None).await.unwrap();
    for sequence in 1..=3 {
        handle.audio(frame(sequence)).unwrap();
    }
    state(&handle, CoreState::Standby).await;
    assert!(
        std::iter::from_fn(|| events.try_recv().ok())
            .any(|event| matches!(event, furica_core::runtime::CoreEvent::Error(_)))
    );
    handle.shutdown().await.unwrap();
    output.runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn task_announcement_waits_until_current_audio_has_actually_played() {
    let workspace = tempfile::tempdir().unwrap();
    let executor = ControlledExecutor::new();
    let tasks = manager(
        Arc::new(TaskStore::in_memory().unwrap()),
        executor.clone(),
        &workspace,
    );
    let (backend, mut connections) = voice(1);
    let (handle, mut output) = Core::start(CoreConfig::default(), backend, tasks.clone()).unwrap();
    handle.wake(None).await.unwrap();
    let mut channels = connections.recv().await.unwrap();
    state(&handle, CoreState::Active).await;
    channels
        .events
        .send(VoiceEvent::ResponseStarted)
        .await
        .unwrap();
    channels
        .events
        .send(VoiceEvent::Audio {
            item_id: "acknowledgement".into(),
            content_index: 0,
            sample_rate: 24000,
            samples: vec![1; 2400],
        })
        .await
        .unwrap();
    let chunk = output.audio.recv().await.unwrap();
    let task = tasks.submit("announcement".into(), "task".into()).unwrap();
    task_state(&tasks, task.id, TaskState::Running).await;
    executor.finish_one();
    task_state(&tasks, task.id, TaskState::Completed).await;
    channels
        .events
        .send(VoiceEvent::ResponseFinished)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), channels.commands.recv())
            .await
            .is_err()
    );
    handle
        .report_playback(
            chunk.generation,
            PlaybackPosition {
                item_id: "acknowledgement".into(),
                content_index: 0,
                played_ms: 100,
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        channels.commands.recv().await.unwrap(),
        VoiceCommand::TaskUpdate(_)
    ));
    handle.shutdown().await.unwrap();
    output.runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn new_voice_session_can_query_tasks_from_before_wake() {
    let workspace = tempfile::tempdir().unwrap();
    let executor = ControlledExecutor::new();
    let tasks = manager(
        Arc::new(TaskStore::in_memory().unwrap()),
        executor.clone(),
        &workspace,
    );
    let task = tasks
        .submit("previous-session".into(), "previous task".into())
        .unwrap();
    task_state(&tasks, task.id, TaskState::Running).await;
    executor.finish_one();
    task_state(&tasks, task.id, TaskState::Completed).await;
    let (backend, mut connections) = voice(2);
    let (handle, output) = Core::start(CoreConfig::default(), backend, tasks).unwrap();
    for _ in 0..2 {
        handle.wake(None).await.unwrap();
        let mut channels = connections.recv().await.unwrap();
        state(&handle, CoreState::Active).await;
        let metadata = channels.commands.recv().await.unwrap();
        let task_id = match metadata {
            VoiceCommand::TaskContext(tasks) => tasks[0]["task_id"].clone(),
            _ => panic!("expected restored task metadata"),
        };
        channels
            .events
            .send(VoiceEvent::ToolCall {
                call_id: "status".into(),
                name: "get_task_status".into(),
                arguments: json!({"task_id":task_id}),
            })
            .await
            .unwrap();
        assert!(
            matches!(channels.commands.recv().await.unwrap(), VoiceCommand::ToolResult { result, .. } if result["state"] == "completed")
        );
        handle.end_conversation().await.unwrap();
    }
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    handle.shutdown().await.unwrap();
    output.runner.await.unwrap().unwrap();
}
