use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    sync::{broadcast, mpsc, oneshot, watch},
    task::JoinHandle,
    time::Instant,
};
use uuid::Uuid;

use crate::{
    Error, Result,
    audio::{AudioBuffer, AudioFrame},
    config::CoreConfig,
    task::{TaskEvent, TaskManager},
    voice::{PlaybackPosition, VoiceBackend, VoiceCommand, VoiceEvent, VoiceSession},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoreState {
    Standby,
    Connecting,
    Active,
    Muted,
    Stopped,
}

#[derive(Clone, Debug)]
pub enum CoreEvent {
    State(CoreState),
    Transcript {
        user: bool,
        text: String,
        final_chunk: bool,
    },
    Task(TaskEvent),
    ClearPlayback,
    Error(String),
}

#[derive(Clone, Debug)]
pub struct PlaybackChunk {
    pub generation: u64,
    pub position: PlaybackPosition,
    pub sample_rate: u32,
    pub samples: Vec<i16>,
}

/// Hosts must clear playback when this generation changes and ignore chunks from
/// previous generations, even if their event subscriber has lagged.
#[derive(Clone, Copy, Debug)]
pub struct PlaybackState {
    pub generation: u64,
    pub active: bool,
}

pub struct CoreOutput {
    pub audio: mpsc::Receiver<PlaybackChunk>,
    pub playback: watch::Receiver<PlaybackState>,
    pub runner: JoinHandle<Result<()>>,
}

#[derive(Clone)]
pub struct CoreHandle {
    commands: mpsc::Sender<(Action, oneshot::Sender<Result<()>>)>,
    audio: mpsc::Sender<AudioFrame>,
    events: broadcast::Sender<CoreEvent>,
    state: watch::Receiver<CoreState>,
    sample_rate: u32,
    capture_sequence: Arc<Mutex<Option<u64>>>,
}

enum Action {
    Wake(Option<u64>),
    EndConversation,
    Mute(bool),
    Text(String),
    Interrupt,
    Playback {
        generation: u64,
        position: PlaybackPosition,
    },
    Shutdown,
}

impl CoreHandle {
    pub fn state(&self) -> CoreState {
        *self.state.borrow()
    }
    pub fn subscribe(&self) -> broadcast::Receiver<CoreEvent> {
        self.events.subscribe()
    }

    /// Nonblocking ingress. If Busy is returned the host must report the capture
    /// overrun; it must not pretend that the frame was delivered.
    pub fn audio(&self, frame: AudioFrame) -> Result<()> {
        frame.validate(self.sample_rate)?;
        let mut latest = self
            .capture_sequence
            .lock()
            .map_err(|_| Error::State("capture lock poisoned".into()))?;
        if latest.is_some_and(|sequence| frame.sequence <= sequence) {
            return Err(Error::Audio("capture sequence did not increase".into()));
        }
        let sequence = frame.sequence;
        self.audio.try_send(frame).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => Error::Busy("capture queue"),
            mpsc::error::TrySendError::Closed(_) => Error::Closed,
        })?;
        *latest = Some(sequence);
        Ok(())
    }

    async fn action(&self, action: Action) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send((action, tx))
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Only explicitly selected post-wake frames may be replayed. None means
    /// manual activation with no idle-audio replay. A frame boundary is required.
    pub async fn wake(&self, first_request_sequence: Option<u64>) -> Result<()> {
        let first = match first_request_sequence {
            Some(sequence) => sequence,
            None => self
                .capture_sequence
                .lock()
                .map_err(|_| Error::State("capture lock poisoned".into()))?
                .map(|sequence| {
                    sequence
                        .checked_add(1)
                        .ok_or_else(|| Error::Audio("capture sequence exhausted".into()))
                })
                .transpose()?
                .unwrap_or(0),
        };
        self.action(Action::Wake(Some(first))).await
    }
    pub async fn end_conversation(&self) -> Result<()> {
        self.action(Action::EndConversation).await
    }
    pub async fn mute(&self, muted: bool) -> Result<()> {
        self.action(Action::Mute(muted)).await
    }
    pub async fn text(&self, text: String) -> Result<()> {
        self.action(Action::Text(text)).await
    }
    pub async fn interrupt(&self) -> Result<()> {
        self.action(Action::Interrupt).await
    }
    pub async fn report_playback(&self, generation: u64, position: PlaybackPosition) -> Result<()> {
        self.action(Action::Playback {
            generation,
            position,
        })
        .await
    }
    /// Closing the conversation leaves tasks running. Explicit shutdown cancels
    /// tasks and waits for their execution-side cancellation acknowledgement.
    pub async fn shutdown(&self) -> Result<()> {
        self.action(Action::Shutdown).await
    }
}

pub struct Core;

impl Core {
    /// Must be called inside a Tokio runtime. TaskManager and the database have a
    /// single owner; frontends may clone the returned handle.
    pub fn start(
        config: CoreConfig,
        backend: Arc<dyn VoiceBackend>,
        tasks: TaskManager,
    ) -> Result<(CoreHandle, CoreOutput)> {
        config.validate()?;
        tokio::runtime::Handle::try_current()
            .map_err(|_| Error::State("Core::start requires a Tokio runtime".into()))?;
        if backend.sample_rate() != config.sample_rate {
            return Err(Error::Config(
                "backend sample rate does not match core".into(),
            ));
        }
        let (commands, command_rx) = mpsc::channel(config.channel_capacity);
        let (audio, audio_rx) = mpsc::channel(config.channel_capacity);
        let (events, _) = broadcast::channel(config.channel_capacity);
        let (state_tx, state) = watch::channel(CoreState::Standby);
        let (playback_tx, playback) = watch::channel(PlaybackState {
            generation: 0,
            active: false,
        });
        let (output_tx, output_rx) = mpsc::channel(config.channel_capacity);
        let (connected_tx, connected_rx) = mpsc::channel(1);
        let task_events = tasks.subscribe();
        let handle = CoreHandle {
            commands,
            audio,
            events: events.clone(),
            state,
            sample_rate: config.sample_rate,
            capture_sequence: Arc::new(Mutex::new(None)),
        };
        let actor = Actor {
            recent: AudioBuffer::new(config.sample_rate, config.pre_roll),
            pending: AudioBuffer::new(config.sample_rate, config.connect_buffer),
            config,
            backend,
            tasks,
            commands: command_rx,
            audio: audio_rx,
            events,
            state: state_tx,
            playback: playback_tx,
            output: output_tx,
            connected_tx,
            connected_rx,
            task_events,
            session: None,
            connection: None,
            epoch: Uuid::new_v4(),
            last_sequence: None,
            last_activity: Instant::now(),
            playing: None,
            received_samples: 0,
            discarded_items: HashSet::new(),
            authorized_from: 0,
            speaking: false,
            responding: false,
            task_results: VecDeque::new(),
        };
        let runner = tokio::spawn(actor.run());
        Ok((
            handle,
            CoreOutput {
                audio: output_rx,
                playback,
                runner,
            },
        ))
    }
}

struct Actor {
    config: CoreConfig,
    backend: Arc<dyn VoiceBackend>,
    tasks: TaskManager,
    commands: mpsc::Receiver<(Action, oneshot::Sender<Result<()>>)>,
    audio: mpsc::Receiver<AudioFrame>,
    events: broadcast::Sender<CoreEvent>,
    state: watch::Sender<CoreState>,
    playback: watch::Sender<PlaybackState>,
    output: mpsc::Sender<PlaybackChunk>,
    connected_tx: mpsc::Sender<(Uuid, Result<VoiceSession>)>,
    connected_rx: mpsc::Receiver<(Uuid, Result<VoiceSession>)>,
    task_events: broadcast::Receiver<TaskEvent>,
    recent: AudioBuffer,
    pending: AudioBuffer,
    session: Option<VoiceSession>,
    connection: Option<JoinHandle<()>>,
    epoch: Uuid,
    last_sequence: Option<u64>,
    last_activity: Instant,
    playing: Option<PlaybackPosition>,
    received_samples: u64,
    discarded_items: HashSet<String>,
    authorized_from: u64,
    speaking: bool,
    responding: bool,
    task_results: VecDeque<Value>,
}

impl Actor {
    async fn run(mut self) -> Result<()> {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        loop {
            let sender = self.session.as_ref().map(|s| s.commands.clone());
            tokio::select! {
                biased;
                command = self.commands.recv() => {
                    let Some((action, response)) = command else { break; };
                    if matches!(action, Action::Shutdown) {
                        self.end(CoreState::Stopped);
                        let result = self.tasks.shutdown(Duration::from_secs(10)).await;
                        let failed = result.is_err();
                        let _ = response.send(result);
                        return if failed { Err(Error::Timeout("core task shutdown")) } else { Ok(()) };
                    }
                    let result = self.action(action);
                    let _ = response.send(result);
                }
                Some((epoch, result)) = self.connected_rx.recv() => {
                    if epoch == self.epoch && *self.state.borrow() == CoreState::Connecting {
                        self.connection.take();
                        match result {
                            Ok(session) => {
                                let history: Vec<Value> = self.tasks.recent(8)?.into_iter().map(|task| json!({
                                    "task_id":task.id,
                                    "prompt":task.prompt.chars().take(512).collect::<String>(),
                                    "state":task.state,
                                    "summary":task.summary.chars().take(512).collect::<String>(),
                                    "error":task.error.map(|text| text.chars().take(256).collect::<String>()),
                                })).collect();
                                if !history.is_empty() {
                                    session.try_send(VoiceCommand::TaskContext(json!(history)))?;
                                }
                                self.session = Some(session);
                                self.last_activity = Instant::now();
                                self.set_state(CoreState::Active);
                                self.playback.send_modify(|state| state.active = true);
                            }
                            Err(error) => self.fail(error),
                        }
                    }
                }
                event = async { match &mut self.session {
                    Some(session) => session.receive().await,
                    None => std::future::pending().await,
                }} => match event {
                    Some(event) => if let Err(error) = self.voice_event(event) { self.fail(error); },
                    None => self.fail(Error::Closed),
                },
                event = self.task_events.recv() => match event {
                    Ok(event) => {
                        // Results remain in SQLite if no voice session is active.
                        // Do not promote untrusted task output to a system instruction.
                        if let TaskEvent::Changed(record) = &event
                            && record.state.is_terminal() && self.session.is_some() {
                                if self.task_results.len() < 32 { self.task_results.push_back(serde_json::to_value(record)?); }
                                else { self.emit(CoreEvent::Error("task result announcements full; results remain in storage".into())); }
                            }
                        self.emit(CoreEvent::Task(event));
                        if let Err(error) = self.announce_result() { self.fail(error); }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => self.emit(CoreEvent::Error("task events lagged; reload task snapshots".into())),
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                _ = tick.tick() => {
                    if *self.state.borrow() == CoreState::Active && self.last_activity.elapsed() >= self.config.idle_timeout {
                        self.end(CoreState::Standby);
                    }
                }
                permit = async { sender.expect("guarded sender").reserve_owned().await },
                    if self.session.is_some() && !self.pending.is_empty() => {
                    match permit {
                        Ok(permit) => if let Some(frame) = self.pending.pop() { permit.send(VoiceCommand::Audio(frame)); },
                        Err(_) => self.fail(Error::Closed),
                    }
                }
                Some(frame) = self.audio.recv() => {
                    if let Err(error) = self.input(frame) { self.fail(error); }
                }
            }
        }
        self.end(CoreState::Stopped);
        self.tasks.shutdown(Duration::from_secs(10)).await
    }

    fn emit(&self, event: CoreEvent) {
        let _ = self.events.send(event);
    }
    fn set_state(&self, state: CoreState) {
        self.state.send_replace(state);
        self.emit(CoreEvent::State(state));
    }

    fn clear_playback(&mut self) {
        self.playback
            .send_modify(|state| state.generation = state.generation.wrapping_add(1));
        self.emit(CoreEvent::ClearPlayback);
    }

    fn end(&mut self, state: CoreState) {
        if let Some(connection) = self.connection.take() {
            connection.abort();
        }
        self.session.take();
        self.epoch = Uuid::new_v4();
        self.recent.clear();
        self.pending.clear();
        self.playing = None;
        self.received_samples = 0;
        self.discarded_items.clear();
        self.speaking = false;
        self.responding = false;
        self.task_results.clear();
        self.clear_playback();
        self.playback.send_modify(|state| state.active = false);
        self.set_state(state);
    }

    fn fail(&mut self, error: Error) {
        let state = if *self.state.borrow() == CoreState::Muted {
            CoreState::Muted
        } else {
            CoreState::Standby
        };
        self.emit(CoreEvent::Error(error.to_string()));
        self.end(state);
    }

    fn action(&mut self, action: Action) -> Result<()> {
        match action {
            Action::Wake(first) => {
                match *self.state.borrow() {
                    CoreState::Active | CoreState::Connecting => return Ok(()),
                    CoreState::Muted | CoreState::Stopped => {
                        return Err(Error::State("microphone is muted or core stopped".into()));
                    }
                    CoreState::Standby => (),
                }
                if let Some(first) = first
                    && self.last_sequence.is_some_and(|last| first <= last)
                    && self
                        .recent
                        .oldest_sequence()
                        .is_none_or(|oldest| first < oldest)
                {
                    return Err(Error::Audio(
                        "requested wake audio is no longer buffered".into(),
                    ));
                }
                for frame in self.recent.take_from(first) {
                    self.pending.push(frame)?;
                }
                self.authorized_from = first.unwrap_or(0);
                let backend = self.backend.clone();
                let tx = self.connected_tx.clone();
                let timeout = self.config.connect_timeout;
                let epoch = self.epoch;
                self.connection = Some(tokio::spawn(async move {
                    let result = tokio::time::timeout(timeout, backend.connect())
                        .await
                        .unwrap_or(Err(Error::Timeout("voice connection")));
                    let _ = tx.send((epoch, result)).await;
                }));
                self.set_state(CoreState::Connecting);
            }
            Action::EndConversation => {
                let state = if *self.state.borrow() == CoreState::Muted {
                    CoreState::Muted
                } else {
                    CoreState::Standby
                };
                self.end(state);
            }
            Action::Mute(muted) => self.end(if muted {
                CoreState::Muted
            } else {
                CoreState::Standby
            }),
            Action::Text(text) => {
                if text.trim().is_empty() || text.len() > 64 * 1024 {
                    return Err(Error::Config("invalid voice text".into()));
                }
                self.send(VoiceCommand::Text(text))?;
                self.responding = true;
                self.last_activity = Instant::now();
            }
            Action::Interrupt => self.interrupt(true)?,
            Action::Playback {
                generation,
                position,
            } => {
                if generation != self.playback.borrow().generation {
                    return Ok(());
                }
                if let Some(current) = &mut self.playing
                    && current.item_id == position.item_id
                    && current.content_index == position.content_index
                {
                    if u64::from(position.played_ms)
                        > self.received_samples * 1000 / u64::from(self.config.sample_rate)
                        || position.played_ms < current.played_ms
                    {
                        return Err(Error::Audio(
                            "playback position is outside delivered audio".into(),
                        ));
                    }
                    *current = position;
                    self.last_activity = Instant::now();
                }
                self.announce_result()?;
            }
            Action::Shutdown => unreachable!("handled by run"),
        }
        Ok(())
    }

    fn send(&self, command: VoiceCommand) -> Result<()> {
        self.session
            .as_ref()
            .ok_or_else(|| Error::State("voice session is not active".into()))?
            .try_send(command)
    }

    fn input(&mut self, frame: AudioFrame) -> Result<()> {
        frame.validate(self.config.sample_rate)?;
        if self
            .last_sequence
            .is_some_and(|last| frame.sequence <= last)
        {
            return Err(Error::Audio("capture sequence did not increase".into()));
        }
        self.last_sequence = Some(frame.sequence);
        match *self.state.borrow() {
            CoreState::Standby => self.recent.push_rolling(frame),
            CoreState::Connecting | CoreState::Active => {
                if frame.sequence >= self.authorized_from {
                    self.pending.push(frame)?;
                }
            }
            CoreState::Muted | CoreState::Stopped => (),
        }
        Ok(())
    }

    fn interrupt(&mut self, cancel_response: bool) -> Result<()> {
        if let Some(position) = &self.playing {
            if self.discarded_items.len() >= 64 {
                return Err(Error::Busy("interrupted voice items"));
            }
            self.discarded_items.insert(position.item_id.clone());
        }
        self.clear_playback();
        let playback = self.playing.take();
        self.send(VoiceCommand::Interrupt {
            playback,
            cancel_response,
        })
    }

    fn voice_event(&mut self, event: VoiceEvent) -> Result<()> {
        match event {
            VoiceEvent::SpeechStarted => {
                self.last_activity = Instant::now();
                self.speaking = true;
                self.interrupt(false)?;
            }
            VoiceEvent::SpeechStopped => self.speaking = false,
            VoiceEvent::ResponseStarted => self.responding = true,
            VoiceEvent::Audio {
                item_id,
                content_index,
                sample_rate,
                samples,
            } => {
                if self.discarded_items.contains(&item_id) {
                    return Ok(());
                }
                if sample_rate != self.config.sample_rate
                    || samples.len() > sample_rate as usize
                    || item_id.len() > 512
                {
                    return Err(Error::Audio("invalid provider audio chunk".into()));
                }
                self.last_activity = Instant::now();
                if self
                    .playing
                    .as_ref()
                    .is_none_or(|p| p.item_id != item_id || p.content_index != content_index)
                {
                    self.playing = Some(PlaybackPosition {
                        item_id: item_id.clone(),
                        content_index,
                        played_ms: 0,
                    });
                    self.received_samples = 0;
                }
                self.received_samples += samples.len() as u64;
                let position = self.playing.clone().expect("initialized playback");
                let chunk = PlaybackChunk {
                    generation: self.playback.borrow().generation,
                    position,
                    sample_rate,
                    samples,
                };
                self.output
                    .try_send(chunk)
                    .map_err(|_| Error::Busy("playback queue"))?;
            }
            VoiceEvent::Transcript {
                user,
                text,
                final_chunk,
            } => {
                if user {
                    self.last_activity = Instant::now();
                }
                self.emit(CoreEvent::Transcript {
                    user,
                    text,
                    final_chunk,
                });
            }
            VoiceEvent::ToolCall {
                call_id,
                name,
                arguments,
            } => {
                if call_id.is_empty() || call_id.len() > 256 {
                    return Err(Error::Protocol("invalid tool call ID".into()));
                }
                let result = self.tool(&call_id, &name, arguments);
                let result = match result {
                    Ok(result) => result,
                    Err(error) => json!({"error":error.to_string()}),
                };
                self.send(VoiceCommand::ToolResult { call_id, result })?;
                self.responding = true;
            }
            VoiceEvent::ResponseFinished => {
                self.responding = false;
                self.announce_result()?;
            }
            VoiceEvent::Error(error) => return Err(Error::Protocol(error)),
            VoiceEvent::Closed => return Err(Error::Closed),
        }
        Ok(())
    }

    fn announce_result(&mut self) -> Result<()> {
        if !self.speaking
            && !self.responding
            && self.session.is_some()
            && self.playing.as_ref().is_none_or(|position| {
                u64::from(position.played_ms)
                    >= self.received_samples * 1000 / u64::from(self.config.sample_rate)
            })
            && let Some(result) = self.task_results.pop_front()
        {
            self.send(VoiceCommand::TaskUpdate(result))?;
            self.responding = true;
            self.last_activity = Instant::now();
        }
        Ok(())
    }

    fn tool(&self, call_id: &str, name: &str, arguments: Value) -> Result<Value> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Start {
            prompt: String,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct TaskId {
            task_id: Uuid,
        }
        match name {
            "start_task" => {
                let args: Start = serde_json::from_value(arguments)?;
                let task = self
                    .tasks
                    .submit(format!("{}:{call_id}", self.epoch), args.prompt)?;
                Ok(json!({"task_id":task.id, "state":task.state, "accepted":true}))
            }
            "get_task_status" => {
                let args: TaskId = serde_json::from_value(arguments)?;
                Ok(serde_json::to_value(self.tasks.get(args.task_id)?)?)
            }
            "cancel_task" => {
                let args: TaskId = serde_json::from_value(arguments)?;
                Ok(serde_json::to_value(self.tasks.cancel(args.task_id)?)?)
            }
            _ => Err(Error::Protocol("unknown voice tool".into())),
        }
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            connection.abort();
        }
        self.tasks.request_shutdown();
    }
}
