//! Voice conversation API. The runtime coordinates input, playback and background tasks.

pub mod domain;
pub mod input;
pub mod playback;
mod runtime;

pub use domain::{CoreEvent, CoreState};
pub use playback::{PlaybackChunk, PlaybackState};

use crate::{Error, Result, audio::AudioFrame, voice::PlaybackPosition};
use std::sync::{Arc, Mutex};
use tokio::{
    sync::{broadcast, mpsc, oneshot, watch},
    task::JoinHandle,
};

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
