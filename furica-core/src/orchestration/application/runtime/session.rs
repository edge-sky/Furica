//! Bounded session channels and cancellation on handle release.
//! Workflow scheduling and provider protocol handling are owned by their callers.

use crate::{
    Error, Result,
    orchestration::{VoiceCommand, VoiceEvent},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct VoiceSession {
    pub(crate) commands: mpsc::Sender<VoiceCommand>,
    pub(crate) events: mpsc::Receiver<VoiceEvent>,
    cancel: CancellationToken,
}

/// Backend-side channel endpoints, also useful for deterministic integration tests.
pub struct VoiceChannels {
    pub commands: mpsc::Receiver<VoiceCommand>,
    pub events: mpsc::Sender<VoiceEvent>,
    pub cancelled: CancellationToken,
}

impl VoiceSession {
    pub fn channels(capacity: usize) -> Result<(Self, VoiceChannels)> {
        let (command_tx, command_rx) = mpsc::channel(capacity);
        let (event_tx, event_rx) = mpsc::channel(capacity);
        let cancel = CancellationToken::new();
        Ok((
            Self {
                commands: command_tx,
                events: event_rx,
                cancel: cancel.clone(),
            },
            VoiceChannels {
                commands: command_rx,
                events: event_tx,
                cancelled: cancel,
            },
        ))
    }

    pub fn try_send(&self, command: VoiceCommand) -> Result<()> {
        self.commands
            .try_send(command)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::Busy("voice command queue"),
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })
    }

    pub async fn receive(&mut self) -> Option<VoiceEvent> {
        self.events.recv().await
    }
}

impl Drop for VoiceSession {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
