use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    audio::{AudioFrame, SpeechActivity},
};

pub mod cascade;

/// Both branches share tools, playback, cancellation and task persistence.
pub enum VoicePipeline {
    Cascaded(cascade::CascadedBackend),
    Duplex(std::sync::Arc<dyn VoiceBackend>),
}

impl VoicePipeline {
    pub(crate) fn backend(self) -> std::sync::Arc<dyn VoiceBackend> {
        match self {
            Self::Cascaded(backend) => std::sync::Arc::new(backend),
            Self::Duplex(backend) => backend,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlaybackPosition {
    pub item_id: String,
    pub content_index: u32,
    pub played_ms: u32,
}

#[derive(Clone, Debug)]
pub enum VoiceCommand {
    Audio(AudioFrame),
    /// Authorized PCM with ordered, framework-owned VAD boundaries. Duplex
    /// adapters may use their own turn detection but must accept this input.
    InputAudio {
        frame: AudioFrame,
        activity: SpeechActivity,
    },
    Text(String),
    TaskUpdate(Value),
    /// Restore bounded task metadata without requesting an unsolicited response.
    TaskContext(Value),
    ToolResult {
        call_id: String,
        result: Value,
    },
    Interrupt {
        playback: Option<PlaybackPosition>,
        cancel_response: bool,
    },
}

#[derive(Clone, Debug)]
pub enum VoiceEvent {
    Audio {
        item_id: String,
        content_index: u32,
        sample_rate: u32,
        samples: Vec<i16>,
    },
    Transcript {
        user: bool,
        text: String,
        final_chunk: bool,
    },
    SpeechStarted,
    SpeechStopped,
    ResponseStarted,
    ResponseFinished,
    ToolCall {
        call_id: String,
        name: String,
        arguments: Value,
    },
    Error(String),
    Closed,
}

/// A backend owns conversion of its provider's wire events into this contract.
/// Sending and receiving must remain independent while a task executes.
#[async_trait]
pub trait VoiceBackend: Send + Sync {
    fn sample_rate(&self) -> u32;
    async fn connect(&self) -> Result<VoiceSession>;
}

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
        if capacity == 0 || capacity > 4096 {
            return Err(Error::Config("invalid voice channel capacity".into()));
        }
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

pub fn task_tools() -> Vec<Value> {
    vec![
        serde_json::json!({"type":"function", "name":"start_task",
            "description":"Submit a real task to the ACP executor. Returns acceptance, not completion.",
            "parameters":{"type":"object", "properties":{"prompt":{"type":"string"}},
                "required":["prompt"], "additionalProperties":false}}),
        serde_json::json!({"type":"function", "name":"get_task_status",
            "description":"Read a task's execution status and result.",
            "parameters":{"type":"object", "properties":{"task_id":{"type":"string"}},
                "required":["task_id"], "additionalProperties":false}}),
        serde_json::json!({"type":"function", "name":"cancel_task",
            "description":"Request cancellation; wait for confirmed status before saying it stopped.",
            "parameters":{"type":"object", "properties":{"task_id":{"type":"string"}},
                "required":["task_id"], "additionalProperties":false}}),
    ]
}
