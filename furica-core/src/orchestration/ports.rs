//! Voice-session port shared by cascaded application workflows and duplex adapters.
//! Commands and events describe application interactions; channel mechanics live
//! in the application runtime. Existing port signatures use the standard session handle.

use super::application::VoiceSession;
use crate::{
    Result,
    audio::{AudioFrame, SpeechActivity},
    playback::PlaybackPosition,
};
use async_trait::async_trait;
use serde_json::Value;

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
