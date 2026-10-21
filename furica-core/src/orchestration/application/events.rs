//! Application notifications combining conversation, input and task results.

use crate::{input::SpeakerDecision, orchestration::domain::CoreState, sessions::SessionEvent};

// Execution payloads are low-frequency; keeping the public variant unboxed keeps the
// host event shape stable.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum CoreEvent {
    State(CoreState),
    Transcript {
        user: bool,
        text: String,
        final_chunk: bool,
    },
    Execution(SessionEvent),
    ClearPlayback,
    Speaker(SpeakerDecision),
    Error(String),
}
