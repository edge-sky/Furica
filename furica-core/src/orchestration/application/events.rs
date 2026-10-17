//! Application notifications combining conversation, input and task results.

use crate::{input::SpeakerDecision, orchestration::domain::CoreState, tasks::TaskEvent};

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
    Speaker(SpeakerDecision),
    Error(String),
}
