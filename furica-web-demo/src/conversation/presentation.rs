//! Wire projections live in the host. Core does not acquire serialization concerns.
use furica_core::{
    input::SpeakerDecision,
    orchestration::{CoreEvent, CoreState},
    playback::{PlaybackChunk, PlaybackState},
    sessions::SessionEvent,
};
use serde_json::{Value, json};

pub fn state(state: CoreState) -> &'static str {
    match state {
        CoreState::Standby => "standby",
        CoreState::Verifying => "verifying",
        CoreState::Connecting => "connecting",
        CoreState::Active => "active",
        CoreState::Muted => "muted",
        CoreState::Stopped => "stopped",
    }
}

pub fn event(event: CoreEvent) -> Value {
    match event {
        CoreEvent::State(value) => json!({"type":"state", "state":state(value)}),
        CoreEvent::Transcript {
            user,
            text,
            final_chunk,
        } => json!({"type":"transcript","user":user,"text":text,"final_chunk":final_chunk}),
        CoreEvent::Execution(SessionEvent::Progress {
            session_id,
            run_id,
            update,
        }) => {
            json!({"type":"execution_changed", "kind":"progress", "session_id":session_id, "run_id":run_id, "update":update})
        }
        CoreEvent::Execution(SessionEvent::Error {
            session_id,
            acp_session_id,
            run_id,
            message,
        }) => {
            json!({"type":"execution_changed", "kind":"error", "session_id":session_id, "acp_session_id":acp_session_id, "run_id":run_id, "message":message})
        }
        CoreEvent::Execution(_) => json!({"type":"execution_changed"}),
        CoreEvent::ClearPlayback => json!({"type":"clear_playback"}),
        CoreEvent::Speaker(value) => match value {
            SpeakerDecision::Accepted { speaker_id } => {
                json!({"type":"speaker", "decision":"accepted", "speaker_id":speaker_id, "test":true})
            }
            SpeakerDecision::Rejected => {
                json!({"type":"speaker", "decision":"rejected", "test":true})
            }
            SpeakerDecision::InsufficientAudio => {
                json!({"type":"speaker", "decision":"insufficient_audio", "test":true})
            }
        },
        CoreEvent::Error(message) => json!({"type":"error", "message":message}),
    }
}

pub fn playback(state: PlaybackState) -> Value {
    json!({"type":"playback_state", "generation":state.generation.to_string(), "active":state.active})
}

pub fn audio(chunk: PlaybackChunk) -> Value {
    json!({"type":"audio", "generation":chunk.generation.to_string(), "item_id":chunk.position.item_id,
        "content_index":chunk.position.content_index, "sample_rate":chunk.sample_rate,"samples":chunk.samples})
}
