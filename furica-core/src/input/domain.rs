//! Voice activity and speaker authorization decisions.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VadActivity {
    Speech,
    Silence,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpeakerDecision {
    Accepted { speaker_id: String },
    Rejected,
    InsufficientAudio,
}
