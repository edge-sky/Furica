//! Input capability with domain decisions, host ports and application configuration.

mod application;
mod domain;
mod ports;

pub(crate) use application::InputGate;
pub use application::{InputConfig, SpeechInput};
pub use domain::{SpeakerDecision, VadActivity};
pub use ports::{SpeakerVerifier, Vad};
