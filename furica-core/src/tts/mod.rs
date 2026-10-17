//! Speech synthesis capability: domain values, adapter port and application use case.

mod application;
mod domain;
mod ports;

pub use application::synthesize;
pub use domain::SynthesizedAudio;
pub use ports::{Tts, TtsStream};
