//! Doubao speech: flash file recognition and V3 unidirectional HTTP TTS.
mod config;
mod infrastructure;
pub use config::{SttConfig, TtsConfig};
pub use infrastructure::{VolcengineStt, VolcengineTts};
