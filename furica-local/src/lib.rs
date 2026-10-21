#![doc = include_str!("../README.md")]

pub mod vad;

pub use vad::{FRAME_SAMPLES, SAMPLE_RATE, SileroVad, VadConfig, VadError, VadEvent, VadOutput};

#[cfg(feature = "sensevoice")]
pub mod asr;
