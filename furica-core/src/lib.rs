//! Provider-independent behavior for gated speech input, cascaded or duplex
//! voice sessions, and ACP task orchestration. Devices and model adapters are
//! supplied by the host.
//!
#![doc = include_str!("../README.md")]

pub mod audio;
pub mod config;
pub mod error;
pub mod input;
pub mod orchestration;
pub mod playback;
pub mod stt;
pub mod tasks;
pub mod text_model;
pub mod tts;

pub use error::{Error, Result};
