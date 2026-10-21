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
pub mod sessions;
pub mod stt;
pub mod text_model;
pub mod tts;

pub use error::{Error, Result};

// Internal ACP command tests live inside the crate so these commands do not
// become host APIs merely to support behavioral coverage.
#[cfg(test)]
extern crate self as furica_core;
#[cfg(test)]
#[path = "../tests/behavior/mod.rs"]
mod behavior;
