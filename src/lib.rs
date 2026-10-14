//! Headless core. Native audio capture, AEC, keyword spotting and presentation
//! are supplied by the host; this crate owns voice sessions and ACP task state.
//!
#![doc = include_str!("../README.md")]

pub mod acp;
pub mod audio;
pub mod config;
pub mod error;
pub mod runtime;
pub mod store;
pub mod task;
pub mod voice;

pub use error::{Error, Result};
