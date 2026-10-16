//! Provider-independent behavior for gated speech input, cascaded or duplex
//! voice sessions, and ACP task orchestration. Devices and model adapters are
//! supplied by the host.
//!
#![doc = include_str!("../README.md")]

pub mod audio;
pub mod config;
pub mod conversation;
pub mod error;
pub mod tasks;
pub mod voice;

// Existing host integrations keep their module paths; these are the same types.
pub use conversation as runtime;
pub use conversation::input;
pub use error::{Error, Result};
pub use tasks as task;
pub use tasks::adapters::{acp, sqlite as store};
