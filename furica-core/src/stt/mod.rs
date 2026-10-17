//! Speech recognition capability: domain values, adapter port and application use case.

mod application;
mod domain;
mod ports;

pub use application::transcribe;
pub(crate) use application::validate_buffer;
pub use domain::Utterance;
pub use ports::Stt;
