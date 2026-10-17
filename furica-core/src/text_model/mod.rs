//! Text generation capability: domain values, adapter port and application use case.

mod application;
mod domain;
mod ports;

pub use application::generate;
pub(crate) use application::validate_text;
pub(crate) use domain::message_size;
pub use domain::{ModelMessage, ModelRequest, ModelResponse, ToolCall};
pub use ports::TextModel;
