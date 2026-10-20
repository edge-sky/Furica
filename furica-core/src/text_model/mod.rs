//! Text generation contracts, validated streaming and complete-result collection.
mod application;
mod domain;
mod ports;
pub use application::{generate, generate_stream};
pub(crate) use application::{validate_request, validate_text};
pub use domain::*;
pub(crate) use domain::{json_size, message_size};
pub use ports::{ModelRequest, ModelStream, TextModel};
