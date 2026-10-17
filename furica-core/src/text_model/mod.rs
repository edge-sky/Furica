//! Text generation contracts, validated streaming and complete-result collection.
mod application;
mod domain;
mod ports;
pub(crate) use application::validate_text;
pub use application::{generate, generate_stream};
pub(crate) use domain::message_size;
pub use domain::*;
pub use ports::{ModelRequest, ModelStream, TextModel};
