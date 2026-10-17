//! Stateless Responses API with caller-owned native output items for replay.
mod config;
mod infrastructure;
pub use config::Config;
pub use infrastructure::OpenAiResponses;
