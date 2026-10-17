//! Chat Completions. Compatibility is explicit per configured endpoint/model.
mod config;
mod infrastructure;
pub use config::Config;
pub use infrastructure::OpenAiCompatible;
