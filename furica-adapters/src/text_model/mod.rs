//! Text model protocol adapters implementing Core's TextModel port.
//! Each protocol retains its own configuration, request encoding and stream state.
#[cfg(feature = "anthropic")]
pub mod anthropic;
#[cfg(feature = "openai-compatible")]
pub mod openai_compatible;
#[cfg(feature = "openai-responses")]
pub mod openai_responses;
