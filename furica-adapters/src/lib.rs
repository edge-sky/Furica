//! Replaceable implementations of Furica's capability contracts.
//!
//! Provider modules expose their existing configuration and capability adapters.
//! Infrastructure details remain private; the host selects and injects adapters.
#![doc = include_str!("../README.md")]

#[cfg(feature = "aliyun")]
pub mod aliyun;
#[cfg(feature = "http")]
mod infrastructure;
#[cfg(test)]
mod tests;
#[cfg(feature = "volcengine")]
pub mod volcengine;

#[cfg(any(
    feature = "openai-compatible",
    feature = "openai-responses",
    feature = "anthropic"
))]
pub mod text_model;
