//! Shared provider infrastructure. Business rules and capability ports live in Core.
pub(crate) mod http;
#[cfg(any(feature = "aliyun", feature = "volcengine"))]
pub(crate) mod speech;
#[cfg(any(
    feature = "openai-compatible",
    feature = "openai-responses",
    feature = "anthropic"
))]
pub(crate) mod text;
