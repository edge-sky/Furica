//! Existing protocol/behavior tests and loopback fixtures.
#[cfg(feature = "http")]
mod http;
#[cfg(any(feature = "aliyun", feature = "volcengine"))]
mod speech;
#[cfg(any(
    feature = "openai-compatible",
    feature = "openai-responses",
    feature = "anthropic"
))]
mod text;
