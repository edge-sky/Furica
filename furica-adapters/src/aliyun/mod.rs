//! Alibaba Cloud Model Studio: Fun-ASR HTTP and CosyVoice WebSocket.
mod config;
mod infrastructure;
pub use config::{SttConfig, TtsConfig};
pub use infrastructure::{AliyunStt, AliyunTts};
