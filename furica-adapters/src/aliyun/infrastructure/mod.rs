//! Concrete speech capability implementations; scheduling belongs to Core.
mod stt;
mod tts;
pub use stt::AliyunStt;
pub use tts::AliyunTts;
