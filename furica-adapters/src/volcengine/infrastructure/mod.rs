//! Concrete speech capability implementations; scheduling belongs to Core.
mod stt;
mod tts;
pub use stt::VolcengineStt;
pub use tts::VolcengineTts;
