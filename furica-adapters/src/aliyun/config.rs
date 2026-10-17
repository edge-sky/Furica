//! Provider configuration; model and credential choices remain host-owned.
/// Credentials intentionally do not implement Debug or Serialize.
#[derive(Clone)]
pub struct SttConfig {
    pub api_key: String,
    /// HTTP endpoint, including the generation path. Beijing region only.
    pub endpoint: String,
    /// Fun-ASR-Realtime model supporting the non-realtime HTTP protocol.
    pub model: String,
}

#[derive(Clone)]
pub struct TtsConfig {
    pub api_key: String,
    /// WebSocket inference endpoint, including the path.
    pub endpoint: String,
    pub model: String,
    pub voice: String,
    /// Native mono PCM16 rate: 8000, 16000, 22050, 24000, 44100 or 48000 Hz.
    pub sample_rate: u32,
}
