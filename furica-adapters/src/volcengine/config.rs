//! Provider configuration; model and credential choices remain host-owned.
#[derive(Clone)]
pub struct SttConfig {
    pub api_key: String,
    /// Full /api/v3/auc/bigmodel/recognize/flash HTTP endpoint.
    pub endpoint: String,
    /// Resource permission, e.g. volc.bigasr.auc_turbo.
    pub resource_id: String,
    pub model: String,
}

#[derive(Clone)]
pub struct TtsConfig {
    pub api_key: String,
    /// HTTP Chunked endpoint (not the /sse variant).
    pub endpoint: String,
    pub resource_id: String,
    /// Explicit model version. Must be compatible with the voice and resource.
    pub model: String,
    pub voice: String,
    /// Native mono PCM16 output rate.
    pub sample_rate: u32,
}
