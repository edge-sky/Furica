use std::time::Duration;

/// Host-supplied runtime parameters. The caller validates configuration before use.
/// All limits apply independently of the frontend. Audio is mono PCM16.
#[derive(Clone, Debug)]
pub struct CoreConfig {
    pub sample_rate: u32,
    pub pre_roll: Duration,
    pub connect_buffer: Duration,
    pub connect_timeout: Duration,
    pub idle_timeout: Duration,
    pub channel_capacity: usize,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            sample_rate: 24_000,
            pre_roll: Duration::from_secs(2),
            connect_buffer: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(60),
            channel_capacity: 128,
        }
    }
}
