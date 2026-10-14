use std::time::Duration;

use crate::{Error, Result};

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

impl CoreConfig {
    pub fn validate(&self) -> Result<()> {
        if !(8_000..=48_000).contains(&self.sample_rate)
            || self.channel_capacity == 0
            || self.channel_capacity > 4096
            || self.connect_timeout.is_zero()
            || self.idle_timeout.is_zero()
            || self.connect_buffer.is_zero()
            || self.connect_buffer > Duration::from_secs(30)
            || self.pre_roll > self.connect_buffer
        {
            return Err(Error::Config(
                "invalid audio, timeout or queue limits".into(),
            ));
        }
        Ok(())
    }
}
