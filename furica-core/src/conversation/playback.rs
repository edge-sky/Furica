//! Host playback contract and actual playback completion calculation.

use crate::voice::PlaybackPosition;

#[derive(Clone, Debug)]
pub struct PlaybackChunk {
    pub generation: u64,
    pub position: PlaybackPosition,
    pub sample_rate: u32,
    pub samples: Vec<i16>,
}

/// Hosts must clear playback when this generation changes and ignore chunks from
/// previous generations, even if their event subscriber has lagged.
#[derive(Clone, Copy, Debug)]
pub struct PlaybackState {
    pub generation: u64,
    pub active: bool,
}

pub(super) fn has_finished(
    position: Option<&PlaybackPosition>,
    received_samples: u64,
    sample_rate: u32,
) -> bool {
    position.is_none_or(|position| {
        u64::from(position.played_ms) >= received_samples * 1000 / u64::from(sample_rate)
    })
}
