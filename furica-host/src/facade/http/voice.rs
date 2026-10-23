use crate::{Error, Result};
use bytes::Bytes;
use furica_core::playback::{PlaybackChunk, PlaybackPosition};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenVoice {
    pub session_id: Uuid,
    pub owner: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlaybackReport {
    pub generation: u64,
    pub item_id: String,
    pub content_index: u32,
    pub played_ms: u32,
}
impl PlaybackReport {
    pub fn position(&self) -> Result<PlaybackPosition> {
        if self.item_id.is_empty() || self.item_id.len() > 512 {
            return Err(Error::new(
                400,
                "invalid_playback",
                "playback item identity is invalid",
            ));
        }
        Ok(PlaybackPosition {
            item_id: self.item_id.clone(),
            content_index: self.content_index,
            played_ms: self.played_ms,
        })
    }
}

#[derive(Serialize)]
struct AudioEnvelope<'a> {
    channel_id: Uuid,
    generation: u64,
    item_id: &'a str,
    content_index: u32,
    sample_rate: u32,
    samples: usize,
}

pub fn encode(id: Uuid, chunk: PlaybackChunk) -> Result<Bytes> {
    if chunk.samples.len() > 16_000 || chunk.sample_rate != 16_000 {
        return Err(Error::new(
            503,
            "invalid_audio_output",
            "invalid capability audio output",
        ));
    }
    let header = serde_json::to_vec(&AudioEnvelope {
        channel_id: id,
        generation: chunk.generation,
        item_id: &chunk.position.item_id,
        content_index: chunk.position.content_index,
        sample_rate: chunk.sample_rate,
        samples: chunk.samples.len(),
    })
    .map_err(|_| Error::config())?;
    if header.len() > 4096 {
        return Err(Error::new(
            503,
            "invalid_audio_output",
            "audio envelope exceeds its limit",
        ));
    }
    let mut bytes = Vec::with_capacity(4 + header.len() + chunk.samples.len() * 2);
    bytes.extend_from_slice(&(header.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&header);
    for sample in chunk.samples {
        bytes.extend_from_slice(&sample.to_le_bytes());
    }
    Ok(bytes.into())
}
