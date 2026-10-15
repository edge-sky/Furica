//! Playback delivery, position reports and invalidation on interruption.

use super::Actor;
use crate::{
    Error, Result,
    conversation::{CoreEvent, PlaybackChunk},
    voice::{PlaybackPosition, VoiceCommand},
};
use tokio::time::Instant;

impl Actor {
    pub(super) fn clear_playback(&mut self) {
        self.playback
            .send_modify(|state| state.generation = state.generation.wrapping_add(1));
        self.emit(CoreEvent::ClearPlayback);
    }

    pub(super) fn interrupt(&mut self, cancel_response: bool) -> Result<()> {
        if let Some(position) = &self.playing {
            if self.discarded_items.len() >= 64 {
                return Err(Error::Busy("interrupted voice items"));
            }
            self.discarded_items.insert(position.item_id.clone());
        }
        self.clear_playback();
        let playback = self.playing.take();
        self.send(VoiceCommand::Interrupt {
            playback,
            cancel_response,
        })
    }

    pub(super) fn report_playback(
        &mut self,
        generation: u64,
        position: PlaybackPosition,
    ) -> Result<()> {
        if generation != self.playback.borrow().generation {
            return Ok(());
        }
        if let Some(current) = &mut self.playing
            && current.item_id == position.item_id
            && current.content_index == position.content_index
        {
            if u64::from(position.played_ms)
                > self.received_samples * 1000 / u64::from(self.config.sample_rate)
                || position.played_ms < current.played_ms
            {
                return Err(Error::Audio(
                    "playback position is outside delivered audio".into(),
                ));
            }
            *current = position;
            self.last_activity = Instant::now();
        }
        self.announce_result()?;
        Ok(())
    }

    pub(super) fn output_audio(
        &mut self,
        item_id: String,
        content_index: u32,
        sample_rate: u32,
        samples: Vec<i16>,
    ) -> Result<()> {
        if self.discarded_items.contains(&item_id) {
            return Ok(());
        }
        if sample_rate != self.config.sample_rate
            || samples.len() > sample_rate as usize
            || item_id.len() > 512
        {
            return Err(Error::Audio("invalid provider audio chunk".into()));
        }
        self.last_activity = Instant::now();
        if self
            .playing
            .as_ref()
            .is_none_or(|p| p.item_id != item_id || p.content_index != content_index)
        {
            self.playing = Some(PlaybackPosition {
                item_id: item_id.clone(),
                content_index,
                played_ms: 0,
            });
            self.received_samples = 0;
        }
        self.received_samples += samples.len() as u64;
        let position = self.playing.clone().expect("initialized playback");
        let chunk = PlaybackChunk {
            generation: self.playback.borrow().generation,
            position,
            sample_rate,
            samples,
        };
        self.output
            .try_send(chunk)
            .map_err(|_| Error::Busy("playback queue"))?;
        Ok(())
    }
}
