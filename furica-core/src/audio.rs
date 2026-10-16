use std::{collections::VecDeque, time::Duration};

use crate::{Error, Result};

/// Host capture sequence numbers must increase, including across voice sessions.
/// Hosts must apply AEC and resample before passing frames to the core.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AudioFrame {
    pub sequence: u64,
    pub sample_rate: u32,
    pub samples: Vec<i16>,
}

/// VAD boundaries travel with PCM so endpoint markers cannot overtake queued
/// audio while a backend is connecting or under backpressure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpeechActivity {
    Started,
    Continuing,
    Ended,
    Silence,
}

pub(crate) struct BufferedAudio {
    pub frame: AudioFrame,
    pub activity: Option<SpeechActivity>,
}

impl AudioFrame {
    pub fn validate(&self, sample_rate: u32) -> Result<()> {
        if self.sample_rate != sample_rate {
            return Err(Error::Audio("sample rate does not match the core".into()));
        }
        if self.samples.is_empty() || self.samples.len() > sample_rate as usize / 5 {
            return Err(Error::Audio(
                "frames must contain 0–200 ms of nonempty PCM".into(),
            ));
        }
        Ok(())
    }

    pub fn pcm_bytes(&self) -> Vec<u8> {
        self.samples
            .iter()
            .flat_map(|sample| sample.to_le_bytes())
            .collect()
    }
}

pub(crate) struct AudioBuffer {
    frames: VecDeque<BufferedAudio>,
    samples: usize,
    limit: usize,
}

impl AudioBuffer {
    pub fn new(sample_rate: u32, duration: Duration) -> Self {
        Self {
            frames: VecDeque::new(),
            samples: 0,
            limit: (duration.as_secs_f64() * sample_rate as f64) as usize,
        }
    }

    /// Rolling idle buffer: evict complete oldest frames, never retain raw audio
    /// longer than the configured pre-roll window.
    pub fn push_rolling(&mut self, frame: AudioFrame) {
        self.samples += frame.samples.len();
        self.frames.push_back(BufferedAudio {
            frame,
            activity: None,
        });
        while self.samples > self.limit || self.frames.len() > 4096 {
            if let Some(frame) = self.frames.pop_front() {
                self.samples -= frame.frame.samples.len();
            }
        }
    }

    /// Connecting buffer: fail explicitly instead of losing request beginnings.
    pub fn push(&mut self, frame: AudioFrame) -> Result<()> {
        self.push_input(frame, None)
    }

    pub fn push_input(
        &mut self,
        frame: AudioFrame,
        activity: Option<SpeechActivity>,
    ) -> Result<()> {
        if self.samples + frame.samples.len() > self.limit || self.frames.len() >= 4096 {
            return Err(Error::Busy("voice connection audio buffer"));
        }
        self.samples += frame.samples.len();
        self.frames.push_back(BufferedAudio { frame, activity });
        Ok(())
    }

    pub fn take_from(&mut self, first_sequence: Option<u64>) -> Vec<AudioFrame> {
        let frames = self
            .frames
            .drain(..)
            .map(|buffered| buffered.frame)
            .filter(|frame| first_sequence.is_some_and(|first| frame.sequence >= first))
            .collect();
        self.samples = 0;
        frames
    }

    pub fn oldest_sequence(&self) -> Option<u64> {
        self.frames.front().map(|buffered| buffered.frame.sequence)
    }
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
    pub fn pop(&mut self) -> Option<BufferedAudio> {
        let frame = self.frames.pop_front()?;
        self.samples -= frame.frame.samples.len();
        Some(frame)
    }

    pub fn snapshot(&self) -> Vec<AudioFrame> {
        self.frames
            .iter()
            .map(|buffered| buffered.frame.clone())
            .collect()
    }

    pub fn clear(&mut self) {
        self.frames.clear();
        self.samples = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(sequence: u64) -> AudioFrame {
        AudioFrame {
            sequence,
            sample_rate: 24_000,
            samples: vec![1; 240],
        }
    }

    #[test]
    fn rolls_idle_audio_but_does_not_drop_connecting_audio() {
        let mut buffer = AudioBuffer::new(24_000, Duration::from_millis(20));
        buffer.push_rolling(frame(1));
        buffer.push_rolling(frame(2));
        buffer.push_rolling(frame(3));
        assert_eq!(buffer.oldest_sequence(), Some(2));
        assert_eq!(
            buffer
                .take_from(Some(3))
                .iter()
                .map(|f| f.sequence)
                .collect::<Vec<_>>(),
            vec![3]
        );
        buffer.push(frame(4)).unwrap();
        buffer.push(frame(5)).unwrap();
        assert!(buffer.push(frame(6)).is_err());
        assert_eq!(buffer.pop().unwrap().frame.sequence, 4);
        assert_eq!(buffer.pop().unwrap().frame.sequence, 5);
    }
}
