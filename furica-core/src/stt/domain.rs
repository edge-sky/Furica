//! Complete utterance value passed to recognition.

use crate::audio::AudioFrame;

#[derive(Clone, Debug)]
pub struct Utterance {
    pub sample_rate: u32,
    pub frames: Vec<AudioFrame>,
}
