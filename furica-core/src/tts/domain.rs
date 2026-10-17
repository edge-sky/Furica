//! Synthesized PCM value; stream mechanics belong to the adapter port.

/// Mono PCM16 audio; a stream chunk or the pipeline's collected result.
#[derive(Clone, Debug)]
pub struct SynthesizedAudio {
    pub sample_rate: u32,
    pub samples: Vec<i16>,
}
