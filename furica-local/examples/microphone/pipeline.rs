use furica_local::{FRAME_SAMPLES, SAMPLE_RATE};
use rubato::{
    Async, FixedAsync, Resampler, SincInterpolationParameters,
    audioadapter_buffers::direct::InterleavedSlice,
};

use super::Result;

/// Only the worker touches these buffers. They remain bounded for the stream.
pub(super) struct Pipeline {
    rate: u32,
    resampler: Option<Async<f32>>,
    input: Vec<f32>,
    input_filled: usize,
    output: Vec<f32>,
    frame: [i16; FRAME_SAMPLES],
    frame_filled: usize,
}

impl Pipeline {
    pub fn new(rate: u32) -> Result<Self> {
        if !(8_000..=192_000).contains(&rate) {
            return Err("unsupported input sample rate".into());
        }
        let resampler = if rate == SAMPLE_RATE {
            None
        } else {
            Some(Async::new_sinc(
                f64::from(SAMPLE_RATE) / f64::from(rate),
                1.0,
                &SincInterpolationParameters::default(),
                (rate as usize).div_ceil(100),
                1,
                FixedAsync::Input,
            )?)
        };
        let input = vec![0.0; resampler.as_ref().map_or(0, Resampler::input_frames_next)];
        let output = vec![0.0; resampler.as_ref().map_or(0, Resampler::output_frames_max)];
        Ok(Self {
            rate,
            resampler,
            input,
            input_filled: 0,
            output,
            frame: [0; FRAME_SAMPLES],
            frame_filled: 0,
        })
    }

    pub fn input_rate(&self) -> u32 {
        self.rate
    }
    pub fn input_block(&self) -> usize {
        self.input.len()
    }
    pub fn output_delay(&self) -> usize {
        self.resampler.as_ref().map_or(0, Resampler::output_delay)
    }
    pub fn pending(&self) -> (usize, usize) {
        (self.input_filled, self.frame_filled)
    }

    pub fn push(
        &mut self,
        sample: f32,
        emit: &mut impl FnMut(&[i16; FRAME_SAMPLES]) -> Result<()>,
    ) -> Result<()> {
        if !sample.is_finite() {
            return Err("non-finite input audio".into());
        }
        let Some(resampler) = &mut self.resampler else {
            return frame_sample(sample, &mut self.frame, &mut self.frame_filled, emit);
        };
        self.input[self.input_filled] = sample;
        self.input_filled += 1;
        if self.input_filled == self.input.len() {
            let input = InterleavedSlice::new(&self.input[..], 1, self.input.len())?;
            let output_len = self.output.len();
            let mut output = InterleavedSlice::new_mut(&mut self.output[..], 1, output_len)?;
            let (used, produced) = resampler.process_into_buffer(&input, &mut output, None)?;
            if used != self.input.len() {
                return Err("resampler did not consume its fixed input block".into());
            }
            self.input_filled = 0;
            for &sample in &self.output[..produced] {
                frame_sample(sample, &mut self.frame, &mut self.frame_filled, emit)?;
            }
        }
        Ok(())
    }
}

fn frame_sample(
    sample: f32,
    frame: &mut [i16; FRAME_SAMPLES],
    filled: &mut usize,
    emit: &mut impl FnMut(&[i16; FRAME_SAMPLES]) -> Result<()>,
) -> Result<()> {
    if !sample.is_finite() {
        return Err("resampling produced non-finite audio".into());
    }
    frame[*filled] = (sample.clamp(-1.0, 1.0) * 32768.0).round() as i16;
    *filled += 1;
    if *filled == FRAME_SAMPLES {
        emit(frame)?;
        *filled = 0;
    }
    Ok(())
}
