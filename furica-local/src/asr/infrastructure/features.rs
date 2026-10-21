//! Fixed frontend for the pinned SenseVoiceSmall sherpa-onnx export.
use furica_core::{Error, Result};
use kaldi_fbank::{FbankOptions, OnlineFbank, WindowType};
use tokio_util::sync::CancellationToken;

use super::check_cancel;

pub(super) const BINS: usize = 80;
pub(super) const WINDOW: usize = 7;
pub(super) const SHIFT: usize = 6;
pub(super) const DIM: usize = BINS * WINDOW;

pub(super) fn extract(
    pcm: &[i16],
    neg_mean: &[f32; DIM],
    inv_stddev: &[f32; DIM],
    cancel: &CancellationToken,
) -> Result<Vec<f32>> {
    let mut opts = FbankOptions::default();
    opts.frame_opts.dither = 0.0;
    opts.frame_opts.window_type = WindowType::Hamming;
    opts.frame_opts.snip_edges = true;
    opts.mel_opts.num_bins = BINS as i32;
    opts.mel_opts.high_freq = 0.0;
    let mut fbank = OnlineFbank::new(opts);
    // This export expects PCM magnitude [-32768, 32767], not normalized PCM.
    for chunk in pcm.chunks(1600) {
        check_cancel(cancel)?;
        let wave: Vec<f32> = chunk.iter().map(|&s| f32::from(s)).collect();
        fbank.accept_waveform(16_000.0, &wave);
    }
    fbank.input_finished();
    let frames = fbank.num_frames_ready() as usize;
    if frames == 0 {
        return Err(Error::Audio(
            "SenseVoice input has no complete feature frame".into(),
        ));
    }
    let mut output = Vec::with_capacity(frames.div_ceil(SHIFT) * DIM);
    for center in (0..frames).step_by(SHIFT) {
        check_cancel(cancel)?;
        for offset in 0..WINDOW {
            let index = (center + offset).saturating_sub(WINDOW / 2).min(frames - 1);
            for (bin, &value) in fbank.get_frame(index as i32).iter().enumerate() {
                let d = offset * BINS + bin;
                output.push((value + neg_mean[d]) * inv_stddev[d]);
            }
        }
    }
    if output.iter().any(|x| !x.is_finite()) {
        return Err(Error::Audio(
            "SenseVoice produced non-finite features".into(),
        ));
    }
    Ok(output)
}
