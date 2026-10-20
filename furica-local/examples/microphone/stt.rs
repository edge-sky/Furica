//! Example-only segmentation and bounded ASR worker, separate from capture.
use std::{
    collections::VecDeque,
    io::{self, Write},
    path::PathBuf,
    sync::{Arc, atomic::Ordering, mpsc},
    thread,
};

use furica_core::{
    audio::AudioFrame,
    stt::{Stt, Utterance},
};
use furica_local::{VadEvent, VadOutput, asr::SenseVoiceStt};
use tokio_util::sync::CancellationToken;

use super::{Result, capture::Control};

pub(super) fn load() -> Result<SenseVoiceStt> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("model");
    Ok(SenseVoiceStt::from_files(
        dir.join("sensevoice-small.int8.onnx"),
        dir.join("sensevoice-tokens.txt"),
    )?)
}

#[allow(clippy::type_complexity)]
pub(super) fn start(
    model: Option<SenseVoiceStt>,
    control: Arc<Control>,
) -> Result<(
    Option<mpsc::SyncSender<Vec<i16>>>,
    Option<thread::JoinHandle<Result<()>>>,
    CancellationToken,
)> {
    let cancel = CancellationToken::new();
    let Some(model) = model else {
        return Ok((None, None, cancel));
    };
    let (tx, rx) = mpsc::sync_channel::<Vec<i16>>(1);
    let token = cancel.clone();
    let worker = thread::Builder::new()
        .name("sensevoice-stt".into())
        .spawn(move || {
            let result = (|| -> Result<()> {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                while let Ok(pcm) = rx.recv() {
                    if token.is_cancelled() {
                        break;
                    }
                    let utterance = Utterance {
                        sample_rate: 16_000,
                        frames: pcm
                            .chunks(512)
                            .enumerate()
                            .map(|(i, samples)| AudioFrame {
                                sequence: i as u64,
                                sample_rate: 16_000,
                                samples: samples.to_vec(),
                            })
                            .collect(),
                    };
                    let text = runtime.block_on(model.transcribe(utterance, token.clone()))?;
                    let mut out = io::stdout().lock();
                    writeln!(out, "Transcript: {text}")?;
                    out.flush()?;
                }
                Ok(())
            })();
            if result.is_err() {
                control.stop.store(true, Ordering::Release);
            }
            result
        })?;
    Ok((Some(tx), Some(worker), cancel))
}

pub(super) fn segments(
    sender: mpsc::SyncSender<Vec<i16>>,
) -> impl FnMut(&[i16], u64, VadOutput) -> Result<()> {
    let mut buffer = VecDeque::<i16>::new();
    let mut first = 0_u64;
    let mut start = None;
    move |frame, position, output| {
        if first + buffer.len() as u64 != position {
            return Err("discontinuous VAD audio timeline".into());
        }
        buffer.extend(frame);
        if let Some(VadEvent::SpeechStarted { start_sample }) = output.event {
            if start_sample < first || start.is_some() {
                return Err("invalid speech start".into());
            }
            start = Some(start_sample);
        }
        if let Some(VadEvent::SpeechEnded { end_sample }) = output.event {
            let begin = start.take().ok_or("speech ended without start")?;
            if end_sample < begin || end_sample > first + buffer.len() as u64 {
                return Err("invalid speech end".into());
            }
            let length = (end_sample - begin) as usize;
            if length > 30 * 16_000 {
                return Err("speech exceeds SenseVoice's 30 second limit".into());
            }
            let pcm = buffer
                .iter()
                .skip((begin - first) as usize)
                .take(length)
                .copied()
                .collect();
            sender
                .try_send(pcm)
                .map_err(|_| "ASR queue full or worker stopped")?;
        }
        let end = position + frame.len() as u64;
        // The default VAD confirms start after 64 ms. Preserve that exact
        // pre-roll while idle, and all confirmed speech while active.
        let retain_from = start.unwrap_or(end.saturating_sub(1024));
        let discard = retain_from.saturating_sub(first) as usize;
        buffer.drain(..discard);
        first += discard as u64;
        // Allow the default 320 ms end confirmation beyond a 30 s sentence.
        if buffer.len() > 30 * 16_000 + 5120 {
            return Err("speech exceeds SenseVoice's 30 second limit".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_start_preroll_excludes_end_silence_and_handles_next_sentence() {
        let (tx, rx) = mpsc::sync_channel(2);
        let mut segment = segments(tx);
        let output = |event| VadOutput {
            speech_probability: 0.5,
            event,
        };
        segment(&[1; 512], 0, output(None)).unwrap();
        segment(
            &[2; 512],
            512,
            output(Some(VadEvent::SpeechStarted { start_sample: 0 })),
        )
        .unwrap();
        segment(
            &[0; 512],
            1024,
            output(Some(VadEvent::SpeechEnded { end_sample: 1024 })),
        )
        .unwrap();
        assert_eq!(rx.recv().unwrap(), [[1; 512], [2; 512]].concat());
        segment(&[3; 512], 1536, output(None)).unwrap();
        segment(
            &[4; 512],
            2048,
            output(Some(VadEvent::SpeechStarted { start_sample: 1536 })),
        )
        .unwrap();
        segment(
            &[],
            2560,
            output(Some(VadEvent::SpeechEnded { end_sample: 2560 })),
        )
        .unwrap();
        assert_eq!(rx.recv().unwrap(), [[3; 512], [4; 512]].concat());
    }

    #[test]
    fn overload_is_an_error_instead_of_dropped_speech() {
        let (tx, _rx) = mpsc::sync_channel(0);
        let mut segment = segments(tx);
        segment(
            &[1; 512],
            0,
            VadOutput {
                speech_probability: 1.0,
                event: Some(VadEvent::SpeechStarted { start_sample: 0 }),
            },
        )
        .unwrap();
        assert!(
            segment(
                &[],
                512,
                VadOutput {
                    speech_probability: 0.0,
                    event: Some(VadEvent::SpeechEnded { end_sample: 512 })
                }
            )
            .is_err()
        );
    }
}
