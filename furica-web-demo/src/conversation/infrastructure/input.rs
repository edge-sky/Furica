//! Silero inference runs before Core ingress on a blocking worker. The Core Vad
//! port only reads a bounded precomputed decision for that exact PCM sequence.
use async_trait::async_trait;
use furica_core::{
    Error, Result,
    audio::AudioFrame,
    input::{SpeakerDecision, SpeakerVerifier, Vad, VadActivity},
    orchestration::{CoreHandle, CoreState},
};
use furica_local::{FRAME_SAMPLES, SileroVad};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub struct PreparedVad(Mutex<VecDeque<(u64, VadActivity)>>);

impl Vad for PreparedVad {
    fn detect(&self, frame: &AudioFrame) -> Result<VadActivity> {
        let mut decisions = self
            .0
            .lock()
            .map_err(|_| Error::State("VAD cache poisoned".into()))?;
        while decisions
            .front()
            .is_some_and(|(sequence, _)| *sequence < frame.sequence)
        {
            decisions.pop_front();
        }
        match decisions.pop_front() {
            Some((sequence, activity)) if sequence == frame.sequence => Ok(activity),
            _ => Err(Error::State("missing ordered VAD decision".into())),
        }
    }
}

#[derive(Default)]
pub struct TestSpeaker(pub AtomicU8);

impl TestSpeaker {
    pub fn set(&self, decision: &str) -> Result<()> {
        self.0.store(
            match decision {
                "accepted" => 0,
                "rejected" => 1,
                "insufficient_audio" => 2,
                _ => return Err(Error::Config("unknown test speaker decision".into())),
            },
            Ordering::Relaxed,
        );
        Ok(())
    }
}

#[async_trait]
impl SpeakerVerifier for TestSpeaker {
    async fn verify(
        &self,
        _: Vec<AudioFrame>,
        cancel: CancellationToken,
    ) -> Result<SpeakerDecision> {
        if cancel.is_cancelled() {
            return Err(Error::Closed);
        }
        Ok(match self.0.load(Ordering::Relaxed) {
            0 => SpeakerDecision::Accepted {
                speaker_id: "demo-test-speaker".into(),
            },
            1 => SpeakerDecision::Rejected,
            _ => SpeakerDecision::InsufficientAudio,
        })
    }
}

pub fn decode_audio(bytes: &[u8]) -> Result<AudioFrame> {
    if bytes.len() != 8 + FRAME_SAMPLES * 2 {
        return Err(Error::Audio(
            "expected sequence u64 LE + 512 PCM16 LE samples".into(),
        ));
    }
    Ok(AudioFrame {
        sequence: u64::from_le_bytes(bytes[..8].try_into().expect("checked length")),
        sample_rate: crate::bootstrap::SAMPLE_RATE,
        samples: bytes[8..]
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| i16::from_le_bytes([pair[0], pair[1]]))
            .collect(),
    })
}

pub fn run_capture(
    mut model: SileroVad,
    mut frames: mpsc::Receiver<AudioFrame>,
    decisions: Arc<PreparedVad>,
    core: CoreHandle,
    errors: mpsc::Sender<String>,
) {
    let mut speech = false;
    let mut previous = None;
    while let Some(frame) = frames.blocking_recv() {
        let result = (|| -> Result<()> {
            if previous.is_some_and(|sequence| frame.sequence <= sequence) {
                return Err(Error::Audio("capture sequence did not increase".into()));
            }
            previous = Some(frame.sequence);
            if matches!(core.state(), CoreState::Muted | CoreState::Stopped) {
                model.reset();
                speech = false;
                return Ok(());
            }
            let output = model
                .process(&frame.samples)
                .map_err(|_| Error::Audio("Silero inference failed; restart capture".into()))?;
            // Core owns endpoint timing; do not apply Silero's endpoint delay twice.
            speech = output.speech_probability >= if speech { 0.35 } else { 0.5 };
            {
                let mut cache = decisions
                    .0
                    .lock()
                    .map_err(|_| Error::State("VAD cache poisoned".into()))?;
                cache.push_back((
                    frame.sequence,
                    if speech {
                        VadActivity::Speech
                    } else {
                        VadActivity::Silence
                    },
                ));
                // Core's ingress is bounded to 128; older unused entries belong to
                // frames skipped by mute/end-conversation cutoffs.
                while cache.len() > 256 {
                    cache.pop_front();
                }
            }
            core.audio(frame)
        })();
        if let Err(error) = result {
            let _ = errors.blocking_send(error.to_string());
            return;
        }
    }
}
