mod decode;
mod features;
mod model;

use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use furica_core::{
    Error, Result,
    stt::{Stt, Utterance},
};
use ort::session::{Session, run_options::RunOptions};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

/// One reusable CPU recognizer for the sherpa-onnx SenseVoiceSmall export.
///
/// Clones share one session and one inference slot. Concurrent calls return
/// `Busy`; the host owns scheduling and deadlines. No audio or results persist.
#[derive(Clone)]
pub struct SenseVoiceStt {
    session: Arc<Mutex<Session>>,
    tokens: Arc<[String]>,
    neg_mean: Arc<[f32; features::DIM]>,
    inv_stddev: Arc<[f32; features::DIM]>,
    slot: Arc<Semaphore>,
}

impl SenseVoiceStt {
    /// Load local model and matching tokens; this blocks and performs a warm-up.
    ///
    /// Uses CPU, two intra-op threads, automatic language identification and
    /// inverse text normalization. Never downloads files.
    pub fn from_files(model_path: impl AsRef<Path>, tokens_path: impl AsRef<Path>) -> Result<Self> {
        let session = model::load(model_path.as_ref())?;
        let tokens = decode::load_tokens(tokens_path.as_ref())?;
        model::validate(&session, tokens.len())?;
        let metadata = session.metadata().map_err(model::error)?;
        let neg_mean = model::normalization(&metadata, "neg_mean")?;
        let inv_stddev = model::normalization(&metadata, "inv_stddev")?;
        if inv_stddev.iter().any(|x| *x <= 0.0) {
            return Err(Error::Config(
                "SenseVoice inv_stddev must be positive".into(),
            ));
        }
        drop(metadata);
        let recognizer = Self {
            session: Arc::new(Mutex::new(session)),
            tokens: tokens.into(),
            neg_mean: Arc::new(neg_mean),
            inv_stddev: Arc::new(inv_stddev),
            slot: Arc::new(Semaphore::new(1)),
        };
        // Check execution as well as metadata before exposing a usable adapter.
        recognizer.recognize(
            &[0; 1600],
            &CancellationToken::new(),
            &RunOptions::new().map_err(model::error)?,
        )?;
        Ok(recognizer)
    }

    fn recognize(
        &self,
        pcm: &[i16],
        cancel: &CancellationToken,
        options: &RunOptions,
    ) -> Result<String> {
        let features = features::extract(pcm, &self.neg_mean, &self.inv_stddev, cancel)?;
        check_cancel(cancel)?;
        let mut session = self
            .session
            .lock()
            .map_err(|_| Error::State("SenseVoice session poisoned".into()))?;
        model::infer(&mut session, &features, &self.tokens, cancel, options)
    }
}

#[async_trait]
impl Stt for SenseVoiceStt {
    fn source(&self) -> Option<&str> {
        Some("sensevoice-small-int8/sherpa-onnx-2024-07-17")
    }

    async fn transcribe(&self, utterance: Utterance, cancel: CancellationToken) -> Result<String> {
        check_cancel(&cancel)?;
        validate_audio(&utterance)?;
        let permit = Arc::clone(&self.slot)
            .try_acquire_owned()
            .map_err(|_| Error::Busy("SenseVoice inference"))?;
        let worker_cancel = cancel.child_token();
        // Dropping the caller also cancels feature extraction and decoding.
        let _cancel_on_drop = worker_cancel.clone().drop_guard();
        let options = Arc::new(RunOptions::new().map_err(model::error)?);
        // ONNX execution is interrupted on cancellation *and* future drop.
        let _terminate_on_drop = scopeguard::guard(Arc::clone(&options), |options| {
            let _ = options.terminate();
        });
        let recognizer = self.clone();
        let job = tokio::task::spawn_blocking(move || {
            // Retain the slot until the actual blocking work exits, even when
            // the caller has already returned or dropped its future.
            let _permit = permit;
            check_cancel(&worker_cancel)?;
            let pcm: Vec<i16> = utterance
                .frames
                .into_iter()
                .flat_map(|f| f.samples)
                .collect();
            recognizer.recognize(&pcm, &worker_cancel, &options)
        });
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Error::Cancelled),
            result = job => result.map_err(|_| Error::State("SenseVoice worker panicked".into()))?,
        }
    }
}

fn check_cancel(cancel: &CancellationToken) -> Result<()> {
    if cancel.is_cancelled() {
        Err(Error::Cancelled)
    } else {
        Ok(())
    }
}

fn validate_audio(utterance: &Utterance) -> Result<()> {
    if utterance.sample_rate != 16_000 || utterance.frames.is_empty() {
        return Err(Error::Audio(
            "SenseVoice requires nonempty 16 kHz mono PCM16".into(),
        ));
    }
    if utterance.frames.len() > 4096 {
        return Err(Error::Busy("SenseVoice audio frames"));
    }
    let mut count = 0usize;
    let mut previous = None;
    for frame in &utterance.frames {
        frame.validate(16_000)?;
        if previous.is_some_and(|n| frame.sequence <= n) {
            return Err(Error::Audio(
                "SenseVoice frame sequence did not increase".into(),
            ));
        }
        previous = Some(frame.sequence);
        count += frame.samples.len();
        if count > 30 * 16_000 {
            return Err(Error::Busy("SenseVoice utterance exceeds 30 seconds"));
        }
    }
    if count < 400 {
        return Err(Error::Audio(
            "SenseVoice needs at least 25 ms of audio".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
