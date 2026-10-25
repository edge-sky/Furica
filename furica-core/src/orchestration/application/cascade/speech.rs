//! Session-local speech scheduling: one synthesis/playback and five pending texts.

use super::CascadedBackend;
use crate::{
    Error, Result,
    orchestration::VoiceEvent,
    playback::PlaybackPosition,
    tts::{self, SynthesizedAudio},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::VecDeque;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const WAITING_CAPACITY: usize = 5;

pub(in crate::orchestration::application) struct SpeechQueue {
    backend: CascadedBackend,
    cancel: CancellationToken,
    pending: VecDeque<(String, String)>,
    current_id: Option<String>,
    pub(in crate::orchestration::application) job:
        Option<(JoinHandle<Result<SynthesizedAudio>>, CancellationToken)>,
    audio: Option<SynthesizedAudio>,
    delivered: usize,
}

impl SpeechQueue {
    pub(in crate::orchestration::application) fn new(
        backend: CascadedBackend,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            backend,
            cancel,
            pending: VecDeque::new(),
            current_id: None,
            job: None,
            audio: None,
            delivered: 0,
        }
    }

    pub(in crate::orchestration::application) fn enqueue(
        &mut self,
        arguments: Value,
    ) -> Result<Value> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Speak {
            text: String,
        }
        let Speak { text } = serde_json::from_value(arguments)?;
        if text.trim().is_empty() {
            return Err(Error::Config("speech text must not be blank".into()));
        }
        crate::text_model::validate_text(&text, self.backend.config.max_context_bytes)?;
        let dropped = if self.pending.len() == WAITING_CAPACITY {
            self.pending.pop_front().map(|(id, _)| id)
        } else {
            None
        };
        let id = Uuid::new_v4().to_string();
        self.pending.push_back((id.clone(), text));
        self.start_next();
        let mut result = json!({"accepted": true, "request_id": id});
        if let Some(dropped) = dropped {
            result["dropped_request_id"] = json!(dropped);
        }
        Ok(result)
    }

    pub(in crate::orchestration::application) fn start_next(&mut self) {
        if self.current_id.is_some() {
            return;
        }
        let Some((id, text)) = self.pending.pop_front() else {
            return;
        };
        let backend = self.backend.clone();
        let cancel = self.cancel.child_token();
        let worker_cancel = cancel.clone();
        self.current_id = Some(id);
        self.job = Some((
            tokio::spawn(async move {
                tokio::time::timeout(
                    backend.config.inference_timeout,
                    tts::synthesize(
                        backend
                            .tts
                            .as_deref()
                            .ok_or_else(|| Error::State("TTS is unavailable".into()))?,
                        text,
                        backend.config.sample_rate,
                        backend.config.max_generated_audio,
                        backend.config.max_context_bytes,
                        worker_cancel,
                    ),
                )
                .await
                .unwrap_or(Err(Error::Timeout("speech synthesis")))
            }),
            cancel,
        ));
    }

    pub(in crate::orchestration::application) fn synthesized(&mut self, audio: SynthesizedAudio) {
        self.audio = Some(audio);
        self.delivered = 0;
    }

    pub(in crate::orchestration::application) fn has_audio(&self) -> bool {
        self.audio
            .as_ref()
            .is_some_and(|audio| self.delivered < audio.samples.len())
    }

    /// Called only after reserving output capacity; never accumulates a second
    /// unbounded queue of synthesized or delivered audio.
    pub(in crate::orchestration::application) fn next_audio(&mut self) -> VoiceEvent {
        let audio = self.audio.as_ref().expect("pending speech audio");
        let end = (self.delivered + audio.sample_rate as usize).min(audio.samples.len());
        let samples = audio.samples[self.delivered..end].to_vec();
        self.delivered = end;
        VoiceEvent::Audio {
            item_id: self.current_id.clone().expect("active speech"),
            content_index: 0,
            sample_rate: audio.sample_rate,
            samples,
        }
    }

    pub(in crate::orchestration::application) fn playback(&mut self, position: &PlaybackPosition) {
        let Some(audio) = &self.audio else {
            return;
        };
        if self.current_id.as_deref() == Some(&position.item_id)
            && position.content_index == 0
            && self.delivered == audio.samples.len()
            && u64::from(position.played_ms)
                == audio.samples.len() as u64 * 1000 / u64::from(audio.sample_rate)
        {
            self.current_id = None;
            self.audio = None;
            self.delivered = 0;
        }
    }

    pub(in crate::orchestration::application) fn clear(&mut self) {
        if let Some((job, cancel)) = self.job.take() {
            cancel.cancel();
            job.abort();
        }
        self.pending.clear();
        self.current_id = None;
        self.audio = None;
        self.delivered = 0;
    }
}

impl Drop for SpeechQueue {
    fn drop(&mut self) {
        self.clear();
    }
}
