//! Silero runs off the ingress actor. One bounded utterance is retained until its
//! confirmed endpoint; offsets, not inference-call timing, select the PCM.
use async_trait::async_trait;
use furica_core::{
    Error, Result,
    audio::{AudioFrame, SpeechActivity},
    orchestration::{VoiceBackend, VoiceCommand, VoiceEvent, VoiceSession},
};
use furica_local::vad::{FRAME_SAMPLES, SileroVad, VadConfig, VadEvent};
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;

pub struct VadBackend {
    pub inner: Arc<dyn VoiceBackend>,
    pub model_path: PathBuf,
    pub capacity: usize,
    pub max_samples: usize,
}

enum Boundary {
    Started,
    Ended(Vec<i16>),
}

struct Segmenter {
    vad: SileroVad,
    pending: Vec<i16>,
    retained: VecDeque<i16>,
    base: u64,
    position: u64,
    start: Option<u64>,
    max_samples: usize,
}

impl Segmenter {
    fn process(&mut self, samples: &[i16]) -> Result<Vec<Boundary>> {
        let mut output = Vec::new();
        for &sample in samples {
            self.pending.push(sample);
            if self.pending.len() != FRAME_SAMPLES {
                continue;
            }
            self.retained.extend(self.pending.iter().copied());
            let boundary = self
                .vad
                .process(&self.pending)
                .map_err(|_| Error::Protocol("VAD inference failed".into()))?
                .event;
            self.pending.clear();
            self.position = self
                .position
                .checked_add(FRAME_SAMPLES as u64)
                .ok_or_else(|| Error::Audio("VAD position exhausted".into()))?;
            match boundary {
                Some(VadEvent::SpeechStarted { start_sample }) => {
                    if start_sample < self.base {
                        return Err(Error::Protocol("VAD onset no longer buffered".into()));
                    }
                    self.start = Some(start_sample);
                    output.push(Boundary::Started);
                }
                Some(VadEvent::SpeechEnded { end_sample }) => {
                    let start = self
                        .start
                        .take()
                        .ok_or_else(|| Error::Protocol("VAD endpoint without onset".into()))?;
                    if start < self.base || end_sample <= start || end_sample > self.position {
                        return Err(Error::Protocol("invalid VAD offsets".into()));
                    }
                    let pcm = self
                        .retained
                        .iter()
                        .skip((start - self.base) as usize)
                        .take((end_sample - start) as usize)
                        .copied()
                        .collect();
                    output.push(Boundary::Ended(pcm));
                }
                None => (),
            }
            let keep_from = self
                .start
                .unwrap_or(self.position.saturating_sub(4 * FRAME_SAMPLES as u64));
            while self.base < keep_from {
                self.retained.pop_front();
                self.base += 1;
            }
            // Includes up to 320 ms endpoint confirmation beyond the utterance.
            if self.retained.len() > self.max_samples + 16_000 {
                return Err(Error::Busy("VAD utterance limit"));
            }
        }
        Ok(output)
    }
}

#[async_trait]
impl VoiceBackend for VadBackend {
    fn sample_rate(&self) -> u32 {
        self.inner.sample_rate()
    }
    async fn connect(
        &self,
        sessions: furica_core::sessions::SessionManager,
        session_id: uuid::Uuid,
    ) -> Result<VoiceSession> {
        let path = self.model_path.clone();
        let vad =
            tokio::task::spawn_blocking(move || SileroVad::from_file(path, VadConfig::default()))
                .await
                .map_err(|_| Error::Closed)?
                .map_err(|_| Error::Config("unable to load Silero VAD".into()))?;
        let mut inner = self.inner.connect(sessions, session_id).await?;
        let (outer, mut channels) = VoiceSession::channels(self.capacity)?;
        let segmenter = Arc::new(Mutex::new(Segmenter {
            vad,
            pending: Vec::new(),
            retained: VecDeque::new(),
            base: 0,
            position: 0,
            start: None,
            max_samples: self.max_samples,
        }));
        tokio::spawn(async move {
            let cancel = channels.cancelled.clone();
            let result: Result<()> = async {
                let mut waiting_interrupt = false;
                let mut ready: VecDeque<Boundary> = VecDeque::new();
                let mut interrupt_deadline = tokio::time::Instant::now();
                let mut sequence = 0u64;
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Ok(()),
                        command = channels.commands.recv() => {
                            let Some(command) = command else { return Ok(()); };
                            match command {
                                VoiceCommand::Audio(frame) => {
                                    let worker = segmenter.clone();
                                    let segments = tokio::task::spawn_blocking(move || worker.lock().map_err(|_| Error::Closed)?.process(&frame.samples))
                                        .await.map_err(|_| Error::Closed)??;
                                    if ready.len() + segments.len() > 8 { return Err(Error::Busy("VAD pending boundaries")); }
                                    ready.extend(segments);
                                },
                                VoiceCommand::Interrupt {playback,cancel_response} => {
                                    send_command(&inner,VoiceCommand::Interrupt {playback,cancel_response},&cancel).await?;
                                    if !cancel_response { waiting_interrupt = false; }
                                    // Explicit barge-in cancellation does not invent an endpoint.
                                },
                                other => send_command(&inner,other,&cancel).await?,
                            }
                        },
                        _ = tokio::time::sleep_until(interrupt_deadline), if waiting_interrupt => return Err(Error::Timeout("speech-start interrupt")),
                        event = inner.receive() => {
                            let Some(event) = event else { return Err(Error::Closed); };
                            send_event(&channels.events,event,&cancel).await?;
                        },
                    }
                    if !waiting_interrupt {
                        while let Some(boundary) = ready.pop_front() {
                            let pcm = match boundary {
                                Boundary::Started => {
                                    send_event(&channels.events,VoiceEvent::SpeechStarted,&cancel).await?;
                                    waiting_interrupt = true;
                                    interrupt_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
                                    break;
                                },
                                Boundary::Ended(pcm) => pcm,
                            };
                            if pcm.len() < 2 { return Err(Error::Audio("empty VAD segment".into())); }
                            let mut offset = 0;
                            while offset < pcm.len() {
                                let remaining = pcm.len()-offset;
                                let count = if offset == 0 {remaining.saturating_sub(1).min(3200)} else {remaining.min(3200)};
                                let activity = if offset == 0 {SpeechActivity::Started} else if offset+count == pcm.len() {SpeechActivity::Ended} else {SpeechActivity::Continuing};
                                send_command(&inner,VoiceCommand::InputAudio {frame:AudioFrame {sequence,sample_rate:16_000,samples:pcm[offset..offset+count].to_vec()},activity},&cancel).await?;
                                sequence = sequence.checked_add(1).ok_or_else(|| Error::Audio("input sequence exhausted".into()))?;
                                offset += count;
                            }
                            send_event(&channels.events,VoiceEvent::SpeechStopped,&cancel).await?;
                        }
                    }
                }
            }.await;
            if let Err(error) = result {
                let _ = channels
                    .events
                    .try_send(VoiceEvent::Error(error.to_string()));
            }
        });
        Ok(outer)
    }
}

async fn send_event(
    sender: &tokio::sync::mpsc::Sender<VoiceEvent>,
    event: VoiceEvent,
    cancel: &CancellationToken,
) -> Result<()> {
    tokio::select! { biased; _ = cancel.cancelled() => Err(Error::Cancelled), result = sender.send(event) => result.map_err(|_| Error::Closed) }
}

async fn send_command(
    session: &VoiceSession,
    command: VoiceCommand,
    cancel: &CancellationToken,
) -> Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            match session.try_send(command.clone()) {
                Err(Error::Busy(_)) => tokio::select! {
                    _ = cancel.cancelled() => return Err(Error::Cancelled),
                    _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => (),
                },
                result => return result,
            }
        }
    })
    .await
    .map_err(|_| Error::Timeout("VAD backend backpressure"))?
}
