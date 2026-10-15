//! Command receipt, turn queues, tool continuations and cancellation.

use super::{
    CascadedBackend, ModelMessage, ModelResponse, Utterance,
    inference::{TurnInput, TurnResult, infer, message_size, validate_text},
};
use crate::{
    Error, Result,
    audio::{AudioFrame, SpeechActivity},
    voice::{VoiceChannels, VoiceCommand, VoiceEvent},
};
use std::collections::{HashSet, VecDeque};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub(super) fn spawn(backend: CascadedBackend, channels: VoiceChannels) {
    let worker = CascadeWorker {
        backend,
        channels,
        history: Vec::new(),
        context_bytes: 0,
        frames: Vec::new(),
        samples: 0,
        speaking: false,
        waiting_tools: HashSet::new(),
        pending: VecDeque::new(),
        job: None,
    };
    tokio::spawn(worker.run());
}

struct CascadeWorker {
    backend: CascadedBackend,
    channels: VoiceChannels,
    history: Vec<ModelMessage>,
    context_bytes: usize,
    frames: Vec<AudioFrame>,
    samples: usize,
    speaking: bool,
    waiting_tools: HashSet<String>,
    pending: VecDeque<TurnInput>,
    job: Option<(JoinHandle<Result<TurnResult>>, CancellationToken)>,
}

impl CascadeWorker {
    async fn run(mut self) {
        let result = self.run_inner().await;
        self.cancel_job();
        if let Err(error) = result {
            // Do not wait on an abandoned/full output queue during shutdown.
            let _ = self
                .channels
                .events
                .try_send(VoiceEvent::Error(error.to_string()));
        }
    }

    async fn run_inner(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                biased;
                _ = self.channels.cancelled.cancelled() => return Ok(()),
                command = self.channels.commands.recv() => {
                    let Some(command) = command else { return Ok(()); };
                    self.command(command)?;
                }
                result = async { (&mut self.job.as_mut().expect("guarded job").0).await }, if self.job.is_some() => {
                    let (_, cancel) = self.job.take().expect("completed job");
                    cancel.cancel();
                    let result = result.map_err(|error| Error::Protocol(format!("pipeline worker failed: {error}")))??;
                    self.complete(result)?;
                    self.emit(VoiceEvent::ResponseFinished)?;
                }
            }
            if self.job.is_none()
                && !self.speaking
                && self.waiting_tools.is_empty()
                && let Some(input) = self.pending.pop_front()
            {
                self.emit(VoiceEvent::ResponseStarted)?;
                let backend = self.backend.clone();
                let history = self.history.clone();
                let cancel = self.channels.cancelled.child_token();
                let worker_cancel = cancel.clone();
                let job = tokio::spawn(async move {
                    tokio::time::timeout(
                        backend.config.inference_timeout,
                        infer(backend, history, input, worker_cancel),
                    )
                    .await
                    .unwrap_or(Err(Error::Timeout("cascaded inference")))
                });
                self.job = Some((job, cancel));
            }
        }
    }

    fn emit(&self, event: VoiceEvent) -> Result<()> {
        self.channels
            .events
            .try_send(event)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::Busy("pipeline event queue"),
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })
    }

    fn remember(&mut self, message: ModelMessage) -> Result<()> {
        let size = message_size(&message);
        if self.history.len() >= self.backend.config.max_history_messages
            || self.context_bytes.saturating_add(size) > self.backend.config.max_context_bytes
        {
            return Err(Error::Busy("pipeline conversation context"));
        }
        self.context_bytes += size;
        self.history.push(message);
        Ok(())
    }

    fn queue(&mut self, input: TurnInput) -> Result<()> {
        // Multiple metadata/tool updates queued before inference share the
        // same next history snapshot; generating it twice would repeat a reply.
        if matches!(input, TurnInput::Continue)
            && self
                .pending
                .iter()
                .any(|pending| matches!(pending, TurnInput::Continue))
        {
            return Ok(());
        }
        if self.pending.len() >= self.backend.config.channel_capacity {
            return Err(Error::Busy("pipeline pending turns"));
        }
        self.pending.push_back(input);
        Ok(())
    }

    fn cancel_job(&mut self) {
        if let Some((job, cancel)) = self.job.take() {
            cancel.cancel();
            job.abort();
        }
    }

    fn command(&mut self, command: VoiceCommand) -> Result<()> {
        match command {
            VoiceCommand::InputAudio { frame, activity } => {
                frame.validate(self.backend.config.sample_rate)?;
                match activity {
                    SpeechActivity::Started => {
                        self.frames.clear();
                        self.samples = 0;
                        self.speaking = true;
                    }
                    SpeechActivity::Continuing | SpeechActivity::Ended if !self.speaking => {
                        return Err(Error::Protocol("audio outside an utterance".into()));
                    }
                    _ => (),
                }
                if self.speaking {
                    self.samples += frame.samples.len();
                    if self.samples as f64 / f64::from(frame.sample_rate)
                        > self.backend.config.max_utterance.as_secs_f64()
                        || self.frames.len() >= 4096
                    {
                        return Err(Error::Busy("STT utterance buffer"));
                    }
                    self.frames.push(frame);
                }
                if activity == SpeechActivity::Ended {
                    self.speaking = false;
                    self.samples = 0;
                    let frames = std::mem::take(&mut self.frames);
                    self.queue(TurnInput::Audio(Utterance {
                        sample_rate: self.backend.config.sample_rate,
                        frames,
                    }))?;
                }
            }
            VoiceCommand::Audio(_) => {
                return Err(Error::Protocol(
                    "cascaded input requires VAD boundaries".into(),
                ));
            }
            VoiceCommand::Text(text) => {
                validate_text(&text, self.backend.config.max_context_bytes)?;
                self.queue(TurnInput::Text(text))?;
            }
            VoiceCommand::TaskContext(context) => {
                self.remember(ModelMessage::TaskContext(context))?
            }
            VoiceCommand::TaskUpdate(update) => {
                self.remember(ModelMessage::TaskUpdate(update))?;
                self.queue(TurnInput::Continue)?;
            }
            VoiceCommand::ToolResult { call_id, result } => {
                if !self.waiting_tools.remove(&call_id) {
                    return Err(Error::Protocol("unexpected model tool result".into()));
                }
                self.remember(ModelMessage::ToolResult { call_id, result })?;
                if self.waiting_tools.is_empty() {
                    self.queue(TurnInput::Continue)?;
                }
            }
            VoiceCommand::Interrupt { .. } => {
                self.cancel_job();
                self.pending.clear();
                // ACP tasks already accepted remain managed by Core/TaskManager.
                self.emit(VoiceEvent::ResponseFinished)?;
            }
        }
        Ok(())
    }

    fn complete(&mut self, result: TurnResult) -> Result<()> {
        if let Some(text) = result.user_text {
            self.remember(ModelMessage::User(text.clone()))?;
            self.emit(VoiceEvent::Transcript {
                user: true,
                text,
                final_chunk: true,
            })?;
        }
        match result.response {
            Some(ModelResponse::Text(text)) => {
                self.remember(ModelMessage::Assistant(text.clone()))?;
                self.emit(VoiceEvent::Transcript {
                    user: false,
                    text,
                    final_chunk: true,
                })?;
                if let Some(audio) = result.audio {
                    let item_id = Uuid::new_v4().to_string();
                    for samples in audio.samples.chunks(audio.sample_rate as usize) {
                        self.emit(VoiceEvent::Audio {
                            item_id: item_id.clone(),
                            content_index: 0,
                            sample_rate: audio.sample_rate,
                            samples: samples.to_vec(),
                        })?;
                    }
                }
            }
            Some(ModelResponse::Tools(calls)) => {
                self.remember(ModelMessage::AssistantTools(calls.clone()))?;
                for call in calls {
                    self.waiting_tools.insert(call.call_id.clone());
                    self.emit(VoiceEvent::ToolCall {
                        call_id: call.call_id,
                        name: call.name,
                        arguments: call.arguments,
                    })?;
                }
            }
            None => (),
        }
        Ok(())
    }
}

impl Drop for CascadeWorker {
    fn drop(&mut self) {
        self.cancel_job();
    }
}
