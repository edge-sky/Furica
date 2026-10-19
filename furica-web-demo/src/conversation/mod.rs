pub mod infrastructure;
pub mod presentation;

use crate::bootstrap::{Config, SAMPLE_RATE, resolve_path};
use furica_core::{
    Error, Result,
    audio::AudioFrame,
    config::CoreConfig,
    input::{InputConfig, SpeechInput},
    orchestration::{Core, CoreEvent, CoreHandle, CoreOutput, VoicePipeline},
    playback::PlaybackPosition,
    tasks::{TaskAnswer, TaskManager, infrastructure::sqlite::TaskStore},
};
use furica_vad::{SileroVad, VadConfig};
use infrastructure::input::{PreparedVad, TestSpeaker};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Start {
        mode: Mode,
        speaker: String,
    },
    Text {
        text: String,
    },
    End,
    Wake,
    Mute {
        muted: bool,
    },
    Interrupt,
    Stop,
    Speaker {
        decision: String,
    },
    Playback {
        generation: String,
        item_id: String,
        content_index: u32,
        played_ms: u32,
    },
    Snapshot,
    ContinueTask {
        session_id: Uuid,
        prompt: String,
    },
    AnswerQuestion {
        question_id: Uuid,
        answer: TaskAnswer,
    },
    CancelTask {
        session_id: Uuid,
    },
    CloseTask {
        session_id: Uuid,
    },
}

#[derive(Clone, Copy, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Voice,
    Text,
}

pub struct Session {
    pub core: CoreHandle,
    pub output: CoreOutput,
    pub events: broadcast::Receiver<CoreEvent>,
    pub errors: mpsc::Receiver<String>,
    pub tasks: TaskManager,
    pub store: Arc<TaskStore>,
    pub capture: Option<mpsc::Sender<AudioFrame>>,
    pub speaker: Arc<TestSpeaker>,
    worker: Option<JoinHandle<()>>,
    errors_open: bool,
    runner_done: bool,
}

impl Session {
    pub async fn start(config: &Config, mode: Mode, decision: &str) -> Result<Self> {
        let missing = config.missing(mode == Mode::Voice);
        if !missing.is_empty() {
            return Err(Error::Config(format!("请填写配置：{}", missing.join(", "))));
        }
        let model = if mode == Mode::Voice {
            let path = resolve_path(&config.vad_model_path);
            Some(
                tokio::task::spawn_blocking(move || {
                    SileroVad::from_file(path, VadConfig::default())
                })
                .await
                .map_err(|_| Error::State("VAD worker failed".into()))?
                .map_err(|_| {
                    Error::Config("unable to load VAD model; check the local ONNX file".into())
                })?,
            )
        } else {
            None
        };
        let (pipeline, tasks, store) = config.compose()?;
        let speaker = Arc::new(TestSpeaker::default());
        speaker.set(decision)?;
        let decisions = Arc::new(PreparedVad::default());
        let core_config = CoreConfig {
            sample_rate: SAMPLE_RATE,
            ..Default::default()
        };
        let (core, output) = if mode == Mode::Voice {
            Core::start_gated(
                core_config,
                SpeechInput {
                    vad: decisions.clone(),
                    speaker: speaker.clone(),
                    config: InputConfig::default(),
                },
                VoicePipeline::Cascaded(pipeline),
                tasks.clone(),
            )?
        } else {
            Core::start(core_config, Arc::new(pipeline), tasks.clone())?
        };
        let events = core.subscribe();
        let (error_tx, errors) = mpsc::channel(1);
        let (capture, worker) = if let Some(model) = model {
            let (tx, rx) = mpsc::channel(32);
            let handle = core.clone();
            (
                Some(tx),
                Some(tokio::task::spawn_blocking(move || {
                    infrastructure::input::run_capture(model, rx, decisions, handle, error_tx)
                })),
            )
        } else {
            (None, None)
        };
        let session = Self {
            core,
            output,
            events,
            errors,
            tasks,
            store,
            capture,
            speaker,
            worker,
            errors_open: true,
            runner_done: false,
        };
        if mode == Mode::Text {
            session.core.wake(None).await?;
        }
        Ok(session)
    }

    pub async fn command(&self, command: Command) -> Result<Value> {
        let result = match command {
            Command::Text { text } => {
                self.core.text(text).await?;
                Value::Null
            }
            Command::End => {
                self.core.end_conversation().await?;
                Value::Null
            }
            Command::Wake => {
                self.core.wake(None).await?;
                Value::Null
            }
            Command::Mute { muted } => {
                self.core.mute(muted).await?;
                Value::Null
            }
            Command::Interrupt => {
                self.core.interrupt().await?;
                Value::Null
            }
            Command::Speaker { decision } => {
                self.speaker.set(&decision)?;
                Value::Null
            }
            Command::Playback {
                generation,
                item_id,
                content_index,
                played_ms,
            } => {
                self.core
                    .report_playback(
                        generation
                            .parse()
                            .map_err(|_| Error::Config("invalid playback generation".into()))?,
                        PlaybackPosition {
                            item_id,
                            content_index,
                            played_ms,
                        },
                    )
                    .await?;
                Value::Null
            }
            Command::Snapshot => return crate::tasks::snapshot(&self.store),
            Command::ContinueTask { session_id, prompt } => {
                crate::tasks::continue_run(&self.tasks, session_id, prompt)?
            }
            Command::AnswerQuestion {
                question_id,
                answer,
            } => crate::tasks::answer(&self.tasks, question_id, answer)?,
            Command::CancelTask { session_id } => crate::tasks::cancel(&self.tasks, session_id)?,
            Command::CloseTask { session_id } => crate::tasks::close(&self.tasks, session_id)?,
            Command::Start { .. } | Command::Stop => {
                return Err(Error::State("invalid session command".into()));
            }
        };
        Ok(json!({"type":"ack","result":result}))
    }

    pub async fn next(&mut self) -> Value {
        loop {
            tokio::select! {
                changed = self.output.playback.changed() => {
                    if changed.is_ok() { return presentation::playback(*self.output.playback.borrow_and_update()); }
                }
                event = self.events.recv() => match event {
                    Ok(CoreEvent::ClearPlayback) => return json!({"type":"clear_playback", "generation":self.output.playback.borrow().generation.to_string()}),
                    Ok(event) => return presentation::event(event),
                    Err(broadcast::error::RecvError::Lagged(count)) => return json!({"type":"resync", "lost":count,
                        "state":presentation::state(self.core.state()), "playback":presentation::playback(*self.output.playback.borrow()),
                        "tasks":crate::tasks::snapshot(&self.store).ok()}),
                    Err(broadcast::error::RecvError::Closed) => (),
                },
                Some(audio) = self.output.audio.recv() => {
                    if audio.generation == self.output.playback.borrow().generation { return presentation::audio(audio); }
                }
                error = self.errors.recv(), if self.errors_open => match error {
                    Some(message) => {self.capture.take(); return json!({"type":"capture_error","message":message});},
                    None => self.errors_open=false,
                },
                result = &mut self.output.runner, if !self.runner_done => {
                    self.runner_done=true;
                    return json!({"type":"kernel_stopped", "message":match result {Ok(Ok(()))=>"内核已停止".to_owned(),Ok(Err(error))=>error.to_string(),Err(_)=>"内核运行任务异常退出".to_owned()}});
                }
            }
        }
    }

    pub async fn stop(mut self) -> Result<()> {
        self.capture.take();
        let mut worker_result = Ok(());
        if let Some(worker) = self.worker.take() {
            worker_result = tokio::time::timeout(Duration::from_secs(3), worker)
                .await
                .map_err(|_| Error::Timeout("capture worker shutdown"))
                .and_then(|result| {
                    result.map_err(|_| Error::State("capture worker failed".into()))
                });
        }
        if self.runner_done {
            return worker_result;
        }
        let result = self.core.shutdown().await;
        let runner = (&mut self.output.runner)
            .await
            .map_err(|_| Error::State("core runner failed".into()))?;
        self.runner_done = true;
        result.and(runner).and(worker_result)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.capture.take();
        // Connection errors and normal closure call stop() and await TaskManager.
        // This fallback also releases the Core actor if startup is abandoned.
        self.output.runner.abort();
    }
}
