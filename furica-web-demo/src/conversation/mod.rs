pub mod infrastructure;
pub mod presentation;

use crate::sessions::SnapshotStore;
use crate::{bootstrap::SAMPLE_RATE, settings::Config};
use furica_core::{
    Error, Result,
    audio::AudioFrame,
    config::CoreConfig,
    input::{InputConfig, SpeechInput},
    orchestration::{Core, CoreEvent, CoreHandle, CoreOutput, VoicePipeline},
    playback::PlaybackPosition,
    sessions::SessionManager,
};
use furica_local::{SileroVad, VadConfig};
use infrastructure::input::{PreparedVad, TestSpeaker};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{broadcast, mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Start {
        mode: Mode,
        speaker: String,
        #[serde(default)]
        session_id: Option<Uuid>,
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
    pub manager: SessionManager,
    pub session_id: Uuid,
    store: Arc<SnapshotStore>,
    changes: watch::Receiver<u64>,
    save_errors: mpsc::Receiver<Value>,
    saver: Option<JoinHandle<()>>,
    save_cancel: CancellationToken,
    pub capture: Option<mpsc::Sender<AudioFrame>>,
    pub speaker: Arc<TestSpeaker>,
    worker: Option<JoinHandle<()>>,
    errors_open: bool,
    runner_done: bool,
}

impl Session {
    pub async fn start(
        config: &Config,
        mode: Mode,
        decision: &str,
        selected: Option<Uuid>,
    ) -> Result<Self> {
        let missing = config.missing(mode == Mode::Voice);
        if !missing.is_empty() {
            return Err(Error::Config(format!("请填写配置：{}", missing.join(", "))));
        }
        let model = if mode == Mode::Voice {
            let path = config.vad_model_file();
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
        let (pipeline, manager, store) = config.compose().await?;
        // Subscribe before mutations and saving; coalesced notifications cannot
        // lose work prepared while the previous snapshot is written.
        let changes = manager.changes();
        let save_changes = manager.changes();
        let session_id = match selected {
            Some(id) => {
                manager.session(id)?;
                id
            }
            None => manager.create_session()?.id,
        };
        crate::sessions::save(&manager, &store, true).await?;
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
                manager.clone(),
                session_id,
            )?
        } else {
            Core::start(core_config, Arc::new(pipeline), manager.clone(), session_id)?
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
        let save_cancel = CancellationToken::new();
        let (save_error_tx, save_errors) = mpsc::channel(1);
        let saver = Some(tokio::spawn(crate::sessions::autosave(
            manager.clone(),
            store.clone(),
            save_changes,
            save_cancel.clone(),
            save_error_tx,
        )));
        let session = Self {
            core,
            output,
            events,
            errors,
            manager,
            session_id,
            store,
            changes,
            save_errors,
            saver,
            save_cancel,
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
            Command::Snapshot => return Ok(crate::sessions::snapshot(self.manager.snapshot()?)),
            Command::Start { .. } | Command::Stop => {
                return Err(Error::State("invalid session command".into()));
            }
        };
        Ok(json!({"type":"ack","result":result}))
    }

    pub async fn next(&mut self) -> Value {
        loop {
            tokio::select! {
                changed = self.changes.changed() => {
                    if changed.is_ok() {
                        return match self.manager.snapshot() {
                            Ok(data) => crate::sessions::snapshot(data),
                            Err(error) => json!({"type":"error", "message":error.to_string()}),
                        };
                    }
                }
                Some(event) = self.save_errors.recv() => return event,
                changed = self.output.playback.changed() => {
                    if changed.is_ok() { return presentation::playback(*self.output.playback.borrow_and_update()); }
                }
                event = self.events.recv() => match event {
                    Ok(CoreEvent::ClearPlayback) => return json!({"type":"clear_playback", "generation":self.output.playback.borrow().generation.to_string()}),
                    Ok(event) => return presentation::event(event),
                    Err(broadcast::error::RecvError::Lagged(count)) => return json!({"type":"resync", "lost":count,
                        "state":presentation::state(self.core.state()), "playback":presentation::playback(*self.output.playback.borrow()),
                        "sessions":self.manager.snapshot().ok().map(crate::sessions::snapshot)}),
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
        let mut result = Ok(());
        if !self.runner_done {
            result = self.core.shutdown().await;
            let runner = (&mut self.output.runner)
                .await
                .unwrap_or_else(|_| Err(Error::State("core runner failed".into())));
            self.runner_done = true;
            result = result.and(runner);
        }
        // Also await executor cleanup if the Core runner failed before its
        // normal shutdown path, then persist the resulting terminal records.
        let execution = self.manager.shutdown(Duration::from_secs(10)).await;
        self.save_cancel.cancel();
        if let Some(saver) = self.saver.take() {
            let _ = saver.await;
        }
        let saved = crate::sessions::save(&self.manager, &self.store, false).await;
        result.and(execution).and(worker_result).and(saved)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.capture.take();
        self.save_cancel.cancel();
        if let Some(saver) = self.saver.take() {
            saver.abort();
        }
        // Normal closure calls stop(), awaits Core and flushes the final snapshot.
        // This fallback also releases the Core actor if startup is abandoned.
        self.output.runner.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn websocket_controls_resume_business_sessions_and_reject_old_acp_commands() {
        let id = Uuid::new_v4();
        let command: Command = serde_json::from_value(
            json!({"type":"start", "mode":"text", "speaker":"accepted", "session_id":id}),
        )
        .unwrap();
        assert!(
            matches!(command, Command::Start { session_id: Some(selected), .. } if selected == id)
        );
        for kind in [
            "continue_task",
            "answer_question",
            "cancel_task",
            "close_task",
        ] {
            assert!(
                serde_json::from_value::<Command>(json!({"type":kind, "session_id":id})).is_err()
            );
        }
    }
}
