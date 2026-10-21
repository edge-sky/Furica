//! Assemble host handles and the single conversation actor.

use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{broadcast, mpsc, watch},
    time::Instant,
};
use uuid::Uuid;

use super::Actor;
use crate::{
    Error, Result,
    audio::AudioBuffer,
    config::CoreConfig,
    input::{InputGate, SpeechInput},
    orchestration::{Core, CoreHandle, CoreOutput, CoreState, VoiceBackend, VoicePipeline},
    playback::PlaybackState,
    sessions::SessionManager,
};

impl Core {
    /// PRD entry point: VAD and speaker authorization precede either pipeline.
    /// Manual wake is disabled; raw microphone PCM goes through this handle.
    pub fn start_gated(
        config: CoreConfig,
        input: SpeechInput,
        pipeline: VoicePipeline,
        sessions: SessionManager,
        session_id: Uuid,
    ) -> Result<(CoreHandle, CoreOutput)> {
        input.validate(&config)?;
        Self::build(
            config,
            pipeline.backend(),
            sessions,
            session_id,
            Some(input),
        )
    }

    /// Must be called inside a Tokio runtime. The host supplies the business session and owns its data lifecycle.
    pub fn start(
        config: CoreConfig,
        backend: Arc<dyn VoiceBackend>,
        sessions: SessionManager,
        session_id: Uuid,
    ) -> Result<(CoreHandle, CoreOutput)> {
        Self::build(config, backend, sessions, session_id, None)
    }

    fn build(
        config: CoreConfig,
        backend: Arc<dyn VoiceBackend>,
        sessions: SessionManager,
        session_id: Uuid,
        input: Option<SpeechInput>,
    ) -> Result<(CoreHandle, CoreOutput)> {
        config.validate()?;
        if sessions.session(session_id)?.state != crate::sessions::SessionState::Active {
            return Err(Error::State("session is archived".into()));
        }
        tokio::runtime::Handle::try_current()
            .map_err(|_| Error::State("Core::start requires a Tokio runtime".into()))?;
        if backend.sample_rate() != config.sample_rate {
            return Err(Error::Config(
                "backend sample rate does not match core".into(),
            ));
        }
        let (commands, command_rx) = mpsc::channel(config.channel_capacity);
        let (audio, audio_rx) = mpsc::channel(config.channel_capacity);
        let (events, _) = broadcast::channel(config.channel_capacity);
        let (state_tx, state) = watch::channel(CoreState::Standby);
        let (playback_tx, playback) = watch::channel(PlaybackState {
            generation: 0,
            active: false,
        });
        let (output_tx, output_rx) = mpsc::channel(config.channel_capacity);
        let (connected_tx, connected_rx) = mpsc::channel(1);
        let (verified_tx, verified_rx) = mpsc::channel(1);
        let execution_events = sessions.subscribe();
        let handle = CoreHandle {
            commands,
            audio,
            events: events.clone(),
            state,
            sample_rate: config.sample_rate,
            capture_sequence: Arc::new(Mutex::new(None)),
        };
        let actor = Actor {
            recent: AudioBuffer::new(config.sample_rate, config.pre_roll),
            pending: AudioBuffer::new(config.sample_rate, config.connect_buffer),
            config,
            backend,
            tools: super::CoreTools::new(sessions.clone(), session_id, false),
            sessions,
            commands: command_rx,
            audio: audio_rx,
            events,
            state: state_tx,
            playback: playback_tx,
            output: output_tx,
            connected_tx,
            connected_rx,
            verified_tx,
            verified_rx,
            gate: input.map(InputGate::new),
            verification: None,
            execution_events,
            session: None,
            connection: None,
            epoch: Uuid::new_v4(),
            session_id,
            last_sequence: None,
            last_activity: Instant::now(),
            playing: None,
            received_samples: 0,
            discarded_items: HashSet::new(),
            authorized_from: 0,
            capture_sequence: handle.capture_sequence.clone(),
            speaking: false,
            responding: false,
            execution_results: VecDeque::new(),
            execution_questions: VecDeque::new(),
        };
        let runner = tokio::spawn(actor.run());
        Ok((
            handle,
            CoreOutput {
                audio: output_rx,
                playback,
                runner,
            },
        ))
    }
}
