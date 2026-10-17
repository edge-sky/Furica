//! Ordered capture ingestion, VAD boundaries and asynchronous speaker verification.

use super::Actor;
use crate::{
    Error, Result, audio::AudioFrame, input::VadActivity, orchestration::CoreState,
    orchestration::VoiceEvent,
};
use tokio::time::Instant;

impl Actor {
    pub(super) fn input(&mut self, frame: AudioFrame) -> Result<()> {
        frame.validate(self.config.sample_rate)?;
        if self
            .last_sequence
            .is_some_and(|last| frame.sequence <= last)
        {
            return Err(Error::Audio("capture sequence did not increase".into()));
        }
        self.last_sequence = Some(frame.sequence);
        if self.gate.is_some() {
            return self.gated_input(frame);
        }
        match *self.state.borrow() {
            CoreState::Standby => self.recent.push_rolling(frame),
            CoreState::Connecting | CoreState::Active => {
                if frame.sequence >= self.authorized_from {
                    self.pending.push(frame)?;
                }
            }
            CoreState::Muted | CoreState::Stopped | CoreState::Verifying => (),
        }
        Ok(())
    }

    fn gated_input(&mut self, frame: AudioFrame) -> Result<()> {
        use crate::audio::SpeechActivity;
        let state = *self.state.borrow();
        if matches!(state, CoreState::Muted | CoreState::Stopped)
            || frame.sequence < self.authorized_from
        {
            return Ok(());
        }
        let gate = self.gate.as_mut().expect("gated input");
        let activity = match gate.input.vad.detect(&frame)? {
            VadActivity::Speech => {
                gate.voiced_samples = gate.voiced_samples.saturating_add(frame.samples.len());
                gate.silent_samples = 0;
                self.last_activity = Instant::now();
                if std::mem::replace(&mut gate.speaking, true) {
                    SpeechActivity::Continuing
                } else {
                    SpeechActivity::Started
                }
            }
            VadActivity::Silence if gate.speaking => {
                gate.silent_samples += frame.samples.len();
                if gate.silent_samples as f64 / f64::from(self.config.sample_rate)
                    >= gate.input.config.endpoint_silence.as_secs_f64()
                {
                    gate.speaking = false;
                    SpeechActivity::Ended
                } else {
                    SpeechActivity::Continuing
                }
            }
            VadActivity::Silence => SpeechActivity::Silence,
        };
        if state == CoreState::Standby && activity == SpeechActivity::Silence {
            return Ok(());
        }
        let should_verify = !gate.requested
            && (activity == SpeechActivity::Ended
                || gate.voiced_samples as f64 / f64::from(self.config.sample_rate)
                    >= gate.input.config.verification_window.as_secs_f64());
        self.pending.push_input(frame, Some(activity))?;
        if state == CoreState::Standby {
            self.authorized_from = self.pending.oldest_sequence().expect("buffered onset");
            self.set_state(CoreState::Verifying);
        }
        // The authorization decision is made once; subsequent speakers are
        // deliberately not checked until this conversation ends.
        if *self.state.borrow() == CoreState::Verifying && should_verify {
            let gate = self.gate.as_mut().expect("gated input");
            gate.requested = true;
            let verifier = gate.input.speaker.clone();
            let timeout = gate.input.config.verification_timeout;
            let frames = self.pending.snapshot();
            let epoch = self.epoch;
            let tx = self.verified_tx.clone();
            let cancel = tokio_util::sync::CancellationToken::new();
            let worker_cancel = cancel.clone();
            let worker = tokio::spawn(async move {
                let result =
                    tokio::time::timeout(timeout, verifier.verify(frames, worker_cancel.clone()))
                        .await
                        .unwrap_or(Err(Error::Timeout("speaker verification")));
                worker_cancel.cancel();
                let _ = tx.send((epoch, result)).await;
            });
            self.verification = Some((worker, cancel));
        }
        match activity {
            SpeechActivity::Started => {
                if self.session.is_some() {
                    self.voice_event(VoiceEvent::SpeechStarted)?;
                } else {
                    self.speaking = true;
                }
            }
            SpeechActivity::Ended => self.speaking = false,
            _ => (),
        }
        Ok(())
    }
}
