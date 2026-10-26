use super::{HostState, VoiceLease, voice_lease::gone};
use crate::{Error, Host, Result};
use furica_core::{
    audio::AudioFrame,
    playback::{PlaybackChunk, PlaybackPosition, PlaybackState},
};
use serde_json::{Value, json};
use std::{sync::atomic::Ordering, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
impl Host {
    fn authorize_voice(&self, lease: &VoiceLease, id: Uuid, token: &str) -> Result<()> {
        lease.authorize(id, token)?;
        let components = self.components.current();
        if lease.last_seen.elapsed()
            >= Duration::from_millis(components.config.limits.heartbeat_timeout_ms)
            || lease.retry_expired(Duration::from_millis(
                components.config.limits.upload_retry_window_ms,
            ))
        {
            lease.cancelled.cancel();
            return Err(gone());
        }
        Ok(())
    }
    pub async fn open_voice(&self, session_id: Uuid, owner: String) -> Result<Value> {
        self.accepting()?;
        if owner.trim().is_empty() || owner.len() > 128 {
            return Err(Error::new(
                400,
                "invalid_owner",
                "owner must contain 1–128 bytes",
            ));
        }
        let mut state = self.state.lock().await;
        self.usable(&state)?;
        if state.voice.is_some() {
            return Err(Error::new(
                409,
                "voice_occupied",
                "another voice channel is active",
            ));
        }
        let components = self.components.current();
        if !components.voice_available {
            return Err(Error::new(
                503,
                "voice_unavailable",
                "speech capabilities are not configured",
            ));
        }
        self.select_locked(&mut state, session_id, false).await?;
        let runtime = state.runtime.as_ref().ok_or_else(Error::conflict)?;
        self.reset_backend(runtime).await?;
        components.speech_enabled.store(true, Ordering::Release);
        let (tx, rx) = mpsc::channel(components.config.limits.channel_capacity);
        let lease = VoiceLease::new(session_id, owner, rx);
        let response = json!({"channel_id":lease.id,"voice_token":lease.token,"session_id":lease.session_id,"sample_rate":16000,"channels":1,"encoding":"pcm_s16le","next_sequence":0,"heartbeat_interval_ms":5000,"heartbeat_timeout_ms":components.config.limits.heartbeat_timeout_ms});
        *self.delivery.lock().map_err(|_| Error::conflict())? =
            Some((lease.id, tx, lease.cancelled.clone()));
        state.voice = Some(lease);
        let activated = self
            .active(state.runtime.as_ref().expect("runtime selected"))
            .await;
        if let Err(error) = activated {
            let _ = self
                .close_voice_locked(&mut state, "initialization_failed")
                .await;
            return Err(error);
        }
        state.voice.as_mut().expect("initialized lease").last_seen = Instant::now();
        Ok(response)
    }
    pub(super) async fn close_voice_locked(
        &self,
        state: &mut HostState,
        reason: &str,
    ) -> Result<()> {
        let Some(lease) = state.voice.take() else {
            return Ok(());
        };
        lease.cancelled.cancel();
        let components = self.components.current();
        if let Ok(mut delivery) = self.delivery.lock() {
            *delivery = None;
        }
        components.speech_enabled.store(false, Ordering::Release);
        let result = match state.runtime.as_ref() {
            Some(runtime) => self.reset_backend(runtime).await,
            None => Ok(()),
        };
        self.emit(json!({"kind":"voice_closed","channel_id":lease.id,"reason":reason}));
        result
    }
    pub async fn close_voice(&self, id: Uuid, token: &str) -> Result<()> {
        self.accepting()?;
        let mut state = self.state.lock().await;
        self.authorize_voice(state.voice.as_ref().ok_or_else(gone)?, id, token)?;
        self.close_voice_locked(&mut state, "closed").await
    }
    pub async fn heartbeat(&self, id: Uuid, token: &str) -> Result<()> {
        self.accepting()?;
        let mut state = self.state.lock().await;
        let lease = state.voice.as_mut().ok_or_else(gone)?;
        self.authorize_voice(lease, id, token)?;
        lease.last_seen = Instant::now();
        Ok(())
    }
    pub async fn upload(
        &self,
        id: Uuid,
        token: &str,
        sequence: u64,
        pcm: Vec<u8>,
    ) -> Result<Value> {
        self.accepting()?;
        let mut state = self.state.lock().await;
        self.usable(&state)?;
        let lease = state.voice.as_ref().ok_or_else(gone)?;
        self.authorize_voice(lease, id, token)?;
        if lease.check(sequence, &pcm)? {
            return Ok(
                json!({"sequence":sequence,"next_sequence":lease.next_sequence,"duplicate":true}),
            );
        }
        if state.capture_sequence == u64::MAX {
            let _ = self
                .close_voice_locked(&mut state, "sequence_exhausted")
                .await;
            return Err(gone());
        }
        let runtime = state.runtime.as_ref().ok_or_else(Error::conflict)?;
        // Wake after the existing Core idle timeout; never acknowledge audio queued to Standby.
        if let Err(error) = self.active(runtime).await {
            let _ = self
                .close_voice_locked(&mut state, "activation_failed")
                .await;
            return Err(error);
        }
        let frame = AudioFrame {
            sequence: state.capture_sequence,
            sample_rate: 16000,
            samples: pcm
                .as_chunks::<2>()
                .0
                .iter()
                .map(|s| i16::from_le_bytes([s[0], s[1]]))
                .collect(),
        };
        let components = self.components.current();
        match runtime.handle.audio(frame) {
            Ok(()) => {
                state.capture_sequence += 1;
                let lease = state.voice.as_mut().expect("authorized");
                lease.accept(sequence, pcm);
                Ok(
                    json!({"sequence":sequence,"next_sequence":lease.next_sequence,"duplicate":false}),
                )
            }
            Err(furica_core::Error::Busy(_)) => {
                let exhausted = state.voice.as_mut().expect("authorized").backpressure(
                    sequence,
                    &pcm,
                    components.config.limits.upload_retry_limit,
                    Duration::from_millis(components.config.limits.upload_retry_window_ms),
                );
                if exhausted {
                    self.close_voice_locked(&mut state, "upload_retry_exhausted")
                        .await?;
                    Err(gone())
                } else {
                    Err(Error::new(
                        429,
                        "backpressure",
                        "retry only this batch within the configured budget",
                    ))
                }
            }
            Err(error) => {
                let _ = self.close_voice_locked(&mut state, "input_failed").await;
                Err(error.into())
            }
        }
    }
    pub async fn take_audio(
        &self,
        id: Uuid,
        token: &str,
    ) -> Result<(
        mpsc::Receiver<PlaybackChunk>,
        watch::Receiver<PlaybackState>,
        CancellationToken,
    )> {
        self.accepting()?;
        let mut state = self.state.lock().await;
        let playback = state.runtime.as_ref().ok_or_else(gone)?.playback.clone();
        let lease = state.voice.as_mut().ok_or_else(gone)?;
        self.authorize_voice(lease, id, token)?;
        let receiver = lease.receiver.take().ok_or_else(|| {
            Error::new(
                409,
                "download_claimed",
                "voice audio already has a consumer",
            )
        })?;
        Ok((receiver, playback, lease.cancelled.clone()))
    }
    pub async fn playback(
        &self,
        id: Uuid,
        token: &str,
        generation: u64,
        position: PlaybackPosition,
    ) -> Result<()> {
        self.accepting()?;
        let state = self.state.lock().await;
        self.authorize_voice(state.voice.as_ref().ok_or_else(gone)?, id, token)?;
        state
            .runtime
            .as_ref()
            .ok_or_else(gone)?
            .handle
            .report_playback(generation, position)
            .await
            .map_err(Into::into)
    }
}
