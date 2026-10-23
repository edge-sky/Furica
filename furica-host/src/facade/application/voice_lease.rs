use crate::facade::constant_eq;
use crate::{Error, Result};
use furica_core::playback::PlaybackChunk;
use std::time::Duration;
use tokio::{sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub(super) struct VoiceLease {
    pub id: Uuid,
    pub token: String,
    pub session_id: Uuid,
    // Retained for debugging a live channel; not an authorization credential.
    #[expect(dead_code)]
    pub owner: String,
    pub last_seen: Instant,
    pub next_sequence: u64,
    pub receiver: Option<mpsc::Receiver<PlaybackChunk>>,
    pub cancelled: CancellationToken,
    accepted: Option<(u64, Vec<u8>)>,
    retry: Option<(u64, Vec<u8>, u32, Instant)>,
}

impl VoiceLease {
    pub fn new(session_id: Uuid, owner: String, receiver: mpsc::Receiver<PlaybackChunk>) -> Self {
        Self {
            id: Uuid::new_v4(),
            token: format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple()),
            session_id,
            owner,
            last_seen: Instant::now(),
            next_sequence: 0,
            receiver: Some(receiver),
            cancelled: CancellationToken::new(),
            accepted: None,
            retry: None,
        }
    }
    pub fn authorize(&self, id: Uuid, token: &str) -> Result<()> {
        if id != self.id || self.cancelled.is_cancelled() {
            return Err(gone());
        }
        if !constant_eq(token.as_bytes(), self.token.as_bytes()) {
            return Err(Error::new(
                401,
                "invalid_voice_token",
                "voice ownership credential is invalid",
            ));
        }
        Ok(())
    }
    /// True means the exact previous batch was already accepted.
    pub fn check(&self, sequence: u64, pcm: &[u8]) -> Result<bool> {
        if pcm.is_empty() || !pcm.len().is_multiple_of(2) || pcm.len() > 6400 {
            return Err(Error::new(
                if pcm.len() > 6400 { 413 } else { 400 },
                "invalid_pcm",
                "expected 1–3200 mono PCM16 little-endian samples",
            ));
        }
        if let Some((previous, bytes)) = &self.accepted
            && sequence == *previous
        {
            return if bytes == pcm {
                Ok(true)
            } else {
                Err(Error::new(
                    409,
                    "batch_conflict",
                    "accepted sequence has different audio",
                ))
            };
        }
        if let Some((failed, bytes, _, _)) = &self.retry
            && sequence == *failed
            && bytes != pcm
        {
            return Err(Error::new(
                409,
                "batch_conflict",
                "retried sequence has different audio",
            ));
        }
        if sequence != self.next_sequence {
            return Err(Error::new(
                409,
                "sequence_gap",
                "upload must use the next sequence",
            ));
        }
        if sequence == u64::MAX {
            return Err(Error::new(
                409,
                "sequence_exhausted",
                "reopen the voice channel",
            ));
        }
        Ok(false)
    }
    pub fn accept(&mut self, sequence: u64, pcm: Vec<u8>) {
        self.next_sequence = sequence + 1;
        self.accepted = Some((sequence, pcm));
        self.retry = None;
    }
    pub fn backpressure(
        &mut self,
        sequence: u64,
        pcm: &[u8],
        limit: u32,
        window: Duration,
    ) -> bool {
        let (_, _, attempts, started) = self
            .retry
            .get_or_insert_with(|| (sequence, pcm.to_vec(), 0, Instant::now()));
        *attempts += 1;
        // First failure plus at most `limit` retries, bounded independently by time.
        *attempts > limit || started.elapsed() >= window
    }
    pub fn retry_expired(&self, window: Duration) -> bool {
        self.retry
            .as_ref()
            .is_some_and(|(_, _, _, started)| started.elapsed() >= window)
    }
}

pub fn gone() -> Error {
    Error::new(410, "voice_closed", "voice channel is no longer active")
}
