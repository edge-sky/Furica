//! Shared speech encoding, PCM conversion and bounded JSON reads.
#[cfg(feature = "volcengine")]
use std::pin::Pin;

use base64::{Engine, engine::general_purpose::STANDARD};
#[cfg(feature = "volcengine")]
use bytes::Bytes;
use furica_core::{Error, Result, stt::Utterance, tts::SynthesizedAudio};
#[cfg(feature = "volcengine")]
use futures_core::Stream;
use futures_util::StreamExt;
use reqwest::Response;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

pub(crate) const MESSAGE_LIMIT: usize = 2 * 1024 * 1024;
const RESPONSE_LIMIT: usize = 1024 * 1024;

use super::http::{cancellable, status, transport};

pub(crate) async fn json_response(response: Response, cancel: &CancellationToken) -> Result<Value> {
    status(&response)?;
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(part) = cancellable(cancel, async {
        stream.next().await.transpose().map_err(transport)
    })
    .await?
    {
        if part.len() > RESPONSE_LIMIT - bytes.len() {
            return Err(Error::Busy("speech response size"));
        }
        bytes.extend_from_slice(&part);
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| Error::Protocol("invalid speech JSON response".into()))
}

/// Validate the complete utterance before allocating its WAV representation.
pub(crate) fn wav_base64(utterance: Utterance) -> Result<String> {
    if !(8_000..=48_000).contains(&utterance.sample_rate)
        || utterance.frames.is_empty()
        || utterance.frames.len() > 4096
    {
        return Err(Error::Audio("invalid STT utterance".into()));
    }
    let mut samples = 0usize;
    let mut last = None;
    for frame in &utterance.frames {
        frame.validate(utterance.sample_rate)?;
        if last.is_some_and(|sequence| frame.sequence <= sequence) {
            return Err(Error::Audio("STT frame order did not increase".into()));
        }
        last = Some(frame.sequence);
        samples += frame.samples.len();
        if samples > utterance.sample_rate as usize * 120 {
            return Err(Error::Busy("STT utterance duration"));
        }
    }
    let size = (samples * 2) as u32;
    let mut wav = Vec::with_capacity(44 + size as usize);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36 + size).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&utterance.sample_rate.to_le_bytes());
    wav.extend_from_slice(&(utterance.sample_rate * 2).to_le_bytes());
    wav.extend_from_slice(&2u16.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&size.to_le_bytes());
    for frame in utterance.frames {
        for sample in frame.samples {
            wav.extend_from_slice(&sample.to_le_bytes());
        }
    }
    Ok(STANDARD.encode(wav))
}

pub(crate) fn validate_tts(text: &str, limit: usize) -> Result<()> {
    if text.trim().is_empty() || text.len() > limit {
        return Err(Error::Config("empty or oversized TTS text".into()));
    }
    Ok(())
}

/// Network chunks may split a little-endian sample in half.
pub(crate) struct Pcm {
    sample_rate: u32,
    carry: Option<u8>,
    emitted: bool,
}

impl Pcm {
    pub(crate) fn new(sample_rate: u32) -> Self {
        Self {
            sample_rate,
            carry: None,
            emitted: false,
        }
    }

    pub(crate) fn push(&mut self, bytes: &[u8]) -> Result<Option<SynthesizedAudio>> {
        if bytes.len() > MESSAGE_LIMIT {
            return Err(Error::Busy("speech PCM message size"));
        }
        let mut samples = Vec::with_capacity((bytes.len() + usize::from(self.carry.is_some())) / 2);
        let mut offset = 0;
        if let Some(low) = self.carry.take() {
            if let Some(&high) = bytes.first() {
                samples.push(i16::from_le_bytes([low, high]));
                offset = 1;
            } else {
                self.carry = Some(low);
            }
        }
        let (pairs, remainder) = bytes[offset..].as_chunks::<2>();
        samples.extend(pairs.iter().copied().map(i16::from_le_bytes));
        if let Some(&low) = remainder.first() {
            self.carry = Some(low);
        }
        if samples.is_empty() {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(SynthesizedAudio {
            sample_rate: self.sample_rate,
            samples,
        }))
    }

    pub(crate) fn finish(&self) -> Result<()> {
        if self.carry.is_some() {
            Err(Error::Audio("truncated PCM sample".into()))
        } else if !self.emitted {
            Err(Error::Audio("speech synthesis returned no audio".into()))
        } else {
            Ok(())
        }
    }
}

/// Incremental concatenated/newline-delimited JSON, independent of HTTP chunks.
#[cfg(feature = "volcengine")]
pub(crate) struct JsonStream {
    source: Pin<Box<dyn Stream<Item = std::result::Result<Bytes, reqwest::Error>> + Send>>,
    buffer: Vec<u8>,
    ended: bool,
}

#[cfg(feature = "volcengine")]
impl JsonStream {
    pub(crate) fn new(response: Response) -> Self {
        Self {
            source: Box::pin(response.bytes_stream()),
            buffer: Vec::new(),
            ended: false,
        }
    }

    pub(crate) async fn next(&mut self, cancel: &CancellationToken) -> Result<Option<Value>> {
        loop {
            if let Some(start) = self
                .buffer
                .iter()
                .position(|byte| !byte.is_ascii_whitespace())
            {
                self.buffer.drain(..start);
                let mut decoder =
                    serde_json::Deserializer::from_slice(&self.buffer).into_iter::<Value>();
                match decoder.next() {
                    Some(Ok(value)) => {
                        let used = decoder.byte_offset();
                        self.buffer.drain(..used);
                        return Ok(Some(value));
                    }
                    Some(Err(error)) if error.is_eof() && !self.ended => (),
                    _ => {
                        return Err(Error::Protocol(
                            "invalid or truncated speech JSON stream".into(),
                        ));
                    }
                }
            } else {
                self.buffer.clear();
            }
            if self.ended {
                return Ok(None);
            }
            match cancellable(cancel, async {
                self.source.next().await.transpose().map_err(transport)
            })
            .await?
            {
                Some(part) => {
                    if part.len() > MESSAGE_LIMIT - self.buffer.len() {
                        return Err(Error::Busy("speech JSON message size"));
                    }
                    self.buffer.extend_from_slice(&part);
                }
                None => self.ended = true,
            }
        }
    }
}
