//! Pull-based HTTP/SSE transport; provider event meanings belong to each adapter.
use super::{LIMIT, bad, bounded};
use crate::infrastructure::http;
use bytes::Bytes;
use furica_core::{Error, Result};
use futures_util::StreamExt;
use reqwest::{Client, Response, header::HeaderValue};
use serde_json::Value;
use std::{pin::Pin, time::Duration};
use tokio_util::sync::CancellationToken;

const EVENT_LIMIT: usize = 256 * 1024;

pub(crate) struct Http {
    client: Client,
    endpoint: String,
    credential: HeaderValue,
    pub(crate) model: String,
    pub(crate) scope: String,
    anthropic_version: Option<HeaderValue>,
}
impl Http {
    pub(crate) fn new(
        endpoint: String,
        key: &str,
        model: String,
        scope: String,
        timeout: Duration,
        version: Option<&str>,
    ) -> Result<Self> {
        let anthropic_version = version
            .map(|v| {
                http::text(v, "Anthropic version")?;
                HeaderValue::from_str(v).map_err(|_| bad())
            })
            .transpose()?;
        Ok(Self {
            client: http::client_with_timeout(&endpoint, timeout)?,
            endpoint,
            credential: http::secret(key, version.is_none())?,
            model,
            scope,
            anthropic_version,
        })
    }
    pub(crate) async fn open(&self, body: Value, cancel: CancellationToken) -> Result<Sse> {
        bounded(&body)?;
        let mut request = self
            .client
            .post(&self.endpoint)
            .header("accept", "text/event-stream")
            .json(&body);
        if let Some(version) = &self.anthropic_version {
            request = request
                .header("x-api-key", self.credential.clone())
                .header("anthropic-version", version.clone());
        } else {
            request = request.header("authorization", self.credential.clone());
        }
        let response = http::cancellable(&cancel, async {
            request.send().await.map_err(http::transport)
        })
        .await?;
        http::status(&response)?;
        if !response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                v.split(';')
                    .next()
                    .is_some_and(|v| v.trim().eq_ignore_ascii_case("text/event-stream"))
            })
        {
            return Err(bad());
        }
        Ok(Sse::new(response, cancel))
    }
}

/// Reads only when polled, holds at most one HTTP chunk, one line and one event.
pub(crate) struct Sse {
    source: Pin<
        Box<dyn futures_core::Stream<Item = std::result::Result<Bytes, reqwest::Error>> + Send>,
    >,
    chunk: Bytes,
    offset: usize,
    line: Vec<u8>,
    data: String,
    event: String,
    cr: bool,
    first: bool,
    cancel: CancellationToken,
    read: usize,
}
impl Sse {
    fn new(response: Response, cancel: CancellationToken) -> Self {
        Self {
            source: Box::pin(response.bytes_stream()),
            chunk: Bytes::new(),
            offset: 0,
            line: Vec::new(),
            data: String::new(),
            event: String::new(),
            cr: false,
            first: true,
            cancel,
            read: 0,
        }
    }
    fn line(&mut self) -> Result<Option<(String, String)>> {
        let bytes = std::mem::take(&mut self.line);
        let mut line = std::str::from_utf8(&bytes).map_err(|_| bad())?;
        if self.first {
            line = line.trim_start_matches('\u{feff}');
            self.first = false;
        }
        if line.is_empty() {
            let event = std::mem::take(&mut self.event);
            if self.data.is_empty() {
                return Ok(None);
            }
            self.data.pop(); // SSE removes the final data newline.
            return Ok(Some((event, std::mem::take(&mut self.data))));
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "data" => {
                if value.len().saturating_add(1) > EVENT_LIMIT.saturating_sub(self.data.len()) {
                    return Err(Error::Busy("SSE event"));
                }
                self.data.push_str(value);
                self.data.push('\n');
            }
            "event" => self.event = value.to_owned(),
            _ => (),
        }
        Ok(None)
    }
    pub(crate) async fn next(&mut self) -> Result<(String, String)> {
        loop {
            if self.cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            while self.offset < self.chunk.len() {
                let byte = self.chunk[self.offset];
                self.offset += 1;
                if self.cr && byte == b'\n' {
                    self.cr = false;
                    continue;
                }
                self.cr = byte == b'\r';
                if byte == b'\n' || byte == b'\r' {
                    if let Some(event) = self.line()? {
                        return Ok(event);
                    }
                } else {
                    if self.line.len() >= EVENT_LIMIT {
                        return Err(Error::Busy("SSE line"));
                    }
                    self.line.push(byte);
                }
            }
            let part = http::cancellable(&self.cancel, async {
                self.source
                    .next()
                    .await
                    .transpose()
                    .map_err(http::transport)
            })
            .await?
            .ok_or_else(bad)?;
            // Bound raw traffic too, including ignored control events and repeated snapshots.
            if part.len() > LIMIT || part.len() > (16 * LIMIT).saturating_sub(self.read) {
                return Err(Error::Busy("SSE transport"));
            }
            self.read += part.len();
            self.chunk = part;
            self.offset = 0;
        }
    }
    #[cfg(any(feature = "openai-responses", feature = "anthropic"))]
    pub(crate) async fn json(&mut self) -> Result<Value> {
        let (event, data) = self.next().await?;
        let value: Value = serde_json::from_str(&data).map_err(|_| bad())?;
        if !event.is_empty() && value["type"].as_str() != Some(event.as_str()) {
            return Err(bad());
        }
        if value.get("error").is_some() || value["type"] == "error" {
            return Err(bad());
        }
        Ok(value)
    }
}
