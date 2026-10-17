//! One validation/assembly state machine for streaming and complete calls.
use super::{
    domain::{continuation_size, json_size},
    *,
};
use crate::{Error, Result};
use futures_core::Stream;
use std::{
    collections::{BTreeMap, HashSet},
    future::{Future, poll_fn},
    pin::Pin,
    task::{Context, Poll},
};
use tokio_util::sync::CancellationToken;

const MAX_BLOCKS: usize = 256;

fn protocol() -> Error {
    Error::Protocol("invalid model event lifecycle or tool call".into())
}

fn valid_call(call: &ToolCall) -> bool {
    !call.call_id.is_empty()
        && call.call_id.len() <= 256
        && !call.name.is_empty()
        && call.name.len() <= 128
        && call.arguments.is_object()
}

fn validate_request(
    request: &ModelRequest,
    source: &str,
    max_messages: usize,
    limit: usize,
) -> Result<usize> {
    if max_messages == 0
        || limit == 0
        || request.max_output_tokens == 0
        || matches!(request.reasoning, Some(Reasoning::BudgetTokens(0)))
    {
        return Err(Error::Config("invalid text model request or limits".into()));
    }
    if request.messages.len() >= max_messages {
        return Err(Error::Busy("pipeline conversation context"));
    }
    let mut bytes = request.instructions.as_ref().map_or(0, String::len);
    if bytes > limit {
        return Err(Error::Busy("pipeline conversation context"));
    }
    let mut add = |size: usize| -> Result<()> {
        if size > limit.saturating_sub(bytes) {
            return Err(Error::Busy("pipeline conversation context"));
        }
        bytes += size;
        Ok(())
    };
    let mut names = HashSet::new();
    if request.tools.len() > 32 {
        return Err(Error::Config("too many model tools".into()));
    }
    for tool in &request.tools {
        if tool.name.is_empty()
            || tool.name.len() > 128
            || !names.insert(&tool.name)
            || !tool.parameters.is_object()
        {
            return Err(Error::Config("invalid tool definition".into()));
        }
        add(tool.name.len())?;
        add(tool.description.as_ref().map_or(0, String::len))?;
        add(json_size(&tool.parameters))?;
    }
    let mut pending = HashSet::new();

    for message in &request.messages {
        add(message_size(message))?;
        match message {
            ModelMessage::Assistant(message) => {
                if !pending.is_empty() {
                    return Err(protocol());
                }
                if let Some(c) = &message.continuation
                    && (source.is_empty() || c.source != source)
                {
                    return Err(Error::Config(
                        "incompatible model continuation source".into(),
                    ));
                }
                let mut count = 0;
                let mut used = HashSet::new();
                for call in message.tool_calls() {
                    count += 1;
                    if !valid_call(call) || !used.insert(call.call_id.clone()) || count > 32 {
                        return Err(protocol());
                    }
                    pending.insert(call.call_id.clone());
                }
            }
            ModelMessage::ToolResult { call_id, .. } => {
                if !pending.remove(call_id) {
                    return Err(protocol());
                }
            }
            ModelMessage::User(_) if !pending.is_empty() => return Err(protocol()),
            _ => (),
        }
    }
    if !pending.is_empty() {
        return Err(protocol());
    }
    Ok(bytes)
}

struct Block {
    kind: ModelContent,
    json: String,
    closed: bool,
    valid_json: bool,
}

struct ValidatedStream {
    source: Option<ModelStream>,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    blocks: BTreeMap<usize, Block>,
    tools: HashSet<String>,
    ids: HashSet<String>,
    scope: String,
    remaining: usize,
    response: Option<ModelResponse>,
    stopped: bool,
}

impl ValidatedStream {
    fn charge(&mut self, size: usize) -> Result<()> {
        if size > self.remaining {
            return Err(Error::Busy("pipeline conversation context"));
        }
        self.remaining -= size;
        Ok(())
    }

    fn accept(&mut self, event: &ModelEvent) -> Result<()> {
        let start = match event {
            ModelEvent::TextStart { index } => Some((*index, ModelContent::Text(String::new()))),
            ModelEvent::ReasoningStart { index } => {
                Some((*index, ModelContent::ReasoningSummary(String::new())))
            }
            ModelEvent::ToolStart {
                index,
                call_id,
                name,
            } => {
                if call_id.is_empty()
                    || call_id.len() > 256
                    || name.is_empty()
                    || name.len() > 128
                    || !self.tools.contains(name)
                    || self.ids.len() >= 32
                    || !self.ids.insert(call_id.clone())
                {
                    return Err(protocol());
                }
                self.charge(call_id.len().saturating_add(name.len()))?;
                Some((
                    *index,
                    ModelContent::ToolCall(ToolCall {
                        call_id: call_id.clone(),
                        name: name.clone(),
                        arguments: serde_json::Value::Null,
                    }),
                ))
            }
            _ => None,
        };
        if let Some((index, kind)) = start {
            if self.blocks.len() >= MAX_BLOCKS {
                return Err(Error::Busy("model content blocks"));
            }
            if self.blocks.contains_key(&index) {
                return Err(protocol());
            }
            self.blocks.insert(
                index,
                Block {
                    kind,
                    json: String::new(),
                    closed: false,
                    valid_json: true,
                },
            );
            return Ok(());
        }
        let delta = match event {
            ModelEvent::TextDelta { index, text } | ModelEvent::ReasoningDelta { index, text } => {
                Some((*index, text.as_str()))
            }
            ModelEvent::ToolArgumentsDelta { index, json } => Some((*index, json.as_str())),
            _ => None,
        };
        if let Some((index, delta)) = delta {
            self.charge(delta.len())?;
            let block = self.blocks.get_mut(&index).ok_or_else(protocol)?;
            if block.closed {
                return Err(protocol());
            }
            match (&mut block.kind, event) {
                (ModelContent::Text(s), ModelEvent::TextDelta { .. })
                | (ModelContent::ReasoningSummary(s), ModelEvent::ReasoningDelta { .. }) => {
                    s.push_str(delta)
                }
                (ModelContent::ToolCall(_), ModelEvent::ToolArgumentsDelta { .. }) => {
                    block.json.push_str(delta)
                }
                _ => return Err(protocol()),
            }
            return Ok(());
        }
        match event {
            ModelEvent::ContentEnd { index } => {
                let block = self.blocks.get_mut(index).ok_or_else(protocol)?;
                if block.closed {
                    return Err(protocol());
                }
                block.closed = true;
                if let ModelContent::ToolCall(call) = &mut block.kind {
                    let raw_size = block.json.len();
                    match serde_json::from_str::<serde_json::Value>(&block.json) {
                        Ok(value) if value.is_object() => {
                            let size = json_size(&value);
                            if size > raw_size && size - raw_size > self.remaining {
                                return Err(Error::Busy("pipeline conversation context"));
                            }
                            self.remaining =
                                self.remaining.saturating_add(raw_size).saturating_sub(size);
                            call.arguments = value;
                        }
                        _ => {
                            self.remaining = self.remaining.saturating_add(raw_size);
                            block.valid_json = false;
                        }
                    }
                    block.json = String::new();
                }
            }
            ModelEvent::Finished {
                reason,
                usage,
                continuation,
            } => {
                if self.blocks.values().any(|b| !b.closed) {
                    return Err(protocol());
                }
                if let Some(c) = continuation {
                    if self.scope.is_empty() || c.source != self.scope {
                        return Err(protocol());
                    }
                    self.charge(continuation_size(c))?;
                }
                let has_tools = !self.ids.is_empty();
                if (*reason == FinishReason::ToolCalls && !has_tools)
                    || (matches!(reason, FinishReason::Complete | FinishReason::Refused)
                        && has_tools)
                    || (*reason != FinishReason::Truncated
                        && self.blocks.values().any(|b| !b.valid_json))
                {
                    return Err(protocol());
                }
                let content = std::mem::take(&mut self.blocks)
                    .into_values()
                    .filter(|b| b.valid_json)
                    .map(|b| b.kind)
                    .collect();
                self.response = Some(ModelResponse {
                    message: AssistantMessage {
                        content,
                        continuation: continuation.clone(),
                    },
                    finish_reason: *reason,
                    usage: usage.clone(),
                });
            }
            _ => return Err(protocol()),
        }
        Ok(())
    }
}

impl futures_core::Stream for ValidatedStream {
    type Item = Result<ModelEvent>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.stopped {
            return Poll::Ready(None);
        }
        if self.cancelled.as_mut().poll(cx).is_ready() {
            self.stopped = true;
            self.source = None;
            return Poll::Ready(Some(Err(Error::Cancelled)));
        }
        let next = self
            .source
            .as_mut()
            .expect("active stream")
            .as_mut()
            .poll_next(cx);
        let event = match next {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(Ok(event))) => event,
            Poll::Ready(Some(Err(error))) => {
                self.stopped = true;
                self.source = None;
                return Poll::Ready(Some(Err(error)));
            }
            Poll::Ready(None) => {
                self.stopped = true;
                self.source = None;
                return Poll::Ready(Some(Err(protocol())));
            }
        };
        if let Err(error) = self.accept(&event) {
            self.stopped = true;
            self.source = None;
            return Poll::Ready(Some(Err(error)));
        }
        if matches!(event, ModelEvent::Finished { .. }) {
            // Detect already buffered trailing events without waiting for an open connection.
            let trailing = self
                .source
                .as_mut()
                .expect("active stream")
                .as_mut()
                .poll_next(cx);
            self.stopped = true;
            self.source = None;
            if matches!(trailing, Poll::Ready(Some(_))) {
                return Poll::Ready(Some(Err(protocol())));
            }
        }
        Poll::Ready(Some(Ok(event)))
    }
}

async fn open(
    adapter: &dyn TextModel,
    request: ModelRequest,
    max_messages: usize,
    limit: usize,
    cancel: CancellationToken,
) -> Result<ValidatedStream> {
    let bytes = validate_request(&request, adapter.continuation_source(), max_messages, limit)?;
    let tools = request.tools.iter().map(|t| t.name.clone()).collect();
    let source = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(Error::Cancelled),
        result = adapter.generate_stream(request, cancel.clone()) => result?,
    };
    Ok(ValidatedStream {
        source: Some(source),
        cancelled: Box::pin(cancel.cancelled_owned()),
        blocks: BTreeMap::new(),
        tools,
        ids: HashSet::new(),
        scope: adapter.continuation_source().to_owned(),
        remaining: limit - bytes,
        response: None,
        stopped: false,
    })
}

/// Stream provisional events through the same state machine as `generate`.
/// The context budget includes tools, instructions, continuation and output.
pub async fn generate_stream(
    adapter: &dyn TextModel,
    request: ModelRequest,
    max_messages: usize,
    max_context_bytes: usize,
    cancel: CancellationToken,
) -> Result<ModelStream> {
    Ok(Box::pin(
        open(adapter, request, max_messages, max_context_bytes, cancel).await?,
    ))
}

/// Collect from the validator's own assembly; no second response accumulator.
pub async fn generate(
    adapter: &dyn TextModel,
    request: ModelRequest,
    max_messages: usize,
    max_context_bytes: usize,
    cancel: CancellationToken,
) -> Result<ModelResponse> {
    let mut stream = open(adapter, request, max_messages, max_context_bytes, cancel).await?;
    while let Some(event) = poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)).await {
        event?;
    }
    stream.response.take().ok_or_else(protocol)
}

pub(crate) fn validate_text(text: &str, limit: usize) -> Result<()> {
    if text.len() > limit {
        return Err(Error::Busy("pipeline text"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
