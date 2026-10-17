//! Request-local protocol lifecycle, bounded buffering and Core event conversion.
use crate::infrastructure::text::{self, Sse, bad};
use furica_core::{Error, Result, text_model::*};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};

pub(super) fn open(sse: Sse) -> ModelStream {
    let state = State {
        sse,
        queue: VecDeque::new(),
        tools: BTreeMap::new(),
        text: None,
        next: 0,
        finish: None,
        usage: None,
        refused: false,
        done: false,
        retained: 0,
        arguments: VecDeque::new(),
    };
    Box::pin(futures_util::stream::try_unfold(
        state,
        |mut s| async move {
            loop {
                if let Some(event) = s.queue.pop_front() {
                    return Ok(Some((event, s)));
                }
                if s.done {
                    return Ok(None);
                }
                s.read().await?;
            }
        },
    ))
}

struct Tool {
    index: usize,
    id: String,
    name: String,
}
struct State {
    sse: Sse,
    queue: VecDeque<ModelEvent>,
    tools: BTreeMap<usize, Tool>,
    text: Option<usize>,
    next: usize,
    finish: Option<FinishReason>,
    usage: Option<ModelUsage>,
    refused: bool,
    done: bool,
    retained: usize,
    arguments: VecDeque<(usize, String)>,
}
impl State {
    fn allocate(&mut self) -> Result<usize> {
        if self.next >= 256 {
            return Err(Error::Busy("model content blocks"));
        }
        let i = self.next;
        self.next += 1;
        Ok(i)
    }
    fn close_text(&mut self) {
        if let Some(index) = self.text.take() {
            self.queue.push_back(ModelEvent::ContentEnd { index });
        }
    }
    async fn read(&mut self) -> Result<()> {
        let (event, data) = self.sse.next().await?;
        if !event.is_empty() && event != "message" {
            return Err(bad());
        }
        if data == "[DONE]" {
            let mut reason = self.finish.ok_or_else(bad)?;
            if self.refused && reason == FinishReason::Complete {
                reason = FinishReason::Refused;
            }
            self.queue.push_back(ModelEvent::Finished {
                reason,
                usage: self.usage.take(),
                continuation: None,
            });
            self.done = true;
            return Ok(());
        }
        let v: Value = serde_json::from_str(&data).map_err(|_| bad())?;
        if v.get("error").is_some() {
            return Err(bad());
        }
        if let Some(u) = v.get("usage").filter(|u| !u.is_null()) {
            self.usage = Some(ModelUsage {
                input_tokens: u["prompt_tokens"].as_u64(),
                output_tokens: u["completion_tokens"].as_u64(),
                reasoning_tokens: u["completion_tokens_details"]["reasoning_tokens"].as_u64(),
                cached_input_tokens: u["prompt_tokens_details"]["cached_tokens"].as_u64(),
            });
        }
        let choices = v["choices"].as_array().ok_or_else(bad)?;
        if choices.is_empty() {
            if v.get("usage").is_none() {
                return Err(bad());
            }
            return Ok(());
        }
        if choices.len() != 1 || choices[0]["index"].as_u64() != Some(0) || self.finish.is_some() {
            return Err(bad());
        }
        let choice = &choices[0];
        let d = &choice["delta"];
        if !d.is_object() {
            return Err(bad());
        }
        if let Some(role) = d.get("role")
            && role != "assistant"
        {
            return Err(bad());
        }
        if d.get("reasoning_content").is_some_and(|v| !v.is_null())
            || d.get("audio").is_some_and(|v| !v.is_null())
            || d.get("function_call").is_some_and(|v| !v.is_null())
        {
            return Err(bad());
        }
        for field in ["content", "refusal"] {
            if let Some(value) = d.get(field).filter(|v| !v.is_null()) {
                let text = value.as_str().ok_or_else(bad)?;
                if field == "refusal" {
                    self.refused = true;
                }
                let index = if let Some(i) = self.text {
                    i
                } else {
                    let i = self.allocate()?;
                    self.text = Some(i);
                    self.queue.push_back(ModelEvent::TextStart { index: i });
                    i
                };
                self.queue.push_back(ModelEvent::TextDelta {
                    index,
                    text: text.into(),
                });
            }
        }
        if let Some(calls) = d.get("tool_calls").filter(|v| !v.is_null()) {
            for call in calls.as_array().ok_or_else(bad)? {
                self.close_text();
                let key = text::index(call, "index")?;
                if !self.tools.contains_key(&key) {
                    if self.tools.len() >= 32 {
                        return Err(Error::Busy("model tool batch"));
                    }
                    let index = self.allocate()?;
                    self.tools.insert(
                        key,
                        Tool {
                            index,
                            id: String::new(),
                            name: String::new(),
                        },
                    );
                }
                let t = self.tools.get_mut(&key).expect("inserted");
                if let Some(kind) = call.get("type")
                    && kind != "function"
                {
                    return Err(bad());
                }
                for (field, target) in [("id", &mut t.id), ("name", &mut t.name)] {
                    let value = if field == "id" {
                        call.get(field)
                    } else {
                        call["function"].get(field)
                    };
                    if let Some(value) = value {
                        let part = value.as_str().ok_or_else(bad)?;
                        let limit: usize = if field == "id" { 256 } else { 128 };
                        if part.len() > limit.saturating_sub(target.len()) {
                            return Err(bad());
                        }
                        target.push_str(part);
                    }
                }
                if let Some(args) = call["function"].get("arguments") {
                    let args = args.as_str().ok_or_else(bad)?;
                    if !args.is_empty() {
                        if args.len() > text::LIMIT.saturating_sub(self.retained)
                            || self.arguments.len() >= 4096
                        {
                            return Err(Error::Busy("Chat tool parameter buffer"));
                        }
                        self.retained += args.len();
                        self.arguments.push_back((key, args.into()));
                    }
                }
            }
        }
        if let Some(reason) = choice.get("finish_reason").filter(|v| !v.is_null()) {
            self.finish = Some(match reason.as_str().ok_or_else(bad)? {
                "stop" => FinishReason::Complete,
                "tool_calls" => FinishReason::ToolCalls,
                "length" => FinishReason::Truncated,
                "content_filter" => FinishReason::Refused,
                _ => return Err(bad()),
            });
            self.close_text();
            // Header fields may be fragmented alongside arguments. Only the
            // finish marker proves them stable; retain bounded argument fragments
            // and replay their interleaving after publishing complete headers.
            let mut positions: Vec<_> = self.tools.values().map(|t| t.index).collect();
            positions.sort_unstable();
            for (t, index) in self.tools.values_mut().zip(positions) {
                t.index = index;
                self.queue.push_back(ModelEvent::ToolStart {
                    index: t.index,
                    call_id: t.id.clone(),
                    name: t.name.clone(),
                });
            }
            for (key, json) in self.arguments.drain(..) {
                let index = self.tools[&key].index;
                self.queue
                    .push_back(ModelEvent::ToolArgumentsDelta { index, json });
            }
            for (_, t) in std::mem::take(&mut self.tools) {
                self.queue
                    .push_back(ModelEvent::ContentEnd { index: t.index });
            }
        }
        Ok(())
    }
}
