//! Request-local protocol lifecycle, bounded buffering and Core event conversion.
use super::continuation::project;
use crate::infrastructure::text::{self, Sse, bad, push, string};
use furica_core::{Error, Result, text_model::*};
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};

pub(super) fn open(sse: Sse, scope: String) -> ModelStream {
    let state = State {
        sse,
        scope,
        queue: VecDeque::new(),
        blocks: BTreeMap::new(),
        started: false,
        finish: None,
        usage: None,
        done: false,
    };
    Box::pin(futures_util::stream::try_unfold(
        state,
        |mut s| async move {
            loop {
                if let Some(e) = s.queue.pop_front() {
                    return Ok(Some((e, s)));
                }
                if s.done {
                    return Ok(None);
                }
                s.read().await?;
            }
        },
    ))
}

struct Block {
    raw: Value,
    args: String,
    closed: bool,
    partial: bool,
}
struct State {
    sse: Sse,
    scope: String,
    queue: VecDeque<ModelEvent>,
    blocks: BTreeMap<usize, Block>,
    started: bool,
    finish: Option<FinishReason>,
    usage: Option<ModelUsage>,
    done: bool,
}
impl State {
    fn usage(&mut self, v: &Value) {
        if !v.is_object() {
            return;
        }
        let u = self.usage.get_or_insert_with(ModelUsage::default);
        if let Some(n) = v["input_tokens"].as_u64() {
            u.input_tokens = Some(n);
        }
        if let Some(n) = v["output_tokens"].as_u64() {
            u.output_tokens = Some(n);
        }
        if let Some(n) = v["cache_read_input_tokens"].as_u64() {
            u.cached_input_tokens = Some(n);
        }
        if let Some(n) = v["output_tokens_details"]["thinking_tokens"].as_u64() {
            u.reasoning_tokens = Some(n);
        }
    }
    async fn read(&mut self) -> Result<()> {
        let v = self.sse.json().await?;
        let retained = self
            .blocks
            .values()
            .map(|b| text::value_size(&b.raw).saturating_add(b.args.len()))
            .sum::<usize>();
        let growth = match v["type"].as_str() {
            Some("content_block_start") => text::value_size(&v["content_block"]),
            Some("content_block_delta") => {
                let d = &v["delta"];
                let field = match d["type"].as_str() {
                    Some("text_delta") => "text",
                    Some("thinking_delta") => "thinking",
                    Some("signature_delta") => "signature",
                    Some("input_json_delta") => "partial_json",
                    _ => return Err(bad()),
                };
                // JSON strings may need escaping when retained in native blocks.
                if field == "partial_json" {
                    string(d, field)?.len()
                } else {
                    text::value_size(&d[field]).saturating_sub(2)
                }
            }
            _ => 0,
        };
        if growth > text::LIMIT.saturating_sub(retained) {
            return Err(Error::Busy("Anthropic retained blocks"));
        }
        match string(&v, "type")? {
            "ping" => (),
            "message_start" => {
                if self.started
                    || v["message"]["role"] != "assistant"
                    || v["message"]["content"]
                        .as_array()
                        .is_none_or(|a| !a.is_empty())
                {
                    return Err(bad());
                }
                self.started = true;
                self.usage(&v["message"]["usage"]);
            }
            "content_block_start" => {
                if !self.started || self.finish.is_some() {
                    return Err(bad());
                }
                let i = text::index(&v, "index")?;
                if i != self.blocks.len() || self.blocks.values().any(|b| !b.closed) {
                    return Err(bad());
                }
                let raw = v["content_block"].clone();
                match string(&raw, "type")? {
                    "text" => {
                        self.queue.push_back(ModelEvent::TextStart { index: i });
                        let t = string(&raw, "text")?;
                        if !t.is_empty() {
                            self.queue.push_back(ModelEvent::TextDelta {
                                index: i,
                                text: t.into(),
                            });
                        }
                    }
                    "thinking" => {
                        self.queue
                            .push_back(ModelEvent::ReasoningStart { index: i });
                        let t = string(&raw, "thinking")?;
                        if !t.is_empty() {
                            self.queue.push_back(ModelEvent::ReasoningDelta {
                                index: i,
                                text: t.into(),
                            });
                        }
                    }
                    "redacted_thinking" => {
                        string(&raw, "data")?;
                    }
                    "tool_use" => {
                        if !raw["input"].is_object() {
                            return Err(bad());
                        }
                        self.queue.push_back(ModelEvent::ToolStart {
                            index: i,
                            call_id: string(&raw, "id")?.into(),
                            name: string(&raw, "name")?.into(),
                        });
                    }
                    _ => return Err(bad()),
                }
                self.blocks.insert(
                    i,
                    Block {
                        raw,
                        args: String::new(),
                        closed: false,
                        partial: false,
                    },
                );
            }
            "content_block_delta" => {
                let i = text::index(&v, "index")?;
                let b = self.blocks.get_mut(&i).ok_or_else(bad)?;
                if b.closed {
                    return Err(bad());
                }
                let d = &v["delta"];
                match (string(&b.raw, "type")?, string(d, "type")?) {
                    ("text", "text_delta") | ("thinking", "thinking_delta") => {
                        let reasoning = b.raw["type"] == "thinking";
                        let field = if reasoning { "thinking" } else { "text" };
                        let delta = string(d, field)?;
                        let Value::String(text) = &mut b.raw[field] else {
                            return Err(bad());
                        };
                        push(text, delta)?;
                        self.queue.push_back(if reasoning {
                            ModelEvent::ReasoningDelta {
                                index: i,
                                text: delta.into(),
                            }
                        } else {
                            ModelEvent::TextDelta {
                                index: i,
                                text: delta.into(),
                            }
                        });
                    }
                    ("thinking", "signature_delta") => {
                        let signature = b
                            .raw
                            .as_object_mut()
                            .ok_or_else(bad)?
                            .entry("signature")
                            .or_insert_with(|| json!(""));
                        let Value::String(signature) = signature else {
                            return Err(bad());
                        };
                        push(signature, string(d, "signature")?)?;
                    }
                    ("tool_use", "input_json_delta") => {
                        let delta = string(d, "partial_json")?;
                        b.partial = true;
                        push(&mut b.args, delta)?;
                        self.queue.push_back(ModelEvent::ToolArgumentsDelta {
                            index: i,
                            json: delta.into(),
                        });
                    }
                    _ => return Err(bad()),
                }
            }
            "content_block_stop" => {
                let i = text::index(&v, "index")?;
                let b = self.blocks.get_mut(&i).ok_or_else(bad)?;
                if b.closed {
                    return Err(bad());
                }
                b.closed = true;
                match string(&b.raw, "type")? {
                    "tool_use" => {
                        if b.partial {
                            // Core alone decides whether incomplete arguments can be discarded on truncation.
                            if let Ok(value) = serde_json::from_str::<Value>(&b.args) {
                                b.raw["input"] = value;
                            } else {
                                b.raw["input"] = Value::Null;
                            }
                        } else {
                            self.queue.push_back(ModelEvent::ToolArgumentsDelta {
                                index: i,
                                json: b.raw["input"].to_string(),
                            });
                        }
                        b.args = String::new();
                        self.queue.push_back(ModelEvent::ContentEnd { index: i });
                    }
                    "thinking" => {
                        self.queue.push_back(ModelEvent::ContentEnd { index: i });
                    }
                    "text" => self.queue.push_back(ModelEvent::ContentEnd { index: i }),
                    "redacted_thinking" => (),
                    _ => return Err(bad()),
                }
            }
            "message_delta" => {
                if !self.started || self.blocks.values().any(|b| !b.closed) {
                    return Err(bad());
                }
                if let Some(reason) = v["delta"].get("stop_reason").filter(|v| !v.is_null()) {
                    if self.finish.is_some() {
                        return Err(bad());
                    }
                    self.finish = Some(match reason.as_str().ok_or_else(bad)? {
                        "end_turn" | "stop_sequence" => FinishReason::Complete,
                        "tool_use" => FinishReason::ToolCalls,
                        "max_tokens" => FinishReason::Truncated,
                        "refusal" => FinishReason::Refused,
                        _ => return Err(bad()),
                    });
                }
                self.usage(&v["usage"]);
            }
            "message_stop" => {
                let reason = self.finish.ok_or_else(bad)?;
                if !self.started || self.blocks.values().any(|b| !b.closed) {
                    return Err(bad());
                }
                let raw: Vec<Value> = std::mem::take(&mut self.blocks)
                    .into_values()
                    .map(|b| b.raw)
                    .collect();
                if reason != FinishReason::Truncated {
                    project(&raw)?;
                }
                self.queue.push_back(ModelEvent::Finished {
                    reason,
                    usage: self.usage.take(),
                    continuation: Some(ModelContinuation {
                        source: self.scope.clone(),
                        data: json!(raw),
                    }),
                });
                self.done = true;
            }
            _ => return Err(bad()),
        }
        let retained = self
            .blocks
            .values()
            .map(|b| text::value_size(&b.raw).saturating_add(b.args.len()))
            .sum::<usize>();
        if retained > text::LIMIT {
            return Err(Error::Busy("Anthropic retained blocks"));
        }
        Ok(())
    }
}
