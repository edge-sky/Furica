//! Request-local protocol lifecycle, bounded buffering and Core event conversion.
use super::continuation::project;
use crate::infrastructure::text::{self, Sse, bad, bounded, push, string};
use furica_core::{Error, Result, text_model::*};
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};

pub(super) fn open(sse: Sse, scope: String) -> ModelStream {
    let state = State {
        sse,
        scope,
        items: BTreeMap::new(),
        queue: VecDeque::new(),
        done: false,
        refused: false,
        started: false,
        retained: 0,
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

struct Part {
    text: String,
    reasoning: bool,
    refusal: bool,
}
struct Item {
    raw: Value,
    parts: BTreeMap<usize, Part>,
    args: String,
    closed: bool,
}
struct State {
    sse: Sse,
    scope: String,
    items: BTreeMap<usize, Item>,
    queue: VecDeque<ModelEvent>,
    done: bool,
    refused: bool,
    started: bool,
    retained: usize,
}
impl State {
    async fn read(&mut self) -> Result<()> {
        let v = self.sse.json().await?;
        let kind = string(&v, "type")?;
        let growth = match kind {
            "response.output_item.added" => text::value_size(&v["item"]),
            "response.content_part.added" | "response.reasoning_summary_part.added" => {
                v["part"]["text"]
                    .as_str()
                    .or_else(|| v["part"]["refusal"].as_str())
                    .map_or(0, str::len)
            }
            "response.output_text.delta"
            | "response.refusal.delta"
            | "response.reasoning_summary_text.delta"
            | "response.function_call_arguments.delta" => string(&v, "delta")?.len(),
            "response.output_item.done" => {
                let i = text::index(&v, "output_index")?;
                let item = self.items.get(&i).ok_or_else(bad)?;
                let old = text::value_size(&item.raw)
                    .saturating_add(item.args.len())
                    .saturating_add(item.parts.values().map(|p| p.text.len()).sum::<usize>());
                text::value_size(&v["item"]).saturating_sub(old)
            }
            _ => 0,
        };
        if growth > text::LIMIT.saturating_sub(self.retained) {
            return Err(Error::Busy("Responses retained items"));
        }
        match kind {
            "response.created" => {
                if self.started {
                    return Err(bad());
                }
                self.started = true;
            }
            "response.in_progress" => {
                if !self.started {
                    return Err(bad());
                }
            }
            "response.output_item.added" => {
                if !self.started {
                    return Err(bad());
                }
                let i = text::index(&v, "output_index")?;
                if self.items.contains_key(&i) {
                    return Err(bad());
                }
                let raw = v["item"].clone();
                bounded(&raw)?;
                if string(&raw, "id")?.is_empty() {
                    return Err(bad());
                }
                match string(&raw, "type")? {
                    "function_call" => {
                        self.queue.push_back(ModelEvent::ToolStart {
                            index: i * 256,
                            call_id: string(&raw, "call_id")?.into(),
                            name: string(&raw, "name")?.into(),
                        });
                        if raw["arguments"].as_str() != Some("") {
                            return Err(bad());
                        }
                    }
                    "message" => {
                        if raw["role"] != "assistant" {
                            return Err(bad());
                        }
                    }
                    "reasoning" => (),
                    _ => return Err(bad()),
                }
                self.items.insert(
                    i,
                    Item {
                        raw,
                        parts: BTreeMap::new(),
                        args: String::new(),
                        closed: false,
                    },
                );
            }
            "response.content_part.added" | "response.reasoning_summary_part.added" => {
                let i = text::index(&v, "output_index")?;
                let p = text::index(
                    &v,
                    if kind.contains("summary") {
                        "summary_index"
                    } else {
                        "content_index"
                    },
                )?;
                let item = self.items.get_mut(&i).ok_or_else(bad)?;
                if item.closed || item.parts.contains_key(&p) {
                    return Err(bad());
                }
                let part = &v["part"];
                let reasoning = kind.contains("summary");
                let (field, refusal) = match string(part, "type")? {
                    "summary_text" if reasoning && item.raw["type"] == "reasoning" => {
                        ("text", false)
                    }
                    "output_text" if !reasoning && item.raw["type"] == "message" => ("text", false),
                    "refusal" if !reasoning && item.raw["type"] == "message" => ("refusal", true),
                    _ => return Err(bad()),
                };
                let text = string(part, field)?.to_owned();
                self.refused |= refusal;
                self.queue.push_back(if reasoning {
                    ModelEvent::ReasoningStart { index: i * 256 + p }
                } else {
                    ModelEvent::TextStart { index: i * 256 + p }
                });
                if !text.is_empty() {
                    self.queue.push_back(if reasoning {
                        ModelEvent::ReasoningDelta {
                            index: i * 256 + p,
                            text: text.clone(),
                        }
                    } else {
                        ModelEvent::TextDelta {
                            index: i * 256 + p,
                            text: text.clone(),
                        }
                    });
                }
                item.parts.insert(
                    p,
                    Part {
                        text,
                        reasoning,
                        refusal,
                    },
                );
            }
            "response.output_text.delta"
            | "response.refusal.delta"
            | "response.reasoning_summary_text.delta" => {
                let i = text::index(&v, "output_index")?;
                let p = text::index(
                    &v,
                    if kind.contains("summary") {
                        "summary_index"
                    } else {
                        "content_index"
                    },
                )?;
                let item = self.items.get_mut(&i).ok_or_else(bad)?;
                if item.closed {
                    return Err(bad());
                }
                let part = item.parts.get_mut(&p).ok_or_else(bad)?;
                if part.reasoning != kind.contains("summary")
                    || part.refusal != kind.contains("refusal")
                {
                    return Err(bad());
                }
                let delta = string(&v, "delta")?;
                push(&mut part.text, delta)?;
                self.queue.push_back(if part.reasoning {
                    ModelEvent::ReasoningDelta {
                        index: i * 256 + p,
                        text: delta.into(),
                    }
                } else {
                    ModelEvent::TextDelta {
                        index: i * 256 + p,
                        text: delta.into(),
                    }
                });
            }
            "response.function_call_arguments.delta" => {
                let i = text::index(&v, "output_index")?;
                let item = self.items.get_mut(&i).ok_or_else(bad)?;
                if item.closed || item.raw["type"] != "function_call" {
                    return Err(bad());
                }
                let delta = string(&v, "delta")?;
                push(&mut item.args, delta)?;
                self.queue.push_back(ModelEvent::ToolArgumentsDelta {
                    index: i * 256,
                    json: delta.into(),
                });
            }
            "response.output_item.done" => {
                let i = text::index(&v, "output_index")?;
                let item = self.items.get_mut(&i).ok_or_else(bad)?;
                if item.closed {
                    return Err(bad());
                }
                let raw = &v["item"];
                if raw["type"] != item.raw["type"] || raw["id"] != item.raw["id"] {
                    return Err(bad());
                }
                if raw["type"] == "function_call" {
                    if raw["name"] != item.raw["name"]
                        || raw["call_id"] != item.raw["call_id"]
                        || raw["arguments"].as_str() != Some(item.args.as_str())
                    {
                        return Err(bad());
                    }
                    self.queue
                        .push_back(ModelEvent::ContentEnd { index: i * 256 });
                } else {
                    if !item.parts.keys().copied().eq(0..item.parts.len()) {
                        return Err(bad());
                    }
                    let projected = project(std::slice::from_ref(raw))?;
                    if projected.len() != item.parts.len() {
                        return Err(bad());
                    }
                    for ((p, part), content) in item.parts.iter().zip(projected) {
                        let expected = if part.reasoning {
                            ModelContent::ReasoningSummary(part.text.clone())
                        } else {
                            ModelContent::Text(part.text.clone())
                        };
                        if expected != content {
                            return Err(bad());
                        }
                        self.queue
                            .push_back(ModelEvent::ContentEnd { index: i * 256 + p });
                    }
                }
                item.raw = raw.clone();
                item.closed = true;
                item.parts.clear();
                item.args.clear();
            }
            "response.completed" | "response.incomplete" => {
                let response = &v["response"];
                if !self.started || self.items.values().any(|i| !i.closed) {
                    return Err(bad());
                }
                if !self.items.keys().copied().eq(0..self.items.len()) {
                    return Err(bad());
                }
                let output = response["output"].as_array().ok_or_else(bad)?;
                if !output.iter().eq(self.items.values().map(|i| &i.raw)) {
                    return Err(bad());
                }
                let has_tools = self
                    .items
                    .values()
                    .any(|i| i.raw["type"] == "function_call");
                let reason = match (kind, response["status"].as_str()) {
                    ("response.completed", Some("completed")) => {
                        if self.refused {
                            FinishReason::Refused
                        } else if has_tools {
                            FinishReason::ToolCalls
                        } else {
                            FinishReason::Complete
                        }
                    }
                    ("response.incomplete", Some("incomplete")) => {
                        match response["incomplete_details"]["reason"].as_str() {
                            Some("max_output_tokens") => FinishReason::Truncated,
                            Some("content_filter") => FinishReason::Refused,
                            _ => return Err(bad()),
                        }
                    }
                    _ => return Err(bad()),
                };
                let usage = response
                    .get("usage")
                    .filter(|u| !u.is_null())
                    .map(|u| ModelUsage {
                        input_tokens: u["input_tokens"].as_u64(),
                        output_tokens: u["output_tokens"].as_u64(),
                        reasoning_tokens: u["output_tokens_details"]["reasoning_tokens"].as_u64(),
                        cached_input_tokens: u["input_tokens_details"]["cached_tokens"].as_u64(),
                    });
                let raw: Vec<Value> = std::mem::take(&mut self.items)
                    .into_values()
                    .map(|i| i.raw)
                    .collect();
                self.queue.push_back(ModelEvent::Finished {
                    reason,
                    usage,
                    continuation: Some(ModelContinuation {
                        source: self.scope.clone(),
                        data: json!(raw),
                    }),
                });
                self.done = true;
            }
            "response.output_text.done"
            | "response.refusal.done"
            | "response.function_call_arguments.done"
            | "response.reasoning_summary_text.done"
            | "response.reasoning_summary_part.done"
            | "response.content_part.done"
            | "response.output_text.annotation.added" => (),
            _ => return Err(bad()),
        }
        // Includes both native snapshots and in-flight content, before further reads.
        self.retained = self
            .items
            .values()
            .map(|i| {
                text::value_size(&i.raw)
                    .saturating_add(i.args.len())
                    .saturating_add(i.parts.values().map(|p| p.text.len()).sum::<usize>())
            })
            .sum();
        if self.retained > text::LIMIT {
            return Err(Error::Busy("Responses retained items"));
        }
        Ok(())
    }
}
