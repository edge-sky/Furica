//! Translate the Core application input to this provider's request.
use super::Anthropic;
use super::continuation::project;
use crate::infrastructure::text::{self, bad, bounded, data_message, effort};
use furica_core::{Error, Result, text_model::*};
use serde_json::{Value, json};

impl Anthropic {
    pub(super) fn encode(&self, request: &ModelRequest) -> Result<Value> {
        text::check_request(request, &self.http.scope)?;
        let mut messages: Vec<Value> = Vec::new();
        for message in &request.messages {
            let (role, content) = if let Some(data) = data_message(message) {
                ("user", vec![json!({"type":"text","text":data})])
            } else {
                match message {
                    ModelMessage::Assistant(m) => {
                        let blocks = if let Some(c) = &m.continuation {
                            bounded(&c.data)?;
                            let blocks = c.data.as_array().ok_or_else(bad)?;
                            if project(blocks)? != m.content {
                                return Err(bad());
                            }
                            blocks.clone()
                        } else {
                            m.content.iter().map(|p| match p {
                                ModelContent::Text(t) => Ok(json!({"type":"text","text":t})),
                                ModelContent::ToolCall(c) => Ok(json!({"type":"tool_use","id":c.call_id,"name":c.name,"input":c.arguments})),
                                ModelContent::ReasoningSummary(_) => Err(Error::Config("thinking summary requires signed continuation".into())),
                            }).collect::<Result<Vec<_>>>()?
                        };
                        ("assistant", blocks)
                    }
                    ModelMessage::ToolResult { call_id, result } => (
                        "user",
                        vec![
                            json!({"type":"tool_result","tool_use_id":call_id,"content":result.to_string()}),
                        ],
                    ),
                    _ => return Err(bad()),
                }
            };
            // Consecutive tool results must remain a single user message. Results
            // precede task/user data within that message as required by Messages.
            if let Some(last) = messages.last_mut().filter(|v| v["role"] == role) {
                let parts = last["content"].as_array_mut().expect("constructed content");
                for block in content {
                    if block["type"] == "tool_result" {
                        let at = parts
                            .iter()
                            .position(|p| p["type"] != "tool_result")
                            .unwrap_or(parts.len());
                        parts.insert(at, block);
                    } else {
                        parts.push(block);
                    }
                }
            } else {
                messages.push(json!({"role":role,"content":content}));
            }
        }
        let tools: Vec<Value> = request
            .tools
            .iter()
            .map(|t| {
                let mut v = json!({"name":t.name,"input_schema":t.parameters});
                if let Some(d) = &t.description {
                    v["description"] = json!(d);
                }
                v
            })
            .collect();
        let mut body = json!({"model":self.http.model,"messages":messages,"stream":true,"max_tokens":request.max_output_tokens});
        if let Some(i) = &request.instructions {
            body["system"] = json!(i);
        }
        if !tools.is_empty() {
            body["tools"] = json!(tools);
        }
        if let Some(r) = request.reasoning {
            match r {
                Reasoning::BudgetTokens(n)
                    if self.budget && n >= 1024 && n < request.max_output_tokens =>
                {
                    body["thinking"] = json!({"type":"enabled","budget_tokens":n})
                }
                Reasoning::Low | Reasoning::Medium | Reasoning::High if self.adaptive => {
                    body["thinking"] = json!({"type":"adaptive"});
                    body["output_config"] = json!({"effort":effort(r)?});
                }
                _ => {
                    return Err(Error::Config(
                        "unsupported Anthropic reasoning control or budget".into(),
                    ));
                }
            }
        }
        bounded(&body)?;
        Ok(body)
    }
}
