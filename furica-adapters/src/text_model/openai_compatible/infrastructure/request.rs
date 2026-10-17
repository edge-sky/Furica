//! Translate the Core application input to this provider's request.
use super::OpenAiCompatible;
use crate::infrastructure::text::{self, bad, bounded, data_message, effort};
use furica_core::{Error, Result, text_model::*};
use serde_json::{Value, json};

impl OpenAiCompatible {
    pub(super) fn encode(&self, request: &ModelRequest) -> Result<Value> {
        text::check_request(request, &self.http.scope)?;
        let mut messages = Vec::new();
        if let Some(instructions) = &request.instructions {
            messages.push(json!({"role":"system", "content":instructions}));
        }
        for message in &request.messages {
            if let Some(data) = data_message(message) {
                messages.push(json!({"role":"user","content":data}));
                continue;
            }
            match message {
                ModelMessage::Assistant(m) => {
                    if m.continuation.is_some()
                        || m.content
                            .iter()
                            .any(|c| matches!(c, ModelContent::ReasoningSummary(_)))
                    {
                        return Err(bad());
                    }
                    let calls: Vec<Value> = m.tool_calls().map(|c| json!({"id":c.call_id,"type":"function","function":{"name":c.name,"arguments":c.arguments.to_string()}})).collect();
                    let mut v = json!({"role":"assistant","content":m.text()});
                    if !calls.is_empty() {
                        v["tool_calls"] = json!(calls);
                    }
                    messages.push(v);
                }
                ModelMessage::ToolResult { call_id, result } => messages.push(
                    json!({"role":"tool","tool_call_id":call_id,"content":result.to_string()}),
                ),
                _ => (),
            }
        }
        let tools: Vec<Value> = request
            .tools
            .iter()
            .map(|t| {
                let mut f = json!({"name":t.name,"parameters":t.parameters,"strict":false});
                if let Some(d) = &t.description {
                    f["description"] = json!(d);
                }
                json!({"type":"function","function":f})
            })
            .collect();
        let mut body = json!({"model":self.http.model,"messages":messages,"stream":true,"stream_options":{"include_usage":true}});
        body[&self.token_field] = json!(request.max_output_tokens);
        if !tools.is_empty() {
            body["tools"] = json!(tools);
        }
        if let Some(r) = request.reasoning {
            if !self.reasoning {
                return Err(Error::Config(
                    "reasoning effort not enabled for this Chat model".into(),
                ));
            }
            body["reasoning_effort"] = json!(effort(r)?);
        }
        bounded(&body)?;
        Ok(body)
    }
}
