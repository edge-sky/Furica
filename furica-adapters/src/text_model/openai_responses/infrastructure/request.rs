//! Translate the Core application input to this provider's request.
use super::OpenAiResponses;
use super::continuation::project;
use crate::infrastructure::text::{self, bad, bounded, data_message, effort};
use furica_core::{Error, Result, text_model::*};
use serde_json::{Value, json};

impl OpenAiResponses {
    pub(super) fn encode(&self, request: &ModelRequest) -> Result<Value> {
        text::check_request(request, &self.http.scope)?;
        let mut input = Vec::new();
        for message in &request.messages {
            if let Some(data) = data_message(message) {
                input.push(json!({"role":"user","content":data}));
                continue;
            }
            match message {
                ModelMessage::Assistant(m) => {
                    if let Some(c) = &m.continuation {
                        bounded(&c.data)?;
                        let items = c.data.as_array().ok_or_else(bad)?;
                        if project(items)? != m.content { return Err(bad()); }
                        input.extend(items.iter().cloned());
                    } else {
                        for part in &m.content {
                            match part {
                                ModelContent::Text(t) => input.push(json!({"role":"assistant","content":[{"type":"output_text","text":t,"annotations":[]}]})),
                                ModelContent::ToolCall(c) => input.push(json!({"type":"function_call","call_id":c.call_id,"name":c.name,"arguments":c.arguments.to_string()})),
                                ModelContent::ReasoningSummary(_) => return Err(Error::Config("reasoning summary requires native continuation".into())),
                            }
                        }
                    }
                }
                ModelMessage::ToolResult { call_id, result } => input.push(json!({"type":"function_call_output","call_id":call_id,"output":result.to_string()})),
                _ => (),
            }
        }
        let tools: Vec<Value> = request.tools.iter().map(|t| {
            let mut v = json!({"type":"function","name":t.name,"parameters":t.parameters,"strict":false});
            if let Some(d) = &t.description { v["description"] = json!(d); } v
        }).collect();
        let mut body = json!({"model":self.http.model,"input":input,"stream":true,"store":false,"max_output_tokens":request.max_output_tokens,"include":["reasoning.encrypted_content"]});
        if let Some(i) = &request.instructions {
            body["instructions"] = json!(i);
        }
        if !tools.is_empty() {
            body["tools"] = json!(tools);
        }
        if let Some(r) = request.reasoning {
            if !self.reasoning {
                return Err(Error::Config(
                    "reasoning not enabled for this Responses model".into(),
                ));
            }
            body["reasoning"] = json!({"effort":effort(r)?,"summary":"auto"});
        }
        bounded(&body)?;
        Ok(body)
    }
}
