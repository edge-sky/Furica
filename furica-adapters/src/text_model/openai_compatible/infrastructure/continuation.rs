//! Chat reasoning is replayed only with its original source and matching public projection.
use crate::infrastructure::text::{bad, bounded};
use furica_core::{Result, text_model::*};
use serde_json::{Value, json};

pub(super) fn encode(message: &AssistantMessage) -> Result<Value> {
    let reasoning: String = message
        .content
        .iter()
        .filter_map(|part| match part {
            ModelContent::ReasoningSummary(text) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    if let Some(continuation) = &message.continuation {
        let native = &continuation.data;
        bounded(native)?;
        let object = native.as_object().ok_or_else(bad)?;
        if object.keys().any(|key| {
            !["role", "content", "reasoning_content", "tool_calls"].contains(&key.as_str())
        }) || native["role"] != "assistant"
            || native["content"].as_str() != Some(message.text().as_str())
            || native["reasoning_content"].as_str() != Some(reasoning.as_str())
        {
            return Err(bad());
        }
        let raw_calls = match native.get("tool_calls") {
            None => &[][..],
            Some(value) => value.as_array().ok_or_else(bad)?.as_slice(),
        };
        let calls: Vec<_> = message.tool_calls().collect();
        if raw_calls.len() != calls.len() {
            return Err(bad());
        }
        for (raw, call) in raw_calls.iter().zip(calls) {
            let object = raw.as_object().ok_or_else(bad)?;
            let function = raw["function"].as_object().ok_or_else(bad)?;
            if object
                .keys()
                .any(|key| !["id", "type", "function"].contains(&key.as_str()))
                || function
                    .keys()
                    .any(|key| !["name", "arguments"].contains(&key.as_str()))
                || raw["type"] != "function"
                || raw["id"].as_str() != Some(call.call_id.as_str())
                || raw["function"]["name"].as_str() != Some(call.name.as_str())
            {
                return Err(bad());
            }
            let arguments: Value =
                serde_json::from_str(raw["function"]["arguments"].as_str().ok_or_else(bad)?)
                    .map_err(|_| bad())?;
            if !arguments.is_object() || arguments != call.arguments {
                return Err(bad());
            }
        }
        // Return the native message once: do not duplicate public reasoning, text, or tools.
        return Ok(native.clone());
    }
    if message
        .content
        .iter()
        .any(|part| matches!(part, ModelContent::ReasoningSummary(_)))
    {
        return Err(bad());
    }
    let mut native = json!({"role":"assistant", "content":message.text()});
    let calls: Vec<_> = message.tool_calls().map(|call| json!({"id":call.call_id, "type":"function", "function":{"name":call.name,"arguments":call.arguments.to_string()}})).collect();
    if !calls.is_empty() {
        native["tool_calls"] = json!(calls);
    }
    Ok(native)
}
