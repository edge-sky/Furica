//! Shared request bounds, continuation scope and protocol value conversion.
use crate::infrastructure::http;
use furica_core::{Error, Result, text_model::*};
use serde_json::{Value, json};

pub(crate) const LIMIT: usize = 1024 * 1024;

pub(crate) fn bad() -> Error {
    Error::Protocol("invalid or unsupported text model protocol data".into())
}
#[cfg(any(feature = "openai-responses", feature = "anthropic"))]
pub(crate) fn string<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v[key].as_str().ok_or_else(bad)
}
pub(crate) fn index(v: &Value, key: &str) -> Result<usize> {
    let n = v[key].as_u64().ok_or_else(bad)?;
    if n >= 256 {
        return Err(Error::Busy("text protocol items"));
    }
    Ok(n as usize)
}
#[cfg(any(feature = "openai-responses", feature = "anthropic"))]
pub(crate) fn push(s: &mut String, part: &str) -> Result<()> {
    if part.len() > LIMIT.saturating_sub(s.len()) {
        return Err(Error::Busy("text protocol buffer"));
    }
    s.push_str(part);
    Ok(())
}
pub(crate) fn value_size(v: &Value) -> usize {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(b.len());
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut n = Count(0);
    serde_json::to_writer(&mut n, v).expect("JSON value");
    n.0
}
pub(crate) fn bounded(v: &Value) -> Result<()> {
    if value_size(v) > LIMIT {
        return Err(Error::Busy("text protocol payload"));
    }
    Ok(())
}
pub(crate) fn effort(reasoning: Reasoning) -> Result<&'static str> {
    match reasoning {
        Reasoning::Low => Ok("low"),
        Reasoning::Medium => Ok("medium"),
        Reasoning::High => Ok("high"),
        _ => Err(Error::Config(
            "token reasoning budgets unsupported by this protocol".into(),
        )),
    }
}
pub(crate) fn scope(
    protocol: &str,
    endpoint: &str,
    model: &str,
    isolation: &str,
    settings: &str,
) -> Result<String> {
    http::text(endpoint, "text endpoint")?;
    http::endpoint(endpoint, false)?;
    if reqwest::Url::parse(endpoint)
        .map_err(|_| bad())?
        .query()
        .is_some()
    {
        return Err(Error::Config(
            "text endpoint must not contain query credentials or parameters".into(),
        ));
    }
    http::text(model, "model")?;
    http::text(isolation, "credential-free account/tenant scope")?;
    // Length-delimited serialization prevents ambiguous scope concatenation.
    Ok(json!([protocol, endpoint, model, isolation, settings]).to_string())
}
/// Bound input payloads before constructing a second provider-shaped copy.
/// The final serialized request has its own exact wire-size check in Http::open.
pub(crate) fn check_request(request: &ModelRequest, scope: &str) -> Result<()> {
    if request.messages.len() > 4096 || request.tools.len() > 32 {
        return Err(Error::Busy("text request items"));
    }
    let mut bytes = request.instructions.as_ref().map_or(0, String::len);
    let mut items = 0usize;
    let mut add = |size: usize| -> Result<()> {
        if size > LIMIT.saturating_sub(bytes) || bytes > LIMIT {
            return Err(Error::Busy("text request payload"));
        }
        bytes += size;
        Ok(())
    };
    add(0)?;
    for tool in &request.tools {
        add(tool.name.len())?;
        add(tool.description.as_ref().map_or(0, String::len))?;
        add(value_size(&tool.parameters))?;
    }
    for message in &request.messages {
        match message {
            ModelMessage::User(s) => add(s.len())?,
            ModelMessage::ExecutionContext(v) | ModelMessage::ExecutionUpdate(v) => {
                add(value_size(v))?
            }
            ModelMessage::ToolResult { call_id, result } => {
                add(call_id.len())?;
                add(value_size(result))?;
            }
            ModelMessage::Assistant(m) => {
                items = items.saturating_add(m.content.len());
                if items > 4096 {
                    return Err(Error::Busy("text request items"));
                }
                if let Some(c) = &m.continuation {
                    if c.source != scope {
                        return Err(Error::Config(
                            "incompatible model continuation source".into(),
                        ));
                    }
                    add(value_size(&c.data))?;
                }
                for part in &m.content {
                    match part {
                        ModelContent::Text(s) | ModelContent::ReasoningSummary(s) => add(s.len())?,
                        ModelContent::ToolCall(c) => {
                            add(c.call_id.len())?;
                            add(c.name.len())?;
                            add(value_size(&c.arguments))?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
pub(crate) fn data_message(message: &ModelMessage) -> Option<String> {
    match message {
        ModelMessage::User(s) => Some(s.clone()),
        ModelMessage::ExecutionContext(v) => Some(json!({"task_context":v}).to_string()),
        ModelMessage::ExecutionUpdate(v) => Some(json!({"task_update":v}).to_string()),
        _ => None,
    }
}
