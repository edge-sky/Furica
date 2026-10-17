//! Native continuation validation and its public content projection.
use crate::infrastructure::text::{bad, string};
use furica_core::{Result, text_model::*};
use serde_json::Value;

pub(super) fn project(blocks: &[Value]) -> Result<Vec<ModelContent>> {
    let mut content = Vec::new();
    for b in blocks {
        match string(b, "type")? {
            "text" => content.push(ModelContent::Text(string(b, "text")?.into())),
            "thinking" => {
                if string(b, "signature")?.is_empty() {
                    return Err(bad());
                }
                content.push(ModelContent::ReasoningSummary(
                    string(b, "thinking")?.into(),
                ));
            }
            "redacted_thinking" => {
                string(b, "data")?;
            }
            "tool_use" => {
                if !b["input"].is_object() {
                    return Err(bad());
                }
                content.push(ModelContent::ToolCall(ToolCall {
                    call_id: string(b, "id")?.into(),
                    name: string(b, "name")?.into(),
                    arguments: b["input"].clone(),
                }));
            }
            _ => return Err(bad()),
        }
    }
    Ok(content)
}
