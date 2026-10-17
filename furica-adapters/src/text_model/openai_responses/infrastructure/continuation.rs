//! Native continuation validation and its public content projection.
use crate::infrastructure::text::{bad, string};
use furica_core::{Result, text_model::*};
use serde_json::Value;

/// Validate replayable item types and their public projection together.
pub(super) fn project(items: &[Value]) -> Result<Vec<ModelContent>> {
    let mut result = Vec::new();
    for item in items {
        match string(item, "type")? {
            "message" => {
                if item["role"] != "assistant" {
                    return Err(bad());
                }
                for part in item["content"].as_array().ok_or_else(bad)? {
                    result.push(ModelContent::Text(
                        match string(part, "type")? {
                            "output_text" => string(part, "text")?,
                            "refusal" => string(part, "refusal")?,
                            _ => return Err(bad()),
                        }
                        .into(),
                    ));
                }
            }
            "function_call" => {
                let arguments: Value =
                    serde_json::from_str(string(item, "arguments")?).map_err(|_| bad())?;
                if !arguments.is_object() {
                    return Err(bad());
                }
                result.push(ModelContent::ToolCall(ToolCall {
                    call_id: string(item, "call_id")?.into(),
                    name: string(item, "name")?.into(),
                    arguments,
                }));
            }
            "reasoning" => {
                // Raw encrypted material remains untouched, even without a visible summary.
                for part in item["summary"].as_array().ok_or_else(bad)? {
                    if part["type"] != "summary_text" {
                        return Err(bad());
                    }
                    result.push(ModelContent::ReasoningSummary(string(part, "text")?.into()));
                }
            }
            _ => return Err(bad()),
        }
    }
    Ok(result)
}
