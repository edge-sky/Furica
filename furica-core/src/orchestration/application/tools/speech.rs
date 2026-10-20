//! Explicit asynchronous speech tool.
use super::ToolEntry;
use crate::{Error, text_model::ToolDefinition};
use serde_json::json;
pub(super) fn entry() -> ToolEntry {
    ToolEntry {
        handler: |_, call, speech| {
            speech.ok_or_else(|| Error::Protocol("speech unavailable".into()))?.enqueue(call.arguments.clone())
        },
        definition: ToolDefinition {
        name: "speak".into(),
        description: Some(
            "Speak text aloud asynchronously. All speech, including final answers, must use this tool; ordinary assistant text is display-only. Returns acceptance and a request_id immediately, not synthesis or playback completion. One request runs through synthesis and playback while up to five wait FIFO. When full, the oldest waiting request is dropped and its dropped_request_id is returned. Interrupted conversations discard pending speech. Do not repeat a request merely because playback has not finished."
                .into(),
        ),
        parameters: json!({
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "minLength": 1,
                    "description": "The complete text to speak. Must fit the configured text budget and the speech provider's limits."
                }
            },
            "required": ["text"],
            "additionalProperties": false
        }),
    }
    }
}
