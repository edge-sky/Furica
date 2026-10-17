//! Task tool declarations selected by the conversation workflow.
use crate::text_model::ToolDefinition;
use serde_json::json;

pub fn task_tools() -> Vec<ToolDefinition> {
    [
        (
            "start_task",
            "Submit a real task to the ACP executor. Returns acceptance, not completion.",
            "prompt",
        ),
        (
            "get_task_status",
            "Read a task's execution status and result.",
            "task_id",
        ),
        (
            "cancel_task",
            "Request cancellation; wait for confirmed status before saying it stopped.",
            "task_id",
        ),
    ]
    .into_iter()
    .map(|(name, description, parameter)| ToolDefinition {
        name: name.into(),
        description: Some(description.into()),
        parameters: json!({"type":"object", "properties":{parameter:{"type":"string"}},
            "required":[parameter], "additionalProperties":false}),
    })
    .collect()
}
