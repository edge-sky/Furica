//! Task tool declarations selected by the conversation workflow.

use serde_json::Value;

pub fn task_tools() -> Vec<Value> {
    vec![
        serde_json::json!({"type":"function", "name":"start_task",
            "description":"Submit a real task to the ACP executor. Returns acceptance, not completion.",
            "parameters":{"type":"object", "properties":{"prompt":{"type":"string"}},
                "required":["prompt"], "additionalProperties":false}}),
        serde_json::json!({"type":"function", "name":"get_task_status",
            "description":"Read a task's execution status and result.",
            "parameters":{"type":"object", "properties":{"task_id":{"type":"string"}},
                "required":["task_id"], "additionalProperties":false}}),
        serde_json::json!({"type":"function", "name":"cancel_task",
            "description":"Request cancellation; wait for confirmed status before saying it stopped.",
            "parameters":{"type":"object", "properties":{"task_id":{"type":"string"}},
                "required":["task_id"], "additionalProperties":false}}),
    ]
}
