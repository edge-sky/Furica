//! Translate model tool calls into task use cases and feed results back to voice.

use super::Actor;
use crate::{Error, Result};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

impl Actor {
    pub(super) fn tool(&self, call_id: &str, name: &str, arguments: Value) -> Result<Value> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Start {
            prompt: String,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct TaskId {
            task_id: Uuid,
        }
        match name {
            "start_task" => {
                let args: Start = serde_json::from_value(arguments)?;
                let task = self
                    .tasks
                    .submit(format!("{}:{call_id}", self.epoch), args.prompt)?;
                Ok(json!({"task_id":task.id, "state":task.state, "accepted":true}))
            }
            "get_task_status" => {
                let args: TaskId = serde_json::from_value(arguments)?;
                Ok(serde_json::to_value(self.tasks.get(args.task_id)?)?)
            }
            "cancel_task" => {
                let args: TaskId = serde_json::from_value(arguments)?;
                Ok(serde_json::to_value(self.tasks.cancel(args.task_id)?)?)
            }
            _ => Err(Error::Protocol("unknown voice tool".into())),
        }
    }
}
