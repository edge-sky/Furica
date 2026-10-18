//! Translate model tool calls into task use cases and feed results back to voice.

use super::Actor;
use crate::{Error, Result, tasks::TaskAnswer};
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
        struct Continue {
            session_id: Uuid,
            prompt: String,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct SessionId {
            session_id: Uuid,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct AnswerQuestion {
            question_id: Uuid,
            action: AnswerAction,
            #[serde(default)]
            content: Option<Value>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum AnswerAction {
            Submit,
            Decline,
            Cancel,
        }
        match name {
            "start_task" => {
                let args: Start = serde_json::from_value(arguments)?;
                let run = self.tasks.start(self.epoch, call_id, args.prompt)?;
                Ok(json!({
                    "session_id": run.acp_session_id,
                    "run_id": run.id,
                    "state": run.state,
                    "accepted": true
                }))
            }
            "continue_task" => {
                let args: Continue = serde_json::from_value(arguments)?;
                let run =
                    self.tasks
                        .continue_task(self.epoch, call_id, args.session_id, args.prompt)?;
                Ok(json!({
                    "session_id": run.acp_session_id,
                    "run_id": run.id,
                    "state": run.state,
                    "accepted": true
                }))
            }
            "get_task_status" => {
                let args: SessionId = serde_json::from_value(arguments)?;
                Ok(serde_json::to_value(
                    self.tasks.status(self.epoch, args.session_id)?,
                )?)
            }
            "answer_task_question" => {
                let args: AnswerQuestion = serde_json::from_value(arguments)?;
                let answer = match args.action {
                    AnswerAction::Submit => TaskAnswer::Submit {
                        content: args.content.unwrap_or(Value::Null),
                    },
                    AnswerAction::Decline => {
                        if args.content.is_some() {
                            return Err(Error::Config("content is only valid for submit".into()));
                        }
                        TaskAnswer::Decline
                    }
                    AnswerAction::Cancel => {
                        if args.content.is_some() {
                            return Err(Error::Config("content is only valid for submit".into()));
                        }
                        TaskAnswer::Cancel
                    }
                };
                let question = self.tasks.answer(self.epoch, args.question_id, answer)?;
                Ok(json!({
                    "question_id": question.id,
                    "state": question.state,
                    "accepted": true
                }))
            }
            "cancel_task_run" => {
                let args: SessionId = serde_json::from_value(arguments)?;
                let run = self.tasks.cancel_run(self.epoch, args.session_id)?;
                Ok(json!({
                    "session_id": run.acp_session_id,
                    "run_id": run.id,
                    "state": run.state,
                    "accepted": true
                }))
            }
            "close_task" => {
                let args: SessionId = serde_json::from_value(arguments)?;
                let session = self.tasks.close(self.epoch, args.session_id)?;
                Ok(json!({
                    "session_id": session.id,
                    "state": session.state,
                    "accepted": true
                }))
            }
            _ => Err(Error::Protocol("unknown voice tool".into())),
        }
    }
}
