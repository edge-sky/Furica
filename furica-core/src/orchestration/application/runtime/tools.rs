//! Translate model tool calls into task use cases and feed results back to voice.

use super::Actor;
use crate::{Error, Result, sessions::Answer};
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
            acp_session_id: Uuid,
            prompt: String,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct SessionId {
            acp_session_id: Uuid,
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
            "prepare_run" => {
                let args: Start = serde_json::from_value(arguments)?;
                let run = self
                    .sessions
                    .prepare_start(self.session_id, call_id, args.prompt)?;
                Ok(json!({
                    "acp_session_id": run.acp_session_id,
                    "run_id": run.id,
                    "state": run.state,
                    "accepted": true
                }))
            }
            "prepare_next_run" => {
                let args: Continue = serde_json::from_value(arguments)?;
                let run = self.sessions.prepare_continue(
                    self.session_id,
                    call_id,
                    args.acp_session_id,
                    args.prompt,
                )?;
                Ok(json!({
                    "acp_session_id": run.acp_session_id,
                    "run_id": run.id,
                    "state": run.state,
                    "accepted": true
                }))
            }
            "get_execution_status" => {
                let args: SessionId = serde_json::from_value(arguments)?;
                Ok(serde_json::to_value(
                    self.sessions.status(self.session_id, args.acp_session_id)?,
                )?)
            }
            "answer_question" => {
                let args: AnswerQuestion = serde_json::from_value(arguments)?;
                let answer = match args.action {
                    AnswerAction::Submit => Answer::Submit {
                        content: args.content.unwrap_or(Value::Null),
                    },
                    AnswerAction::Decline => {
                        if args.content.is_some() {
                            return Err(Error::Config("content is only valid for submit".into()));
                        }
                        Answer::Decline
                    }
                    AnswerAction::Cancel => {
                        if args.content.is_some() {
                            return Err(Error::Config("content is only valid for submit".into()));
                        }
                        Answer::Cancel
                    }
                };
                let question = self
                    .sessions
                    .answer(self.session_id, args.question_id, answer)?;
                Ok(json!({
                    "question_id": question.id,
                    "state": question.state,
                    "accepted": true
                }))
            }
            "cancel_run" => {
                let args: SessionId = serde_json::from_value(arguments)?;
                let run = self
                    .sessions
                    .cancel_run(self.session_id, args.acp_session_id)?;
                Ok(json!({
                    "acp_session_id": run.acp_session_id,
                    "run_id": run.id,
                    "state": run.state,
                    "accepted": true
                }))
            }
            "close_acp_session" => {
                let args: SessionId = serde_json::from_value(arguments)?;
                let session = self.sessions.close(self.session_id, args.acp_session_id)?;
                Ok(json!({
                    "acp_session_id": session.id,
                    "state": session.state,
                    "accepted": true
                }))
            }
            _ => Err(Error::Protocol("unknown voice tool".into())),
        }
    }
}
