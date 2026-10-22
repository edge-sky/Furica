use crate::{
    Error, Host, Result,
    sessions::application::{message_view, page},
};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenameSession {
    pub name: String,
}

pub async fn list(host: &Host, offset: usize, limit: usize) -> Result<Value> {
    let (data, _, _) = host.query().await?;
    serde_json::to_value(page(data.sessions, offset, limit)).map_err(|_| Error::config())
}

pub async fn query(
    host: &Host,
    id: Uuid,
    resource: Option<&str>,
    offset: usize,
    limit: usize,
) -> Result<Value> {
    let (data, selected, state) = host.query().await?;
    let session = data
        .sessions
        .iter()
        .find(|s| s.id == id)
        .ok_or_else(|| Error::new(404, "not_found", "session does not exist"))?;
    let value = match resource {
        None => {
            json!({"session":session,"selected":selected==Some(id),"interaction_state":if selected==Some(id) {state} else {"Standby".into()}})
        }
        Some("messages") => {
            let mut messages: Vec<_> = data
                .messages
                .iter()
                .filter(|m| m.session_id == id)
                .collect();
            messages.sort_by_key(|m| m.sequence);
            json!(page(
                messages.into_iter().map(message_view).collect(),
                offset,
                limit
            ))
        }
        Some("acp-sessions") => json!(page(
            data.acp_sessions
                .into_iter()
                .filter(|v| v.session_id == id)
                .collect(),
            offset,
            limit
        )),
        Some("runs") => json!(page(
            data.runs
                .into_iter()
                .filter(|v| v.session_id == id)
                .collect(),
            offset,
            limit
        )),
        Some("tool-calls") => json!(page(
            data.tool_calls
                .into_iter()
                .filter(|v| v.session_id == id)
                .collect(),
            offset,
            limit
        )),
        Some("questions") => json!(page(
            data.questions
                .into_iter()
                .filter(|v| v.session_id == id)
                .collect(),
            offset,
            limit
        )),
        _ => return Err(Error::new(404, "not_found", "resource does not exist")),
    };
    Ok(value)
}
