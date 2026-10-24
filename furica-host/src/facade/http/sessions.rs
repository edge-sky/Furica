use crate::{Error, Host, Result, facade::http::projection::message_view};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenameSession {
    pub name: String,
}

pub async fn list(host: &Host, offset: usize, limit: usize) -> Result<Value> {
    host.page(None, "sessions".into(), offset, limit).await
}
pub async fn query(
    host: &Host,
    id: Uuid,
    resource: Option<&str>,
    offset: usize,
    limit: usize,
) -> Result<Value> {
    let Some(resource) = resource else {
        return host.session_status(id).await;
    };
    let mut page = host.page(Some(id), resource.into(), offset, limit).await?;
    if resource == "messages" {
        for value in page["items"].as_array_mut().ok_or_else(Error::storage)? {
            let message = serde_json::from_value(value.clone()).map_err(|_| Error::storage())?;
            *value = message_view(&message);
        }
    }
    Ok(page)
}
