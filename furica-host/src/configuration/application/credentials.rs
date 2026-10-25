//! Direct credentials in HostConfig; the HTTP view exposes only presence flags.
use super::HostConfig;
use crate::{Error, Result};
use serde_json::Value;

const OWNERS: [&str; 3] = ["/model", "/speech/stt", "/speech/tts"];

pub fn valid(value: Option<&str>) -> bool {
    value.is_none_or(|value| {
        !value.trim().is_empty()
            && value.len() <= 8192
            && value.parse::<hyper::header::HeaderValue>().is_ok()
    })
}

pub fn required(value: Option<&str>) -> Result<String> {
    match value {
        Some(value) if valid(Some(value)) => Ok(value.to_owned()),
        Some(_) => Err(Error::new(
            400,
            "credential_invalid",
            "stored API key is invalid",
        )),
        None => Err(Error::new(
            503,
            "credential_missing",
            "configure the API key in Host settings",
        )),
    }
}

fn uses_api_key(owner: &Value) -> bool {
    owner.get("protocol").is_some()
        || owner
            .get("provider")
            .is_some_and(|v| v == "aliyun" || v == "volcengine")
}

pub fn view(config: &HostConfig) -> Result<Value> {
    let mut value = serde_json::to_value(config).map_err(|_| Error::config())?;
    for path in OWNERS {
        if let Some(owner) = value.pointer_mut(path).filter(|owner| uses_api_key(owner)) {
            let owner = owner.as_object_mut().ok_or_else(Error::config)?;
            let configured = owner.remove("api_key").is_some_and(|key| key.is_string());
            owner.insert("api_key_configured".into(), Value::Bool(configured));
        }
    }
    let http = value
        .get_mut("http")
        .and_then(Value::as_object_mut)
        .ok_or_else(Error::config)?;
    http.remove("token");
    http.insert("token_configured".into(), Value::Bool(true));
    Ok(value)
}

/// Missing key keeps the stored value on the same protocol/provider. Null clears it.
/// Read-modify-write callers must hold the database transaction through this merge.
pub fn update(mut value: Value, previous: &HostConfig) -> Result<HostConfig> {
    let previous = serde_json::to_value(previous).map_err(|_| Error::config())?;
    for path in OWNERS {
        let Some(owner) = value.pointer_mut(path).filter(|owner| uses_api_key(owner)) else {
            continue;
        };
        let owner = owner.as_object_mut().ok_or_else(Error::config)?;
        if let Some(flag) = owner.remove("api_key_configured")
            && !flag.is_boolean()
        {
            return Err(Error::config());
        }
        if !owner.contains_key("api_key")
            && let Some(old) = previous.pointer(path)
        {
            let same_kind = owner.get("protocol") == old.get("protocol")
                && owner.get("provider") == old.get("provider");
            if same_kind && let Some(key) = old.get("api_key") {
                owner.insert("api_key".into(), key.clone());
            }
        }
    }
    if let Some(http) = value.get_mut("http").and_then(Value::as_object_mut) {
        if let Some(flag) = http.remove("token_configured")
            && !flag.is_boolean()
        {
            return Err(Error::config());
        }
        if !http.contains_key("token")
            && let Some(token) = previous.pointer("/http/token")
        {
            http.insert("token".into(), token.clone());
        }
    }
    let config: HostConfig = serde_json::from_value(value).map_err(|_| Error::config())?;
    config.validate()?;
    Ok(config)
}
