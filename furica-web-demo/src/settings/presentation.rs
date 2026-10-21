//! Editable values never include stored API keys.
use super::Config;
use serde_json::{Value, json};

pub fn editable(config: &Config) -> Value {
    let mut value = json!(config);
    for name in ["stt", "tts", "text_model"] {
        let section = value[name].as_object_mut().expect("configuration section");
        let key = section.remove("api_key").expect("API key field");
        section.insert(
            "api_key_configured".into(),
            json!(!key.as_str().unwrap_or("").is_empty()),
        );
    }
    value
}

pub fn response(config: &Config) -> Value {
    json!({"settings":editable(config), "status":config.public_status()})
}
