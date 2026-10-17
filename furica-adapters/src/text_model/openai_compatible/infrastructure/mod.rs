//! Concrete implementation of Core's TextModel port.
mod request;
mod stream;
use super::config::Config;
use crate::infrastructure::text::{self, Http};
use async_trait::async_trait;
use furica_core::{
    Error, Result,
    text_model::{ModelRequest, ModelStream, TextModel},
};
use tokio_util::sync::CancellationToken;

pub struct OpenAiCompatible {
    http: Http,
    token_field: String,
    reasoning: bool,
}
impl OpenAiCompatible {
    pub fn new(config: Config) -> Result<Self> {
        if !matches!(
            config.max_tokens_field.as_str(),
            "max_completion_tokens" | "max_tokens"
        ) {
            return Err(Error::Config(
                "unsupported Chat Completions token field".into(),
            ));
        }
        let scope = text::scope(
            "chat-completions",
            &config.endpoint,
            &config.model,
            &config.isolation_scope,
            &format!(
                "{}:{}",
                config.max_tokens_field, config.supports_reasoning_effort
            ),
        )?;
        Ok(Self {
            http: Http::new(
                config.endpoint,
                &config.api_key,
                config.model,
                scope,
                config.connect_timeout,
                None,
            )?,
            token_field: config.max_tokens_field,
            reasoning: config.supports_reasoning_effort,
        })
    }
}
#[async_trait]
impl TextModel for OpenAiCompatible {
    fn continuation_source(&self) -> &str {
        &self.http.scope
    }
    async fn generate_stream(
        &self,
        request: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelStream> {
        let sse = self.http.open(self.encode(&request)?, cancel).await?;
        Ok(stream::open(sse))
    }
}
