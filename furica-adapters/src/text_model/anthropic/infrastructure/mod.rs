//! Concrete implementation of Core's TextModel port.
mod continuation;
mod request;
mod stream;
use super::config::Config;
use crate::infrastructure::text::{self, Http};
use async_trait::async_trait;
use furica_core::{
    Result,
    text_model::{ModelRequest, ModelStream, TextModel},
};
use tokio_util::sync::CancellationToken;

pub struct Anthropic {
    http: Http,
    adaptive: bool,
    budget: bool,
}
impl Anthropic {
    pub fn new(c: Config) -> Result<Self> {
        let scope = text::scope(
            "anthropic-messages",
            &c.endpoint,
            &c.model,
            &c.isolation_scope,
            &format!(
                "{}:{}:{}",
                c.version, c.supports_adaptive_thinking, c.supports_thinking_budget
            ),
        )?;
        Ok(Self {
            http: Http::new(
                c.endpoint,
                &c.api_key,
                c.model,
                scope,
                c.connect_timeout,
                Some(&c.version),
            )?,
            adaptive: c.supports_adaptive_thinking,
            budget: c.supports_thinking_budget,
        })
    }
}
#[async_trait]
impl TextModel for Anthropic {
    fn continuation_source(&self) -> &str {
        &self.http.scope
    }
    async fn generate_stream(
        &self,
        request: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelStream> {
        let sse = self.http.open(self.encode(&request)?, cancel).await?;
        Ok(stream::open(sse, self.http.scope.clone()))
    }
}
