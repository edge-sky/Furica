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

pub struct OpenAiResponses {
    http: Http,
    reasoning: bool,
}
impl OpenAiResponses {
    pub fn new(c: Config) -> Result<Self> {
        let scope = text::scope(
            "responses",
            &c.endpoint,
            &c.model,
            &c.isolation_scope,
            &c.supports_reasoning.to_string(),
        )?;
        Ok(Self {
            http: Http::new(
                c.endpoint,
                &c.api_key,
                c.model,
                scope,
                c.connect_timeout,
                None,
            )?,
            reasoning: c.supports_reasoning,
        })
    }
}
#[async_trait]
impl TextModel for OpenAiResponses {
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
