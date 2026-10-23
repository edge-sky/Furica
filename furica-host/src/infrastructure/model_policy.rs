use async_trait::async_trait;
use furica_core::{
    Error, Result,
    text_model::{ModelEvent, ModelRequest, ModelStream, TextModel},
};
use futures_util::StreamExt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio_util::sync::CancellationToken;

pub struct SpeechPolicy {
    inner: Arc<dyn TextModel>,
    pub enabled: Arc<AtomicBool>,
}

impl SpeechPolicy {
    pub fn new(inner: Arc<dyn TextModel>, enabled: Arc<AtomicBool>) -> Self {
        Self { inner, enabled }
    }
}

#[async_trait]
impl TextModel for SpeechPolicy {
    fn continuation_source(&self) -> &str {
        self.inner.continuation_source()
    }
    async fn generate_stream(
        &self,
        mut request: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelStream> {
        // Freeze the policy for the entire call; mode transitions close the old backend first.
        if !self.enabled.load(Ordering::Acquire) {
            request.tools.retain(|tool| tool.name != "speak");
        }
        let allowed: std::collections::HashSet<_> =
            request.tools.iter().map(|t| t.name.clone()).collect();
        let stream = self.inner.generate_stream(request, cancel).await?;
        Ok(Box::pin(stream.scan(false, move |failed, event| {
            let result = if *failed {
                None
            } else {
                let event = match event {
                    Ok(ModelEvent::ToolStart { ref name, .. }) if !allowed.contains(name) => Err(
                        Error::Protocol("model requested a tool outside this call policy".into()),
                    ),
                    other => other,
                };
                if event.is_err() {
                    *failed = true;
                }
                Some(event)
            };
            std::future::ready(result)
        })))
    }
}
