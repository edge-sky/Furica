//! Configuration-bound capability instances. Business task lifecycle remains in Host.
mod build;

pub(crate) use build::secret;
pub use build::{assemble, load};

use crate::{Error, Result, configuration::application::HostConfig};
use furica_core::{
    orchestration::VoiceBackend, sessions::AgentBinding, stt::Stt, text_model::TextModel, tts::Tts,
};
use std::sync::{Arc, RwLock, atomic::AtomicBool};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub struct Components {
    pub(crate) config: HostConfig,
    pub(crate) instructions: Option<String>,
    pub(crate) model: Option<Arc<dyn TextModel>>,
    pub(crate) stt: Option<Arc<dyn Stt>>,
    pub(crate) tts: Option<Arc<dyn Tts>>,
    pub(crate) backend: Option<Arc<dyn VoiceBackend>>,
    pub(crate) agents: Vec<AgentBinding>,
    pub(crate) speech_enabled: Arc<AtomicBool>,
    pub(crate) voice_available: bool,
    pub(crate) cancelled: CancellationToken,
}

impl Components {
    pub fn config(&self) -> &HostConfig {
        &self.config
    }
    pub fn instructions(&self) -> Option<&str> {
        self.instructions.as_deref()
    }
    pub fn model(&self) -> Option<&dyn TextModel> {
        self.model.as_deref()
    }
    pub fn stt(&self) -> Option<&dyn Stt> {
        self.stt.as_deref()
    }
    pub fn tts(&self) -> Option<&dyn Tts> {
        self.tts.as_deref()
    }
    pub fn backend(&self) -> Option<&dyn VoiceBackend> {
        self.backend.as_deref()
    }
    pub fn agents(&self) -> &[AgentBinding] {
        &self.agents
    }

    /// Supply a composed backend, for hosts embedding their own capability implementations.
    pub fn new(
        config: HostConfig,
        backend: Option<Arc<dyn VoiceBackend>>,
        agents: Vec<AgentBinding>,
        speech_enabled: Arc<AtomicBool>,
        voice_available: bool,
    ) -> Self {
        Self {
            config,
            instructions: None,
            model: None,
            stt: None,
            tts: None,
            backend,
            agents,
            speech_enabled,
            voice_available,
            cancelled: CancellationToken::new(),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.config.validate()?;
        use crate::configuration::application::runtime;
        // Keep validation at the host boundary, including caller-supplied backends.
        runtime::validate_agents(&self.agents, &self.config.session_limits())?;
        if self.voice_available && self.backend.is_none() {
            return Err(Error::config());
        }
        if let Some(backend) = &self.backend {
            runtime::validate_core(&self.config.core_config())?;
            if backend.sample_rate() != self.config.core_config().sample_rate {
                return Err(Error::config());
            }
        }
        Ok(())
    }

    pub(crate) async fn shutdown(&self) -> Result<()> {
        self.cancelled.cancel();
        let mut result = Ok(());
        for agent in &self.agents {
            let stopped = tokio::time::timeout(
                std::time::Duration::from_secs(12),
                agent.executor.shutdown(),
            )
            .await
            .map_err(|_| Error::new(504, "shutdown_failed", "executor cleanup did not complete"))
            .and_then(|result| result.map_err(Into::into));
            result = result.and(stopped);
        }
        result
    }
}

impl Drop for Components {
    fn drop(&mut self) {
        self.cancelled.cancel();
    }
}

pub(crate) struct ComponentManager {
    current: RwLock<Arc<Components>>,
    pub(crate) update_lock: Mutex<()>,
}

impl ComponentManager {
    pub(crate) fn new(components: Components) -> Self {
        Self {
            current: RwLock::new(Arc::new(components)),
            update_lock: Mutex::new(()),
        }
    }

    pub(crate) fn current(&self) -> Arc<Components> {
        self.current
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub(crate) async fn prepare(
        &self,
        config: &HostConfig,
        directory: &std::path::Path,
    ) -> Result<Components> {
        let components = load(config, directory).await?;
        if let Err(error) = components.validate() {
            let _ = components.shutdown().await;
            return Err(error);
        }
        Ok(components)
    }

    pub(crate) fn publish(&self, components: Arc<Components>) {
        *self
            .current
            .write()
            .unwrap_or_else(|error| error.into_inner()) = components;
    }
}
