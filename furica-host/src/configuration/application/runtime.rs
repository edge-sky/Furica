//! Validate host-assembled runtime parameters before handing them to Core.
use crate::{Error, Result};
use furica_core::{
    config::CoreConfig,
    input::SpeechInput,
    orchestration::CascadeConfig,
    sessions::{AgentBinding, SessionLimits},
};
use std::{collections::HashSet, time::Duration};

pub fn validate_core(config: &CoreConfig) -> Result<()> {
    if !(8_000..=48_000).contains(&config.sample_rate)
        || config.channel_capacity == 0
        || config.channel_capacity > 4096
        || config.connect_timeout.is_zero()
        || config.idle_timeout.is_zero()
        || config.connect_buffer.is_zero()
        || config.connect_buffer > Duration::from_secs(30)
        || config.pre_roll > config.connect_buffer
    {
        return Err(Error::config());
    }
    Ok(())
}

pub fn validate_speech_input(input: &SpeechInput, core: &CoreConfig) -> Result<()> {
    if input.config.verification_window.is_zero()
        || input.config.verification_window >= core.connect_buffer
        || input.config.verification_timeout.is_zero()
        || input.config.endpoint_silence.is_zero()
        || input.config.endpoint_silence > core.connect_buffer
    {
        return Err(Error::config());
    }
    Ok(())
}

pub fn validate_cascade(config: &CascadeConfig, stt: bool, tts: bool) -> Result<()> {
    if !(8_000..=48_000).contains(&config.sample_rate)
        || config.channel_capacity == 0
        || config.channel_capacity > 4096
        || config.inference_timeout.is_zero()
        || config.max_history_messages == 0
        || config.max_history_messages > 4096
        || config.max_output_tokens == 0
        || matches!(
            config.reasoning,
            Some(furica_core::text_model::Reasoning::BudgetTokens(0))
        )
        || config.max_context_bytes == 0
        || config.max_context_bytes > 4 * 1024 * 1024
        || (stt
            && (config.max_utterance.is_zero() || config.max_utterance > Duration::from_secs(120)))
        || (tts
            && (config.max_generated_audio.is_zero()
                || config.max_generated_audio > Duration::from_secs(120)))
    {
        return Err(Error::config());
    }
    Ok(())
}

pub fn validate_session_limits(limits: &SessionLimits) -> Result<()> {
    if limits.max_acp_sessions == 0
        || limits.max_acp_sessions > 64
        || limits.max_runs == 0
        || limits.max_runs > 1024
        || limits.question_timeout.is_zero()
        || limits.question_timeout > Duration::from_secs(86_400)
    {
        return Err(Error::config());
    }
    Ok(())
}

pub fn validate_agents(agents: &[AgentBinding], limits: &SessionLimits) -> Result<()> {
    if agents.is_empty() {
        return Ok(());
    }
    validate_session_limits(limits)?;
    let mut ids = HashSet::new();
    for agent in agents {
        if agent.id.trim().is_empty() || !ids.insert(&agent.id) || agent.workspaces.is_empty() {
            return Err(Error::config());
        }
        let mut workspaces = HashSet::new();
        for workspace in &agent.workspaces {
            if workspace.id.trim().is_empty()
                || !workspaces.insert(&workspace.id)
                || !workspace.path.is_absolute()
            {
                return Err(Error::config());
            }
        }
    }
    Ok(())
}
