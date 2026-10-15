//! One bounded STT/model/TTS turn, without command receipt or playback scheduling.

use super::{
    CascadedBackend, ModelMessage, ModelRequest, ModelResponse, SynthesizedAudio, Utterance,
};
use crate::{Error, Result, voice::task_tools};
use std::collections::HashSet;
use tokio_util::sync::CancellationToken;

pub(super) enum TurnInput {
    Audio(Utterance),
    Text(String),
    Continue,
}

pub(super) struct TurnResult {
    pub(super) user_text: Option<String>,
    pub(super) response: Option<ModelResponse>,
    pub(super) audio: Option<SynthesizedAudio>,
}

pub(super) fn message_size(message: &ModelMessage) -> usize {
    match message {
        ModelMessage::User(text) | ModelMessage::Assistant(text) => text.len(),
        ModelMessage::AssistantTools(calls) => calls
            .iter()
            .map(|call| call.call_id.len() + call.name.len() + call.arguments.to_string().len())
            .sum(),
        ModelMessage::ToolResult { call_id, result } => call_id.len() + result.to_string().len(),
        ModelMessage::TaskContext(value) | ModelMessage::TaskUpdate(value) => {
            value.to_string().len()
        }
    }
}

pub(super) fn validate_text(text: &str, limit: usize) -> Result<()> {
    if text.len() > limit {
        return Err(Error::Busy("pipeline text"));
    }
    Ok(())
}

pub(super) async fn infer(
    backend: CascadedBackend,
    mut history: Vec<ModelMessage>,
    input: TurnInput,
    cancel: CancellationToken,
) -> Result<TurnResult> {
    let text = match input {
        TurnInput::Audio(utterance) => {
            Some(backend.stt.transcribe(utterance, cancel.clone()).await?)
        }
        TurnInput::Text(text) => Some(text),
        TurnInput::Continue => None,
    };
    if let Some(text) = &text {
        validate_text(text, backend.config.max_context_bytes)?;
        if text.trim().is_empty() {
            return Ok(TurnResult {
                user_text: None,
                response: None,
                audio: None,
            });
        }
        history.push(ModelMessage::User(text.clone()));
    }
    let history_len = history.len();
    let history_bytes = history.iter().map(message_size).sum::<usize>();
    if history_len > backend.config.max_history_messages
        || history_bytes > backend.config.max_context_bytes
    {
        return Err(Error::Busy("pipeline conversation context"));
    }
    let response = backend
        .model
        .generate(
            ModelRequest {
                messages: history,
                tools: task_tools(),
            },
            cancel.clone(),
        )
        .await?;
    let response_bytes = match &response {
        ModelResponse::Text(text) => text.len(),
        ModelResponse::Tools(calls) => calls
            .iter()
            .map(|call| call.call_id.len() + call.name.len() + call.arguments.to_string().len())
            .sum(),
    };
    if history_len >= backend.config.max_history_messages
        || history_bytes.saturating_add(response_bytes) > backend.config.max_context_bytes
    {
        return Err(Error::Busy("pipeline conversation context"));
    }
    let audio = match &response {
        ModelResponse::Text(reply) => {
            validate_text(reply, backend.config.max_context_bytes)?;
            if reply.trim().is_empty() {
                None
            } else {
                let audio = backend.tts.synthesize(reply.clone(), cancel).await?;
                if audio.sample_rate != backend.config.sample_rate
                    || audio.samples.is_empty()
                    || audio.samples.len() as f64 / f64::from(audio.sample_rate)
                        > backend.config.max_generated_audio.as_secs_f64()
                {
                    return Err(Error::Audio("invalid synthesized audio".into()));
                }
                Some(audio)
            }
        }
        ModelResponse::Tools(calls) => {
            let mut ids = HashSet::new();
            if calls.is_empty() || calls.len() > 32 {
                return Err(Error::Protocol("invalid model tool batch".into()));
            }
            for call in calls {
                if call.call_id.is_empty()
                    || call.call_id.len() > 256
                    || !ids.insert(&call.call_id)
                    || call.name.len() > 128
                    || call.arguments.to_string().len() > backend.config.max_context_bytes
                {
                    return Err(Error::Protocol("invalid model tool call".into()));
                }
            }
            None
        }
    };
    Ok(TurnResult {
        user_text: text,
        response: Some(response),
        audio,
    })
}
