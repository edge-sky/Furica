//! One turn's control flow. Capability modules own validation and provider calls.

use super::CascadedBackend;
use crate::{
    Result,
    orchestration::task_tools,
    stt::{self, Utterance},
    text_model::{self, ModelMessage, ModelRequest, ModelResponse},
    tts::{self, SynthesizedAudio},
};
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

pub(super) async fn infer(
    backend: CascadedBackend,
    mut history: Vec<ModelMessage>,
    input: TurnInput,
    cancel: CancellationToken,
) -> Result<TurnResult> {
    let text = match input {
        TurnInput::Audio(utterance) => Some(
            stt::transcribe(
                backend.stt.as_ref(),
                utterance,
                backend.config.max_utterance,
                backend.config.max_context_bytes,
                cancel.clone(),
            )
            .await?,
        ),
        TurnInput::Text(text) => Some(text),
        TurnInput::Continue => None,
    };
    if let Some(text) = &text {
        text_model::validate_text(text, backend.config.max_context_bytes)?;
        if text.trim().is_empty() {
            return Ok(TurnResult {
                user_text: None,
                response: None,
                audio: None,
            });
        }
        history.push(ModelMessage::User(text.clone()));
    }
    let response = text_model::generate(
        backend.model.as_ref(),
        ModelRequest {
            messages: history,
            tools: task_tools(),
        },
        backend.config.max_history_messages,
        backend.config.max_context_bytes,
        cancel.clone(),
    )
    .await?;
    let audio = match &response {
        ModelResponse::Text(reply) if !reply.trim().is_empty() => Some(
            tts::synthesize(
                backend.tts.as_ref(),
                reply.clone(),
                backend.config.sample_rate,
                backend.config.max_generated_audio,
                backend.config.max_context_bytes,
                cancel,
            )
            .await?,
        ),
        _ => None,
    };
    Ok(TurnResult {
        user_text: text,
        response: Some(response),
        audio,
    })
}
