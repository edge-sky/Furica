//! One turn's control flow. Capability modules own validation and provider calls.

use super::CascadedBackend;
use crate::{
    Result,
    orchestration::execution_tools,
    stt::{self, Utterance},
    text_model::{self, FinishReason, ModelMessage, ModelRequest, ModelResponse},
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
    sessions: crate::sessions::SessionManager,
    session_id: uuid::Uuid,
    input: TurnInput,
    cancel: CancellationToken,
) -> Result<TurnResult> {
    let user_source = match &input {
        TurnInput::Audio(_) => Some(crate::sessions::MessageSource::UserVoice {
            stt: backend.stt.source().map(str::to_owned),
        }),
        TurnInput::Text(_) => Some(crate::sessions::MessageSource::UserText),
        TurnInput::Continue => None,
    };
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
        let message = ModelMessage::User(text.clone());
        sessions.append_message(
            session_id,
            user_source.clone().expect("user input has a source"),
            message.clone(),
            crate::sessions::MessageState::Complete,
        )?;
        history.push(message);
    }
    let response = text_model::generate(
        backend.model.as_ref(),
        ModelRequest {
            messages: history,
            tools: execution_tools(),
            instructions: backend.config.instructions.clone(),
            max_output_tokens: backend.config.max_output_tokens,
            reasoning: backend.config.reasoning,
        },
        backend.config.max_history_messages,
        backend.config.max_context_bytes,
        cancel.clone(),
    )
    .await?;
    let state = if response.finish_reason == FinishReason::Truncated {
        crate::sessions::MessageState::Interrupted
    } else {
        crate::sessions::MessageState::Complete
    };
    sessions.append_message(
        session_id,
        crate::sessions::MessageSource::Model {
            source: backend.model.continuation_source().to_owned(),
        },
        ModelMessage::Assistant(response.message.clone()),
        state,
    )?;
    let reply = response.message.text();
    let audio = if matches!(
        response.finish_reason,
        FinishReason::Complete | FinishReason::Refused
    ) && !reply.trim().is_empty()
    {
        Some(
            tts::synthesize(
                backend.tts.as_ref(),
                reply,
                backend.config.sample_rate,
                backend.config.max_generated_audio,
                backend.config.max_context_bytes,
                cancel,
            )
            .await?,
        )
    } else {
        None
    };
    Ok(TurnResult {
        user_text: text,
        response: Some(response),
        audio,
    })
}
