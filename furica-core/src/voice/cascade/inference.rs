//! One bounded STT/model/TTS turn, without command receipt or playback scheduling.

use super::{
    CascadedBackend, ModelMessage, ModelRequest, ModelResponse, SynthesizedAudio, TtsStream,
    Utterance,
};
use crate::{Error, Result, voice::task_tools};
use std::{collections::HashSet, future::poll_fn, time::Duration};
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

/// The pipeline still commits playback only after complete success. Providers
/// expose only streams; collection and the conversation's limit belong here.
async fn collect_audio(
    mut stream: TtsStream,
    sample_rate: u32,
    max_audio: Duration,
    cancel: CancellationToken,
) -> Result<SynthesizedAudio> {
    let limit = (max_audio.as_nanos() * u128::from(sample_rate) / 1_000_000_000) as usize;
    let mut samples = Vec::new();
    loop {
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            next = poll_fn(|cx| stream.as_mut().poll_next(cx)) => next,
        };
        let Some(chunk) = next else {
            break;
        };
        let chunk = chunk?;
        if chunk.sample_rate != sample_rate
            || !(8_000..=48_000).contains(&chunk.sample_rate)
            || chunk.samples.is_empty()
        {
            return Err(Error::Audio("invalid synthesized audio chunk".into()));
        }
        if chunk.samples.len() > limit.saturating_sub(samples.len()) {
            return Err(Error::Busy("synthesized audio duration"));
        }
        samples.extend(chunk.samples);
    }
    if samples.is_empty() {
        return Err(Error::Audio("empty synthesized audio stream".into()));
    }
    Ok(SynthesizedAudio {
        sample_rate,
        samples,
    })
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
                let stream = backend
                    .tts
                    .synthesize_stream(reply.clone(), cancel.clone())
                    .await?;
                Some(
                    collect_audio(
                        stream,
                        backend.config.sample_rate,
                        backend.config.max_generated_audio,
                        cancel,
                    )
                    .await?,
                )
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    fn audio(samples: usize) -> SynthesizedAudio {
        SynthesizedAudio {
            sample_rate: 24_000,
            samples: vec![7; samples],
        }
    }

    async fn collect(
        items: Vec<Result<SynthesizedAudio>>,
        limit: Duration,
    ) -> Result<SynthesizedAudio> {
        collect_audio(
            Box::pin(stream::iter(items)),
            24_000,
            limit,
            CancellationToken::new(),
        )
        .await
    }

    #[tokio::test]
    async fn collects_chunks_in_order_at_exact_duration_limit() {
        let mut second = audio(120);
        second.samples.fill(9);
        let result = collect(vec![Ok(audio(120)), Ok(second)], Duration::from_millis(10))
            .await
            .unwrap();
        assert_eq!(result.samples, [vec![7; 120], vec![9; 120]].concat());
    }

    #[tokio::test]
    async fn rejects_empty_bad_rate_and_empty_chunks() {
        assert!(collect(vec![], Duration::from_secs(1)).await.is_err());
        assert!(
            collect(vec![Ok(audio(0))], Duration::from_secs(1))
                .await
                .is_err()
        );
        let mut wrong = audio(1);
        wrong.sample_rate = 16_000;
        assert!(
            collect(vec![Ok(audio(1)), Ok(wrong)], Duration::from_secs(1))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn duration_overflow_and_midstream_errors_return_no_partial_audio() {
        assert!(matches!(
            collect(
                vec![Ok(audio(240)), Ok(audio(1))],
                Duration::from_millis(10)
            )
            .await,
            Err(Error::Busy(_))
        ));
        assert!(matches!(
            collect(
                vec![Ok(audio(10)), Err(Error::Closed)],
                Duration::from_secs(1)
            )
            .await,
            Err(Error::Closed)
        ));
    }

    #[tokio::test]
    async fn cancellation_interrupts_a_pending_stream() {
        let cancel = CancellationToken::new();
        let output = collect_audio(
            Box::pin(stream::pending()),
            24_000,
            Duration::from_secs(1),
            cancel.clone(),
        );
        tokio::pin!(output);
        assert!(matches!(
            futures_util::poll!(&mut output),
            std::task::Poll::Pending
        ));
        cancel.cancel();
        assert!(matches!(output.await, Err(Error::Cancelled)));
    }
}
