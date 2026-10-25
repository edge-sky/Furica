//! Bounded synthesis use case and complete-result collection, without playback scheduling.

use super::{SynthesizedAudio, Tts, TtsStream};
use crate::{Error, Result};
use std::{future::poll_fn, time::Duration};
use tokio_util::sync::CancellationToken;

/// Synthesize and collect one bounded result without scheduling playback.
/// Adapters implement only `synthesize_stream`; collection is shared here.
/// The caller owns the deadline; cancellation covers opening and consumption.
pub async fn synthesize(
    adapter: &dyn Tts,
    text: String,
    sample_rate: u32,
    max_audio: Duration,
    max_text_bytes: usize,
    cancel: CancellationToken,
) -> Result<SynthesizedAudio> {
    if text.trim().is_empty() {
        return Err(Error::Protocol("TTS text must not be blank".into()));
    }
    if text.len() > max_text_bytes {
        return Err(Error::Busy("TTS text"));
    }
    let stream = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(Error::Cancelled),
        result = adapter.synthesize_stream(text, cancel.clone()) => result?,
    };
    collect_audio(stream, sample_rate, max_audio, cancel).await
}

/// Collect only a complete successful synthesis; partial audio is discarded.
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
