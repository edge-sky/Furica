use super::*;
use furica_core::audio::AudioFrame;

fn fixture(name: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn recognizer() -> SenseVoiceStt {
    SenseVoiceStt::from_files(
        fixture("sensevoice-ctc.onnx"),
        fixture("sensevoice-tokens.txt"),
    )
    .unwrap()
}

fn audio(samples: usize) -> Utterance {
    Utterance {
        sample_rate: 16_000,
        frames: vec![0; samples]
            .chunks(512)
            .enumerate()
            .map(|(i, pcm)| AudioFrame {
                sequence: i as u64,
                sample_rate: 16_000,
                samples: pcm.to_vec(),
            })
            .collect(),
    }
}

#[test]
fn validates_audio_before_inference() {
    assert!(validate_audio(&audio(400)).is_ok());
    assert!(validate_audio(&audio(480_000)).is_ok());
    assert!(validate_audio(&audio(0)).is_err());
    assert!(validate_audio(&audio(399)).is_err());
    assert!(matches!(
        validate_audio(&audio(480_001)),
        Err(Error::Busy(_))
    ));
    let mut input = audio(1600);
    input.sample_rate = 24_000;
    assert!(validate_audio(&input).is_err());
    input.sample_rate = 16_000;
    input.frames[1].sequence = 0;
    assert!(validate_audio(&input).is_err());
    input.frames[1].sequence = 1;
    input.frames[1].sample_rate = 8_000;
    assert!(validate_audio(&input).is_err());
}

#[test]
fn rejects_wrong_model_and_mismatched_vocabulary() {
    assert!(
        SenseVoiceStt::from_files(fixture("mean.onnx"), fixture("sensevoice-tokens.txt")).is_err()
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tokens.txt");
    std::fs::write(&path, "<unk> 0\nbroken 2\n").unwrap();
    assert!(SenseVoiceStt::from_files(fixture("sensevoice-ctc.onnx"), &path).is_err());
    std::fs::write(&path, "<unk> 0\none 1\n").unwrap();
    assert!(SenseVoiceStt::from_files(fixture("sensevoice-ctc.onnx"), &path).is_err());
}

#[test]
fn features_match_native_reference_including_lfr_edge_replication_and_cmvn() {
    let pcm: Vec<i16> = (0..3200)
        .map(|n| ((n * 73 + (n % 97) * 111) % 60001 - 30000) as i16)
        .collect();
    let reference: Vec<f32> = include_str!("../../../tests/fixtures/sensevoice-fbank.txt")
        .lines()
        .map(|s| s.parse().unwrap())
        .collect();
    let frames = reference.len() / features::BINS;
    let actual = features::extract(
        &pcm,
        &[-2.0; features::DIM],
        &[0.5; features::DIM],
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(actual.len(), frames.div_ceil(6) * 560);
    for (i, &value) in actual.iter().enumerate() {
        let frame = (i / 560 * 6 + i % 560 / 80)
            .saturating_sub(3)
            .min(frames - 1);
        let expected = (reference[frame * 80 + i % 80] - 2.0) * 0.5;
        assert!(
            (value - expected).abs() < 0.001,
            "{i}: {value} != {expected}"
        );
    }
}

#[test]
fn ctc_collapse_blanks_tags_and_sentencepiece_spaces() {
    let tokens = decode::load_tokens(&fixture("sensevoice-tokens.txt")).unwrap();
    let ids = [1, 2, 3, 4, 5, 5, 0, 5, 7, 6];
    let mut logits = vec![-10.0; ids.len() * tokens.len()];
    for (frame, id) in ids.into_iter().enumerate() {
        logits[frame * tokens.len() + id] = 10.0;
    }
    assert_eq!(
        decode::text(&logits, &tokens, &CancellationToken::new()).unwrap(),
        "你你 好"
    );
    assert!(
        decode::text(
            &logits[4 * tokens.len()..],
            &tokens,
            &CancellationToken::new()
        )
        .is_err()
    );
    logits[0] = f32::NAN;
    assert!(decode::text(&logits, &tokens, &CancellationToken::new()).is_err());
}

#[tokio::test]
async fn final_text_is_repeatable_and_pre_cancelled_calls_fail() {
    let model = recognizer();
    for _ in 0..2 {
        assert_eq!(
            model
                .transcribe(audio(1600), CancellationToken::new())
                .await
                .unwrap(),
            "你好"
        );
    }
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        model.transcribe(audio(1600), cancel).await,
        Err(Error::Cancelled)
    ));
}

#[tokio::test]
async fn dropped_calls_retain_slot_until_worker_exits_then_allow_reuse() {
    let model = recognizer();
    // Hold the model on a separate thread, so this test never holds a blocking
    // guard across an await and the inference worker cannot finish before abort.
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let lock_model = model.clone();
    let lock_thread = std::thread::spawn(move || {
        let _session = lock_model.session.lock().unwrap();
        locked_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    locked_rx.await.unwrap();
    let other = model.clone();
    let call = tokio::spawn(async move {
        other
            .transcribe(audio(1600), CancellationToken::new())
            .await
    });
    for _ in 0..100 {
        if model.slot.available_permits() == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(model.slot.available_permits(), 0);
    assert!(matches!(
        model
            .transcribe(audio(1600), CancellationToken::new())
            .await,
        Err(Error::Busy(_))
    ));
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    release_tx.send(()).unwrap();
    lock_thread.join().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while model.slot.available_permits() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        model
            .transcribe(audio(1600), CancellationToken::new())
            .await
            .unwrap(),
        "你好"
    );
}
