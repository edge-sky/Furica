use std::{ffi::OsString, sync::atomic::Ordering};

use furica_vad::FRAME_SAMPLES;

use super::{
    capture::{self, Control},
    *,
};

fn args(values: &[&str]) -> Vec<OsString> {
    values.iter().map(OsString::from).collect()
}

#[test]
fn arguments_default_to_manifest_model_and_allow_help_without_a_device() {
    let defaults = Options::parse(args(&[])).unwrap().unwrap();
    assert_eq!(
        defaults.model,
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("model/silero_vad.onnx")
    );
    assert!(!defaults.probabilities);
    let custom = Options::parse(args(&["--probabilities", "/tmp/custom.onnx"]))
        .unwrap()
        .unwrap();
    assert_eq!(custom.model, PathBuf::from("/tmp/custom.onnx"));
    assert!(custom.probabilities);
    assert!(Options::parse(args(&["--help"])).unwrap().is_none());
    assert!(Options::parse(args(&["a", "b"])).is_err());
    assert!(Options::parse(args(&["--unknown"])).is_err());
}

#[test]
fn sample_formats_are_normalized_downmixed_and_clipped() {
    let control = Control::default();
    let (mut producer, mut consumer) = rtrb::RingBuffer::new(16);
    capture::enqueue(&[i16::MIN, i16::MAX, 0, 0], 2, &mut producer, &control);
    assert!((consumer.pop().unwrap() + 1.0 / 65536.0).abs() < 1e-7);
    assert_eq!(consumer.pop().unwrap(), 0.0);
    capture::enqueue(&[0_u16, u16::MAX, 32768, 32768], 2, &mut producer, &control);
    assert!((consumer.pop().unwrap() + 1.0 / 65536.0).abs() < 1e-7);
    assert_eq!(consumer.pop().unwrap(), 0.0);
    capture::enqueue(&[2.0_f32, 0.0, -2.0, 0.0], 2, &mut producer, &control);
    assert_eq!(consumer.pop().unwrap(), 0.5);
    assert_eq!(consumer.pop().unwrap(), -0.5);
    control.check().unwrap();
}

#[test]
fn overflow_never_overwrites_or_resumes_after_a_gap() {
    let control = Control::default();
    let (mut producer, mut consumer) = rtrb::RingBuffer::new(3);
    capture::enqueue(&[0.1_f32, 0.2], 1, &mut producer, &control);
    capture::enqueue(&[0.3_f32, 0.4], 1, &mut producer, &control);
    assert!(
        control
            .check()
            .unwrap_err()
            .to_string()
            .contains("overflow")
    );
    assert_eq!(consumer.pop().unwrap(), 0.1);
    assert_eq!(consumer.pop().unwrap(), 0.2);
    assert!(consumer.pop().is_err());
    capture::enqueue(&[0.5_f32], 1, &mut producer, &control);
    assert!(consumer.pop().is_err());
}

#[test]
fn invalid_capture_and_device_faults_stop_instead_of_synthesizing_silence() {
    for input in [&[f32::NAN, 0.0][..], &[f32::INFINITY, 0.0], &[0.0]] {
        let control = Control::default();
        let (mut producer, mut consumer) = rtrb::RingBuffer::new(8);
        capture::enqueue(input, 2, &mut producer, &control);
        assert!(control.check().is_err());
        assert!(consumer.pop().is_err());
    }
    assert!(capture::capacity(0, 1).is_err());
    assert!(capture::capacity(192001, 1).is_err());
    assert!(capture::capacity(48000, 0).is_err());
    assert!(capture::capacity(48000, 33).is_err());
    assert_eq!(capture::capacity(48000, 2).unwrap(), 12000);
    let control = Control::default();
    control.fail(DEVICE_ERROR);
    assert!(
        control
            .check()
            .unwrap_err()
            .to_string()
            .contains("stream failed")
    );
}

fn convert(rate: u32, mono: &[f32], blocks: &[usize]) -> (Vec<i16>, (usize, usize)) {
    let mut pipeline = Pipeline::new(rate).unwrap();
    let control = Control::default();
    let (mut producer, mut consumer) = rtrb::RingBuffer::new(mono.len());
    let mut output = Vec::new();
    let mut position = 0;
    let mut index = 0;
    while position < mono.len() {
        let end = (position + blocks[index % blocks.len()]).min(mono.len());
        capture::enqueue(&mono[position..end], 1, &mut producer, &control);
        control.check().unwrap();
        while let Ok(sample) = consumer.pop() {
            pipeline
                .push(sample, &mut |frame| {
                    output.extend_from_slice(frame);
                    Ok(())
                })
                .unwrap();
        }
        position = end;
        index += 1;
    }
    (output, pipeline.pending())
}

#[test]
fn framing_and_resampling_do_not_depend_on_callback_boundaries() {
    for rate in [8000, 16000, 44100, 48000, 96000, 192000] {
        let audio: Vec<f32> = (0..rate / 3)
            .map(|n| ((n * 17 % 503) as f32 - 251.0) / 300.0)
            .collect();
        let contiguous = convert(rate, &audio, &[audio.len()]);
        let fragmented = convert(rate, &audio, &[1, 37, 512, 3, 1024, 111]);
        assert_eq!(contiguous, fragmented, "sample rate {rate}");
        assert_eq!(contiguous.0.len() % FRAME_SAMPLES, 0);
        assert!(contiguous.1.1 < FRAME_SAMPLES);
        let expected = audio.len() as f64 * 16000.0 / f64::from(rate);
        assert!((expected - contiguous.0.len() as f64).abs() < 1024.0);
    }
}

#[test]
fn pcm16_conversion_saturates_and_reports_unpadded_partial_frame() {
    let audio: Vec<f32> = [-2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0]
        .into_iter()
        .cycle()
        .take(520)
        .collect();
    let (converted, pending) = convert(16000, &audio, &[5, 17, 1]);
    assert_eq!(
        &converted[..7],
        &[i16::MIN, i16::MIN, -16384, 0, 16384, i16::MAX, i16::MAX]
    );
    assert_eq!(converted.len(), FRAME_SAMPLES);
    assert_eq!(pending, (0, 8));
    let mut pipeline = Pipeline::new(48000).unwrap();
    for _ in 0..17 {
        pipeline
            .push(0.0, &mut |_| panic!("must not pad a short block"))
            .unwrap();
    }
    assert_eq!(pipeline.pending(), (17, 0));
    assert!(pipeline.push(f32::NAN, &mut |_| Ok(())).is_err());
}

#[test]
fn resampling_preserves_speech_band_and_rejects_aliasing() {
    for rate in [44100, 48000] {
        let tone = |hz: f32| {
            (0..rate / 2)
                .map(|n| (std::f32::consts::TAU * hz * n as f32 / rate as f32).sin() * 0.5)
                .collect::<Vec<_>>()
        };
        let rms = |samples: &[i16]| {
            (samples[512..]
                .iter()
                .map(|&x| (f64::from(x) / 32768.0).powi(2))
                .sum::<f64>()
                / (samples.len() - 512) as f64)
                .sqrt()
        };
        let speech = convert(rate, &tone(1000.0), &[127]).0;
        let ultrasonic = convert(rate, &tone(12000.0), &[127]).0;
        assert!((rms(&speech) - 0.35355).abs() < 0.015);
        assert!(rms(&ultrasonic) < rms(&speech) * 0.02);
    }
}

fn test_vad(name: &str) -> SileroVad {
    SileroVad::from_file(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name),
        VadConfig::default(),
    )
    .unwrap()
}

#[test]
fn shutdown_drains_queued_audio_and_finishes_once_without_padding() {
    let control = Arc::new(Control::default());
    let (mut producer, consumer) = rtrb::RingBuffer::new(2048);
    capture::enqueue(
        &[0.9_f32; 3 * FRAME_SAMPLES + 19],
        1,
        &mut producer,
        &control,
    );
    let worker_control = Arc::clone(&control);
    let (tx, rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let mut output = Vec::new();
        let result = consume(
            consumer,
            &worker_control,
            Pipeline::new(16000).unwrap(),
            test_vad("mean.onnx"),
            false,
            &mut output,
        );
        tx.send((result, output)).unwrap();
    });
    // Simulate Ctrl+C and device closure while a worker is running. A stop
    // request alone must not discard already queued audio.
    assert!(rx.try_recv().is_err());
    control.stop.store(true, Ordering::Release);
    drop(producer);
    control.closed.store(true, Ordering::Release);
    let (result, output) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    worker.join().unwrap();
    result.unwrap();
    let output = String::from_utf8(output).unwrap();
    assert_eq!(output.lines().count(), 2);
    assert!(output.contains("SpeechStarted sample=0"));
    assert!(output.contains("SpeechEnded sample=1536"));
}

#[test]
fn ending_an_utterance_keeps_listening_for_the_next_one() {
    let control = Control::default();
    let (mut producer, consumer) = rtrb::RingBuffer::new(24 * FRAME_SAMPLES);
    for _ in 0..2 {
        capture::enqueue(&[0.9_f32; 2 * FRAME_SAMPLES], 1, &mut producer, &control);
        capture::enqueue(&[0.0_f32; 10 * FRAME_SAMPLES], 1, &mut producer, &control);
    }
    control.check().unwrap();
    drop(producer);
    control.closed.store(true, Ordering::Release);
    let mut output = Vec::new();
    consume(
        consumer,
        &control,
        Pipeline::new(16000).unwrap(),
        test_vad("mean.onnx"),
        false,
        &mut output,
    )
    .unwrap();
    let output = String::from_utf8(output).unwrap();
    let lines: Vec<_> = output.lines().collect();
    assert_eq!(lines.len(), 4);
    assert!(lines[0].contains("SpeechStarted sample=0"));
    assert!(lines[1].contains("SpeechEnded sample=1024"));
    assert!(lines[2].contains("SpeechStarted sample=6144"));
    assert!(lines[3].contains("SpeechEnded sample=7168"));
}

#[test]
fn fault_and_inference_failure_do_not_emit_normal_finish() {
    for fault in [true, false] {
        let control = Control::default();
        let (mut producer, consumer) = rtrb::RingBuffer::new(1024);
        capture::enqueue(&[0.9_f32; FRAME_SAMPLES], 1, &mut producer, &control);
        drop(producer);
        control.closed.store(true, Ordering::Release);
        if fault {
            control.fail(DEVICE_ERROR);
        }
        let mut output = Vec::new();
        assert!(
            consume(
                consumer,
                &control,
                Pipeline::new(16000).unwrap(),
                test_vad("inference-error.onnx"),
                false,
                &mut output
            )
            .is_err()
        );
        assert!(output.is_empty());
    }
}
