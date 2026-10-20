//! Offline file example; does not access a microphone or a network.
use std::{env, path::PathBuf, time::Instant};

use furica_core::{
    audio::AudioFrame,
    stt::{Stt, Utterance},
};
use furica_local::asr::SenseVoiceStt;
use tokio_util::sync::CancellationToken;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() != 1 || args[0] == "--help" {
        println!(
            "usage: transcribe <16kHz-mono-pcm16.wav>\nUses model/sensevoice-small.int8.onnx and model/sensevoice-tokens.txt."
        );
        return if args.first().is_some_and(|arg| arg == "--help") {
            Ok(())
        } else {
            Err("expected one WAV path".into())
        };
    }
    let mut wav = hound::WavReader::open(&args[0])?;
    let spec = wav.spec();
    if spec.sample_rate != 16_000
        || spec.channels != 1
        || spec.bits_per_sample != 16
        || spec.sample_format != hound::SampleFormat::Int
    {
        return Err("expected 16 kHz mono PCM16 WAV".into());
    }
    if wav.duration() > 30 * 16_000 {
        return Err("audio exceeds 30 seconds".into());
    }
    let samples = wav.samples::<i16>().collect::<Result<Vec<_>, _>>()?;
    let utterance = Utterance {
        sample_rate: 16_000,
        frames: samples
            .chunks(512)
            .enumerate()
            .map(|(i, samples)| AudioFrame {
                sequence: i as u64,
                sample_rate: 16_000,
                samples: samples.to_vec(),
            })
            .collect(),
    };
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("model");
    let started = Instant::now();
    let recognizer = SenseVoiceStt::from_files(
        dir.join("sensevoice-small.int8.onnx"),
        dir.join("sensevoice-tokens.txt"),
    )?;
    eprintln!(
        "Model loaded and warmed up in {:.3}s",
        started.elapsed().as_secs_f64()
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let started = Instant::now();
    let text = runtime.block_on(recognizer.transcribe(utterance, CancellationToken::new()))?;
    println!("{text}");
    eprintln!("Transcribed in {:.3}s", started.elapsed().as_secs_f64());
    Ok(())
}
