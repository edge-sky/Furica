//! Read a local model and raw little-endian 16 kHz mono PCM16; no audio device.
use std::{
    env,
    error::Error,
    fs::File,
    io::{BufRead, BufReader, Read},
};

use furica_vad::{FRAME_SAMPLES, SileroVad, VadConfig};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: detect <silero_vad.onnx> <16khz-mono-s16le.pcm>".into());
    }
    let mut vad = SileroVad::from_file(&args[0], VadConfig::default())?;
    let mut input = BufReader::new(File::open(&args[1])?);
    let mut bytes = [0_u8; FRAME_SAMPLES * 2];
    let mut frame = [0_i16; FRAME_SAMPLES];
    let mut position = 0_u64;
    while !input.fill_buf()?.is_empty() {
        // Reject a partial final frame; the caller decides how to handle it.
        input.read_exact(&mut bytes)?;
        for (sample, pair) in frame.iter_mut().zip(bytes.as_chunks::<2>().0) {
            *sample = i16::from_le_bytes(*pair);
        }
        let output = vad.process(&frame)?;
        println!(
            "{position}\t{:.6}\t{:?}",
            output.speech_probability, output.event
        );
        position += FRAME_SAMPLES as u64;
    }
    if let Some(event) = vad.finish()? {
        println!("{event:?}");
    }
    Ok(())
}
