mod capture;
mod pipeline;
#[cfg(feature = "sensevoice")]
mod stt;

use std::{
    env,
    error::Error,
    ffi::OsString,
    io::{self, Write},
    path::PathBuf,
    sync::{Arc, atomic::Ordering, mpsc},
    thread,
    time::Duration,
};

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use furica_local::{SAMPLE_RATE, SileroVad, VadConfig, VadEvent, VadOutput};
use rtrb::Consumer;

use capture::{Control, DEVICE_ERROR};
use pipeline::Pipeline;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

struct Options {
    model: PathBuf,
    probabilities: bool,
    transcribe: bool,
}

impl Options {
    fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Option<Self>> {
        let mut model = None;
        let mut probabilities = false;
        let mut transcribe = false;
        for arg in args {
            if arg == "--help" || arg == "-h" {
                return Ok(None);
            } else if arg == "--probabilities" {
                probabilities = true;
            } else if arg == "--transcribe" {
                transcribe = true;
            } else if arg.to_string_lossy().starts_with('-') || model.is_some() {
                return Err(
                    "usage: microphone [model.onnx] [--probabilities] [--transcribe]".into(),
                );
            } else {
                model = Some(PathBuf::from(arg));
            }
        }
        Ok(Some(Self {
            model: model.unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("model/silero_vad.onnx")
            }),
            probabilities,
            transcribe,
        }))
    }
}

pub fn run() -> Result<()> {
    let Some(options) = Options::parse(env::args_os().skip(1))? else {
        println!(
            "usage: microphone [model.onnx] [--probabilities] [--transcribe]\ndefault model: {}/model/silero_vad.onnx\n--transcribe requires --features sensevoice and the bundled local ASR model.\nCaptures the default macOS microphone until Ctrl+C. No audio is saved.",
            env!("CARGO_MANIFEST_DIR")
        );
        return Ok(());
    };
    #[cfg(not(feature = "sensevoice"))]
    if options.transcribe {
        return Err("--transcribe requires --features sensevoice".into());
    }
    #[cfg(feature = "sensevoice")]
    let recognizer = if options.transcribe {
        Some(stt::load()?)
    } else {
        None
    };
    // All model loading and processing buffers precede starting the device.
    let vad = SileroVad::from_file(&options.model, VadConfig::default())?;
    let host = cpal::default_host();
    // CPAL's macOS backend snapshots the current physical input device.
    let device = host
        .default_input_device()
        .ok_or("no default microphone is available")?;
    let supported = device.default_input_config().map_err(device_error)?;
    let channels = usize::from(supported.channels());
    let rate = supported.sample_rate();
    let capacity = capture::capacity(rate, channels)?;
    let pipeline = Pipeline::new(rate)?;
    let (producer, consumer) = rtrb::RingBuffer::new(capacity);
    let control = Arc::new(Control::default());
    let stop_signal = Arc::clone(&control);
    ctrlc::set_handler(move || stop_signal.stop.store(true, Ordering::Release))?;
    let (error_tx, error_rx) = mpsc::sync_channel(1);
    let stream = capture::build_stream(
        &device,
        &supported,
        producer,
        Arc::clone(&control),
        error_tx,
    )
    .map_err(device_error)?;
    #[cfg(feature = "sensevoice")]
    let (speech_tx, speech_worker, speech_cancel) = stt::start(recognizer, Arc::clone(&control))?;
    eprintln!("模型: {}", options.model.display());
    eprintln!(
        "麦克风: {} | {rate} Hz | {channels} 声道 | {:?}",
        device.description().map_err(device_error)?.name(),
        supported.sample_format()
    );
    eprintln!(
        "VAD: 16000 Hz 单声道 PCM16, 512 样本/帧; 缓冲容量: 250 ms; 重采样块: {} 输入样本, 滤波延迟: {} 输出样本",
        pipeline.input_block(),
        pipeline.output_delay()
    );
    let worker_control = Arc::clone(&control);
    let worker = thread::Builder::new()
        .name("microphone-vad".into())
        .spawn(move || {
            #[cfg(feature = "sensevoice")]
            if let Some(sender) = speech_tx {
                return consume_with(
                    consumer,
                    &worker_control,
                    pipeline,
                    vad,
                    options.probabilities,
                    &mut io::stdout(),
                    &mut stt::segments(sender),
                );
            }
            consume(
                consumer,
                &worker_control,
                pipeline,
                vad,
                options.probabilities,
                &mut io::stdout(),
            )
        })?;
    let started = stream.play();
    if started.is_ok() {
        eprintln!(
            "持续监听中，按 Ctrl+C 停止。时间位置使用 VAD 输出音频时间轴。\n首次使用请按系统提示授权麦克风；拒绝后可检查“系统设置 → 隐私与安全性 → 麦克风”。"
        );
        while !control.stop.load(Ordering::Acquire) && !worker.is_finished() {
            thread::sleep(Duration::from_millis(10));
        }
    }
    control.stop.store(true, Ordering::Release);
    let stopped = if started.is_ok() {
        stream.pause()
    } else {
        Ok(())
    };
    // No callback can append new audio after closure; only then allow draining
    // to terminate and emit finish. A failed play must not emit a normal end.
    if started.is_err() || stopped.is_err() {
        control.fail(DEVICE_ERROR);
    }
    drop(stream);
    control.closed.store(true, Ordering::Release);
    let result = worker
        .join()
        .map_err(|_| "microphone inference thread panicked")?;
    #[cfg(feature = "sensevoice")]
    let speech_result = {
        if result.is_err() || started.is_err() || stopped.is_err() {
            speech_cancel.cancel();
        }
        if let Some(worker) = speech_worker {
            worker
                .join()
                .map_err(|_| "speech recognition thread panicked")?
        } else {
            Ok(())
        }
    };
    started.map_err(device_error)?;
    stopped.map_err(device_error)?;
    if control.fault() == DEVICE_ERROR
        && let Ok(error) = error_rx.try_recv()
    {
        return Err(device_error(error));
    }
    result?;
    #[cfg(feature = "sensevoice")]
    speech_result?;
    Ok(())
}

fn device_error(error: cpal::Error) -> Box<dyn Error + Send + Sync> {
    if error.kind() == cpal::ErrorKind::PermissionDenied {
        eprintln!(
            "麦克风权限被拒绝：请在“系统设置 → 隐私与安全性 → 麦克风”中允许当前终端或 IDE 访问。"
        );
    }
    Box::new(error)
}

fn consume(
    input: Consumer<f32>,
    control: &Control,
    pipeline: Pipeline,
    vad: SileroVad,
    probabilities: bool,
    output: &mut impl Write,
) -> Result<()> {
    consume_with(
        input,
        control,
        pipeline,
        vad,
        probabilities,
        output,
        &mut |_, _, _| Ok(()),
    )
}

fn consume_with(
    mut input: Consumer<f32>,
    control: &Control,
    mut pipeline: Pipeline,
    mut vad: SileroVad,
    probabilities: bool,
    output: &mut impl Write,
    recognize: &mut impl FnMut(&[i16], u64, VadOutput) -> Result<()>,
) -> Result<()> {
    let mut position = 0_u64;
    loop {
        control.check()?;
        let mut consumed = false;
        while let Ok(sample) = input.pop() {
            consumed = true;
            control.check()?;
            pipeline.push(sample, &mut |frame| {
                control.check()?;
                let result = vad.process(frame)?;
                control.check()?;
                write_output(output, position, result, probabilities)?;
                recognize(frame, position, result)?;
                position = position
                    .checked_add(frame.len() as u64)
                    .ok_or("VAD output position overflow")?;
                Ok(())
            })?;
        }
        if control.closed.load(Ordering::Acquire) {
            // The final producer callback may have published after the last
            // pop but before closed was observed. Recheck after its release.
            if input.slots() != 0 {
                continue;
            }
            control.check()?;
            if let Some(event) = vad.finish()? {
                write_event(output, event)?;
                recognize(
                    &[],
                    position,
                    VadOutput {
                        speech_probability: 0.0,
                        event: Some(event),
                    },
                )?;
            }
            let (native, pcm) = pipeline.pending();
            eprintln!(
                "监听已停止。舍弃尾部: {native} 输入样本 @ {} Hz, {pcm} 未满帧样本 @ 16000 Hz；重采样滤波尾部未冲刷（滤波延迟 {} 输出样本）。",
                pipeline.input_rate(),
                pipeline.output_delay()
            );
            return Ok(());
        }
        if !consumed {
            thread::sleep(Duration::from_millis(2));
        }
    }
}

fn write_output(
    output: &mut impl Write,
    position: u64,
    result: VadOutput,
    probabilities: bool,
) -> Result<()> {
    if probabilities {
        writeln!(
            output,
            "frame={position} time={:.3}s probability={:.6}",
            position as f64 / f64::from(SAMPLE_RATE),
            result.speech_probability
        )?;
    }
    if let Some(event) = result.event {
        write_event(output, event)?;
    }
    Ok(())
}

fn write_event(output: &mut impl Write, event: VadEvent) -> Result<()> {
    let (name, sample) = match event {
        VadEvent::SpeechStarted { start_sample } => ("SpeechStarted", start_sample),
        VadEvent::SpeechEnded { end_sample } => ("SpeechEnded", end_sample),
    };
    writeln!(
        output,
        "{name} sample={sample} time={:.3}s",
        sample as f64 / f64::from(SAMPLE_RATE)
    )?;
    output.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests;
