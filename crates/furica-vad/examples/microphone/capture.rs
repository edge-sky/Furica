use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, Ordering},
    mpsc::SyncSender,
};

use cpal::{FromSample, Sample, SampleFormat, SizedSample, traits::DeviceTrait};
use rtrb::Producer;

use super::Result;

const OVERFLOW: u8 = 1;
const INVALID_AUDIO: u8 = 2;
pub(super) const DEVICE_ERROR: u8 = 3;

#[derive(Default)]
pub(super) struct Control {
    pub stop: AtomicBool,
    pub closed: AtomicBool,
    fault: AtomicU8,
}

impl Control {
    pub fn fail(&self, fault: u8) {
        let _ = self
            .fault
            .compare_exchange(0, fault, Ordering::AcqRel, Ordering::Acquire);
        self.stop.store(true, Ordering::Release);
    }

    pub fn fault(&self) -> u8 {
        self.fault.load(Ordering::Acquire)
    }

    pub fn check(&self) -> Result<()> {
        match self.fault() {
            0 => Ok(()),
            OVERFLOW => Err("microphone buffer overflow: input was not continuous; listening stopped".into()),
            INVALID_AUDIO => Err("microphone delivered invalid channel layout or non-finite audio; listening stopped".into()),
            _ => Err("microphone stream failed; listening stopped".into()),
        }
    }
}

pub(super) fn capacity(rate: u32, channels: usize) -> Result<usize> {
    if !(8_000..=192_000).contains(&rate) || !(1..=32).contains(&channels) {
        return Err("microphone example requires 8–192 kHz and 1–32 channels".into());
    }
    Ok((rate as usize).div_ceil(4))
}

pub(super) fn build_stream(
    device: &cpal::Device,
    supported: &cpal::SupportedStreamConfig,
    producer: Producer<f32>,
    control: Arc<Control>,
    error_tx: SyncSender<cpal::Error>,
) -> std::result::Result<cpal::Stream, cpal::Error> {
    match supported.sample_format() {
        SampleFormat::F32 => build::<f32>(device, supported, producer, control, error_tx),
        SampleFormat::F64 => build::<f64>(device, supported, producer, control, error_tx),
        SampleFormat::I16 => build::<i16>(device, supported, producer, control, error_tx),
        SampleFormat::U16 => build::<u16>(device, supported, producer, control, error_tx),
        SampleFormat::I32 => build::<i32>(device, supported, producer, control, error_tx),
        SampleFormat::U32 => build::<u32>(device, supported, producer, control, error_tx),
        _ => Err(cpal::Error::with_message(
            cpal::ErrorKind::UnsupportedConfig,
            "supported microphone formats: f32, f64, i16, u16, i32, u32",
        )),
    }
}

fn build<T: SizedSample>(
    device: &cpal::Device,
    supported: &cpal::SupportedStreamConfig,
    mut producer: Producer<f32>,
    control: Arc<Control>,
    error_tx: SyncSender<cpal::Error>,
) -> std::result::Result<cpal::Stream, cpal::Error>
where
    f32: FromSample<T>,
{
    let channels = usize::from(supported.channels());
    let error_control = Arc::clone(&control);
    device.build_input_stream(
        (*supported).into(),
        move |data: &[T], _| enqueue(data, channels, &mut producer, &control),
        move |error| {
            if !error_control.stop.load(Ordering::Acquire) {
                // Only the error callback uses this bounded channel. The data
                // callback below uses no locks, allocations, or I/O.
                let _ = error_tx.try_send(error);
                error_control.fail(DEVICE_ERROR);
            }
        },
        Some(std::time::Duration::from_secs(5)),
    )
}

pub(super) fn enqueue<T: Sample>(
    data: &[T],
    channels: usize,
    producer: &mut Producer<f32>,
    control: &Control,
) where
    f32: FromSample<T>,
{
    if control.stop.load(Ordering::Acquire) {
        return;
    }
    if channels == 0 || !data.len().is_multiple_of(channels) {
        control.fail(INVALID_AUDIO);
        return;
    }
    // Fail before appending a partial callback if the whole block cannot fit.
    if producer.slots() < data.len() / channels {
        control.fail(OVERFLOW);
        return;
    }
    for interleaved in data.chunks_exact(channels) {
        let mut mono = 0.0;
        for &sample in interleaved {
            let sample: f32 = sample.to_sample();
            if !sample.is_finite() {
                control.fail(INVALID_AUDIO);
                return;
            }
            mono += sample.clamp(-1.0, 1.0) / channels as f32;
        }
        if producer.push(mono).is_err() {
            control.fail(OVERFLOW);
            return;
        }
    }
}
