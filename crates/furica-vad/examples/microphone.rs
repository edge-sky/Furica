//! Continuous default-microphone VAD; run with --help without opening a device.
#[cfg(target_os = "macos")]
#[path = "microphone/mod.rs"]
mod microphone;

#[cfg(target_os = "macos")]
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    microphone::run()
}

#[cfg(not(target_os = "macos"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err("the microphone example currently supports macOS only".into())
}
