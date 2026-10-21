mod bootstrap;
mod conversation;
mod infrastructure;
mod sessions;
mod settings;

use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let mut port = 8765_u16;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--port" => port = args.next().ok_or("--port needs a number")?.parse()?,
            "--help" | "-h" => {
                println!(
                    "furica-web-demo [--port 8765]\nOnly listens on 127.0.0.1. Configure services in browser settings; stored in runtime/settings.sqlite3 (FURICA_DEMO_DATA_DIR overrides the directory)."
                );
                return Ok(());
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    let settings = Arc::new(settings::SettingsStore::configured());
    settings.read().await?;
    infrastructure::web::serve(settings, port).await
}
