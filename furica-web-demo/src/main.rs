mod bootstrap;
mod conversation;
mod infrastructure;
mod sessions;

use std::{path::PathBuf, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let mut config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.json");
    let mut port = 8765_u16;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config_path = args.next().ok_or("--config needs a path")?.into(),
            "--port" => port = args.next().ok_or("--port needs a number")?.parse()?,
            "--help" | "-h" => {
                println!(
                    "furica-web-demo [--config PATH] [--port 8765]\nOnly listens on 127.0.0.1. Missing config opens the configuration checklist."
                );
                return Ok(());
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    let config = bootstrap::Config::load(&config_path)?;
    infrastructure::web::serve(Arc::new(config), port).await
}
