use furica_host::{bootstrap, facade::http};
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{}: {}", error.code, error.message);
        std::process::exit(1);
    }
}

async fn run() -> furica_host::Result<()> {
    let mut args = std::env::args_os().skip(1);
    let Some(directory) = args.next() else {
        eprintln!("usage: furica-host <data-directory> [--initialize <configuration.json>]");
        return Err(furica_host::Error::config());
    };
    let initial = match args.next() {
        Some(flag) if flag == "--initialize" => Some(PathBuf::from(
            args.next().ok_or_else(furica_host::Error::config)?,
        )),
        None => None,
        _ => return Err(furica_host::Error::config()),
    };
    if args.next().is_some() {
        return Err(furica_host::Error::config());
    }
    let (host, store, token) = bootstrap::start(PathBuf::from(directory), initial).await?;
    let listener = match tokio::net::TcpListener::bind(host.config.http.listen).await {
        Ok(listener) => listener,
        Err(_) => {
            let _ = host.shutdown().await;
            return Err(furica_host::Error::new(
                503,
                "listen_failed",
                "could not bind the loopback listener",
            ));
        }
    };
    eprintln!(
        "furica-host listening on {}",
        listener
            .local_addr()
            .map_err(|_| furica_host::Error::config())?
    );
    let result = http::serve(listener, host.clone(), store, token, async {
        #[cfg(unix)]
        {
            if let Ok(mut terminate) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                tokio::select! {_=tokio::signal::ctrl_c()=>(),_=terminate.recv()=>()}
            } else {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    })
    .await;
    let shutdown = host.shutdown().await;
    result.and(shutdown)
}
