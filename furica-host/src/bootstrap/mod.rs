use crate::{
    Error, Host, Result,
    configuration::application::HostConfig,
    infrastructure::{components::secret, sqlite::ConfigStore},
};
use std::{
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

pub fn prepare_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path).map_err(|_| Error::storage())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| Error::storage())?;
    }
    Ok(())
}

pub async fn start(
    data_dir: PathBuf,
    initial: Option<PathBuf>,
) -> Result<(Arc<Host>, ConfigStore, String)> {
    let data_dir = if data_dir.is_absolute() {
        data_dir
    } else {
        std::env::current_dir()
            .map_err(|_| Error::config())?
            .join(data_dir)
    };
    prepare_directory(&data_dir)?;
    // Acquire before accessing the session database. A second process must not
    // overwrite the live host's data while discovering that its port is busy.
    let directory_lock = tokio::task::spawn_blocking({
        let path = data_dir.join(".host.lock.sqlite3");
        move || {
            let connection = rusqlite::Connection::open(path).map_err(|_| Error::storage())?;
            connection.execute_batch("BEGIN EXCLUSIVE").map_err(|_| {
                Error::new(
                    503,
                    "host_data_in_use",
                    "another host owns this data directory",
                )
            })?;
            Ok::<_, Error>(connection)
        }
    })
    .await
    .map_err(|_| Error::storage())??;
    let (store, config) = tokio::task::spawn_blocking({
        let dir = data_dir.clone();
        move || {
            let config = initial
                .map(|path| {
                    let mut bytes = Vec::new();
                    std::fs::File::open(path)
                        .map_err(|_| Error::config())?
                        .take(1024 * 1024 + 1)
                        .read_to_end(&mut bytes)
                        .map_err(|_| Error::config())?;
                    if bytes.len() > 1024 * 1024 {
                        return Err(Error::config());
                    }
                    serde_json::from_slice::<HostConfig>(&bytes).map_err(|_| Error::config())
                })
                .transpose()?;
            ConfigStore::open(dir.join("configuration.sqlite3"), config.as_ref())
        }
    })
    .await
    .map_err(|_| Error::storage())??;
    std::fs::create_dir_all(&config.workspace).map_err(|_| Error::config())?;
    let token = secret(&config.http.token_env)?;
    if token.len() < 32 {
        return Err(Error::new(
            400,
            "weak_access_token",
            "host access token must contain at least 32 bytes",
        ));
    }
    let components = crate::infrastructure::components::load(&config, &data_dir).await?;
    let path = data_dir.join("configuration.sqlite3");
    let host = Host::new(components, path).await?;
    host.hold_directory_lock(directory_lock).await;
    if let Err(error) = host.flush().await {
        let _ = host.shutdown().await;
        return Err(error);
    }
    Ok((host, store, token))
}
