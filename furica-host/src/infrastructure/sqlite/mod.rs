//! SQLite persistence for host configuration and session messages.
mod configuration;
mod sessions;

pub use configuration::ConfigStore;
pub use sessions::SessionStore;

use crate::{Error, Result};
use rusqlite::{Connection, OpenFlags};
use std::path::Path;

fn connection(path: &Path, create: bool) -> Result<Connection> {
    let mut flags = OpenFlags::SQLITE_OPEN_READ_WRITE;
    if create {
        flags |= OpenFlags::SQLITE_OPEN_CREATE;
    }
    let db = Connection::open_with_flags(path, flags).map_err(|_| Error::storage())?;
    db.busy_timeout(std::time::Duration::from_secs(2))
        .map_err(|_| Error::storage())?;
    db.execute_batch("PRAGMA synchronous=FULL;")
        .map_err(|_| Error::storage())?;
    Ok(db)
}
