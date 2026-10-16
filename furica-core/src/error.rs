#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("invalid audio: {0}")]
    Audio(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("core channel closed")]
    Closed,
    #[error("operation timed out: {0}")]
    Timeout(&'static str),
    #[error("operation cancelled")]
    Cancelled,
    #[error("task not found: {0}")]
    TaskNotFound(String),
    #[error("resource limit reached: {0}")]
    Busy(&'static str),
    #[error("invalid state: {0}")]
    State(String),
    #[error("storage error: {0}")]
    Storage(#[from] rusqlite::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
