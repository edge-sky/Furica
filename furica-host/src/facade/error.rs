#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct Error {
    pub status: u16,
    pub code: &'static str,
    pub message: &'static str,
    pub retryable: bool,
}

impl Error {
    pub const fn new(status: u16, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
            retryable: status == 429 || status == 503,
        }
    }
    pub const fn storage() -> Self {
        Self::new(503, "storage_failed", "persistent storage is unavailable")
    }
    pub const fn config() -> Self {
        Self::new(
            400,
            "invalid_configuration",
            "configuration is invalid or incomplete",
        )
    }
    pub const fn conflict() -> Self {
        Self::new(
            409,
            "interaction_conflict",
            "interaction is occupied or session must be selected explicitly",
        )
    }
}

impl From<furica_core::Error> for Error {
    fn from(error: furica_core::Error) -> Self {
        use furica_core::Error::*;
        match error {
            NotFound(_) => Self::new(404, "not_found", "resource does not exist"),
            Busy(_) => Self::new(
                429,
                "backpressure",
                "bounded input queue or resource is full",
            ),
            Timeout(_) => Self::new(504, "runtime_timeout", "runtime operation timed out"),
            Config(_) | Audio(_) | Json(_) => Self::new(
                400,
                "invalid_input",
                "input violates the capability contract",
            ),
            State(_) => Self::conflict(),
            _ => Self::new(503, "runtime_failed", "capability operation failed"),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
