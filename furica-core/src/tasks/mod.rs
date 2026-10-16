//! Background task lifecycle and use cases. Execution and persistence live in adapters.

pub mod adapters;
pub mod domain;
mod manager;
pub mod ports;

pub use domain::{TaskEvent, TaskOutcome, TaskRecord, TaskState};
pub use manager::{TaskLimits, TaskManager};
pub use ports::{ExecutionEvent, TaskExecutor};
