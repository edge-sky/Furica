//! Task bounded context: domain rules, application use cases, execution port and infrastructure.

mod application;
pub mod domain;
pub mod infrastructure;
pub mod ports;

pub use application::{TaskLimits, TaskManager};
pub use domain::{TaskEvent, TaskOutcome, TaskRecord, TaskState};
pub use ports::{ExecutionEvent, TaskExecutor};
