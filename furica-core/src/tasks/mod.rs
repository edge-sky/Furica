//! Task bounded context: ACP sessions, execution rounds, questions and persistence.

mod application;
pub mod domain;
pub mod infrastructure;
pub mod ports;

pub use application::{TaskLimits, TaskManager};
pub use domain::{
    AcpSessionRecord, AcpSessionState, PermissionOption, PermissionOptionKind, QuestionState,
    RunOutcome, TaskAnswer, TaskEvent, TaskQuestion, TaskQuestionDraft, TaskQuestionKind, TaskRun,
    TaskRunState, TaskStatus,
};
pub use ports::{ExecutionEvent, SessionInput, TaskExecutor};
