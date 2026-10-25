//! Host-constructed execution candidates. Paths belong to agents, not Core storage.
use crate::sessions::Executor;
use std::{path::PathBuf, sync::Arc};

#[derive(Clone, Debug)]
pub struct WorkspaceOption {
    pub id: String,
    pub description: String,
    pub path: PathBuf,
}

#[derive(Clone)]
pub struct AgentBinding {
    pub id: String,
    pub description: String,
    pub executor: Arc<dyn Executor>,
    pub workspaces: Vec<WorkspaceOption>,
}
