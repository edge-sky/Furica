use super::now_ms;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionState {
    Active,
    Archived,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    pub id: Uuid,
    pub name: String,
    pub state: SessionState,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl Session {
    /// The first main-context user message supplies the automatic name.
    pub fn new() -> Self {
        let now = now_ms();
        Self {
            id: Uuid::new_v4(),
            name: "新会话".into(),
            state: SessionState::Active,
            created_at_ms: now,
            updated_at_ms: now,
        }
    }
}
impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}
