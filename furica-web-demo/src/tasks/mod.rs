//! Direct host controls reuse TaskManager ownership checks and existing objects.
use furica_core::{
    Result,
    tasks::{TaskAnswer, TaskEvent, TaskManager, infrastructure::sqlite::TaskStore},
};
use serde_json::{Value, json};
use uuid::Uuid;

pub fn snapshot(store: &TaskStore) -> Result<Value> {
    Ok(
        json!({"type":"task_snapshot", "sessions":store.list_sessions()?,
        "runs":store.list_all_runs()?, "questions":store.list_all_questions()?}),
    )
}

pub fn event(event: TaskEvent) -> Value {
    match event {
        TaskEvent::SessionChanged(session) => {
            json!({"type":"task", "kind":"session", "session":session})
        }
        TaskEvent::RunChanged {
            conversation_id,
            run,
        } => json!({"type":"task", "kind":"run", "conversation_id":conversation_id,"run":run}),
        TaskEvent::Progress {
            conversation_id,
            run_id,
            update,
        } => {
            json!({"type":"task", "kind":"progress", "conversation_id":conversation_id,"run_id":run_id,"update":update})
        }
        TaskEvent::QuestionAsked {
            conversation_id,
            question,
        } => {
            json!({"type":"task", "kind":"question", "conversation_id":conversation_id,"question":question})
        }
        TaskEvent::QuestionResolved {
            conversation_id,
            run_id,
            question_id,
            state,
        } => {
            json!({"type":"task", "kind":"question_resolved", "conversation_id":conversation_id,"run_id":run_id,"question_id":question_id,"state":state})
        }
        TaskEvent::Error {
            conversation_id,
            session_id,
            run_id,
            message,
        } => {
            json!({"type":"task", "kind":"error", "conversation_id":conversation_id,"session_id":session_id,"run_id":run_id,"message":message})
        }
    }
}

pub fn continue_run(manager: &TaskManager, session: Uuid, prompt: String) -> Result<Value> {
    let owner = manager.session(session)?.conversation_id;
    Ok(serde_json::to_value(manager.continue_task(
        owner,
        &Uuid::new_v4().to_string(),
        session,
        prompt,
    )?)?)
}

pub fn answer(manager: &TaskManager, question: Uuid, answer: TaskAnswer) -> Result<Value> {
    let owner = manager.question(question)?.conversation_id;
    Ok(serde_json::to_value(
        manager.answer(owner, question, answer)?,
    )?)
}

pub fn cancel(manager: &TaskManager, session: Uuid) -> Result<Value> {
    let owner = manager.session(session)?.conversation_id;
    Ok(serde_json::to_value(manager.cancel_run(owner, session)?)?)
}

pub fn close(manager: &TaskManager, session: Uuid) -> Result<Value> {
    let owner = manager.session(session)?.conversation_id;
    Ok(serde_json::to_value(manager.close(owner, session)?)?)
}
