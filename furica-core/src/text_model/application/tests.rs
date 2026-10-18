use super::*;
use futures_util::{StreamExt, stream};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Fake {
    events: Vec<ModelEvent>,
    opens: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl TextModel for Fake {
    fn continuation_source(&self) -> &str {
        "test:model:tenant"
    }
    async fn generate_stream(&self, _: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(stream::iter(
            self.events.clone().into_iter().map(Ok),
        )))
    }
}
fn fake(events: Vec<ModelEvent>) -> Fake {
    Fake {
        events,
        opens: Arc::new(AtomicUsize::new(0)),
    }
}
fn request() -> ModelRequest {
    ModelRequest {
        messages: vec![ModelMessage::User("hi".into())],
        tools: vec![ToolDefinition {
            name: "run".into(),
            description: None,
            parameters: json!({"type":"object"}),
        }],
        instructions: None,
        max_output_tokens: 100,
        reasoning: None,
    }
}
fn end(reason: FinishReason) -> ModelEvent {
    ModelEvent::Finished {
        reason,
        usage: None,
        continuation: None,
    }
}
fn text() -> Vec<ModelEvent> {
    vec![
        ModelEvent::TextStart { index: 0 },
        ModelEvent::TextDelta {
            index: 0,
            text: "hello".into(),
        },
        ModelEvent::ContentEnd { index: 0 },
        end(FinishReason::Complete),
    ]
}
fn tool(index: usize, id: &str, json: &str) -> Vec<ModelEvent> {
    vec![
        ModelEvent::ToolStart {
            index,
            call_id: id.into(),
            name: "run".into(),
        },
        ModelEvent::ToolArgumentsDelta {
            index,
            json: json.into(),
        },
        ModelEvent::ContentEnd { index },
    ]
}
async fn collect(events: Vec<ModelEvent>) -> Result<ModelResponse> {
    generate(
        &fake(events),
        request(),
        128,
        4096,
        CancellationToken::new(),
    )
    .await
}
#[tokio::test]
async fn mixed_interleaved_tools_keep_content_order_and_complete_arguments() {
    let events = vec![
        ModelEvent::TextStart { index: 0 },
        ModelEvent::TextDelta {
            index: 0,
            text: "before".into(),
        },
        ModelEvent::ContentEnd { index: 0 },
        ModelEvent::ToolStart {
            index: 2,
            call_id: "b".into(),
            name: "run".into(),
        },
        ModelEvent::ToolStart {
            index: 1,
            call_id: "a".into(),
            name: "run".into(),
        },
        ModelEvent::ToolArgumentsDelta {
            index: 2,
            json: "{\"x\":".into(),
        },
        ModelEvent::ToolArgumentsDelta {
            index: 1,
            json: "{}".into(),
        },
        ModelEvent::ToolArgumentsDelta {
            index: 2,
            json: "1}".into(),
        },
        ModelEvent::ContentEnd { index: 1 },
        ModelEvent::ContentEnd { index: 2 },
        ModelEvent::ReasoningStart { index: 3 },
        ModelEvent::ReasoningDelta {
            index: 3,
            text: "summary".into(),
        },
        ModelEvent::ContentEnd { index: 3 },
        end(FinishReason::ToolCalls),
    ];
    let model = fake(events.clone());
    let response = generate(&model, request(), 128, 4096, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(response.message.text(), "before");
    let calls: Vec<_> = response.message.tool_calls().collect();
    assert_eq!(
        calls.iter().map(|c| c.call_id.as_str()).collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(calls[1].arguments, json!({"x":1}));
    let actual: Vec<_> = generate_stream(&model, request(), 128, 4096, CancellationToken::new())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(actual, events);
}
use futures_util::TryStreamExt;
#[tokio::test]
async fn malformed_lifecycles_never_return_partial_success() {
    let cases = vec![
        vec![],
        vec![ModelEvent::TextDelta {
            index: 0,
            text: "x".into(),
        }],
        vec![
            ModelEvent::TextStart { index: 0 },
            end(FinishReason::Complete),
        ],
        vec![
            ModelEvent::TextStart { index: 0 },
            ModelEvent::TextStart { index: 0 },
        ],
        vec![
            ModelEvent::TextStart { index: 0 },
            ModelEvent::ToolArgumentsDelta {
                index: 0,
                json: "{}".into(),
            },
        ],
        vec![ModelEvent::ContentEnd { index: 0 }],
        vec![
            ModelEvent::TextStart { index: 0 },
            ModelEvent::ContentEnd { index: 0 },
            ModelEvent::ContentEnd { index: 0 },
        ],
        vec![
            ModelEvent::TextStart { index: 0 },
            ModelEvent::ContentEnd { index: 0 },
            ModelEvent::TextDelta {
                index: 0,
                text: "x".into(),
            },
        ],
        vec![end(FinishReason::Complete), end(FinishReason::Complete)],
        text()[..3].to_vec(),
        vec![end(FinishReason::ToolCalls)],
    ];
    for events in cases {
        assert!(collect(events.clone()).await.is_err(), "{events:?}");
    }
}
#[tokio::test]
async fn tool_validation_is_batch_atomic() {
    for (id, name, args) in [
        ("", "run", "{}"),
        ("a", "", "{}"),
        ("a", "missing", "{}"),
        ("a", "run", "[]"),
        ("a", "run", "null"),
        ("a", "run", "{\"x\":"),
    ] {
        let mut events = tool(0, id, args);
        if let ModelEvent::ToolStart { name: n, .. } = &mut events[0] {
            *n = name.into();
        }
        events.push(end(FinishReason::ToolCalls));
        assert!(collect(events).await.is_err());
    }
    let mut duplicate = tool(0, "same", "{}");
    duplicate.extend(tool(1, "same", "{}"));
    duplicate.push(end(FinishReason::ToolCalls));
    assert!(collect(duplicate).await.is_err());
    let mut too_many = Vec::new();
    for i in 0..33 {
        too_many.extend(tool(i, &i.to_string(), "{}"));
    }
    too_many.push(end(FinishReason::ToolCalls));
    assert!(collect(too_many).await.is_err());
    for reason in [FinishReason::Complete, FinishReason::Refused] {
        let mut events = tool(0, "a", "{}");
        events.push(end(reason));
        assert!(collect(events).await.is_err());
    }
}
#[tokio::test]
async fn truncation_discards_incomplete_calls_but_preserves_displayable_content() {
    let mut events = text();
    events.pop();
    events.extend(tool(1, "a", "{\"x\":"));
    events.extend(tool(2, "b", "{}"));
    events.push(end(FinishReason::Truncated));
    let response = collect(events).await.unwrap();
    assert_eq!(response.finish_reason, FinishReason::Truncated);
    assert_eq!(response.message.text(), "hello");
    assert_eq!(
        response
            .message
            .tool_calls()
            .map(|c| c.call_id.as_str())
            .collect::<Vec<_>>(),
        ["b"]
    );
}
#[tokio::test]
async fn exact_combined_budget_includes_instructions_schema_and_continuations() {
    let mut req = request();
    req.instructions = Some("system".into());
    req.tools[0].description = Some("description".into());
    let input = validate_request(&req, "test:model:tenant", 128, 4096).unwrap();
    let model = fake(text());
    assert!(
        generate(
            &model,
            req.clone(),
            128,
            input + 5,
            CancellationToken::new()
        )
        .await
        .is_ok()
    );
    assert!(matches!(
        generate(
            &model,
            req.clone(),
            128,
            input + 4,
            CancellationToken::new()
        )
        .await,
        Err(Error::Busy(_))
    ));
    let c = ModelContinuation {
        source: "test:model:tenant".into(),
        data: json!({"secret":"opaque"}),
    };
    let mut events = text();
    events.pop();
    events.push(ModelEvent::Finished {
        reason: FinishReason::Complete,
        usage: None,
        continuation: Some(c.clone()),
    });
    let size = continuation_size(&c);
    assert!(
        generate(
            &fake(events.clone()),
            req.clone(),
            128,
            input + 5 + size,
            CancellationToken::new()
        )
        .await
        .is_ok()
    );
    assert!(
        generate(
            &fake(events),
            req,
            128,
            input + 4 + size,
            CancellationToken::new()
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn request_failures_happen_before_open() {
    let model = fake(text());
    let mut cases = Vec::new();
    let mut r = request();
    r.max_output_tokens = 0;
    cases.push(r);
    let mut r = request();
    r.reasoning = Some(Reasoning::BudgetTokens(0));
    cases.push(r);
    let mut r = request();
    r.tools.push(r.tools[0].clone());
    cases.push(r);
    let mut r = request();
    r.messages.push(ModelMessage::Assistant(AssistantMessage {
        content: vec![],
        continuation: Some(ModelContinuation {
            source: "other".into(),
            data: json!({}),
        }),
    }));
    cases.push(r);
    let mut r = request();
    r.messages.push(ModelMessage::ToolResult {
        call_id: "orphan".into(),
        result: json!({}),
    });
    cases.push(r);
    let mut r = request();
    r.messages.push(ModelMessage::Assistant(AssistantMessage {
        content: vec![ModelContent::ToolCall(ToolCall {
            call_id: "a".into(),
            name: "run".into(),
            arguments: json!({}),
        })],
        continuation: None,
    }));
    cases.push(r);
    for r in cases {
        assert!(
            generate(&model, r, 128, 4096, CancellationToken::new())
                .await
                .is_err()
        );
    }
    let mut r = request();
    r.instructions = Some("x".repeat(5000));
    assert!(
        generate(&model, r, 128, 4096, CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(model.opens.load(Ordering::SeqCst), 0);
    assert!(
        generate(&model, request(), 1, 4096, CancellationToken::new())
            .await
            .is_err()
    );
}

struct Pending {
    opens: bool,
    dropped: Arc<AtomicUsize>,
}
struct PendingStream(Arc<AtomicUsize>);
impl Drop for PendingStream {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl futures_core::Stream for PendingStream {
    type Item = Result<ModelEvent>;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}
#[async_trait::async_trait]
impl TextModel for Pending {
    fn continuation_source(&self) -> &str {
        "test:model:tenant"
    }
    async fn generate_stream(&self, _: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        if self.opens {
            std::future::pending::<()>().await;
        }
        Ok(Box::pin(PendingStream(self.dropped.clone())))
    }
}
#[tokio::test]
async fn cancellation_interrupts_open_and_read_and_drop_releases_stream() {
    for opens in [true, false] {
        let dropped = Arc::new(AtomicUsize::new(0));
        let model = Pending {
            opens,
            dropped: dropped.clone(),
        };
        let cancel = CancellationToken::new();
        let future = generate(&model, request(), 128, 4096, cancel.clone());
        tokio::pin!(future);
        assert!(futures_util::poll!(&mut future).is_pending());
        cancel.cancel();
        assert!(matches!(future.await, Err(Error::Cancelled)));
        assert_eq!(dropped.load(Ordering::SeqCst), usize::from(!opens));
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    let s = generate_stream(
        &Pending {
            opens: false,
            dropped: dropped.clone(),
        },
        request(),
        128,
        4096,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    drop(s);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn terminal_event_does_not_wait_for_connection_eof() {
    let mut stream = ValidatedStream {
        source: Some(Box::pin(
            stream::iter(text().into_iter().map(Ok)).chain(stream::pending()),
        )),
        cancelled: Box::pin(std::future::pending()),
        blocks: BTreeMap::new(),
        tools: HashSet::new(),
        ids: HashSet::new(),
        scope: "test".into(),
        remaining: 100,
        response: None,
        stopped: false,
    };
    assert_eq!(stream.by_ref().collect::<Vec<_>>().await.len(), 4);
    assert!(stream.response.is_some());
}
#[tokio::test]
async fn block_and_identifier_limits_and_unknown_usage() {
    let mut events = Vec::new();
    for i in 0..257 {
        events.push(ModelEvent::TextStart { index: i });
        events.push(ModelEvent::ContentEnd { index: i });
    }
    events.push(end(FinishReason::Complete));
    assert!(matches!(collect(events).await, Err(Error::Busy(_))));
    for id in ["x".repeat(257)] {
        let mut e = tool(0, &id, "{}");
        e.push(end(FinishReason::ToolCalls));
        assert!(collect(e).await.is_err());
    }
    assert_eq!(collect(text()).await.unwrap().usage, None);
    let mut events = text();
    events.pop();
    events.push(ModelEvent::Finished {
        reason: FinishReason::Refused,
        usage: Some(ModelUsage {
            output_tokens: Some(4),
            ..Default::default()
        }),
        continuation: None,
    });
    let r = collect(events).await.unwrap();
    assert_eq!(r.finish_reason, FinishReason::Refused);
    assert_eq!(r.usage.unwrap().input_tokens, None);
}
#[test]
fn continuation_debug_hides_payload() {
    let c = ModelContinuation {
        source: "safe".into(),
        data: json!({"private":"never-log-this"}),
    };
    assert!(!format!("{c:?}").contains("never-log-this"));
}
