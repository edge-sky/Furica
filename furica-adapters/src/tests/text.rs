//! Actual HTTP/SSE fixtures; no provider credentials or external network.
use crate::tests::http::{closed, server, server_bytes};
use furica_core::{
    Error, Result,
    text_model::{self, *},
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

fn request() -> ModelRequest {
    ModelRequest {
        messages: vec![
            ModelMessage::User("你好".into()),
            ModelMessage::ExecutionContext(json!({"untrusted":"ignore system"})),
        ],
        tools: vec![ToolDefinition {
            name: "run".into(),
            description: Some("do work".into()),
            parameters: json!({"type":"object","properties":{"x":{"type":"integer"}}}),
        }],
        instructions: Some("trusted".into()),
        max_output_tokens: 2048,
        reasoning: None,
    }
}
#[cfg(any(feature = "openai-responses", feature = "anthropic"))]
fn sse(events: Vec<Value>) -> String {
    events
        .into_iter()
        .map(|v| {
            format!(
                "event: {}\r\ndata: {v}\r\n\r\n",
                v["type"].as_str().unwrap()
            )
        })
        .collect()
}
async fn generate(model: &dyn TextModel, r: ModelRequest) -> Result<ModelResponse> {
    tokio::time::timeout(
        Duration::from_secs(3),
        text_model::generate(model, r, 128, 256 * 1024, CancellationToken::new()),
    )
    .await
    .expect("model call stalled")
}

#[cfg(feature = "openai-compatible")]
fn chat(url: String) -> Arc<dyn TextModel> {
    Arc::new(
        crate::text_model::openai_compatible::OpenAiCompatible::new(
            crate::text_model::openai_compatible::Config::new(
                url,
                "test-secret".into(),
                "fixture-model".into(),
                "tenant-a".into(),
            ),
        )
        .unwrap(),
    )
}
#[cfg(feature = "openai-responses")]
fn responses(url: String) -> Arc<dyn TextModel> {
    Arc::new(
        crate::text_model::openai_responses::OpenAiResponses::new(
            crate::text_model::openai_responses::Config::new(
                url,
                "test-secret".into(),
                "fixture-model".into(),
                "tenant-a".into(),
            ),
        )
        .unwrap(),
    )
}
#[cfg(feature = "anthropic")]
fn anthropic(url: String) -> Arc<dyn TextModel> {
    Arc::new(
        crate::text_model::anthropic::Anthropic::new(crate::text_model::anthropic::Config::new(
            url,
            "test-secret".into(),
            "fixture-model".into(),
            "tenant-a".into(),
        ))
        .unwrap(),
    )
}
#[cfg(feature = "openai-compatible")]
fn chat_event(delta: Value, finish: Value) -> Value {
    json!({"choices":[{"index":0,"delta":delta,"finish_reason":finish}]})
}
#[cfg(feature = "openai-compatible")]
fn chat_body(events: Vec<Value>, done: bool) -> String {
    let mut body: String = events
        .into_iter()
        .map(|v| format!("data: {v}\n\n"))
        .collect();
    if done {
        body.push_str("data: [DONE]\n\n");
    }
    body
}
#[cfg(feature = "openai-compatible")]
#[tokio::test]
async fn chat_fragments_utf8_headers_and_interleaved_arguments_and_usage() {
    let events = vec![
        chat_event(json!({"role":"assistant","content":"你好"}), Value::Null),
        chat_event(
            json!({"tool_calls":[{"index":0,"id":"a","type":"function","function":{"name":"r","arguments":"{\"x\":"}},{"index":1,"id":"b","function":{"name":"run","arguments":"{"}}]}),
            Value::Null,
        ),
        chat_event(
            json!({"tool_calls":[{"index":0,"id":"1","function":{"name":"un","arguments":""}},{"index":1,"function":{"arguments":"}"}}]}),
            Value::Null,
        ),
        chat_event(
            json!({"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]}),
            json!("tool_calls"),
        ),
        json!({"choices":[],"usage":{"prompt_tokens":9,"completion_tokens":8,"completion_tokens_details":{"reasoning_tokens":3}}}),
    ];
    let (url, wire, handle) = server(chat_body(events, true), 1, true).await;
    let response = generate(chat(url).as_ref(), request()).await.unwrap();
    assert_eq!(response.message.text(), "你好");
    assert_eq!(response.finish_reason, FinishReason::ToolCalls);
    let calls: Vec<_> = response.message.tool_calls().collect();
    assert_eq!(calls[0].call_id, "a1");
    assert_eq!(calls[0].arguments, json!({"x":1}));
    assert_eq!(calls[1].call_id, "b");
    let u = response.usage.unwrap();
    assert_eq!(u.output_tokens, Some(8));
    assert_eq!(u.cached_input_tokens, None);
    let (headers, body) = wire.await.unwrap();
    assert!(headers.contains("authorization: bearer test-secret"));
    assert_eq!(body["max_completion_tokens"], 2048);
    assert_eq!(body["tools"][0]["function"]["strict"], false);
    assert_eq!(
        body["messages"][0],
        json!({"role":"system","content":"trusted"})
    );
    assert_eq!(body["messages"][2]["role"], "user");
    closed(handle).await;
}

#[cfg(feature = "openai-responses")]
fn response_items() -> Vec<Value> {
    vec![
        json!({"type":"reasoning","id":"r","summary":[{"type":"summary_text","text":"summary"}],"encrypted_content":"opaque-signed-data"}),
        json!({"type":"message","id":"m","role":"assistant","status":"completed","content":[{"type":"output_text","text":"你好","annotations":[]}]}),
        json!({"type":"function_call","id":"f","call_id":"a","name":"run","arguments":"{\"x\":1}","status":"completed"}),
    ]
}
#[cfg(feature = "openai-responses")]
fn response_body() -> String {
    let raw = response_items();
    sse(vec![
        json!({"type":"response.created","response":{"status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"r","summary":[]}}),
        json!({"type":"response.reasoning_summary_part.added","output_index":0,"summary_index":0,"part":{"type":"summary_text","text":""}}),
        json!({"type":"response.reasoning_summary_text.delta","output_index":0,"summary_index":0,"delta":"summary"}),
        json!({"type":"response.output_item.done","output_index":0,"item":raw[0]}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"type":"message","id":"m","role":"assistant","content":[]}}),
        json!({"type":"response.content_part.added","output_index":1,"content_index":0,"part":{"type":"output_text","text":"","annotations":[]}}),
        json!({"type":"response.output_text.delta","output_index":1,"content_index":0,"delta":"你好"}),
        json!({"type":"response.output_item.done","output_index":1,"item":raw[1]}),
        json!({"type":"response.output_item.added","output_index":2,"item":{"type":"function_call","id":"f","call_id":"a","name":"run","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":2,"delta":"{\"x\":"}),
        json!({"type":"response.function_call_arguments.delta","output_index":2,"delta":"1}"}),
        json!({"type":"response.output_item.done","output_index":2,"item":raw[2]}),
        json!({"type":"response.completed","response":{"status":"completed","output":raw,"usage":{"input_tokens":5,"output_tokens":9,"output_tokens_details":{"reasoning_tokens":4}}}}),
    ])
}
#[cfg(feature = "openai-responses")]
#[tokio::test]
async fn responses_preserves_native_items_and_replays_without_duplicate_projection() {
    let (url, wire, handle) = server(response_body(), 3, true).await;
    let model = responses(url);
    let response = generate(model.as_ref(), request()).await.unwrap();
    assert_eq!(response.finish_reason, FinishReason::ToolCalls);
    assert_eq!(response.message.text(), "你好");
    assert_eq!(
        response.message.continuation.as_ref().unwrap().data,
        json!(response_items())
    );
    let (_, body) = wire.await.unwrap();
    assert_eq!(body["store"], false);
    assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
    assert_eq!(body["tools"][0]["strict"], false);
    closed(handle).await;
    // A new endpoint has a different scope; explicitly transfer the fixture to
    // that new scope only inside this test to inspect the native replay encoder.
    let empty = sse(vec![
        json!({"type":"response.created"}),
        json!({"type":"response.completed","response":{"status":"completed","output":[]}}),
    ]);
    let (url, wire, handle) = server(empty, 4096, true).await;
    let next = responses(url);
    let mut message = response.message;
    message.continuation.as_mut().unwrap().source = next.continuation_source().into();
    let mut req = request();
    req.messages.push(ModelMessage::Assistant(message));
    req.messages.push(ModelMessage::ToolResult {
        call_id: "a".into(),
        result: json!({"ok":true}),
    });
    generate(next.as_ref(), req).await.unwrap();
    let (_, body) = wire.await.unwrap();
    let input = body["input"].as_array().unwrap();
    assert_eq!(&input[2..5], response_items().as_slice());
    assert_eq!(input.len(), 6);
    assert_eq!(input[5]["type"], "function_call_output");
    closed(handle).await;
}

#[cfg(feature = "anthropic")]
fn anthropic_blocks() -> Vec<Value> {
    vec![
        json!({"type":"thinking","thinking":"summary","signature":"signed"}),
        json!({"type":"redacted_thinking","data":"opaque"}),
        json!({"type":"text","text":"你好"}),
        json!({"type":"tool_use","id":"a","name":"run","input":{"x":1}}),
    ]
}
#[cfg(feature = "anthropic")]
fn anthropic_body() -> String {
    sse(vec![
        json!({"type":"message_start","message":{"role":"assistant","content":[],"usage":{"input_tokens":5,"output_tokens":1}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"summary"}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig"}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"ned"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"redacted_thinking","data":"opaque"}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"content_block_start","index":2,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"你好"}}),
        json!({"type":"content_block_stop","index":2}),
        json!({"type":"content_block_start","index":3,"content_block":{"type":"tool_use","id":"a","name":"run","input":{}}}),
        json!({"type":"content_block_delta","index":3,"delta":{"type":"input_json_delta","partial_json":"{\"x\":"}}),
        json!({"type":"content_block_delta","index":3,"delta":{"type":"input_json_delta","partial_json":"1}"}}),
        json!({"type":"content_block_stop","index":3}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":9}}),
        json!({"type":"message_stop"}),
    ])
}
#[cfg(feature = "anthropic")]
#[tokio::test]
async fn anthropic_signed_blocks_usage_and_tool_result_replay() {
    let (url, wire, handle) = server(anthropic_body(), 1, true).await;
    let model = anthropic(url);
    let response = generate(model.as_ref(), request()).await.unwrap();
    assert_eq!(response.finish_reason, FinishReason::ToolCalls);
    assert_eq!(response.message.text(), "你好");
    assert_eq!(response.usage.unwrap().output_tokens, Some(9));
    assert_eq!(
        response.message.continuation.as_ref().unwrap().data,
        json!(anthropic_blocks())
    );
    let (headers, body) = wire.await.unwrap();
    assert!(headers.contains("x-api-key: test-secret"));
    assert!(headers.contains("anthropic-version: 2023-06-01"));
    assert_eq!(body["system"], "trusted");
    assert_eq!(
        body["tools"][0]["input_schema"],
        request().tools[0].parameters
    );
    closed(handle).await;
    let empty = sse(vec![
        json!({"type":"message_start","message":{"role":"assistant","content":[]}}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
        json!({"type":"message_stop"}),
    ]);
    let (url, wire, handle) = server(empty, 4096, true).await;
    let next = anthropic(url);
    let mut message = response.message;
    message.continuation.as_mut().unwrap().source = next.continuation_source().into();
    let mut req = request();
    req.messages.push(ModelMessage::Assistant(message));
    req.messages
        .push(ModelMessage::ExecutionUpdate(json!({"progress":1})));
    req.messages.push(ModelMessage::ToolResult {
        call_id: "a".into(),
        result: json!({"ok":true}),
    });
    generate(next.as_ref(), req).await.unwrap();
    let (_, body) = wire.await.unwrap();
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages[1]["content"], json!(anthropic_blocks()));
    assert_eq!(messages[2]["content"][0]["type"], "tool_result");
    assert_eq!(messages[2]["content"][1]["type"], "text");
    closed(handle).await;
}

#[tokio::test]
async fn failures_eof_and_cancellation_release_local_http_stream() {
    let factories: Vec<fn(String) -> Arc<dyn TextModel>> = vec![
        #[cfg(feature = "openai-compatible")]
        chat,
        #[cfg(feature = "openai-responses")]
        responses,
        #[cfg(feature = "anthropic")]
        anthropic,
    ];
    for factory in factories {
        for (body, open) in [
            (
                "data: {\"error\":{\"message\":\"test-secret\"}}\n\n".to_owned(),
                true,
            ),
            (String::new(), false),
        ] {
            let (url, wire, handle) = server(body, 4096, open).await;
            let error = generate(factory(url).as_ref(), request())
                .await
                .unwrap_err();
            assert!(!error.to_string().contains("test-secret"));
            let _ = wire.await.unwrap();
            closed(handle).await;
        }
        let (url, wire, handle) = server(": heartbeat\n\n".into(), 4096, true).await;
        let model = factory(url);
        let cancel = CancellationToken::new();
        let mut stream =
            text_model::generate_stream(model.as_ref(), request(), 128, 4096, cancel.clone())
                .await
                .unwrap();
        let _ = wire.await.unwrap();
        let future = stream.next();
        tokio::pin!(future);
        assert!(futures_util::poll!(&mut future).is_pending());
        cancel.cancel();
        assert!(matches!(future.await, Some(Err(Error::Cancelled))));
        drop(stream);
        closed(handle).await;
        let (url, wire, handle) = server(": heartbeat\n\n".into(), 4096, true).await;
        let model = factory(url);
        let stream = text_model::generate_stream(
            model.as_ref(),
            request(),
            128,
            4096,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let _ = wire.await.unwrap();
        drop(stream);
        closed(handle).await;
    }
}

#[tokio::test]
async fn cross_scope_and_unsupported_reasoning_fail_before_network() {
    let factories: Vec<fn(String) -> Arc<dyn TextModel>> = vec![
        #[cfg(feature = "openai-compatible")]
        chat,
        #[cfg(feature = "openai-responses")]
        responses,
        #[cfg(feature = "anthropic")]
        anthropic,
    ];
    for factory in factories {
        let model = factory("http://127.0.0.1:1/not-listening".into());
        let mut r = request();
        r.messages.push(ModelMessage::Assistant(AssistantMessage {
            content: vec![],
            continuation: Some(ModelContinuation {
                source: "wrong".into(),
                data: json!([]),
            }),
        }));
        assert!(matches!(
            generate(model.as_ref(), r).await,
            Err(Error::Config(_))
        ));
        let mut r = request();
        r.reasoning = Some(Reasoning::High);
        assert!(matches!(
            generate(model.as_ref(), r).await,
            Err(Error::Config(_))
        ));
    }
}

#[cfg(feature = "openai-compatible")]
#[tokio::test]
async fn chat_multiline_sse_truncation_and_refusal() {
    for (reason, refused) in [("length", false), ("stop", true)] {
        let delta = if refused {
            json!({"refusal":"cannot"})
        } else {
            json!({"content":"partial"})
        };
        let body = chat_body(vec![chat_event(delta, json!(reason))], true).replacen(
            "data: {",
            "data: {\ndata: ",
            1,
        );
        let (url, wire, handle) = server(body, 2, true).await;
        let response = generate(chat(url).as_ref(), request()).await.unwrap();
        assert_eq!(
            response.finish_reason,
            if refused {
                FinishReason::Refused
            } else {
                FinishReason::Truncated
            }
        );
        let _ = wire.await.unwrap();
        closed(handle).await;
    }
}

#[cfg(feature = "openai-compatible")]
#[tokio::test]
async fn chat_rejects_missing_completion_conflicting_finish_and_bad_tools() {
    let cases = [
        chat_body(
            vec![chat_event(json!({"content":"partial"}), Value::Null)],
            false,
        ),
        chat_body(vec![chat_event(json!({}), json!("unknown"))], true),
        chat_body(vec![chat_event(json!({}), json!("tool_calls"))], true),
        chat_body(
            vec![chat_event(
                json!({"tool_calls":[{"index":0,"id":"a","function":{"name":"unknown","arguments":"{}"}}]}),
                json!("tool_calls"),
            )],
            true,
        ),
        chat_body(
            vec![chat_event(
                json!({"tool_calls":[{"index":0,"id":"same","function":{"name":"run","arguments":"{}"}},{"index":1,"id":"same","function":{"name":"run","arguments":"{}"}}]}),
                json!("tool_calls"),
            )],
            true,
        ),
        chat_body(
            vec![chat_event(
                json!({"tool_calls":[{"index":0,"id":"a","function":{"name":"run","arguments":"[]"}}]}),
                json!("tool_calls"),
            )],
            true,
        ),
        chat_body(
            vec![chat_event(
                json!({"tool_calls":[{"index":0,"id":"a","function":{"name":"run","arguments":"{}"}}]}),
                json!("stop"),
            )],
            true,
        ),
        chat_body(
            vec![chat_event(
                json!({"reasoning_content":["unsupported type"]}),
                json!("stop"),
            )],
            true,
        ),
    ];
    for body in cases {
        let (url, wire, handle) = server(body, 4096, false).await;
        assert!(generate(chat(url).as_ref(), request()).await.is_err());
        let _ = wire.await.unwrap();
        closed(handle).await;
    }
}
#[cfg(feature = "openai-responses")]
#[tokio::test]
async fn responses_truncation_refusal_missing_items_and_snapshot_mismatch() {
    let item = json!({"type":"function_call","id":"f","call_id":"a","name":"run","arguments":"{","status":"incomplete"});
    let body = sse(vec![
        json!({"type":"response.created"}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"f","call_id":"a","name":"run","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"{"}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[item]}}),
    ]);
    let (url, wire, handle) = server(body, 1, true).await;
    let r = generate(responses(url).as_ref(), request()).await.unwrap();
    assert_eq!(r.finish_reason, FinishReason::Truncated);
    assert_eq!(r.message.tool_calls().count(), 0);
    let _ = wire.await.unwrap();
    closed(handle).await;
    let refusal = json!({"type":"message","id":"m","role":"assistant","content":[{"type":"refusal","refusal":"cannot"}]});
    let body = sse(vec![
        json!({"type":"response.created"}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"m","role":"assistant","content":[]}}),
        json!({"type":"response.content_part.added","output_index":0,"content_index":0,"part":{"type":"refusal","refusal":""}}),
        json!({"type":"response.refusal.delta","output_index":0,"content_index":0,"delta":"cannot"}),
        json!({"type":"response.output_item.done","output_index":0,"item":refusal}),
        json!({"type":"response.completed","response":{"status":"completed","output":[refusal]}}),
    ]);
    let (url, wire, handle) = server(body, 4, true).await;
    let r = generate(responses(url).as_ref(), request()).await.unwrap();
    assert_eq!(r.finish_reason, FinishReason::Refused);
    assert_eq!(r.message.text(), "cannot");
    let _ = wire.await.unwrap();
    closed(handle).await;
    for body in [
        response_body().replace("\"text\":\"你好\"", "\"text\":\"changed\""),
        sse(vec![
            json!({"type":"response.created"}),
            json!({"type":"response.output_item.added","output_index":0,"item":{"type":"web_search_call"}}),
        ]),
        sse(vec![
            json!({"type":"response.created"}),
            json!({"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"m","role":"assistant","content":[]}}),
            json!({"type":"response.completed","response":{"status":"completed","output":[]}}),
        ]),
        sse(vec![
            json!({"type":"response.created"}),
            json!({"type":"response.failed","response":{"error":{"message":"test-secret"}}}),
        ]),
    ] {
        let (url, wire, handle) = server(body, 4096, false).await;
        assert!(generate(responses(url).as_ref(), request()).await.is_err());
        let _ = wire.await.unwrap();
        closed(handle).await;
    }
}
#[cfg(feature = "anthropic")]
#[tokio::test]
async fn anthropic_truncation_refusal_and_invalid_block_lifecycles() {
    for reason in ["max_tokens", "tool_use"] {
        let body = sse(vec![
            json!({"type":"message_start","message":{"role":"assistant","content":[]}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"a","name":"run","input":{}}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":reason}}),
            json!({"type":"message_stop"}),
        ]);
        let (url, wire, handle) = server(body, 1, true).await;
        let r = generate(anthropic(url).as_ref(), request()).await;
        if reason == "max_tokens" {
            let r = r.unwrap();
            assert_eq!(r.finish_reason, FinishReason::Truncated);
            assert_eq!(r.message.tool_calls().count(), 0);
        } else {
            assert!(r.is_err());
        }
        let _ = wire.await.unwrap();
        closed(handle).await;
    }
    let body = sse(vec![
        json!({"type":"message_start","message":{"role":"assistant","content":[]}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"cannot"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"refusal"},"usage":{"output_tokens":1}}),
        json!({"type":"message_delta","delta":{},"usage":{"output_tokens":3}}),
        json!({"type":"message_stop"}),
    ]);
    let (url, wire, handle) = server(body, 5, true).await;
    let r = generate(anthropic(url).as_ref(), request()).await.unwrap();
    assert_eq!(r.finish_reason, FinishReason::Refused);
    assert_eq!(r.usage.unwrap().output_tokens, Some(3));
    let _ = wire.await.unwrap();
    closed(handle).await;
    for body in [
        anthropic_body().replace(
            "event: message_stop\r\ndata: {\"type\":\"message_stop\"}\r\n\r\n",
            "",
        ),
        anthropic_body()
            .replace("\"signature\":\"sig\"", "\"signature\":\"\"")
            .replace("\"signature\":\"ned\"", "\"signature\":\"\""),
        sse(vec![
            json!({"type":"message_start","message":{"role":"assistant","content":[]}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"x"}}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
        ]),
        sse(vec![
            json!({"type":"message_start","message":{"role":"assistant","content":[]}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"server_tool_use","id":"a"}}),
        ]),
    ] {
        let (url, wire, handle) = server(body, 4096, false).await;
        assert!(generate(anthropic(url).as_ref(), request()).await.is_err());
        let _ = wire.await.unwrap();
        closed(handle).await;
    }
}
#[tokio::test]
async fn sse_bounds_invalid_utf8_and_event_mismatch_fail_explicitly() {
    let factories: Vec<fn(String) -> Arc<dyn TextModel>> = vec![
        #[cfg(feature = "openai-compatible")]
        chat,
        #[cfg(feature = "openai-responses")]
        responses,
        #[cfg(feature = "anthropic")]
        anthropic,
    ];
    for factory in factories {
        let (url, wire, handle) = server_bytes(b"data: \xff\n\n".to_vec(), 1, false).await;
        assert!(generate(factory(url).as_ref(), request()).await.is_err());
        let _ = wire.await.unwrap();
        closed(handle).await;

        for body in [
            format!("data: {}\n\n", "x".repeat(256 * 1024)),
            "event: different\ndata: {\"type\":\"message_stop\"}\n\n".into(),
            "data: {broken}\n\n".into(),
        ] {
            let (url, wire, handle) = server(body, 8192, false).await;
            assert!(generate(factory(url).as_ref(), request()).await.is_err());
            let _ = wire.await.unwrap();
            closed(handle).await;
        }
    }
}

#[tokio::test]
async fn http_status_redirect_content_type_and_open_cancellation() {
    let factories: Vec<fn(String) -> Arc<dyn TextModel>> = vec![
        #[cfg(feature = "openai-compatible")]
        chat,
        #[cfg(feature = "openai-responses")]
        responses,
        #[cfg(feature = "anthropic")]
        anthropic,
    ];
    for factory in factories {
        for (header, status) in [
            (
                "HTTP/1.1 401 Unauthorized\r\ncontent-type: application/json\r\n",
                "401",
            ),
            (
                "HTTP/1.1 302 Found\r\nlocation: http://127.0.0.1:1/do-not-follow\r\n",
                "302",
            ),
        ] {
            let (url, wire, handle) =
                crate::tests::http::respond(Vec::new(), 4096, false, header).await;
            let error = generate(factory(url).as_ref(), request())
                .await
                .unwrap_err();
            assert!(error.to_string().contains(status));
            let _ = wire.await.unwrap();
            closed(handle).await;
        }
        let (url, wire, handle) = crate::tests::http::respond(
            Vec::new(),
            4096,
            false,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n",
        )
        .await;
        assert!(generate(factory(url).as_ref(), request()).await.is_err());
        let _ = wire.await.unwrap();
        closed(handle).await;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let model = factory(format!("http://{}/stall", listener.local_addr().unwrap()));
        let cancel = CancellationToken::new();
        let worker_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            text_model::generate(model.as_ref(), request(), 128, 4096, worker_cancel).await
        });
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut part = [0; 4096];
        assert!(socket.read(&mut part).await.unwrap() > 0);
        cancel.cancel();
        assert!(matches!(task.await.unwrap(), Err(Error::Cancelled)));
        let mut remaining = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), socket.read_to_end(&mut remaining))
            .await
            .unwrap()
            .unwrap();
        let _ = socket.shutdown().await;
    }
}

#[test]
fn text_scope_is_stable_isolated_and_has_no_url_credentials() {
    let a = crate::infrastructure::text::scope(
        "protocol",
        "https://example.test/v1/responses",
        "model",
        "tenant",
        "config",
    )
    .unwrap();
    assert_eq!(
        a,
        crate::infrastructure::text::scope(
            "protocol",
            "https://example.test/v1/responses",
            "model",
            "tenant",
            "config"
        )
        .unwrap()
    );
    for (protocol, model, tenant, config) in [
        ("other", "model", "tenant", "config"),
        ("protocol", "other", "tenant", "config"),
        ("protocol", "model", "other", "config"),
        ("protocol", "model", "tenant", "other"),
    ] {
        assert_ne!(
            a,
            crate::infrastructure::text::scope(
                protocol,
                "https://example.test/v1/responses",
                model,
                tenant,
                config
            )
            .unwrap()
        );
    }
    for url in [
        "http://example.test/v1",
        "https://key:secret@example.test/v1",
        "https://example.test/v1?api_key=secret",
        "https://example.test/v1#secret",
    ] {
        assert!(crate::infrastructure::text::scope("p", url, "m", "tenant", "").is_err());
    }
    assert!(
        crate::infrastructure::http::secret("test-secret", true)
            .unwrap()
            .is_sensitive()
    );
}

#[cfg(feature = "openai-compatible")]
#[tokio::test]
async fn chat_explicit_legacy_token_field_and_effort_mapping() {
    let body = chat_body(
        vec![chat_event(json!({"content":"ok"}), json!("stop"))],
        true,
    );
    let (url, wire, handle) = server(body, 4096, true).await;
    let mut c = crate::text_model::openai_compatible::Config::new(
        url,
        "test-secret".into(),
        "fixture-model".into(),
        "tenant".into(),
    );
    c.max_tokens_field = "max_tokens".into();
    c.supports_reasoning_effort = true;
    let model = crate::text_model::openai_compatible::OpenAiCompatible::new(c).unwrap();
    let mut req = request();
    req.reasoning = Some(Reasoning::Medium);
    generate(&model, req).await.unwrap();
    let (_, body) = wire.await.unwrap();
    assert_eq!(body["max_tokens"], 2048);
    assert!(body.get("max_completion_tokens").is_none());
    assert_eq!(body["reasoning_effort"], "medium");
    closed(handle).await;
}
#[cfg(feature = "openai-responses")]
#[tokio::test]
async fn responses_effort_mapping_and_no_budget_coercion() {
    let body = sse(vec![
        json!({"type":"response.created"}),
        json!({"type":"response.completed","response":{"status":"completed","output":[]}}),
    ]);
    let (url, wire, handle) = server(body, 4096, true).await;
    let mut c = crate::text_model::openai_responses::Config::new(
        url,
        "test-secret".into(),
        "fixture-model".into(),
        "tenant".into(),
    );
    c.supports_reasoning = true;
    let model = crate::text_model::openai_responses::OpenAiResponses::new(c).unwrap();
    let mut req = request();
    req.reasoning = Some(Reasoning::Low);
    generate(&model, req).await.unwrap();
    let (_, body) = wire.await.unwrap();
    assert_eq!(body["reasoning"], json!({"effort":"low","summary":"auto"}));
    closed(handle).await;
    let mut req = request();
    req.reasoning = Some(Reasoning::BudgetTokens(1024));
    assert!(matches!(generate(&model, req).await, Err(Error::Config(_))));
}
#[cfg(feature = "anthropic")]
#[tokio::test]
async fn anthropic_effort_and_budget_remain_distinct_controls() {
    for control in [Reasoning::High, Reasoning::BudgetTokens(1024)] {
        let body = sse(vec![
            json!({"type":"message_start","message":{"role":"assistant","content":[]}}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
            json!({"type":"message_stop"}),
        ]);
        let (url, wire, handle) = server(body, 4096, true).await;
        let mut c = crate::text_model::anthropic::Config::new(
            url,
            "test-secret".into(),
            "fixture-model".into(),
            "tenant".into(),
        );
        c.supports_adaptive_thinking = true;
        c.supports_thinking_budget = true;
        let model = crate::text_model::anthropic::Anthropic::new(c).unwrap();
        let mut req = request();
        req.reasoning = Some(control);
        generate(&model, req).await.unwrap();
        let (_, body) = wire.await.unwrap();
        if control == Reasoning::High {
            assert_eq!(body["thinking"]["type"], "adaptive");
            assert_eq!(body["output_config"]["effort"], "high");
        } else {
            assert_eq!(
                body["thinking"],
                json!({"type":"enabled","budget_tokens":1024})
            );
        }
        closed(handle).await;
        let mut req = request();
        req.reasoning = Some(Reasoning::BudgetTokens(2048));
        assert!(matches!(generate(&model, req).await, Err(Error::Config(_))));
    }
}

#[cfg(feature = "openai-compatible")]
#[tokio::test]
async fn chat_reasoning_fragments_survive_persistence_and_tool_continuation() {
    let body = chat_body(
        vec![
            chat_event(
                json!({"role":"assistant","content":"","reasoning_content":""}),
                Value::Null,
            ),
            chat_event(json!({"reasoning_content":"先检查"}), Value::Null),
            chat_event(
                json!({"reasoning_content":"参数。","content":null}),
                Value::Null,
            ),
            chat_event(json!({"content":"执行中"}), Value::Null),
            chat_event(
                json!({"tool_calls":[{"index":0,"id":"call_","type":"function","function":{"name":"r","arguments":"{ \"x\" :"}}]}),
                Value::Null,
            ),
            chat_event(
                json!({"tool_calls":[{"index":0,"id":"1","function":{"name":"un","arguments":" 1 }"}}]}),
                json!("tool_calls"),
            ),
            json!({"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":20,"completion_tokens_details":{"reasoning_tokens":8}}}),
        ],
        true,
    );
    let (url, wire, handle) = server(body, 1, true).await;
    let response = generate(chat(url).as_ref(), request()).await.unwrap();
    assert_eq!(response.finish_reason, FinishReason::ToolCalls);
    assert_eq!(response.message.text(), "执行中");
    assert_eq!(
        response.message.content[0],
        ModelContent::ReasoningSummary("先检查参数。".into())
    );
    assert_eq!(
        response.message.tool_calls().next().unwrap().arguments,
        json!({"x":1})
    );
    let native = response.message.continuation.as_ref().unwrap().data.clone();
    assert_eq!(native["reasoning_content"], "先检查参数。");
    assert_eq!(
        native["tool_calls"][0]["function"]["arguments"],
        "{ \"x\" : 1 }"
    );
    assert_eq!(response.usage.unwrap().reasoning_tokens, Some(8));
    let _ = wire.await.unwrap();
    closed(handle).await;
    // Session persistence round trip retains source, native reasoning and exact argument bytes.
    let mut saved: AssistantMessage =
        serde_json::from_str(&serde_json::to_string(&response.message).unwrap()).unwrap();
    let reply = chat_body(
        vec![
            chat_event(json!({"reasoning_content":"工具已返回。"}), Value::Null),
            chat_event(json!({"content":"完成"}), json!("stop")),
        ],
        true,
    );
    let (url, wire, handle) = server(reply, 3, true).await;
    let model = chat(url);
    // Only this fixture changes endpoint identity to inspect replay through a second listener.
    saved.continuation.as_mut().unwrap().source = model.continuation_source().into();
    let mut next = request();
    next.messages.push(ModelMessage::Assistant(saved));
    next.messages.push(ModelMessage::ToolResult {
        call_id: "call_1".into(),
        result: json!({"ok":true}),
    });
    let result = generate(model.as_ref(), next).await.unwrap();
    assert_eq!(result.message.text(), "完成");
    let (_, outgoing) = wire.await.unwrap();
    assert_eq!(outgoing["messages"][3], native);
    assert_eq!(outgoing["messages"].as_array().unwrap().len(), 5);
    assert_eq!(outgoing["messages"][4]["tool_call_id"], "call_1");
    closed(handle).await;
}

#[cfg(feature = "openai-compatible")]
#[tokio::test]
async fn chat_empty_and_null_reasoning_headers_are_compatible() {
    for placeholder in [Value::Null, json!("")] {
        let body = chat_body(
            vec![
                chat_event(
                    json!({"role":"assistant","content":"","reasoning_content":placeholder}),
                    Value::Null,
                ),
                chat_event(json!({"content":"OK"}), json!("stop")),
            ],
            true,
        );
        let (url, wire, handle) = server(body, 1, true).await;
        let result = generate(chat(url).as_ref(), request()).await.unwrap();
        assert_eq!(
            result.message.content,
            vec![ModelContent::Text("OK".into())]
        );
        assert_eq!(
            result.message.continuation.is_some(),
            !placeholder.is_null()
        );
        let _ = wire.await.unwrap();
        closed(handle).await;
    }
}

#[cfg(feature = "openai-compatible")]
#[tokio::test]
async fn chat_native_reasoning_rejects_changed_projection_and_foreign_sources_before_network() {
    let model = chat("http://127.0.0.1:1/not-listening".into());
    let message = AssistantMessage {
        content: vec![
            ModelContent::ReasoningSummary("plan".into()),
            ModelContent::Text("answer".into()),
            ModelContent::ToolCall(ToolCall {
                call_id: "a".into(),
                name: "run".into(),
                arguments: json!({"x":1}),
            }),
        ],
        continuation: Some(ModelContinuation {
            source: model.continuation_source().into(),
            data: json!({"role":"assistant","content":"answer","reasoning_content":"plan","tool_calls":[{"id":"a","type":"function","function":{"name":"run","arguments":"{ \"x\": 1 }"}}]}),
        }),
    };
    let mut changes = Vec::new();
    let mut changed = message.clone();
    changed.content[0] = ModelContent::ReasoningSummary("modified".into());
    changes.push(changed);
    let mut changed = message.clone();
    changed.content[1] = ModelContent::Text("modified".into());
    changes.push(changed);
    let mut changed = message.clone();
    if let ModelContent::ToolCall(call) = &mut changed.content[2] {
        call.arguments = json!({"x":2});
    }
    changes.push(changed);
    let mut changed = message.clone();
    changed.continuation.as_mut().unwrap().data["role"] = json!("system");
    changes.push(changed);
    let mut changed = message.clone();
    changed.continuation.as_mut().unwrap().data["unexpected"] = json!(true);
    changes.push(changed);
    let mut changed = message.clone();
    changed.continuation.as_mut().unwrap().source = "another-account".into();
    changes.push(changed);
    let mut changed = message.clone();
    changed.continuation = None;
    changes.push(changed);
    for changed in changes {
        let mut r = request();
        r.messages.push(ModelMessage::Assistant(changed));
        r.messages.push(ModelMessage::ToolResult {
            call_id: "a".into(),
            result: json!({}),
        });
        let error = generate(model.as_ref(), r).await.unwrap_err();
        assert!(matches!(error, Error::Config(_) | Error::Protocol(_)));
        assert!(
            !error.to_string().contains("transport failed"),
            "replay validation reached network"
        );
    }
}

#[cfg(feature = "openai-compatible")]
#[tokio::test]
async fn chat_reasoning_remains_bounded_and_requires_terminal_event() {
    let (url, wire, handle) = server(
        chat_body(
            vec![chat_event(
                json!({"reasoning_content":"partial"}),
                json!("stop"),
            )],
            false,
        ),
        4096,
        false,
    )
    .await;
    assert!(generate(chat(url).as_ref(), request()).await.is_err());
    let _ = wire.await.unwrap();
    closed(handle).await;
    for invalid in [
        json!(false),
        json!(10),
        json!([]),
        json!({"secret":"test-secret"}),
    ] {
        let (url, wire, handle) = server(
            chat_body(
                vec![chat_event(
                    json!({"reasoning_content":invalid}),
                    json!("stop"),
                )],
                true,
            ),
            4096,
            true,
        )
        .await;
        let error = generate(chat(url).as_ref(), request()).await.unwrap_err();
        assert!(!error.to_string().contains("test-secret"));
        let _ = wire.await.unwrap();
        closed(handle).await;
    }
    let events = (0..9)
        .map(|_| {
            chat_event(
                json!({"reasoning_content":"x".repeat(128*1024)}),
                Value::Null,
            )
        })
        .collect();
    let (url, wire, handle) = server(chat_body(events, true), 8192, true).await;
    let error = text_model::generate(
        chat(url).as_ref(),
        request(),
        128,
        4 * 1024 * 1024,
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, Error::Busy(_)));
    let _ = wire.await.unwrap();
    closed(handle).await;
}
