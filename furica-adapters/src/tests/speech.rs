//! Regression coverage for the speech consumers of extracted HTTP mechanics.
use crate::tests::http::{closed, respond};
use base64::{Engine, engine::general_purpose::STANDARD};
use furica_core::{
    audio::AudioFrame,
    stt::{Stt, Utterance},
};
use serde_json::json;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
fn utterance() -> Utterance {
    Utterance {
        sample_rate: 24_000,
        frames: vec![AudioFrame {
            sequence: 1,
            sample_rate: 24_000,
            samples: vec![1, -2],
        }],
    }
}
fn wav(encoded: &str) {
    let bytes = STANDARD.decode(encoded).unwrap();
    assert_eq!(&bytes[..4], b"RIFF");
    assert_eq!(&bytes[8..12], b"WAVE");
    assert_eq!(&bytes[44..], [1, 0, 254, 255]);
}
#[cfg(feature = "aliyun")]
#[tokio::test]
async fn aliyun_http_stt_still_encodes_wav_and_reads_final_result() {
    let (url, wire, handle) = respond(
        json!({"output":{"text":"heard"}}).to_string().into_bytes(),
        1,
        false,
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n",
    )
    .await;
    let adapter = crate::aliyun::AliyunStt::new(crate::aliyun::SttConfig {
        api_key: "test-secret".into(),
        endpoint: url,
        model: "asr".into(),
    })
    .unwrap();
    assert_eq!(
        adapter
            .transcribe(utterance(), CancellationToken::new())
            .await
            .unwrap(),
        "heard"
    );
    let (headers, body) = wire.await.unwrap();
    assert!(headers.contains("authorization: bearer test-secret"));
    wav(body["input"]["messages"][0]["content"][0]["audio"]
        .as_str()
        .unwrap()
        .strip_prefix("data:audio/wav;base64,")
        .unwrap());
    closed(handle).await;
}
#[cfg(feature = "volcengine")]
#[tokio::test]
async fn volcengine_http_stt_and_chunked_tts_keep_wire_behavior() {
    let (url, wire, handle) = respond(
        json!({"result":{"text":"heard"}}).to_string().into_bytes(),
        1,
        false,
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nx-api-status-code: 20000000\r\n",
    )
    .await;
    let adapter = crate::volcengine::VolcengineStt::new(crate::volcengine::SttConfig {
        api_key: "test-secret".into(),
        endpoint: url,
        resource_id: "resource".into(),
        model: "asr".into(),
    })
    .unwrap();
    assert_eq!(
        adapter
            .transcribe(utterance(), CancellationToken::new())
            .await
            .unwrap(),
        "heard"
    );
    let (headers, body) = wire.await.unwrap();
    assert!(headers.contains("x-api-key: test-secret"));
    wav(body["audio"]["data"].as_str().unwrap());
    closed(handle).await;
    let body = format!(
        "{}\n{}\n",
        json!({"code":0,"data":STANDARD.encode([1])}),
        json!({"code":20000000,"data":STANDARD.encode([0,254,255])})
    );
    let (url, wire, handle) = respond(
        body.into_bytes(),
        1,
        true,
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n",
    )
    .await;
    let adapter = crate::volcengine::VolcengineTts::new(crate::volcengine::TtsConfig {
        api_key: "test-secret".into(),
        endpoint: url,
        resource_id: "resource".into(),
        model: "tts".into(),
        voice: "speaker".into(),
        sample_rate: 24_000,
    })
    .unwrap();
    let audio = furica_core::tts::synthesize(
        &adapter,
        "hello".into(),
        24_000,
        Duration::from_secs(1),
        100,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(audio.samples, [1, -2]);
    let (_, body) = wire.await.unwrap();
    assert_eq!(body["req_params"]["audio_params"]["format"], "pcm");
    closed(handle).await;
}
#[test]
fn pcm_half_samples_empty_and_truncation_still_validate() {
    let mut pcm = crate::infrastructure::speech::Pcm::new(24_000);
    assert!(pcm.finish().is_err());
    assert!(pcm.push(&[1]).unwrap().is_none());
    assert!(pcm.finish().is_err());
    assert_eq!(pcm.push(&[0, 254, 255]).unwrap().unwrap().samples, [1, -2]);
    assert!(pcm.finish().is_ok());
    let mut u = utterance();
    u.frames.push(u.frames[0].clone());
    assert!(crate::infrastructure::speech::wav_base64(u).is_err());
}

#[cfg(feature = "aliyun")]
#[tokio::test]
async fn cosyvoice_websocket_task_and_split_pcm_still_complete() {
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/tts", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(socket).await.unwrap();
        let start: serde_json::Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(start["header"]["action"], "run-task");
        let id = start["header"]["task_id"].as_str().unwrap();
        socket
            .send(Message::Text(
                json!({"header":{"event":"task-started","task_id":id}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        for action in ["continue-task", "finish-task"] {
            let input: serde_json::Value =
                serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap())
                    .unwrap();
            assert_eq!(input["header"]["action"], action);
        }
        socket.send(Message::Binary(vec![1].into())).await.unwrap();
        socket
            .send(Message::Binary(vec![0, 254, 255].into()))
            .await
            .unwrap();
        socket
            .send(Message::Text(
                json!({"header":{"event":"task-finished","task_id":id}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
    });
    let adapter = crate::aliyun::AliyunTts::new(crate::aliyun::TtsConfig {
        api_key: "test-secret".into(),
        endpoint: url,
        model: "cosyvoice".into(),
        voice: "speaker".into(),
        sample_rate: 24_000,
    })
    .unwrap();
    let audio = furica_core::tts::synthesize(
        &adapter,
        "hello".into(),
        24_000,
        Duration::from_secs(1),
        100,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(audio.samples, [1, -2]);
    closed(handle).await;
}
