//! Loopback fixtures with deliberately fragmented responses and optional open connections.
use serde_json::Value;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    task::JoinHandle,
};
#[cfg(any(
    feature = "openai-compatible",
    feature = "openai-responses",
    feature = "anthropic"
))]
pub(crate) async fn server(
    body: String,
    stride: usize,
    open: bool,
) -> (String, oneshot::Receiver<(String, Value)>, JoinHandle<()>) {
    server_bytes(body.into_bytes(), stride, open).await
}

#[cfg(any(
    feature = "openai-compatible",
    feature = "openai-responses",
    feature = "anthropic"
))]
pub(crate) async fn server_bytes(
    body: Vec<u8>,
    stride: usize,
    open: bool,
) -> (String, oneshot::Receiver<(String, Value)>, JoinHandle<()>) {
    respond(
        body,
        stride,
        open,
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\n",
    )
    .await
}

pub(crate) async fn respond(
    body: Vec<u8>,
    stride: usize,
    open: bool,
    headers: &str,
) -> (String, oneshot::Receiver<(String, Value)>, JoinHandle<()>) {
    let headers = format!("{headers}transfer-encoding: chunked\r\nconnection: close\r\n\r\n");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/model", listener.local_addr().unwrap());
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let (header_end, length) = loop {
            let mut part = [0; 4096];
            let n = socket.read(&mut part).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&part[..n]);
            if let Some(p) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                let h = std::str::from_utf8(&bytes[..p]).unwrap();
                let length = h
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .map(str::to_owned)
                    })
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                break (p + 4, length);
            }
        };
        while bytes.len() < header_end + length {
            let mut p = [0; 4096];
            let n = socket.read(&mut p).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&p[..n]);
        }
        let request_headers = String::from_utf8(bytes[..header_end].to_vec())
            .unwrap()
            .to_ascii_lowercase();
        let payload = serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
        let _ = tx.send((request_headers, payload));
        if socket.write_all(headers.as_bytes()).await.is_err() {
            return;
        }
        for part in body.chunks(stride) {
            let frame = format!("{:x}\r\n", part.len());
            if socket.write_all(frame.as_bytes()).await.is_err()
                || socket.write_all(part).await.is_err()
                || socket.write_all(b"\r\n").await.is_err()
            {
                return;
            }
        }
        if !open {
            let _ = socket.write_all(b"0\r\n\r\n").await;
            return;
        }
        // A terminal/error/drop must close local resources without server EOF.
        let mut byte = [0; 1];
        let n = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut byte))
            .await
            .expect("client retained response after completion/drop")
            .unwrap();
        assert_eq!(n, 0);
    });
    (url, rx, handle)
}
pub(crate) async fn closed(handle: JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(4), handle)
        .await
        .unwrap()
        .unwrap();
}
