//! Loopback HTTP/WebSocket transport. Exactly one browser owns the device session
//! and snapshot writer, including while the owner is shutting down.
use crate::{
    conversation::{Command, Session, infrastructure::input::decode_audio, presentation},
    settings::{SettingsStore, presentation as settings_view},
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    Request, Response, StatusCode, body::Incoming, server::conn::http1, service::service_fn,
};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::net::TcpListener;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        handshake::server::create_response,
        protocol::{Role, WebSocketConfig},
    },
};
use tokio_util::sync::CancellationToken;

type HostResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Body = Full<Bytes>;

pub async fn serve(config: Arc<SettingsStore>, port: u16) -> HostResult<()> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    let port = listener.local_addr()?.port();
    println!(
        "Furica demo: http://127.0.0.1:{port}\nExternal configuration remains server-side. Ctrl+C stops the host."
    );
    let busy = Arc::new(AtomicBool::new(false));
    let stop = CancellationToken::new();
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _=tokio::signal::ctrl_c()=>break,
            incoming=listener.accept()=>{
                let (socket,_)=incoming?;
                let config=config.clone(); let busy=busy.clone(); let stop=stop.clone();
                connections.spawn(async move {
                    let service=service_fn(move |request| http(request,config.clone(),busy.clone(),stop.clone(),port));
                    let connection=http1::Builder::new().serve_connection(TokioIo::new(socket),service).with_upgrades();
                    let _=connection.await;
                });
            }
            Some(_)=connections.join_next()=>(),
        }
    }
    stop.cancel();
    // Upgraded connections have their own task. Its lease remains held until
    // Core shutdown has acknowledged cancellation and released the database.
    let _ = tokio::time::timeout(Duration::from_secs(15), async {
        while busy.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    connections.abort_all();
    Ok(())
}

fn response(status: StatusCode, content_type: &str, body: impl Into<Bytes>) -> Response<Body> {
    Response::builder().status(status).header("Content-Type",content_type)
        .header("Cache-Control","no-store").header("X-Content-Type-Options","nosniff")
        .header("Content-Security-Policy","default-src 'self'; connect-src 'self'; script-src 'self'; style-src 'self'; worker-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
        .body(Full::new(body.into())).expect("static response headers")
}

fn allowed_host(host: &str, port: u16) -> bool {
    host == format!("127.0.0.1:{port}") || host == format!("localhost:{port}")
}

async fn http(
    mut request: Request<Incoming>,
    config: Arc<SettingsStore>,
    busy: Arc<AtomicBool>,
    stop: CancellationToken,
    port: u16,
) -> Result<Response<Body>, Infallible> {
    let host = request
        .headers()
        .get("host")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !allowed_host(host, port) {
        return Ok(response(
            StatusCode::FORBIDDEN,
            "text/plain",
            "Loopback host required",
        ));
    }
    if request.uri().path() == "/api/settings" {
        let host = host.to_owned();
        return Ok(settings_http(request, config, host).await);
    }
    if request.method() != hyper::Method::GET {
        return Ok(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "text/plain",
            "GET required",
        ));
    }
    if request.uri().path() == "/ws" {
        let expected = format!("http://{host}");
        if request
            .headers()
            .get("origin")
            .and_then(|value| value.to_str().ok())
            != Some(expected.as_str())
        {
            return Ok(response(
                StatusCode::FORBIDDEN,
                "text/plain",
                "Same-origin browser required",
            ));
        }
        let handshake_request = {
            let mut builder = hyper::http::Request::builder()
                .method(request.method())
                .uri(request.uri());
            *builder.headers_mut().expect("request builder") = request.headers().clone();
            builder.body(()).expect("valid request")
        };
        let Ok(handshake) = create_response(&handshake_request) else {
            return Ok(response(
                StatusCode::BAD_REQUEST,
                "text/plain",
                "Invalid WebSocket handshake",
            ));
        };
        if busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(response(
                StatusCode::CONFLICT,
                "text/plain",
                "Another browser owns this demo. Close it before reconnecting.",
            ));
        }
        let lease = Lease(busy);
        let upgraded = hyper::upgrade::on(&mut request);
        tokio::spawn(async move {
            let _lease = lease;
            if let Ok(Ok(io)) = tokio::time::timeout(Duration::from_secs(5), upgraded).await {
                let ws_config = WebSocketConfig::default()
                    .max_message_size(Some(128 * 1024))
                    .max_frame_size(Some(128 * 1024));
                let socket = WebSocketStream::from_raw_socket(
                    TokioIo::new(io),
                    Role::Server,
                    Some(ws_config),
                )
                .await;
                if let Err(error) = connection(socket, config, stop).await {
                    eprintln!("demo connection: {error}");
                }
            }
        });
        let (parts, _) = handshake.into_parts();
        return Ok(Response::from_parts(parts, Full::new(Bytes::new())));
    }
    let (kind, body) = match request.uri().path() {
        "/" | "/index.html" => (
            "text/html; charset=utf-8",
            include_str!("../../web/index.html"),
        ),
        "/style.css" => (
            "text/css; charset=utf-8",
            include_str!("../../web/style.css"),
        ),
        "/favicon.svg" => ("image/svg+xml", include_str!("../../web/favicon.svg")),
        "/app.js" => (
            "text/javascript; charset=utf-8",
            include_str!("../../web/app.js"),
        ),
        "/conversation/audio.js" => (
            "text/javascript; charset=utf-8",
            include_str!("../../web/conversation/audio.js"),
        ),
        "/conversation/capture-worklet.js" => (
            "text/javascript; charset=utf-8",
            include_str!("../../web/conversation/capture-worklet.js"),
        ),
        "/sessions/panel.js" => (
            "text/javascript; charset=utf-8",
            include_str!("../../web/sessions/panel.js"),
        ),
        "/settings/panel.js" => (
            "text/javascript; charset=utf-8",
            include_str!("../../web/settings/panel.js"),
        ),
        "/diagnostics.js" => (
            "text/javascript; charset=utf-8",
            include_str!("../../web/diagnostics.js"),
        ),
        "/health" => {
            return Ok(response(
                StatusCode::OK,
                "application/json",
                r#"{"ok":true}"#,
            ));
        }
        _ => return Ok(response(StatusCode::NOT_FOUND, "text/plain", "Not found")),
    };
    Ok(response(StatusCode::OK, kind, body))
}

async fn settings_http(
    request: Request<Incoming>,
    store: Arc<SettingsStore>,
    host: String,
) -> Response<Body> {
    let reading = request.method() == hyper::Method::GET;
    let result = match *request.method() {
        hyper::Method::GET => store.read().await,
        hyper::Method::PUT => {
            let expected = format!("http://{host}");
            if request
                .headers()
                .get("origin")
                .and_then(|v| v.to_str().ok())
                != Some(expected.as_str())
            {
                return response(
                    StatusCode::FORBIDDEN,
                    "application/json",
                    r#"{"error":"仅允许同源页面保存设置"}"#,
                );
            }
            if request
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .map(|v| v.split(';').next().unwrap_or("").trim())
                != Some("application/json")
            {
                return response(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "application/json",
                    r#"{"error":"需要 JSON 设置"}"#,
                );
            }
            let bytes = match tokio::time::timeout(
                Duration::from_secs(5),
                Limited::new(request.into_body(), 64 * 1024).collect(),
            )
            .await
            {
                Ok(Ok(body)) => body.to_bytes(),
                Ok(Err(_)) => {
                    return response(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "application/json",
                        r#"{"error":"设置不能超过 64 KiB"}"#,
                    );
                }
                Err(_) => {
                    return response(
                        StatusCode::REQUEST_TIMEOUT,
                        "application/json",
                        r#"{"error":"读取设置超时"}"#,
                    );
                }
            };
            let value = match serde_json::from_slice::<Value>(&bytes) {
                Ok(value) => value,
                Err(_) => {
                    return response(
                        StatusCode::BAD_REQUEST,
                        "application/json",
                        r#"{"error":"设置 JSON 格式无效"}"#,
                    );
                }
            };
            store.write(value).await
        }
        _ => {
            return response(
                StatusCode::METHOD_NOT_ALLOWED,
                "application/json",
                r#"{"error":"仅支持 GET 和 PUT"}"#,
            );
        }
    };
    match result {
        Ok(config) => response(
            StatusCode::OK,
            "application/json",
            settings_view::response(&config).to_string(),
        ),
        Err(error) => response(
            if reading {
                StatusCode::INTERNAL_SERVER_ERROR
            } else {
                StatusCode::BAD_REQUEST
            },
            "application/json",
            json!({"error":error.to_string()}).to_string(),
        ),
    }
}

struct Lease(Arc<AtomicBool>);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

async fn send<S>(socket: &mut WebSocketStream<S>, value: Value) -> HostResult<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    tokio::time::timeout(
        Duration::from_secs(3),
        socket.send(Message::Text(value.to_string().into())),
    )
    .await??;
    Ok(())
}

async fn connection<S>(
    mut socket: WebSocketStream<S>,
    config: Arc<SettingsStore>,
    stop: CancellationToken,
) -> HostResult<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut session: Option<Session> = None;
    let result=async {
        send(&mut socket,config.read().await?.public_status()).await?;
        match crate::sessions::SnapshotStore::configured().load() {
            Ok(data) => send(&mut socket, crate::sessions::snapshot(data)).await?,
            Err(error) => send(&mut socket, json!({"type":"save_error","message":error.to_string()})).await?,
        }
        loop {
            tokio::select! {
                _=stop.cancelled()=>break,
                event=async {match &mut session {Some(session)=>session.next().await,None=>std::future::pending().await}}=>{
                    let stopped=event["type"]=="kernel_stopped";
                    send(&mut socket,event).await?;
                    if stopped && let Some(old)=session.take(){
                        old.stop().await?;
                        send(&mut socket,crate::sessions::snapshot(crate::sessions::SnapshotStore::configured().load()?)).await?;
                    }
                }
                incoming=socket.next()=>{
                    let Some(incoming)=incoming else {break};
                    match incoming? {
                        Message::Close(_)=>break,
                        Message::Ping(bytes)=>{socket.send(Message::Pong(bytes)).await?;},
                        Message::Text(text)=>{
                            let command=match serde_json::from_str::<Command>(&text) {
                                Ok(command)=>command,
                                Err(_)=>{send(&mut socket,json!({"type":"error","message":"无效的控制消息或字段"})).await?;continue;},
                            };
                            match command {
                                Command::Start {mode,speaker,session_id}=>{
                                    if session.is_some(){send(&mut socket,json!({"type":"error","message":"请先停止当前内核"})).await?;continue;}
                                    let latest = match config.read().await {
                                        Ok(latest) => latest,
                                        Err(_) => {send(&mut socket,json!({"type":"start_error","message":"无法读取服务设置"})).await?;continue;}
                                    };
                                    match Session::start(&latest,mode,&speaker,session_id).await {
                                        Ok(started)=>{
                                            send(&mut socket,json!({"type":"started","state":presentation::state(started.core.state()),"session_id":started.session_id})).await?;
                                            send(&mut socket,crate::sessions::snapshot(started.manager.snapshot()?)).await?;
                                            let playback=presentation::playback(*started.output.playback.borrow());
                                            send(&mut socket,playback).await?;
                                            session=Some(started);
                                        }
                                        Err(error)=>send(&mut socket,json!({"type":"start_error","message":error.to_string()})).await?,
                                    }
                                }
                                Command::Stop=>{
                                    if let Some(old)=session.take(){old.stop().await?;}
                                    send(&mut socket,json!({"type":"stopped"})).await?;
                                    send(&mut socket,crate::sessions::snapshot(crate::sessions::SnapshotStore::configured().load()?)).await?;
                                }
                                Command::Snapshot if session.is_none() => {
                                    match crate::sessions::SnapshotStore::configured().load() {
                                        Ok(data) => send(&mut socket,crate::sessions::snapshot(data)).await?,
                                        Err(error) => send(&mut socket,json!({"type":"save_error","message":error.to_string()})).await?,
                                    }
                                }
                                command=>{
                                    match &session {
                                        Some(current)=>match current.command(command).await {
                                            Ok(value)=>send(&mut socket,value).await?,
                                            Err(error)=>send(&mut socket,json!({"type":"error","message":error.to_string()})).await?,
                                        },
                                        None=>send(&mut socket,json!({"type":"error","message":"请先启动内核"})).await?,
                                    }
                                }
                            }
                        }
                        Message::Binary(bytes)=>{
                            let delivered=(||->furica_core::Result<()> {
                                let frame=decode_audio(&bytes)?;
                                let capture=session.as_ref().and_then(|session|session.capture.as_ref())
                                    .ok_or_else(||furica_core::Error::State("语音采集尚未启动".into()))?;
                                capture.try_send(frame).map_err(|_|furica_core::Error::Busy("demo VAD capture queue"))
                            })();
                            if let Err(error)=delivered {
                                if let Some(current)=session.as_mut(){current.capture.take();let _=current.core.end_conversation().await;}
                                send(&mut socket,json!({"type":"capture_error","message":error.to_string()})).await?;
                            }
                        }
                        _=>(),
                    }
                }
            }
        }
        Ok::<(),Box<dyn std::error::Error+Send+Sync>>(())
    }.await;
    if let Some(current) = session {
        let cleanup = current.stop().await;
        if result.is_ok() {
            cleanup?;
        }
    }
    // SinkExt::close flushes the reply already queued by an incoming Close.
    // Sending a second Message::Close would fail with SendAfterClosing.
    let _ = tokio::time::timeout(Duration::from_secs(1), SinkExt::close(&mut socket)).await;
    result
}
