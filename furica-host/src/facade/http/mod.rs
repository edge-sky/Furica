mod interaction;
mod projection;
mod sessions;
mod voice;

use crate::{Error, Host, Result, configuration::application::HostConfig, facade::constant_eq};
use bytes::Bytes;
use furica_core::playback::{PlaybackChunk, PlaybackState};
use futures_util::stream;
use http_body_util::{BodyExt, Full, StreamBody, combinators::UnsyncBoxBody};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Frame, Incoming},
    header::{self, HeaderValue},
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use interaction::{SelectSession, TextInput};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{convert::Infallible, future::Future, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, broadcast, mpsc, watch},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use voice::{OpenVoice, PlaybackReport, encode};

type Body = UnsyncBoxBody<Bytes, Infallible>;
type HttpResponse = Response<Body>;

struct Access {
    host: Arc<Host>,
    token: String,
    address: SocketAddr,
    stop: CancellationToken,
    streams: Arc<Semaphore>,
}

/// The listener is supplied so tests can bind an ephemeral loopback port.
pub async fn serve(
    listener: TcpListener,
    host: Arc<Host>,
    token: String,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<()> {
    let address = listener.local_addr().map_err(|_| Error::config())?;
    if !address.ip().is_loopback() || token.len() < 32 {
        return Err(Error::config());
    }
    let access = Arc::new(Access {
        streams: Arc::new(Semaphore::new(
            host.config().limits.channel_capacity.min(128),
        )),
        host,
        token,
        address,
        stop: CancellationToken::new(),
    });
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);
    let result = loop {
        tokio::select! {
            biased;
            _=&mut shutdown=>break Ok(()),
            Some(_)=connections.join_next(),if !connections.is_empty()=>(),
            accepted=listener.accept()=>{
                let (socket,peer)=match accepted {Ok(pair)=>pair,Err(_)=>break Err(Error::new(503,"accept_failed","listener failed"))};
                if !peer.ip().is_loopback() || connections.len()>=256 {continue;}
                let access=access.clone();
                connections.spawn(async move {
                    let service=service_fn({let access=access.clone();move |request| handle(request,access.clone())});
                    let mut builder=hyper::server::conn::http1::Builder::new();
                    builder.timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(5)).max_headers(64).max_buf_size(32768);
                    let connection=builder.serve_connection(TokioIo::new(socket),service);
                    tokio::pin!(connection);
                    tokio::select! {
                        _=&mut connection=>(),
                        _=access.stop.cancelled()=>{
                            connection.as_mut().graceful_shutdown();
                            let _=tokio::time::timeout(Duration::from_secs(2),&mut connection).await;
                        },
                    }
                });
            },
        }
    };
    access.stop.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), async {
        while connections.join_next().await.is_some() {}
    })
    .await;
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    result
}

async fn handle(
    request: Request<Incoming>,
    access: Arc<Access>,
) -> std::result::Result<HttpResponse, Infallible> {
    let origin = request.headers().get(header::ORIGIN).cloned();
    let permitted_origin = origin.as_ref().and_then(|v| v.to_str().ok()).filter(|s| {
        access
            .host
            .config()
            .http
            .allowed_origins
            .iter()
            .any(|o| o == s)
    });
    let response = match authorize(&request, &access) {
        Err(error) => error_response(error),
        Ok(()) => {
            if request.method() == Method::OPTIONS {
                preflight(&request)
            } else {
                match route(request, access.clone()).await {
                    Ok(response) => response,
                    Err(error) => error_response(error),
                }
            }
        }
    };
    let mut response = response;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    if permitted_origin.is_some() {
        if let Some(origin) = origin {
            response
                .headers_mut()
                .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        }
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("Origin"));
    }
    Ok(response)
}

fn authorize(request: &Request<Incoming>, access: &Access) -> Result<()> {
    if request.headers().get_all(header::HOST).iter().count() != 1
        || request.headers().get_all(header::ORIGIN).iter().count() > 1
    {
        return Err(Error::new(403, "invalid_host", "Host or Origin is invalid"));
    }
    let authority = request
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Error::new(403, "invalid_host", "Host is required"))?;
    let port = access.address.port();
    if authority != access.address.to_string() && authority != format!("localhost:{port}") {
        return Err(Error::new(
            403,
            "invalid_host",
            "Host is outside the loopback listener",
        ));
    }
    if let Some(origin) = request.headers().get(header::ORIGIN) {
        let value = origin
            .to_str()
            .map_err(|_| Error::new(403, "invalid_origin", "Origin is not allowed"))?;
        if !access
            .host
            .config()
            .http
            .allowed_origins
            .iter()
            .any(|o| o == value)
        {
            return Err(Error::new(403, "invalid_origin", "Origin is not allowed"));
        }
    }
    if request.method() == Method::OPTIONS {
        return Ok(());
    }
    if request
        .headers()
        .get_all(header::AUTHORIZATION)
        .iter()
        .count()
        != 1
    {
        return Err(Error::new(
            401,
            "unauthorized",
            "Bearer credential is required",
        ));
    }
    let token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("");
    if !constant_eq(token.as_bytes(), access.token.as_bytes()) {
        return Err(Error::new(
            401,
            "unauthorized",
            "Bearer credential is invalid",
        ));
    }
    Ok(())
}

fn preflight(request: &Request<Incoming>) -> HttpResponse {
    let method = request
        .headers()
        .get(header::ACCESS_CONTROL_REQUEST_METHOD)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let headers = request
        .headers()
        .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !matches!(method, "GET" | "POST" | "PUT" | "PATCH" | "DELETE")
        || headers.split(',').any(|h| {
            !matches!(
                h.trim().to_ascii_lowercase().as_str(),
                "" | "authorization" | "content-type" | "x-voice-token"
            )
        })
    {
        return error_response(Error::new(
            403,
            "preflight_denied",
            "method or headers are not allowed",
        ));
    }
    let mut response = json_response(204, Value::Null);
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, PUT, PATCH, DELETE"),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Authorization, Content-Type, X-Voice-Token"),
    );
    response
}

async fn route(mut request: Request<Incoming>, access: Arc<Access>) -> Result<HttpResponse> {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let segments: Vec<_> = path.trim_start_matches('/').split('/').collect();
    let (offset, limit) = pagination(request.uri().query())?;
    let host = &access.host;
    match (method.as_str(), segments.as_slice()) {
        ("GET", ["health"]) => Ok(json_response(200, host.health().await)),
        ("GET", ["configuration"]) => {
            let config = host.saved_configuration().await?;
            Ok(json_response(200, json!(config)))
        }
        ("GET", ["configuration", "active"]) => Ok(json_response(200, json!(host.config()))),
        ("PUT", ["configuration"]) => {
            let config: HostConfig = json_body(&mut request).await?;
            host.update_configuration(config).await?;
            Ok(json_response(
                200,
                json!({"saved":true,"applied":true,"restart_required":false}),
            ))
        }
        ("POST", ["configuration", "reload"]) => {
            host.reload_configuration().await?;
            Ok(json_response(
                200,
                json!({"applied":true,"restart_required":false}),
            ))
        }
        ("GET", ["sessions"]) => Ok(json_response(
            200,
            sessions::list(host, offset, limit).await?,
        )),
        ("POST", ["sessions"]) => Ok(json_response(201, json!(host.create_session().await?))),
        ("GET", ["sessions", id]) => Ok(json_response(
            200,
            sessions::query(host, uuid(id)?, None, offset, limit).await?,
        )),
        ("GET", ["sessions", id, resource]) => Ok(json_response(
            200,
            sessions::query(host, uuid(id)?, Some(resource), offset, limit).await?,
        )),
        ("PATCH", ["sessions", id]) => {
            let body: sessions::RenameSession = json_body(&mut request).await?;
            Ok(json_response(
                200,
                json!(host.rename_session(uuid(id)?, body.name).await?),
            ))
        }
        ("POST", ["sessions", id, "archive"]) => {
            Ok(json_response(200, json!(host.archive(uuid(id)?).await?)))
        }
        ("POST", ["interaction", "session"]) => {
            let body: SelectSession = json_body(&mut request).await?;
            host.select(body.session_id).await?;
            Ok(accepted())
        }
        ("POST", ["sessions", id, "text"]) => {
            let body: TextInput = json_body(&mut request).await?;
            host.text(uuid(id)?, body.text).await?;
            Ok(accepted())
        }
        ("POST", ["interaction", "interrupt"]) => {
            host.interrupt().await?;
            Ok(accepted())
        }
        ("POST", ["voice"]) => {
            let body: OpenVoice = json_body(&mut request).await?;
            Ok(json_response(
                201,
                host.open_voice(body.session_id, body.owner).await?,
            ))
        }
        ("DELETE", ["voice", id]) => {
            host.close_voice(uuid(id)?, voice_token(&request)?).await?;
            Ok(accepted())
        }
        ("POST", ["voice", id, "heartbeat"]) => {
            host.heartbeat(uuid(id)?, voice_token(&request)?).await?;
            Ok(accepted())
        }
        ("PUT", ["voice", id, "audio", sequence]) => {
            if request
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                != Some("application/octet-stream")
            {
                return Err(Error::new(
                    400,
                    "invalid_content_type",
                    "audio requires application/octet-stream",
                ));
            }
            let token = voice_token(&request)?.to_owned();
            let sequence = sequence.parse().map_err(|_| {
                Error::new(
                    400,
                    "invalid_sequence",
                    "sequence must be an unsigned integer",
                )
            })?;
            let pcm = read_body(&mut request, 6400).await?;
            Ok(json_response(
                200,
                host.upload(uuid(id)?, &token, sequence, pcm).await?,
            ))
        }
        ("POST", ["voice", id, "playback"]) => {
            let token = voice_token(&request)?.to_owned();
            let report: PlaybackReport = json_body(&mut request).await?;
            host.playback(uuid(id)?, &token, report.generation, report.position()?)
                .await?;
            Ok(accepted())
        }
        ("GET", ["voice", id, "audio"]) => {
            let permit = access
                .streams
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::new(429, "streams_full", "stream limit reached"))?;
            let id = uuid(id)?;
            let (receiver, playback, cancel) = host.take_audio(id, voice_token(&request)?).await?;
            let state = AudioStream {
                id,
                receiver,
                playback,
                cancel,
                stop: access.stop.clone(),
                _permit: permit,
            };
            let stream = stream::unfold(state, |mut state| async move {
                loop {
                    let chunk = tokio::select! { biased; _=state.cancel.cancelled()=>return None,_=state.stop.cancelled()=>return None,chunk=state.receiver.recv()=>chunk? };
                    if chunk.generation != state.playback.borrow().generation {
                        continue;
                    }
                    let bytes = match encode(state.id, chunk) {
                        Ok(bytes) => bytes,
                        Err(_) => return None,
                    };
                    return Some((Ok::<_, Infallible>(Frame::data(bytes)), state));
                }
            });
            Ok(stream_response(
                "application/octet-stream",
                StreamBody::new(stream).boxed_unsync(),
            ))
        }
        ("GET", ["events"]) => {
            let permit = access
                .streams
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::new(429, "streams_full", "stream limit reached"))?;
            let state = (host.subscribe(), access.stop.clone(), permit);
            let stream = stream::unfold(state, |(mut receiver, stop, permit)| async move {
                let value = tokio::select! {biased;
                    _=stop.cancelled()=>return None,
                    event=receiver.recv()=>match event {
                        Ok(value)=>format!("data: {value}\n\n"),
                        Err(broadcast::error::RecvError::Lagged(_))=>"data: {\"kind\":\"resync\"}\n\n".into(),
                        Err(broadcast::error::RecvError::Closed)=>return None,
                    },
                    _=tokio::time::sleep(Duration::from_secs(15))=>": keepalive\n\n".into(),
                };
                Some((
                    Ok::<_, Infallible>(Frame::data(Bytes::from(value))),
                    (receiver, stop, permit),
                ))
            });
            Ok(stream_response(
                "text/event-stream",
                StreamBody::new(stream).boxed_unsync(),
            ))
        }
        _ => Err(Error::new(404, "not_found", "route does not exist")),
    }
}

struct AudioStream {
    id: Uuid,
    receiver: mpsc::Receiver<PlaybackChunk>,
    playback: watch::Receiver<PlaybackState>,
    cancel: CancellationToken,
    stop: CancellationToken,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl Drop for AudioStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

fn voice_token(request: &Request<Incoming>) -> Result<&str> {
    if request.headers().get_all("x-voice-token").iter().count() != 1 {
        return Err(Error::new(
            401,
            "invalid_voice_token",
            "voice ownership credential is required",
        ));
    }
    request
        .headers()
        .get("x-voice-token")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            Error::new(
                401,
                "invalid_voice_token",
                "voice ownership credential is required",
            )
        })
}
fn uuid(value: &str) -> Result<Uuid> {
    Uuid::parse_str(value)
        .map_err(|_| Error::new(400, "invalid_id", "resource identity must be a UUID"))
}
fn pagination(query: Option<&str>) -> Result<(usize, usize)> {
    let (mut offset, mut limit) = (None, None);
    for pair in query.into_iter().flat_map(|q| q.split('&')) {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| Error::new(400, "invalid_page", "invalid pagination query"))?;
        let target = match key {
            "offset" => &mut offset,
            "limit" => &mut limit,
            _ => {
                return Err(Error::new(
                    400,
                    "invalid_page",
                    "unknown pagination parameter",
                ));
            }
        };
        if target.is_some() {
            return Err(Error::new(
                400,
                "invalid_page",
                "duplicate pagination parameter",
            ));
        }
        *target = Some(value.parse::<usize>().map_err(|_| {
            Error::new(
                400,
                "invalid_page",
                "pagination must be an unsigned integer",
            )
        })?);
    }
    let limit = limit.unwrap_or(50);
    if limit == 0 || limit > 200 {
        return Err(Error::new(400, "invalid_page", "page size must be 1–200"));
    }
    Ok((offset.unwrap_or(0), limit))
}
async fn json_body<T: DeserializeOwned>(request: &mut Request<Incoming>) -> Result<T> {
    if request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(';').next())
        != Some("application/json")
    {
        return Err(Error::new(
            400,
            "invalid_content_type",
            "request requires application/json",
        ));
    }
    serde_json::from_slice(&read_body(request, 1024 * 1024).await?)
        .map_err(|_| Error::new(400, "invalid_json", "JSON fields or values are invalid"))
}
async fn read_body(request: &mut Request<Incoming>, limit: usize) -> Result<Vec<u8>> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut bytes = Vec::new();
        while let Some(frame) = request.body_mut().frame().await {
            let frame =
                frame.map_err(|_| Error::new(400, "invalid_body", "request body failed"))?;
            if let Some(data) = frame.data_ref() {
                if data.len() > limit.saturating_sub(bytes.len()) {
                    return Err(Error::new(
                        413,
                        "body_too_large",
                        "request exceeds its byte limit",
                    ));
                }
                bytes.extend_from_slice(data);
            }
        }
        Ok(bytes)
    })
    .await
    .map_err(|_| Error::new(504, "body_timeout", "request body timed out"))?
}
fn accepted() -> HttpResponse {
    json_response(202, json!({"accepted":true}))
}
fn json_response(status: u16, value: Value) -> HttpResponse {
    let bytes = if status == 204 {
        Vec::new()
    } else {
        serde_json::to_vec(&value).expect("JSON value serialization")
    };
    let mut response = Response::new(Full::new(Bytes::from(bytes)).boxed_unsync());
    *response.status_mut() = StatusCode::from_u16(status).expect("static status");
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}
fn error_response(error: Error) -> HttpResponse {
    json_response(
        error.status,
        json!({"code":error.code,"message":error.message,"retryable":error.retryable}),
    )
}
fn stream_response(content_type: &'static str, body: Body) -> HttpResponse {
    let mut response = Response::new(body);
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}
