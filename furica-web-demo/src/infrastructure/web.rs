//! Static Web shell and streaming, same-origin transport to an existing Host.
use bytes::Bytes;
use futures_util::StreamExt;
use http_body_util::{BodyExt, Full, Limited, StreamBody, combinators::UnsyncBoxBody};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Frame, Incoming},
    header,
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use reqwest::{Client, Url};
use std::{convert::Infallible, io, net::IpAddr, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Body = UnsyncBoxBody<Bytes, io::Error>;

pub fn host_url(value: &str) -> Result<Url> {
    let url = Url::parse(value)?;
    let loopback = url.host_str().is_some_and(|host| {
        host.trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
    });
    if url.scheme() != "http"
        || !loopback
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("--host requires an HTTP loopback IP URL without credentials or a path".into());
    }
    Ok(url)
}

pub async fn serve(host: Url, token: String, port: u16) -> Result<()> {
    let client = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .build()?;
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    let port = listener.local_addr()?.port();
    if host.port_or_known_default() == Some(port) && host.host_str() == Some("127.0.0.1") {
        return Err("Web and Host must use different listeners".into());
    }
    println!("Furica Web: http://127.0.0.1:{port}\nHost: {host}");
    let token = Arc::new(token);
    let stop = CancellationToken::new();
    let mut connections = tokio::task::JoinSet::new();
    let shutdown = async {
        #[cfg(unix)]
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => (), _ = signal.recv() => () }
            return;
        }
        let _ = tokio::signal::ctrl_c().await;
    };
    tokio::pin!(shutdown);
    let result = loop {
        tokio::select! {
            _ = &mut shutdown => break Ok(()),
            Some(_) = connections.join_next(), if !connections.is_empty() => (),
            incoming = listener.accept() => {
                let (socket, _) = match incoming { Ok(value) => value, Err(error) => break Err(error.into()) };
                if connections.len() >= 256 { continue; }
                let (client, host, token, stop) = (client.clone(), host.clone(), token.clone(), stop.clone());
                connections.spawn(async move {
                    let service = service_fn(move |request| handle(request, client.clone(), host.clone(), token.clone(), port));
                    let mut builder = http1::Builder::new();
                    builder.timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(5)).max_headers(64).max_buf_size(32768);
                    let connection = builder.serve_connection(TokioIo::new(socket), service);
                    tokio::pin!(connection);
                    tokio::select! { _ = &mut connection => (), _ = stop.cancelled() => () }
                });
            }
        }
    };
    stop.cancel();
    while connections.join_next().await.is_some() {}
    result
}

fn response(status: StatusCode, content_type: &str, body: impl Into<Bytes>) -> Response<Body> {
    let mut response = Response::new(
        Full::new(body.into())
            .map_err(|never| match never {})
            .boxed_unsync(),
    );
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        content_type.parse().expect("static content type"),
    );
    secure(&mut response);
    response
}

fn secure(response: &mut Response<Body>) {
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    headers.insert("x-content-type-options", "nosniff".parse().unwrap());
    headers.insert("content-security-policy", "default-src 'self'; connect-src 'self'; script-src 'self'; style-src 'self'; worker-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'".parse().unwrap());
}

fn allowed(request: &Request<Incoming>, port: u16) -> bool {
    if request.headers().get_all(header::HOST).iter().count() != 1
        || request.headers().get_all(header::ORIGIN).iter().count() > 1
    {
        return false;
    }
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if host != format!("127.0.0.1:{port}") && host != format!("localhost:{port}") {
        return false;
    }
    let origin = request.headers().get(header::ORIGIN);
    if let Some(origin) = origin {
        if origin.to_str().ok() != Some(format!("http://{host}").as_str()) {
            return false;
        }
    } else if !matches!(*request.method(), Method::GET | Method::HEAD) {
        return false;
    }
    true
}

async fn handle(
    request: Request<Incoming>,
    client: Client,
    host: Url,
    token: Arc<String>,
    port: u16,
) -> std::result::Result<Response<Body>, Infallible> {
    if !allowed(&request, port) {
        return Ok(response(
            StatusCode::FORBIDDEN,
            "text/plain",
            "Same-origin loopback request required",
        ));
    }
    if request.uri().path().starts_with("/api/") {
        return Ok(proxy(request, client, host, token).await);
    }
    if request.method() != Method::GET {
        return Ok(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "text/plain",
            "GET required",
        ));
    }
    let (kind, content) = match request.uri().path() {
        "/" | "/index.html" => (
            "text/html; charset=utf-8",
            include_str!("../../web/index.html"),
        ),
        "/style.css" => (
            "text/css; charset=utf-8",
            include_str!("../../web/style.css"),
        ),
        "/app.js" => ("text/javascript", include_str!("../../web/app.js")),
        "/host.js" => ("text/javascript", include_str!("../../web/host.js")),
        "/conversation/audio.js" => (
            "text/javascript",
            include_str!("../../web/conversation/audio.js"),
        ),
        "/conversation/voice.js" => (
            "text/javascript",
            include_str!("../../web/conversation/voice.js"),
        ),
        "/conversation/capture-worklet.js" => (
            "text/javascript",
            include_str!("../../web/conversation/capture-worklet.js"),
        ),
        "/sessions/panel.js" => (
            "text/javascript",
            include_str!("../../web/sessions/panel.js"),
        ),
        "/settings/panel.js" => (
            "text/javascript",
            include_str!("../../web/settings/panel.js"),
        ),
        "/diagnostics.js" => ("text/javascript", include_str!("../../web/diagnostics.js")),
        "/favicon.svg" => ("image/svg+xml", include_str!("../../web/favicon.svg")),
        _ => return Ok(response(StatusCode::NOT_FOUND, "text/plain", "Not found")),
    };
    Ok(response(StatusCode::OK, kind, content))
}

async fn proxy(
    request: Request<Incoming>,
    client: Client,
    mut host: Url,
    token: Arc<String>,
) -> Response<Body> {
    // Only forward the request path, never an authority or browser credentials.
    host.set_path(request.uri().path().strip_prefix("/api").expect("API path"));
    host.set_query(request.uri().query());
    let mut upstream = client
        .request(request.method().clone(), host)
        .bearer_auth(token.as_str());
    for name in [
        header::CONTENT_TYPE,
        header::HeaderName::from_static("x-voice-token"),
    ] {
        if request.headers().get_all(&name).iter().count() > 1 {
            return response(
                StatusCode::BAD_REQUEST,
                "text/plain",
                "Duplicate request header",
            );
        }
        if let Some(value) = request.headers().get(&name) {
            upstream = upstream.header(name, value);
        }
    }
    let bytes = match tokio::time::timeout(
        Duration::from_secs(5),
        Limited::new(request.into_body(), 1024 * 1024).collect(),
    )
    .await
    {
        Ok(Ok(body)) => body.to_bytes(),
        Ok(Err(_)) => {
            return response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "text/plain",
                "Request body exceeds its limit",
            );
        }
        Err(_) => {
            return response(
                StatusCode::REQUEST_TIMEOUT,
                "text/plain",
                "Request body timed out",
            );
        }
    };
    let result = tokio::time::timeout(Duration::from_secs(30), upstream.body(bytes).send()).await;
    let upstream = match result {
        Ok(Ok(value)) => value,
        _ => {
            return response(
                StatusCode::BAD_GATEWAY,
                "application/json",
                r#"{"code":"host_unavailable","message":"无法连接 Host，请检查进程、地址与访问令牌。","retryable":true}"#,
            );
        }
    };
    let status = upstream.status();
    let content_type = upstream.headers().get(header::CONTENT_TYPE).cloned();
    let stream = upstream.bytes_stream().map(|chunk| {
        chunk
            .map(Frame::data)
            .map_err(|_| io::Error::other("Host stream disconnected"))
    });
    let mut response = Response::new(StreamBody::new(stream).boxed_unsync());
    *response.status_mut() = status;
    if let Some(value) = content_type {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    secure(&mut response);
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn host_address_is_explicit_loopback_without_credentials_or_redirects() {
        for url in ["http://127.0.0.1:8765", "http://[::1]:8765"] {
            assert!(host_url(url).is_ok());
        }
        for url in [
            "https://127.0.0.1",
            "http://localhost:8765",
            "http://example.com",
            "http://127.0.0.1/api",
            "http://user:secret@127.0.0.1",
            "http://127.0.0.1?token=secret",
            "http://127.0.0.1#fragment",
        ] {
            assert!(host_url(url).is_err(), "{url}");
        }
    }
}
