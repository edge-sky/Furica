//! Shared HTTP safety and cancellation; no provider or audio semantics.
use furica_core::{Error, Result};
use reqwest::{Client, Response, Url, header::HeaderValue};
use std::{future::Future, time::Duration};
use tokio_util::sync::CancellationToken;

pub(crate) async fn cancellable<T>(
    cancel: &CancellationToken,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(Error::Cancelled),
        result = future => result,
    }
}

pub(crate) fn text(value: &str, name: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > 1024 || value.chars().any(char::is_control) {
        return Err(Error::Config(format!("invalid {name}")));
    }
    Ok(())
}

pub(crate) fn endpoint(value: &str, websocket: bool) -> Result<()> {
    let url = Url::parse(value).map_err(|_| Error::Config("invalid provider endpoint".into()))?;
    let secure = if websocket { "wss" } else { "https" };
    let local_scheme = if websocket { "ws" } else { "http" };
    let local = is_loopback(&url);
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || !(url.scheme() == secure || (local && url.scheme() == local_scheme))
    {
        return Err(Error::Config(
            "provider endpoint requires TLS (except loopback) and no embedded credentials".into(),
        ));
    }
    Ok(())
}

fn is_loopback(url: &Url) -> bool {
    url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    })
}

pub(crate) fn secret(value: &str, bearer: bool) -> Result<HeaderValue> {
    text(value, "provider credential")?;
    let mut header = HeaderValue::from_str(&if bearer {
        format!("Bearer {value}")
    } else {
        value.to_owned()
    })
    .map_err(|_| Error::Config("invalid provider credential".into()))?;
    header.set_sensitive(true);
    Ok(header)
}

#[cfg(any(feature = "aliyun", feature = "volcengine"))]
pub(crate) fn client(endpoint: &str) -> Result<Client> {
    client_with_timeout(endpoint, Duration::from_secs(10))
}

pub(crate) fn client_with_timeout(endpoint: &str, timeout: Duration) -> Result<Client> {
    if timeout.is_zero() {
        return Err(Error::Config("zero HTTP connect timeout".into()));
    }
    let mut builder = Client::builder()
        .connect_timeout(timeout)
        .redirect(reqwest::redirect::Policy::none());
    // Loopback endpoints are local test/proxy services. System proxy settings
    // must not route them through an unrelated forward proxy.
    if Url::parse(endpoint).is_ok_and(|url| is_loopback(&url)) {
        builder = builder.no_proxy();
    }
    builder
        .build()
        .map_err(|_| Error::Config("cannot create provider HTTP client".into()))
}

pub(crate) fn transport(error: reqwest::Error) -> Error {
    if error.is_timeout() {
        Error::Timeout("provider HTTP request")
    } else {
        Error::Protocol("provider HTTP transport failed".into())
    }
}

pub(crate) fn status(response: &Response) -> Result<()> {
    if !response.status().is_success() {
        return Err(Error::Protocol(format!(
            "provider HTTP status {}",
            response.status().as_u16()
        )));
    }
    Ok(())
}
