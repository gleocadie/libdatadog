// Copyright 2024-Present Datadog, Inc. https://www.datadoghq.com/
// SPDX-License-Identifier: Apache-2.0

use hex::FromHex;
use http::{Request, Response, Uri};
use hyper::body::{Body, Incoming};
use hyper::rt::{Read, Write};
use hyper_util::rt::TokioIo;
use std::result::Result as StdResult;
use std::{io, path, sync, time};
use tokio::net::TcpStream;
use tokio::time::error::Elapsed;
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_util::sync::CancellationToken;

pub trait UriExt {
    fn from_path<S, P>(scheme: S, path: P) -> http::Result<Uri>
    where
        http::uri::Scheme: TryFrom<S>,
        <http::uri::Scheme as TryFrom<S>>::Error: Into<http::Error>,
        P: AsRef<path::Path>;
}

impl UriExt for Uri {
    fn from_path<S, P>(scheme: S, path: P) -> http::Result<Uri>
    where
        http::uri::Scheme: TryFrom<S>,
        <http::uri::Scheme as TryFrom<S>>::Error: Into<http::Error>,
        P: AsRef<path::Path>,
    {
        let hex_encoded_path = {
            let path = path.as_ref();
            #[cfg(unix)]
            {
                use std::os::unix::prelude::*;
                hex::encode(path.as_os_str().as_bytes())
            }
            #[cfg(not(unix))]
            {
                hex::encode(path.to_string_lossy())
            }
        };
        Uri::builder()
            .scheme(scheme)
            .authority(hex_encoded_path)
            .build()
    }

    #[cfg(not(unix))]
    fn from_unix_path(path: &path::Path) -> http::Result<Uri> {
        Uri::builder()
            .scheme("unix")
            .authority(path.to_string_lossy())
            .build()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Dns(#[from] rustls::pki_types::InvalidDnsNameError),

    #[error(transparent)]
    Http(#[from] http::Error),

    #[error(transparent)]
    Hyper(#[from] hyper::Error),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Rustls(#[from] rustls::Error),

    #[error(transparent)]
    Timeout(#[from] Elapsed),

    #[error("unsupported scheme: `{0}`")]
    UnsupportedScheme(String),

    #[error("user requested cancellation")]
    UserRequestedCancellation,
}

/// Sends a blocking HTTP request using the provided runtime, inferring the
/// connector type to use from the request's URI scheme. This is the primary
/// API for sending an infrequent HTTP request, such as once per minute for
/// profilers.
pub fn one_shot<B>(
    runtime: &tokio::runtime::Runtime,
    request: Request<B>,
    cancel: Option<&CancellationToken>,
    timeout: Option<time::Duration>,
) -> StdResult<Response<Incoming>, Error>
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    runtime.block_on(async move {
        tokio::select! {
            result = async {
                Ok(match timeout {
                    Some(t) => tokio::time::timeout(t, send_and_infer_connector(request)).await?,
                    None => send_and_infer_connector(request).await,
                }?)}
            => result,
            _ = async { match cancel {
                    Some(token) => token.cancelled().await,
                    // If no token is provided, future::pending() provides a no-op future that never resolves
                    None => std::future::pending().await,
                }}
            => Err(Error::UserRequestedCancellation),
        }
    })
}

pub async fn send_and_infer_connector<B>(
    request: Request<B>,
) -> StdResult<Response<Incoming>, Error>
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let uri = request.uri();
    match uri.scheme() {
        None => Err(Error::UnsupportedScheme(String::new())),
        Some(scheme) => match scheme.as_str() {
            "http" => send_http(request).await,
            "https" => send_https(request).await,
            #[cfg(unix)]
            "unix" => send_via_unix_socket(request).await,
            #[cfg(windows)]
            "windows" => send_via_named_pipe(request).await,
            scheme => Err(Error::UnsupportedScheme(String::from(scheme))),
        },
    }
}

#[cfg(unix)]
pub async fn send_via_unix_socket<B>(request: Request<B>) -> StdResult<Response<Incoming>, Error>
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let path = parse_path_from_uri(&request.uri())?;
    let unix_stream = tokio::net::UnixStream::connect(path).await?;
    let hyper_wrapper = TokioIo::new(unix_stream);

    Ok(send_via_io(request, hyper_wrapper).await?)
}

#[cfg(windows)]
pub async fn send_via_named_pipe<B>(request: Request<B>) -> StdResult<Response<Incoming>, Error>
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let _path = parse_path_from_uri(&request.uri())?;
    todo!("re-implement named pipes on Windows")
}

pub async fn send_http<B>(request: Request<B>) -> StdResult<Response<Incoming>, Error>
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    // This _should_ be a redundant check, caller should only call this if
    // they expect it's an http connection to begin with.
    if request.uri().scheme_str() != Some("http") {
        return Err(Error::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "URI scheme must be http",
        )));
    }

    let authority = request
        .uri()
        .authority()
        .ok_or(Error::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "URI must have host",
        )))?
        .as_str();
    let stream = TcpStream::connect(authority).await?;
    let hyper_wrapper = TokioIo::new(stream);

    Ok(send_via_io(request, hyper_wrapper).await?)
}

pub async fn send_https<B>(request: Request<B>) -> StdResult<Response<Incoming>, Error>
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let uri = request.uri();

    // This _should_ be a redundant check, caller should only call this if
    // they expect it's an https connection to begin with.
    if uri.scheme_str() != Some("https") {
        return Err(Error::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "URI scheme must be https",
        )));
    }

    let server_name = ServerName::try_from(uri.to_string())?;
    let connector = {
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        tokio_rustls::TlsConnector::from(sync::Arc::new(config))
    };

    let tcp_stream = {
        let authority = uri.authority().ok_or(Error::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "URI must have a host",
        )))?;
        TcpStream::connect(authority.as_str()).await?
    };

    let stream = connector.connect(server_name, tcp_stream).await?;
    let hyper_wrapper = TokioIo::new(stream);
    Ok(send_via_io(request, hyper_wrapper).await?)
}

async fn send_via_io<T, B>(
    request: Request<B>,
    io: T,
) -> StdResult<Response<Incoming>, hyper::Error>
where
    T: Read + Write + Send + Unpin + 'static,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let (mut sender, connection) = hyper::client::conn::http1::handshake(io).await?;

    // The docs say we need to poll this to drive it to completion, but they
    // never directly use the return type or anything:
    // https://hyper.rs/guides/1/client/basic/
    let _todo = tokio::spawn(async move { connection.await });

    sender.send_request(request).await
}

pub fn parse_path_from_uri(uri: &Uri) -> io::Result<path::PathBuf> {
    // This _should_ be a redundant check, caller should only call this if
    // they expect it's a unix domain socket or windows named pipe.
    if uri.scheme_str() != Some("unix") || uri.scheme_str() != Some("windows") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "URI scheme must be unix or windows",
        ));
    }

    if let Some(host) = uri.host() {
        let bytes = Vec::from_hex(host).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("URI host must be a hex-encoded path: {err}"),
            )
        })?;
        let str = match std::str::from_utf8(&bytes) {
            Ok(s) => s,
            Err(err) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("URI is invalid: {err}"),
                ))
            }
        };
        Ok(path::PathBuf::from(str))
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "URI is missing host",
        ))
    }
}
