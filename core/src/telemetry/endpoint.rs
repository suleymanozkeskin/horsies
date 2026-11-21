//! The HTTP server of a metrics endpoint: bind a [`ListenAddress`], then
//! serve an axum router until a stop. Built with the `metrics-endpoint`
//! feature.
//!
//! The router decides the routes: a path that no route takes answers 404,
//! and a method that its route does not take answers 405. A `GET` route
//! also answers `HEAD`. axum stays internal to the core: the public items
//! name no axum type.
//!
//! Each connection is served with HTTP/1.1 under three bounds
//! ([`EndpointBounds`]):
//! - at most [`ENDPOINT_MAX_CONNECTIONS`] connections at once; a connection
//!   over it is closed at once, with no answer;
//! - a request head not read within [`ENDPOINT_REQUEST_TIMEOUT`] closes the
//!   connection, with no answer; this also closes a keep-alive connection
//!   that sends no next request;
//! - a connection buffers at most [`ENDPOINT_MAX_REQUEST_BYTES`] of a
//!   request: a head that does not fit is answered 431 and the connection
//!   closes. No route reads a body, so a body is never buffered past it.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum::Router;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::telemetry::exposition::PROMETHEUS_TEXT_CONTENT_TYPE;
use crate::telemetry::listen::ListenAddress;

/// The most connections that a metrics endpoint serves at once. A
/// connection over it is closed at once, with no answer.
// Provisional value: the telemetry overhead measurement (the no-op drain
// with the worker endpoint on, against telemetry off) sets it.
pub const ENDPOINT_MAX_CONNECTIONS: NonZeroUsize = match NonZeroUsize::new(16) {
    Some(connections) => connections,
    None => panic!("16 is not zero"),
};

/// The longest time to read one request head, from the first read of the
/// head. A head not read within it closes the connection, with no answer.
// Provisional value: the telemetry overhead measurement (the no-op drain
// with the worker endpoint on, against telemetry off) sets it.
pub const ENDPOINT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// The most bytes of a request that a connection buffers: a request head
/// that does not fit is answered 431 and the connection closes.
// Provisional value: the telemetry overhead measurement (the no-op drain
// with the worker endpoint on, against telemetry off) sets it.
pub const ENDPOINT_MAX_REQUEST_BYTES: RequestBytes = match RequestBytes::new(8 * 1024) {
    Some(bytes) => bytes,
    None => panic!("8 KiB is the least request buffer of HTTP/1"),
};

/// The wait after a failed accept that is not a failure of one connection
/// (for example no free file descriptor), before the next accept.
const ACCEPT_FAILURE_PAUSE: Duration = Duration::from_secs(1);

/// The least request buffer that hyper's HTTP/1 server takes.
const LEAST_REQUEST_BYTES: usize = 8 * 1024;

/// A request buffer size: at least 8 KiB, the least that HTTP/1 serving
/// takes. Only [`RequestBytes::new`] builds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestBytes(usize);

impl RequestBytes {
    /// `bytes`, or `None` below 8 KiB.
    pub const fn new(bytes: usize) -> Option<Self> {
        match bytes >= LEAST_REQUEST_BYTES {
            true => Some(Self(bytes)),
            false => None,
        }
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

/// The bounds of one metrics endpoint server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndpointBounds {
    /// The most connections served at once.
    pub max_connections: NonZeroUsize,
    /// The longest time to read one request head. At a stop, the
    /// connections in flight also get this long to end.
    pub request_timeout: Duration,
    /// The most bytes of a request that a connection buffers.
    pub max_request_bytes: RequestBytes,
}

impl EndpointBounds {
    /// The bounds of every metrics endpoint server: the named constants.
    pub const PROVISIONAL: Self = Self {
        max_connections: ENDPOINT_MAX_CONNECTIONS,
        request_timeout: ENDPOINT_REQUEST_TIMEOUT,
        max_request_bytes: ENDPOINT_MAX_REQUEST_BYTES,
    };
}

/// Why a metrics endpoint did not serve.
#[derive(Debug, thiserror::Error)]
pub enum EndpointFailed {
    /// The address did not bind: its host does not resolve, its port is in
    /// use, or the process may not bind it. Nothing was served. Another try
    /// may bind once the port is free.
    #[error("could not bind the metrics endpoint to {address}: {cause}")]
    Bind {
        address: ListenAddress,
        cause: io::Error,
    },
    /// The bound address could not be read back from the socket. Nothing
    /// was served.
    #[error(
        "the metrics endpoint bound {address}, but its socket address is not readable: {cause}"
    )]
    LocalAddress {
        address: ListenAddress,
        cause: io::Error,
    },
}

/// A listener bound to the address of a metrics endpoint.
#[derive(Debug)]
pub struct BoundEndpoint {
    listener: TcpListener,
    local: SocketAddr,
}

impl BoundEndpoint {
    /// The socket address that the listener holds.
    pub fn local_address(&self) -> SocketAddr {
        self.local
    }
}

/// Bind `address`. Resolves the host first; the first socket address that
/// binds is the endpoint.
///
/// Errors: [`EndpointFailed::Bind`], [`EndpointFailed::LocalAddress`].
pub async fn bind_endpoint(address: &ListenAddress) -> Result<BoundEndpoint, EndpointFailed> {
    let listener = TcpListener::bind((address.host(), address.port()))
        .await
        .map_err(|cause| EndpointFailed::Bind {
            address: address.clone(),
            cause,
        })?;
    let local = listener
        .local_addr()
        .map_err(|cause| EndpointFailed::LocalAddress {
            address: address.clone(),
            cause,
        })?;
    Ok(BoundEndpoint { listener, local })
}

/// A listener on a free port of `127.0.0.1`, for the tests of a router.
#[cfg(test)]
pub(crate) async fn bind_loopback_free_port() -> BoundEndpoint {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("a free loopback port binds");
    let local = listener
        .local_addr()
        .expect("a bound socket has an address");
    BoundEndpoint { listener, local }
}

/// Serve `router` on `endpoint` under [`EndpointBounds::PROVISIONAL`] until
/// `stop` resolves: [`serve_endpoint_with`].
pub(crate) async fn serve_endpoint<S>(endpoint: BoundEndpoint, router: Router, stop: S)
where
    S: Future<Output = ()> + Send + 'static,
{
    serve_endpoint_with(endpoint, router, stop, EndpointBounds::PROVISIONAL).await;
}

/// Serve `router` on `endpoint` under `bounds` until `stop` resolves.
///
/// Never fails. A failed accept of one connection is skipped; any other
/// failed accept (for example no free file descriptor) is logged, and the
/// next accept waits [`ACCEPT_FAILURE_PAUSE`]. At the stop the listener
/// closes, every connection ends after the request it serves, and a
/// connection that has not ended within `bounds.request_timeout` is
/// aborted. Effects: one task per open connection on the current runtime.
pub(crate) async fn serve_endpoint_with<S>(
    endpoint: BoundEndpoint,
    router: Router,
    stop: S,
    bounds: EndpointBounds,
) where
    S: Future<Output = ()> + Send + 'static,
{
    let BoundEndpoint { listener, local } = endpoint;
    let permits = Arc::new(Semaphore::new(bounds.max_connections.get()));
    let closing = CancellationToken::new();
    let mut connections: JoinSet<()> = JoinSet::new();
    tokio::pin!(stop);
    loop {
        tokio::select! {
            () = &mut stop => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => match Arc::clone(&permits).try_acquire_owned() {
                    Ok(permit) => {
                        let _connection = connections.spawn(serve_connection(
                            stream,
                            router.clone(),
                            bounds,
                            closing.clone(),
                            permit,
                        ));
                    }
                    Err(_all_taken) => refuse_connection(stream, local, peer, bounds),
                },
                Err(cause) => pause_after_failed_accept(local, &cause).await,
            },
            Some(ended) = connections.join_next() => log_connection_end(local, ended),
        }
    }
    drop(listener);
    closing.cancel();
    stop_connections(&mut connections, local, bounds).await;
}

/// Serve one connection until it ends, or until `closing` fires and its
/// request in flight ends. `permit` holds its place under the connection
/// bound until then.
async fn serve_connection(
    stream: TcpStream,
    router: Router,
    bounds: EndpointBounds,
    closing: CancellationToken,
    permit: OwnedSemaphorePermit,
) {
    let mut builder = http1::Builder::new();
    let _configured = builder
        .timer(TokioTimer::new())
        .header_read_timeout(bounds.request_timeout)
        .max_buf_size(bounds.max_request_bytes.get());
    let connection =
        builder.serve_connection(TokioIo::new(stream), TowerToHyperService::new(router));
    tokio::pin!(connection);
    let ended = tokio::select! {
        ended = connection.as_mut() => ended,
        () = closing.cancelled_owned() => {
            connection.as_mut().graceful_shutdown();
            connection.as_mut().await
        }
    };
    if let Err(cause) = ended {
        // A client that closed early, a head over a bound, or a head that
        // timed out: the connection is closed; nothing else depends on it.
        tracing::debug!(%cause, "a metrics endpoint connection ended: {cause}");
    }
    drop(permit);
}

/// Close `stream` with no answer: every place under the connection bound
/// is taken.
fn refuse_connection(
    stream: TcpStream,
    local: SocketAddr,
    peer: SocketAddr,
    bounds: EndpointBounds,
) {
    tracing::warn!(
        endpoint = %local,
        %peer,
        bound = bounds.max_connections.get(),
        "the metrics endpoint on {local} refused a connection from {peer}: it serves {} \
         connections already",
        bounds.max_connections.get(),
    );
    drop(stream);
}

/// After a failed accept: nothing for a failure of the one connection
/// (the client left during the handshake); else log it and wait
/// [`ACCEPT_FAILURE_PAUSE`], since the next accept would fail the same way
/// at once.
async fn pause_after_failed_accept(local: SocketAddr, cause: &io::Error) {
    match cause.kind() {
        io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionRefused => {}
        _ => {
            tracing::warn!(
                endpoint = %local,
                %cause,
                pause_s = ACCEPT_FAILURE_PAUSE.as_secs_f64(),
                "the metrics endpoint on {local} could not accept a connection: {cause}; next \
                 accept in {}s",
                ACCEPT_FAILURE_PAUSE.as_secs_f64(),
            );
            tokio::time::sleep(ACCEPT_FAILURE_PAUSE).await;
        }
    }
}

/// Log a connection task that did not end on its own: a panic is a defect
/// of the router or the server; a cancel only comes from the abort at the
/// stop.
fn log_connection_end(local: SocketAddr, ended: Result<(), tokio::task::JoinError>) {
    match ended {
        Ok(()) => {}
        Err(failure) => tracing::error!(
            endpoint = %local,
            %failure,
            "a connection task of the metrics endpoint on {local} did not end: {failure}",
        ),
    }
}

/// Wait at most `bounds.request_timeout` for every connection to end, then
/// abort the ones left.
async fn stop_connections(
    connections: &mut JoinSet<()>,
    local: SocketAddr,
    bounds: EndpointBounds,
) {
    let drained = tokio::time::timeout(bounds.request_timeout, async {
        while let Some(ended) = connections.join_next().await {
            log_connection_end(local, ended);
        }
    })
    .await;
    match drained {
        Ok(()) => {}
        Err(_elapsed) => {
            tracing::warn!(
                endpoint = %local,
                left = connections.len(),
                "the metrics endpoint on {local} stopped with {} connections that did not end \
                 within {}s; they are closed",
                connections.len(),
                bounds.request_timeout.as_secs_f64(),
            );
            connections.shutdown().await;
        }
    }
}

/// The answer of `GET /metrics`: `text` with the content type of the
/// Prometheus text format.
pub(crate) fn metrics_response(text: String) -> Response {
    ([(CONTENT_TYPE, PROMETHEUS_TEXT_CONTENT_TYPE)], text).into_response()
}

#[cfg(test)]
mod tests;
