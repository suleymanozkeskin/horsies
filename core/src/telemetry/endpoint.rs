//! The HTTP server of a metrics endpoint: bind a [`ListenAddress`], then
//! serve an axum router until a stop. Built with the `metrics-endpoint`
//! feature.
//!
//! The router decides the routes: a path that no route takes answers 404,
//! and a method that its route does not take answers 405. A `GET` route
//! also answers `HEAD`. axum stays internal to the core: the public items
//! name no axum type.

use std::future::Future;
use std::io;
use std::net::SocketAddr;

use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum::Router;
use tokio::net::TcpListener;

use crate::telemetry::exposition::PROMETHEUS_TEXT_CONTENT_TYPE;
use crate::telemetry::listen::ListenAddress;

/// Why a metrics endpoint did not serve, or stopped serving.
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
    /// The server stopped with an I/O failure. The requests in flight
    /// ended with it.
    #[error("the metrics endpoint on {local} failed: {cause}")]
    Serve { local: SocketAddr, cause: io::Error },
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

/// Serve `router` on `endpoint` until `stop` resolves. The requests in
/// flight then end before the return.
///
/// Errors: [`EndpointFailed::Serve`].
pub(crate) async fn serve_endpoint<S>(
    endpoint: BoundEndpoint,
    router: Router,
    stop: S,
) -> Result<(), EndpointFailed>
where
    S: Future<Output = ()> + Send + 'static,
{
    let local = endpoint.local;
    axum::serve(endpoint.listener, router)
        .with_graceful_shutdown(stop)
        .await
        .map_err(|cause| EndpointFailed::Serve { local, cause })
}

/// The answer of `GET /metrics`: `text` with the content type of the
/// Prometheus text format.
pub(crate) fn metrics_response(text: String) -> Response {
    ([(CONTENT_TYPE, PROMETHEUS_TEXT_CONTENT_TYPE)], text).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_address_in_use_is_a_bind_failure_that_names_the_address() {
        let held = bind_loopback_free_port().await;
        let address = ListenAddress::parse(&format!("127.0.0.1:{}", held.local_address().port()))
            .expect("a valid address");
        match bind_endpoint(&address).await {
            Err(EndpointFailed::Bind {
                address: refused,
                cause,
            }) => {
                assert_eq!(refused, address);
                assert_eq!(cause.kind(), io::ErrorKind::AddrInUse);
            }
            other => panic!("expected a bind failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_free_address_binds_and_reports_its_socket_address() {
        let free = bind_loopback_free_port().await;
        let port = free.local_address().port();
        drop(free);
        let address = ListenAddress::parse(&format!("127.0.0.1:{port}")).expect("a valid address");
        let bound = bind_endpoint(&address)
            .await
            .expect("the port was just freed");
        assert_eq!(
            bound.local_address().to_string(),
            format!("127.0.0.1:{port}")
        );
    }
}
