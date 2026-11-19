//! The bind of a metrics endpoint, and the bounds of its server: the
//! connection bound, the request time bound and the request size bound.
//! Each test binds a free port of `127.0.0.1`.

use std::time::Instant;

use axum::routing::get;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::*;

/// The longest a test waits for one answer, or for the server to close a
/// connection.
const TEST_READ_BOUND: Duration = Duration::from_secs(10);

/// The most connections that a test opens while it waits for the server to
/// free the place of a closed connection.
const PLACE_FREED_TRIES: usize = 200;

/// The wait between two of those connections.
const PLACE_FREED_PAUSE: Duration = Duration::from_millis(10);

/// The text of `GET /metrics` in the test router.
const METRICS_TEXT: &str = "up 1\n";

/// A request for `/metrics` that asks the server to close after the answer.
const GET_METRICS: &str = "GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";

/// The router of the tests: `GET /metrics` answers [`METRICS_TEXT`].
fn test_router() -> Router {
    Router::new().route(
        "/metrics",
        get(|| async { metrics_response(METRICS_TEXT.to_owned()) }),
    )
}

/// Bounds with `connections` places and a request time bound of `timeout`,
/// and the provisional request size bound.
fn bounds(connections: usize, timeout: Duration) -> EndpointBounds {
    EndpointBounds {
        max_connections: NonZeroUsize::new(connections).expect("a test bound above zero"),
        request_timeout: timeout,
        max_request_bytes: ENDPOINT_MAX_REQUEST_BYTES,
    }
}

/// A server of [`test_router`] under `bounds` on a free port: its port, the
/// sender that stops it, and its task.
async fn start_server(bounds: EndpointBounds) -> (u16, oneshot::Sender<()>, JoinHandle<()>) {
    let endpoint = bind_loopback_free_port().await;
    let port = endpoint.local_address().port();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(serve_endpoint_with(
        endpoint,
        test_router(),
        async move {
            let _sent_or_dropped = stopped.await;
        },
        bounds,
    ));
    (port, stop, server)
}

async fn connect(port: u16) -> TcpStream {
    TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("the endpoint accepts")
}

/// How a connection ended for its client.
#[derive(Debug, PartialEq, Eq)]
enum ClientEnd {
    /// The server wrote this text, then closed the connection.
    Answered(String),
    /// The server closed the connection with no answer: an end of stream
    /// with no byte, a reset, or a write that failed.
    ClosedWithNoAnswer,
}

/// Read `stream` until the server closes it, at most [`TEST_READ_BOUND`].
async fn read_until_closed(stream: &mut TcpStream) -> ClientEnd {
    let mut bytes = Vec::new();
    match tokio::time::timeout(TEST_READ_BOUND, stream.read_to_end(&mut bytes)).await {
        Err(_elapsed) => panic!("the server did not close the connection within the test bound"),
        Ok(Err(_reset)) => ClientEnd::ClosedWithNoAnswer,
        Ok(Ok(0)) => ClientEnd::ClosedWithNoAnswer,
        Ok(Ok(_read)) => ClientEnd::Answered(String::from_utf8_lossy(&bytes).into_owned()),
    }
}

/// Send `request` on a new connection to `port` and read until the close.
async fn exchange(port: u16, request: &[u8]) -> ClientEnd {
    let mut stream = connect(port).await;
    match stream.write_all(request).await {
        Ok(()) => read_until_closed(&mut stream).await,
        Err(_closed) => ClientEnd::ClosedWithNoAnswer,
    }
}

/// End the server and wait for its task.
async fn stop_server(stop: oneshot::Sender<()>, server: JoinHandle<()>) {
    let _sent = stop.send(());
    tokio::time::timeout(TEST_READ_BOUND, server)
        .await
        .expect("the server ends within the test bound")
        .expect("the server task ends without a panic");
}

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

#[test]
fn every_server_takes_the_named_bounds() {
    assert_eq!(
        EndpointBounds::PROVISIONAL,
        EndpointBounds {
            max_connections: ENDPOINT_MAX_CONNECTIONS,
            request_timeout: ENDPOINT_REQUEST_TIMEOUT,
            max_request_bytes: ENDPOINT_MAX_REQUEST_BYTES,
        }
    );
    assert_eq!(ENDPOINT_MAX_CONNECTIONS.get(), 16);
    assert_eq!(ENDPOINT_REQUEST_TIMEOUT, Duration::from_secs(5));
    assert_eq!(ENDPOINT_MAX_REQUEST_BYTES.get(), 8 * 1024);
    assert_eq!(RequestBytes::new(8 * 1024 - 1), None);
}

#[tokio::test]
async fn get_metrics_answers_under_every_bound() {
    let (port, stop, server) = start_server(bounds(2, Duration::from_secs(30))).await;
    // A head of about 4 KiB fits the 8 KiB request bound.
    let filler = "a".repeat(4 * 1024);
    let request = format!(
        "GET /metrics HTTP/1.1\r\nHost: localhost\r\nX-Filler: {filler}\r\nConnection: \
         close\r\n\r\n"
    );
    match exchange(port, request.as_bytes()).await {
        ClientEnd::Answered(text) => {
            assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "{text}");
            assert!(text.ends_with(METRICS_TEXT), "{text}");
        }
        ClientEnd::ClosedWithNoAnswer => panic!("a request under every bound is answered"),
    }
    stop_server(stop, server).await;
}

#[tokio::test]
async fn a_connection_over_the_bound_is_closed_and_one_after_a_close_is_served() {
    let (port, stop, server) = start_server(bounds(2, Duration::from_secs(30))).await;
    // Two connections that send nothing hold the two places: the server
    // accepts in order, so both are served before the third is accepted.
    let first = connect(port).await;
    let _second = connect(port).await;
    let mut over = connect(port).await;
    assert_eq!(
        read_until_closed(&mut over).await,
        ClientEnd::ClosedWithNoAnswer
    );

    // The close of `first` frees its place once the server reads the end of
    // its stream; a connection after that is answered.
    drop(first);
    let mut answered = None;
    for _ in 0..PLACE_FREED_TRIES {
        match exchange(port, GET_METRICS.as_bytes()).await {
            ClientEnd::Answered(text) => {
                answered = Some(text);
                break;
            }
            ClientEnd::ClosedWithNoAnswer => tokio::time::sleep(PLACE_FREED_PAUSE).await,
        }
    }
    let text = answered.expect("a connection after the close is answered");
    assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "{text}");
    stop_server(stop, server).await;
}

#[tokio::test]
async fn a_head_not_read_within_the_time_bound_closes_the_connection() {
    let timeout = Duration::from_millis(200);
    let (port, stop, server) = start_server(bounds(4, timeout)).await;

    // No byte at all.
    let started = Instant::now();
    let mut silent = connect(port).await;
    assert_eq!(
        read_until_closed(&mut silent).await,
        ClientEnd::ClosedWithNoAnswer
    );
    assert!(started.elapsed() >= timeout, "{:?}", started.elapsed());

    // A head that never ends: the bound counts from the first read of the
    // head, not from the last byte.
    let started = Instant::now();
    let mut partial = connect(port).await;
    partial
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .expect("the start of the head is written");
    assert_eq!(
        read_until_closed(&mut partial).await,
        ClientEnd::ClosedWithNoAnswer
    );
    assert!(started.elapsed() >= timeout, "{:?}", started.elapsed());
    stop_server(stop, server).await;
}

#[tokio::test]
async fn a_head_over_the_size_bound_is_answered_431() {
    let (port, stop, server) = start_server(bounds(4, Duration::from_secs(30))).await;
    // Exactly the bound in bytes and no end of the head: the server reads
    // every byte, so its close leaves nothing unread.
    let start = "GET /metrics HTTP/1.1\r\nHost: localhost\r\nX-Filler: ";
    let filler = "a".repeat(ENDPOINT_MAX_REQUEST_BYTES.get() - start.len());
    let request = format!("{start}{filler}");
    assert_eq!(request.len(), ENDPOINT_MAX_REQUEST_BYTES.get());
    match exchange(port, request.as_bytes()).await {
        ClientEnd::Answered(text) => {
            assert!(text.starts_with("HTTP/1.1 431 "), "{text}");
        }
        ClientEnd::ClosedWithNoAnswer => panic!("a head over the bound is answered 431"),
    }
    stop_server(stop, server).await;
}

#[tokio::test]
async fn the_stop_closes_a_connection_that_sent_nothing_at_once() {
    let (port, stop, server) = start_server(bounds(4, Duration::from_secs(30))).await;
    let mut idle = connect(port).await;
    // The connection is accepted once a request on another one is answered.
    assert!(matches!(
        exchange(port, GET_METRICS.as_bytes()).await,
        ClientEnd::Answered(_)
    ));
    // The request time bound is 30 s: a stop that waited for it would pass
    // the 10 s bound of `stop_server`.
    stop_server(stop, server).await;
    assert_eq!(
        read_until_closed(&mut idle).await,
        ClientEnd::ClosedWithNoAnswer
    );
}
