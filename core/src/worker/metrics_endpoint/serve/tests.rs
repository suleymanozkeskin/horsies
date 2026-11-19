//! The worker endpoint thread: its routes and probe answers, a bind on a
//! port in use and the bind after the port is free, and the `telemetry`
//! column of each endpoint state. The thread tests bind `127.0.0.1`.

use std::net::Ipv4Addr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::*;
use crate::telemetry::endpoint::bind_loopback_free_port;
use crate::telemetry::exposition::PROMETHEUS_TEXT_CONTENT_TYPE;
use crate::test_support::telemetry::worker_registry;
use crate::worker::executor::protocol::ProcessId;
use crate::worker::executor::slots::{ChildLayout, PoolBoard, SlotPhase};
use crate::worker::liveness::LockHoldView;
use crate::worker::metrics_endpoint::{telemetry_json, WorkerEndpoint};
use crate::worker::probes::ServingFacts;

/// The longest a test waits for a state of the endpoint or for an answer.
const TEST_WAIT_BOUND: Duration = Duration::from_secs(15);

/// The wait between two reads of the state.
const STATE_POLL: Duration = Duration::from_millis(10);

/// An endpoint of `127.0.0.1:port` that serves a worker registry.
fn loopback_endpoint(port: u16) -> Arc<ListenEndpoint> {
    let address = ListenAddress::parse(&format!("127.0.0.1:{port}")).expect("a valid address");
    Arc::new(ListenEndpoint::new(address, worker_registry()))
}

/// Start the thread of `endpoint`.
fn start(endpoint: &Arc<ListenEndpoint>) -> EndpointThread {
    match start_endpoint_thread(endpoint) {
        RunningEndpoint::Thread(thread) => thread,
        RunningEndpoint::NotStarted => panic!("the test thread starts"),
    }
}

/// Wait until `endpoint` is in a state that `wanted` accepts, and return it.
async fn wait_for_state(
    endpoint: &ListenEndpoint,
    wanted: impl Fn(&EndpointState) -> bool,
) -> EndpointState {
    tokio::time::timeout(TEST_WAIT_BOUND, async {
        loop {
            let state = endpoint.state();
            match wanted(&state) {
                true => break state,
                false => tokio::time::sleep(STATE_POLL).await,
            }
        }
    })
    .await
    .unwrap_or_else(|_elapsed| panic!("no wanted state in time; last: {:?}", endpoint.state()))
}

/// The answer of one HTTP/1.1 request: its head in lower case, and its body.
async fn answer(port: u16, method: &str, path: &str) -> (String, String) {
    let exchange = async {
        let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
            .await
            .expect("the endpoint accepts");
        let request =
            format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("the request is written");
        let mut text = String::new();
        let _read = stream
            .read_to_string(&mut text)
            .await
            .expect("the answer is read");
        text
    };
    let text = tokio::time::timeout(TEST_WAIT_BOUND, exchange)
        .await
        .expect("the endpoint answers in time");
    match text.split_once("\r\n\r\n") {
        Some((head, body)) => (head.to_ascii_lowercase(), body.to_owned()),
        None => panic!("an HTTP answer has a head and a body: {text:?}"),
    }
}

/// A board of one isolated slot whose child serves.
fn serving_board() -> Arc<PoolBoard> {
    let board = Arc::new(PoolBoard::new(ChildLayout {
        isolated: 1,
        shared_async: 0,
        shared_thread: 0,
    }));
    board.set(
        0,
        SlotPhase::Serving {
            pid: ProcessId::new(4242).expect("4242 is a process id"),
        },
    );
    board
}

#[tokio::test]
async fn the_endpoint_serves_the_registry_and_the_probes_of_each_phase() {
    let free = bind_loopback_free_port().await;
    let port = free.local_address().port();
    drop(free);
    let endpoint = loopback_endpoint(port);
    let thread = start(&endpoint);
    let serving = wait_for_state(&endpoint, |state| {
        matches!(state, EndpointState::Serving { .. })
    })
    .await;
    assert_eq!(
        serving,
        EndpointState::Serving {
            local: SocketAddr::from((Ipv4Addr::LOCALHOST, port))
        }
    );

    let (head, body) = answer(port, "GET", "/metrics").await;
    assert!(head.starts_with("http/1.1 200 ok\r\n"), "{head}");
    assert!(
        head.contains(&format!("\r\ncontent-type: {PROMETHEUS_TEXT_CONTENT_TYPE}")),
        "{head}"
    );
    assert_eq!(body, render(&endpoint.registry));
    assert!(body.contains("\nhorsies_worker_info{"), "{body}");

    // The start: live, not ready.
    let (head, body) = answer(port, "GET", "/livez").await;
    assert!(head.starts_with("http/1.1 200 ok\r\n"), "{head}");
    assert!(
        head.contains("\r\ncontent-type: text/plain; charset=utf-8"),
        "{head}"
    );
    assert_eq!(body, "live: the worker starts\n");
    let (head, body) = answer(port, "GET", "/readyz").await;
    assert!(head.starts_with("http/1.1 503 "), "{head}");
    assert_eq!(body, "not ready: the worker starts\n");

    // The claim loop with a serving child and the lock held: live, ready.
    endpoint.probes().claiming(ServingFacts {
        board: serving_board(),
        lock: LockHoldView::held_for_test(),
    });
    let (head, body) = answer(port, "GET", "/livez").await;
    assert!(head.starts_with("http/1.1 200 ok\r\n"), "{head}");
    assert_eq!(body, "live: the claim loop turns\n");
    let (head, body) = answer(port, "GET", "/readyz").await;
    assert!(head.starts_with("http/1.1 200 ok\r\n"), "{head}");
    assert_eq!(body, "ready\n");

    // The lock is lost: not ready.
    endpoint.probes().claiming(ServingFacts {
        board: serving_board(),
        lock: LockHoldView::lost_for_test("the session ended"),
    });
    let (head, body) = answer(port, "GET", "/readyz").await;
    assert!(head.starts_with("http/1.1 503 "), "{head}");
    assert_eq!(
        body,
        "not ready: the worker liveness lock is not held: the session ended\n"
    );

    // The stop: live, not ready.
    endpoint.probes().stopping();
    let (head, body) = answer(port, "GET", "/livez").await;
    assert!(head.starts_with("http/1.1 200 ok\r\n"), "{head}");
    assert_eq!(body, "live: the worker stops\n");
    let (head, body) = answer(port, "GET", "/readyz").await;
    assert!(head.starts_with("http/1.1 503 "), "{head}");
    assert_eq!(body, "not ready: the worker stops\n");

    // Another path, another method.
    let (head, _body) = answer(port, "GET", "/").await;
    assert!(head.starts_with("http/1.1 404 "), "{head}");
    let (head, _body) = answer(port, "POST", "/livez").await;
    assert!(head.starts_with("http/1.1 405 "), "{head}");
    let (head, body) = answer(port, "HEAD", "/readyz").await;
    assert!(head.starts_with("http/1.1 503 "), "{head}");
    assert_eq!(body, "");

    tokio::time::timeout(TEST_WAIT_BOUND, thread.stop())
        .await
        .expect("the thread stops in time");
}

#[tokio::test]
async fn a_port_in_use_is_down_with_its_cause_then_serves_once_free() {
    let held = bind_loopback_free_port().await;
    let port = held.local_address().port();
    let endpoint = loopback_endpoint(port);
    let thread = start(&endpoint);

    let down = wait_for_state(&endpoint, |state| matches!(state, EndpointState::Down(_))).await;
    let EndpointState::Down(EndpointDown { cause, .. }) = down else {
        panic!("the wait returns a down state");
    };
    match cause {
        EndpointDownCause::BindFailed {
            detail,
            failed_binds,
        } => {
            assert!(detail.contains(&format!("127.0.0.1:{port}")), "{detail}");
            assert!(failed_binds.get() >= 1);
        }
        other => panic!("expected a failed bind, got {other:?}"),
    }
    let column = telemetry_json(&WorkerEndpoint::Listen(Arc::clone(&endpoint)));
    assert_eq!(column["endpoint"]["state"], "down");
    assert_eq!(column["endpoint"]["host"], "127.0.0.1");
    assert_eq!(column["endpoint"]["port"], port);
    assert_eq!(column["endpoint"]["cause"]["kind"], "bind_failed");

    // The port is free: the next bind of the backoff takes it.
    drop(held);
    wait_for_state(&endpoint, |state| {
        matches!(state, EndpointState::Serving { .. })
    })
    .await;
    let (head, body) = answer(port, "GET", "/livez").await;
    assert!(head.starts_with("http/1.1 200 ok\r\n"), "{head}");
    assert_eq!(body, "live: the worker starts\n");
    let column = telemetry_json(&WorkerEndpoint::Listen(Arc::clone(&endpoint)));
    assert_eq!(column["endpoint"]["state"], "serving");
    assert_eq!(
        column["endpoint"]["local_address"],
        format!("127.0.0.1:{port}")
    );

    tokio::time::timeout(TEST_WAIT_BOUND, thread.stop())
        .await
        .expect("the thread stops in time");
}

#[test]
fn the_telemetry_column_names_each_endpoint_state() {
    assert_eq!(
        telemetry_json(&WorkerEndpoint::NoMetricsEndpoint),
        serde_json::json!({ "endpoint": { "state": "off" } })
    );

    let endpoint = loopback_endpoint(9100);
    let listen = || telemetry_json(&WorkerEndpoint::Listen(Arc::clone(&endpoint)));
    assert_eq!(
        listen(),
        serde_json::json!({
            "endpoint": { "state": "binding", "host": "127.0.0.1", "port": 9100 }
        })
    );

    let first = DateTime::parse_from_rfc3339("2026-11-19T10:00:00Z")
        .expect("a valid time")
        .with_timezone(&Utc);
    let later = first + chrono::Duration::seconds(3);
    endpoint.state.down(
        EndpointDownCause::BindFailed {
            detail: "could not bind the metrics endpoint to 127.0.0.1:9100: in use".to_owned(),
            failed_binds: NonZeroU32::MIN,
        },
        first,
    );
    // A second failure keeps the time of the first.
    endpoint.state.down(
        EndpointDownCause::BindFailed {
            detail: "could not bind the metrics endpoint to 127.0.0.1:9100: in use".to_owned(),
            failed_binds: NonZeroU32::MIN.saturating_add(1),
        },
        later,
    );
    assert_eq!(
        listen(),
        serde_json::json!({
            "endpoint": {
                "state": "down",
                "host": "127.0.0.1",
                "port": 9100,
                "cause": {
                    "kind": "bind_failed",
                    "detail": "could not bind the metrics endpoint to 127.0.0.1:9100: in use",
                    "failed_binds": 2,
                },
                "down_since": "2026-11-19T10:00:00.000000+00:00",
            }
        })
    );

    endpoint
        .state
        .serve(SocketAddr::from((Ipv4Addr::LOCALHOST, 9100)));
    assert_eq!(
        listen(),
        serde_json::json!({
            "endpoint": {
                "state": "serving",
                "host": "127.0.0.1",
                "port": 9100,
                "local_address": "127.0.0.1:9100",
            }
        })
    );

    for (cause, kind) in [
        (
            EndpointDownCause::ThreadRefused {
                detail: "no thread".to_owned(),
            },
            "thread_refused",
        ),
        (
            EndpointDownCause::RuntimeRefused {
                detail: "no runtime".to_owned(),
            },
            "runtime_refused",
        ),
    ] {
        let refused = loopback_endpoint(9100);
        refused.state.down(cause, first);
        let column = telemetry_json(&WorkerEndpoint::Listen(Arc::clone(&refused)));
        assert_eq!(column["endpoint"]["state"], "down");
        assert_eq!(column["endpoint"]["cause"]["kind"], kind);
        assert_eq!(
            column["endpoint"]["down_since"],
            "2026-11-19T10:00:00.000000+00:00"
        );
    }
}
