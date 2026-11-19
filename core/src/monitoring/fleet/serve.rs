//! `GET /metrics` of the fleet collector, and the server of `horsies
//! metrics`. Built with the `metrics-endpoint` feature.

use std::future::Future;
use std::sync::Arc;

use axum::extract::State;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use tokio_util::sync::CancellationToken;

use crate::handle::CoreHandle;
use crate::telemetry::endpoint::{metrics_response, serve_endpoint, BoundEndpoint, EndpointFailed};

use super::{collect_fleet_state_through, FleetCollector, FleetLabels};

/// The router of the fleet metrics: `GET /metrics` answers the text of
/// `collector` (`HEAD` too). Another path answers 404, another method 405.
/// A request reads nothing from the database.
pub(crate) fn fleet_metrics_router(collector: Arc<FleetCollector>) -> Router {
    Router::new()
        .route("/metrics", get(serve_metrics))
        .with_state(collector)
}

async fn serve_metrics(State(collector): State<Arc<FleetCollector>>) -> Response {
    metrics_response(collector.render())
}

/// Serve the fleet metrics of the database of `handle` at `GET /metrics` on
/// `endpoint`, until `stop` resolves.
///
/// The fleet state is read through `handle` at once and then at most once
/// per [`FLEET_COLLECT_INTERVAL`](super::FLEET_COLLECT_INTERVAL)
/// ([`collect_fleet_state_through`]); a request only renders the last read.
/// A failed connect or a failed statement is a part without an answer in
/// the text, never an end of the server. At the stop, the requests in
/// flight end, then a read in flight is dropped; the read only reads.
///
/// Errors: [`EndpointFailed::Serve`].
pub async fn serve_fleet_metrics<S>(
    endpoint: BoundEndpoint,
    handle: Arc<CoreHandle>,
    labels: FleetLabels,
    stop: S,
) -> Result<(), EndpointFailed>
where
    S: Future<Output = ()> + Send + 'static,
{
    tracing::info!(
        "serving the fleet metrics on http://{}/metrics",
        endpoint.local_address()
    );
    let collector = Arc::new(FleetCollector::new(labels));
    let collecting = CancellationToken::new();
    let router = fleet_metrics_router(Arc::clone(&collector));
    let serving = async {
        let served = serve_endpoint(endpoint, router, stop).await;
        collecting.cancel();
        served
    };
    let ((), served) = tokio::join!(
        collect_fleet_state_through(&collector, &handle, &collecting),
        serving
    );
    served
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;

    use super::*;
    use crate::broker::client_family::ClientFamily;
    use crate::broker::PoolProfile;
    use crate::manifest::parse_producer_manifest;
    use crate::telemetry::endpoint::bind_loopback_free_port;
    use crate::telemetry::labels::FleetQuery;

    /// The most scrapes that a test waits for the first read through.
    const SCRAPES_UNTIL_FIRST_READ: usize = 400;

    /// The time between two of those scrapes.
    const SCRAPE_PAUSE: Duration = Duration::from_millis(50);

    /// The answer of one HTTP/1.1 request to `port`: its head in lower
    /// case, and its body.
    async fn answer(port: u16, method: &str, path: &str) -> (String, String) {
        let mut stream = TcpStream::connect(("127.0.0.1", port))
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
        match text.split_once("\r\n\r\n") {
            Some((head, body)) => (head.to_ascii_lowercase(), body.to_owned()),
            None => panic!("an HTTP answer has a head and a body: {text:?}"),
        }
    }

    /// A stop future and the sender that resolves it.
    fn stop_pair() -> (
        oneshot::Sender<()>,
        impl Future<Output = ()> + Send + 'static,
    ) {
        let (sender, receiver) = oneshot::channel::<()>();
        (sender, async move {
            let _sent_or_dropped = receiver.await;
        })
    }

    /// A handle of a server that refuses every connection.
    fn unreachable_database_handle() -> CoreHandle {
        let manifest = json!({
            "manifest_version": 1,
            "database": {
                "database_url": "postgresql://postgres:none@127.0.0.1:1/none",
                "session_database": {"kind": "SAME_AS_DATABASE_URL"},
                "pgbouncer_transaction_mode": false,
                "pool_pre_ping": true,
                "pool_size": 1,
                "max_overflow": 0,
                "pool_timeout": 1,
                "pool_recycle": {"kind": "AFTER_SECONDS", "seconds": 1800},
                "echo": false,
                "tcp_keepalives": {"kind": "OFF"},
                "tcp_user_timeout": {"kind": "SYSTEM_DEFAULT"}
            },
            "schema": "ASSUME_INITIALIZED",
            "send": {
                "retain_rerun_input_default": false,
                "idempotency_reservation_window": 86400000000_u64
            }
        });
        let manifest =
            parse_producer_manifest(&manifest.to_string()).expect("the manifest is complete");
        CoreHandle::new(manifest, PoolProfile::Producer, ClientFamily::Rust)
    }

    #[tokio::test]
    async fn get_metrics_answers_the_text_of_the_collector_with_the_prometheus_type() {
        let collector = Arc::new(FleetCollector::new(FleetLabels::none()));
        let endpoint = bind_loopback_free_port().await;
        let port = endpoint.local_address().port();
        let (stop, stopped) = stop_pair();
        let server = tokio::spawn(serve_endpoint(
            endpoint,
            fleet_metrics_router(Arc::clone(&collector)),
            stopped,
        ));

        let (head, body) = answer(port, "GET", "/metrics").await;
        assert!(head.starts_with("http/1.1 200 ok\r\n"), "{head}");
        assert!(
            head.contains("\r\ncontent-type: text/plain; version=0.0.4; charset=utf-8"),
            "{head}"
        );
        assert_eq!(body, collector.render());
        assert!(body.contains("# TYPE horsies_queue_tasks gauge\n"));

        let (head, body) = answer(port, "HEAD", "/metrics").await;
        assert!(head.starts_with("http/1.1 200 ok\r\n"), "{head}");
        assert_eq!(body, "");

        let _sent = stop.send(());
        let served = server.await.expect("the server task ends");
        assert!(served.is_ok(), "{served:?}");
    }

    #[tokio::test]
    async fn another_method_answers_405_and_another_path_404() {
        let collector = Arc::new(FleetCollector::new(FleetLabels::none()));
        let endpoint = bind_loopback_free_port().await;
        let port = endpoint.local_address().port();
        let (stop, stopped) = stop_pair();
        let server = tokio::spawn(serve_endpoint(
            endpoint,
            fleet_metrics_router(collector),
            stopped,
        ));

        let (head, _body) = answer(port, "POST", "/metrics").await;
        assert!(head.starts_with("http/1.1 405 "), "{head}");
        let (head, _body) = answer(port, "GET", "/").await;
        assert!(head.starts_with("http/1.1 404 "), "{head}");
        let (head, _body) = answer(port, "GET", "/metrics/extra").await;
        assert!(head.starts_with("http/1.1 404 "), "{head}");

        let _sent = stop.send(());
        let served = server.await.expect("the server task ends");
        assert!(served.is_ok(), "{served:?}");
    }

    #[tokio::test]
    async fn a_database_that_refuses_connections_is_a_failure_gauge_of_each_query() {
        let endpoint = bind_loopback_free_port().await;
        let port = endpoint.local_address().port();
        let (stop, stopped) = stop_pair();
        let server = tokio::spawn(serve_fleet_metrics(
            endpoint,
            Arc::new(unreachable_database_handle()),
            FleetLabels::none(),
            stopped,
        ));

        let mut body = String::new();
        for _ in 0..SCRAPES_UNTIL_FIRST_READ {
            (_, body) = answer(port, "GET", "/metrics").await;
            match body.contains("horsies_exporter_collect_failed{query=") {
                true => break,
                false => tokio::time::sleep(SCRAPE_PAUSE).await,
            }
        }
        for query in [
            "queue_tasks",
            "oldest_pending",
            "workers",
            "class_coverage",
            "workflows",
            "schedules",
            "schema_version",
        ] {
            assert!(
                body.contains(&format!(
                    "horsies_exporter_collect_failed{{query=\"{query}\"}} 1\n"
                )),
                "{query}: {body}"
            );
            assert!(
                body.contains(&format!(
                    "horsies_exporter_collect_errors_total{{query=\"{query}\"}} 1\n"
                )),
                "{query}: {body}"
            );
        }
        assert_eq!(FleetQuery::ALL.len(), 7);
        assert!(!body.contains("\nhorsies_queue_tasks{"), "{body}");

        let _sent = stop.send(());
        let served = server.await.expect("the server task ends");
        assert!(served.is_ok(), "{served:?}");
    }
}
