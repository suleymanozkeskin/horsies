//! The trace carrier of a send on PostgreSQL (feature `otel`): a task sent
//! inside a span stores the carrier that the global propagator injects for
//! the send span, and the trace id of that span.
//!
//! The test sets the global tracer provider and the global propagator of
//! this test binary; it is the only test of the binary.
//!
//! The database is a copy of the core's test template
//! (`core/src/test_support/database.rs`); a worker broker of the test adds
//! the default retention class that a send writes on its task.

#![cfg(all(feature = "otel", feature = "tokio-postgres"))]

use std::convert::Infallible;

use horsies::{task, AppConfig, Horsies, TaskError};
use horsies_core::{CoverageOutcome, CoverageRequest, CoverageSerialization, PostgresBroker};
use opentelemetry::global;
use opentelemetry::trace::{FutureExt, TraceContextExt, Tracer, TracerProvider as _};
use opentelemetry::Context;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

/// The test databases of the core. This binary creates copies of the
/// template only.
#[allow(dead_code)]
#[path = "../../../../core/src/test_support/database.rs"]
mod test_database;

use test_database::TestDatabase;

/// Installs the schema into a test template through the public API, with
/// the Rust client family.
async fn install_template(url: &str) {
    let broker = PostgresBroker::connect(url, horsies_core::ClientFamily::Rust)
        .await
        .expect("connect the template broker");
    broker
        .migrate()
        .await
        .expect("install the schema into the test template");
    broker.close().await;
}

/// The history leaves that the coverage pass creates ahead, in days and in
/// hours.
const COVERAGE_HISTORY_DAYS: u32 = 2;
const COVERAGE_HEARTBEAT_HOURS: u32 = 2;

const ECHO_TASK: &str = "otel_send_carrier_echo";

#[task("otel_send_carrier_echo")]
async fn echo(value: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(value)
}

/// A test database, and a worker broker on it that ran one coverage pass.
async fn covered_database() -> (TestDatabase, PostgresBroker) {
    let database = TestDatabase::create().await;
    let worker = PostgresBroker::connect(database.url(), horsies_core::ClientFamily::Rust)
        .await
        .expect("connect the worker broker");
    let request = CoverageRequest::new(COVERAGE_HISTORY_DAYS, COVERAGE_HEARTBEAT_HOURS, &[])
        .expect("a coverage request in bounds");
    let coverage = worker
        .ensure_partition_coverage(&request, CoverageSerialization::Gated)
        .await
        .expect("cover the history partitions");
    assert!(
        matches!(coverage, CoverageOutcome::Ensured(_)),
        "partition coverage of the test database: {coverage:?}"
    );
    (database, worker)
}

/// A send inside a caller's span: the send span is a child of the caller's
/// span, and the row stores the W3C carrier of the send span (keys in byte
/// order) and the 16 bytes of its trace id.
#[tokio::test]
async fn a_send_inside_a_span_stores_the_carrier_of_its_send_span() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    global::set_tracer_provider(provider.clone());
    global::set_text_map_propagator(TraceContextPropagator::new());
    let (database, worker) = covered_database().await;
    let mut app = Horsies::new(AppConfig::for_database_url(database.url())).expect("the app");
    let echo_function = echo::register(&mut app).expect("register echo");

    let caller = Context::current_with_span(provider.tracer("caller").start("caller"));
    let handle = echo_function
        .send(echo::Input { value: 7 })
        .with_context(caller.clone())
        .await
        .expect("the send");
    caller.span().end();

    let row = database
        .client()
        .query_one(
            "SELECT trace_carrier, trace_id FROM horsies_tasks WHERE id = $1",
            &[&handle.task_id()],
        )
        .await
        .expect("read the trace columns of the sent row");
    let stored_carrier: Option<String> = row.get(0);
    let stored_trace_id: Option<Vec<u8>> = row.get(1);
    provider
        .force_flush()
        .expect("the in-memory exporter flushes");
    let spans = exporter.get_finished_spans().expect("the finished spans");
    let send_name = format!("send {ECHO_TASK}");
    let send = spans
        .iter()
        .find(|span| span.name == send_name.as_str())
        .unwrap_or_else(|| panic!("the send span is recorded: {spans:?}"));
    let caller_span = spans
        .iter()
        .find(|span| span.name == "caller")
        .unwrap_or_else(|| panic!("the caller span is recorded: {spans:?}"));
    assert_eq!(send.parent_span_id, caller_span.span_context.span_id());
    assert_eq!(
        stored_carrier.as_deref(),
        Some(
            format!(
                r#"{{"traceparent":"00-{}-{}-01","tracestate":""}}"#,
                send.span_context.trace_id(),
                send.span_context.span_id()
            )
            .as_str()
        )
    );
    assert_eq!(
        stored_trace_id,
        Some(send.span_context.trace_id().to_bytes().to_vec())
    );
    worker.close().await;
    database.drop().await;
}
