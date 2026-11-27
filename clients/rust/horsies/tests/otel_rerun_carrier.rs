//! The trace carrier of a rerun on PostgreSQL (feature `otel`): a rerun
//! inside a span stores the carrier that the global propagator injects for
//! that span, and its trace id, on the new task, through both paths
//! (`rerun_task_in` in the caller's transaction, `Horsies::rerun_task`). A
//! rerun outside a span stores no carrier.
//!
//! Each test sets the global propagator of this test binary to the W3C
//! propagator; no other test binary sees it. The caller's span is a remote
//! span context, so the carrier is known before the rerun.
//!
//! Each test runs on a database of its own, a copy of the core's test
//! template (`core/src/test_support/database.rs`).

#![cfg(all(feature = "otel", feature = "tokio-postgres"))]
#![allow(clippy::unwrap_used)]

use chrono::Utc;
use horsies::rerun::{
    rerun_task_in, RegisteredTaskCodes, RerunEnqueuePolicy, RerunOutcome, RerunTask,
    TokioPostgresCaller,
};
use horsies::{AppConfig, Horsies};
use horsies_core::{OperationalCode, TaskError, TaskResult};
use opentelemetry::global;
use opentelemetry::trace::{
    FutureExt, SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
};
use opentelemetry::Context;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use uuid::Uuid;

/// The test databases of the core. This binary creates copies of the
/// template only.
#[allow(dead_code)]
#[path = "../../../../core/src/test_support/database.rs"]
mod test_database;

use test_database::TestDatabase;

/// Installs the schema into a test template through the public API, with
/// the Rust client family.
async fn install_template(url: &str) {
    let broker = horsies_core::PostgresBroker::connect(url, horsies_core::ClientFamily::Rust)
        .await
        .expect("connect the template broker");
    broker
        .migrate()
        .await
        .expect("install the schema into the test template");
    broker.close().await;
}

/// The retention class of the seeded source and of the reruns.
const CLASS: &str = "forever";
const WORKER: &str = "rerun-carrier-worker";

/// The trace and the span of the caller of each rerun.
const CALLER_TRACE_ID: u128 = 0x4bf9_2f35_77b3_4da6_a3ce_929d_0e0e_4736;
const CALLER_SPAN_ID: u64 = 0x00f0_67aa_0ba9_02b7;

/// The carrier that the W3C propagator injects for the caller's span, as
/// the core stores it (keys in byte order).
const CALLER_CARRIER: &str =
    r#"{"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","tracestate":""}"#;

/// The context of a caller that runs inside a sampled span.
fn caller_context() -> Context {
    Context::new().with_remote_span_context(SpanContext::new(
        TraceId::from(CALLER_TRACE_ID),
        SpanId::from(CALLER_SPAN_ID),
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    ))
}

fn policy() -> RerunEnqueuePolicy {
    RerunEnqueuePolicy::new(CLASS, true, chrono::Duration::hours(24)).unwrap()
}

/// A FAILED plain task with a retained input, moved to history by the
/// failure function (the seed of `tests/rerun_in_caller_transaction.rs`).
async fn seed_failed_source(database: &TestDatabase) -> Uuid {
    let task_id = Uuid::new_v4();
    let client = database.client();
    let inserted = client
        .execute(
            "INSERT INTO horsies_tasks (
                 id, task_name, queue_name, priority, input, task_options, execution,
                 status, sent_at, enqueued_at, started_at, claimed_at,
                 claimed_by_worker_id, claim_expires_at, good_until, is_workflow_task,
                 worker_pid, worker_hostname, worker_process_name,
                 retry_count, max_retries,
                 command_fingerprint_version, command_fingerprint,
                 retention_class_key, rerun_of_task_id,
                 rerun_root_task_id, retain_rerun_input,
                 created_at, updated_at
             ) VALUES (
                 $1, 'rerun.carrier', 'default', 50, '{\"k\":\"v\"}', '{\"timeout_ms\":5}',
                 'ISOLATED', 'RUNNING', NOW(), NOW(), NOW(), NOW(), $2, NULL, NULL, FALSE,
                 81, 'carrier-host', 'carrier-process', 0, 3, 1, $3, $4, NULL, NULL, TRUE,
                 NOW(), NOW()
             )",
            &[&task_id, &WORKER, &vec![9_u8; 32], &CLASS],
        )
        .await
        .unwrap();
    assert_eq!(inserted, 1);
    let result_json = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(
        TaskError::builtin(OperationalCode::TaskException, "carrier source"),
    ))
    .unwrap();
    let finished: i64 = client
        .query_one(
            "SELECT count(*) FROM horsies_finish_failed_attempt(
                 $1, $2, (SELECT t.claimed_at FROM horsies_tasks t WHERE t.id = $1),
                 'NONE', NULL, NULL, NULL, $3, $4, $5, $6, $7, $8, 'REFUSE', TRUE,
                 'NOT_OBSERVED', NULL)",
            &[
                &task_id,
                &WORKER,
                &result_json.as_str(),
                &Some("CARRIER"),
                &Some("carrier failure"),
                &"FAILED",
                &None::<&str>,
                &Some("carrier failure"),
            ],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(finished, 1);
    task_id
}

/// The new task id of an enqueued rerun of `source`.
fn enqueued(outcome: &RerunOutcome, source: Uuid) -> Uuid {
    match outcome {
        RerunOutcome::Enqueued {
            new_task_id,
            source_task_id,
            ..
        } if *source_task_id == source => *new_task_id,
        other => panic!("expected an enqueued rerun of {source}, got {other:?}"),
    }
}

/// The stored carrier and trace id of the live task `task_id`.
async fn stored_trace(database: &TestDatabase, task_id: Uuid) -> (Option<String>, Option<Vec<u8>>) {
    let row = database
        .client()
        .query_one(
            "SELECT trace_carrier, trace_id FROM horsies_tasks WHERE id = $1",
            &[&task_id],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1))
}

/// A rerun in the caller's transaction inside the caller's span stores the
/// carrier of that span and its trace id; the same rerun outside a span
/// stores none.
#[tokio::test]
async fn a_rerun_in_the_caller_transaction_stores_the_carrier_of_the_current_span() {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let database = TestDatabase::create().await;
    let source = seed_failed_source(&database).await;
    let command = RerunTask::new(source, None, None);
    let mut client = database.connect().await;

    let mut transaction = client.transaction().await.unwrap();
    let traced = rerun_task_in(
        &mut TokioPostgresCaller::new(&mut transaction),
        &command,
        &policy(),
        &RegisteredTaskCodes::NONE,
        Utc::now(),
    )
    .with_context(caller_context())
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    let traced_id = enqueued(&traced, source);
    assert_eq!(
        stored_trace(&database, traced_id).await,
        (
            Some(CALLER_CARRIER.to_owned()),
            Some(CALLER_TRACE_ID.to_be_bytes().to_vec())
        )
    );

    let mut transaction = client.transaction().await.unwrap();
    let untraced = rerun_task_in(
        &mut TokioPostgresCaller::new(&mut transaction),
        &command,
        &policy(),
        &RegisteredTaskCodes::NONE,
        Utc::now(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    let untraced_id = enqueued(&untraced, source);
    assert_eq!(stored_trace(&database, untraced_id).await, (None, None));

    drop(client);
    database.drop().await;
}

/// The app's own rerun inside the caller's span stores the carrier of that
/// span and its trace id.
#[tokio::test]
async fn the_app_rerun_stores_the_carrier_of_the_current_span() {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let database = TestDatabase::create().await;
    let source = seed_failed_source(&database).await;
    let app = Horsies::new(AppConfig::from_database_url(database.url())).unwrap();

    let outcome = app
        .rerun_task(RerunTask::new(source, None, None), policy())
        .with_context(caller_context())
        .await
        .unwrap();
    let new_task_id = enqueued(&outcome, source);
    assert_eq!(
        stored_trace(&database, new_task_id).await,
        (
            Some(CALLER_CARRIER.to_owned()),
            Some(CALLER_TRACE_ID.to_be_bytes().to_vec())
        )
    );

    database.drop().await;
}
