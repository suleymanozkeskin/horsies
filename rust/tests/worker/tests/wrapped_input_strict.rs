#![allow(clippy::unwrap_used)]

//! e2e: strict deserialization of the macro-generated `Input` (Wrapped) struct.
//!
//! `#[serde(deny_unknown_fields)]` on the generated multi-param `Input` is an
//! execution-path contract. These tests pin both halves of that contract:
//!   1. `args_from` injection of declared fields still deserializes (the
//!      strictness does not break the dependency-injection wire form).
//!   2. An extra/undeclared kwarg is rejected at execution.
//!
//! `tests/worker` otherwise has no multi-param (Wrapped) task, so without this
//! file a regression in the generated `Input` codegen would go uncaught.
//!
//! Run with:
//!   cargo test -p horsies-test-worker --test wrapped_input_strict -- --test-threads=1

use std::sync::Arc;
use std::time::Duration;

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use horsies::{Horsies, PostgresBroker, TaskResult, WorkflowHandle, WorkflowSpecBuilder};
use horsies_test_support::{
    db,
    e2e::{worker::start_worker, workflow::wait_for_workflow_completion},
    fixtures,
};
use horsies_test_worker::tasks::{wf_combine_wrapped, wf_produce_int};

fn ensure_tasks_registered() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let mut app = Horsies::new(fixtures::default_app_config()).unwrap();
        horsies_test_worker::tasks::register(&mut app).unwrap();
    });
}

async fn pool() -> PgPool {
    ensure_tasks_registered();
    let p = db::create_pool().await;
    db::run_migrations(&p).await;
    p
}

async fn start_wf(pool: &PgPool, spec: &horsies::WorkflowSpec) -> Uuid {
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let mut app = Horsies::with_broker(fixtures::default_app_config(), broker).unwrap();
    let handle: WorkflowHandle<serde_json::Value> = app
        .start(spec.clone())
        .await
        .unwrap_or_else(|e| panic!("app.start failed: {}", e));
    handle.workflow_id().to_owned()
}

async fn get_wf_task_result(pool: &PgPool, wf_id: &Uuid, task_index: i32) -> Option<String> {
    sqlx::query_scalar(
        "SELECT result FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = $2",
    )
    .bind(wf_id)
    .bind(task_index)
    .fetch_optional(pool)
    .await
    .unwrap()
    .flatten()
}

// ---------------------------------------------------------------------------
// Positive: args_from into a Wrapped (multi-param) task still deserializes
// under deny_unknown_fields.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn wrapped_input_args_from_deserializes_under_strict() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db::db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_wrapped_args_from");
    let a = b.task(
        wf_produce_int::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"value": 7}"#.to_owned()),
    );
    let bnode = b.task(
        wf_produce_int::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"value": 5}"#.to_owned()),
    );
    b.task(
        wf_combine_wrapped::node()
            .unwrap()
            .node_id("c")
            .arg_from(wf_combine_wrapped::params::first(), a)
            .arg_from(wf_combine_wrapped::params::second(), bnode),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    let c_result = get_wf_task_result(&pool, &wf_id, 2).await.unwrap();
    let tr: TaskResult<serde_json::Value> = serde_json::from_str(&c_result).unwrap();
    assert!(tr.is_ok());
    assert_eq!(tr.unwrap(), serde_json::json!(12));
}

// ---------------------------------------------------------------------------
// Negative: an extra/undeclared kwarg is rejected at execution. Both declared
// fields are valid, so the only failure cause is the unknown `extra` key.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn wrapped_input_extra_kwarg_rejected_at_execution() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db::db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_wrapped_extra_kwarg");
    b.task(wf_combine_wrapped::node().unwrap().node_id("c").kwargs(
        r#"{"first": {"__type": "ok", "value": 1}, "second": {"__type": "ok", "value": 2}, "extra": 99}"#
            .to_owned(),
    ));
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "FAILED");

    let result = get_wf_task_result(&pool, &wf_id, 0).await.unwrap();
    let tr: TaskResult<serde_json::Value> = serde_json::from_str(&result).unwrap();
    assert!(tr.is_err(), "expected the extra kwarg to be rejected");
    assert!(
        result.contains("unknown field"),
        "expected a deny_unknown_fields diagnostic, got: {}",
        result,
    );
}
