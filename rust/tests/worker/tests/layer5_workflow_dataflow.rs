#![allow(clippy::unwrap_used)]

//! Layer 5 e2e tests: workflow data flow (args_from, failure propagation).
//!
//! Mirrors Python's `tests/e2e/test_layer5_workflow_dataflow.py`.
//!
//! Run with:
//!   cargo test -p horsies-test-worker --test layer5_workflow_dataflow -- --test-threads=1

use std::time::Duration;

use serial_test::serial;
use sqlx::PgPool;

use std::sync::Arc;

use horsies::{Horsies, PostgresBroker, TaskNode, TaskResult, WorkflowHandle, WorkflowSpecBuilder};
use horsies_test_support::{
    db,
    e2e::{
        db_poll::wait_for_workflow_terminal,
        worker::start_worker,
        workflow::{get_workflow_task_status, wait_for_workflow_completion},
    },
    fixtures,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn pool() -> PgPool {
    let p = db::create_pool().await;
    db::run_migrations(&p).await;
    p
}

fn db_url() -> String {
    db::db_url()
}

async fn start_wf(pool: &PgPool, spec: &horsies::WorkflowSpec) -> String {
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let mut app = Horsies::with_broker(fixtures::default_app_config(), broker).unwrap();
    let handle: WorkflowHandle<serde_json::Value> = app
        .start(spec.clone())
        .await
        .unwrap_or_else(|e| panic!("app.start failed: {}", e));
    handle.workflow_id().to_owned()
}

/// Get the result JSON of a workflow task by index.
async fn get_wf_task_result(pool: &PgPool, wf_id: &str, task_index: i32) -> Option<String> {
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
// T5.1: args_from single dependency (A produces 5 → B doubles to 10)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_args_from_single_dependency() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_args_from_single");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_produce_int")
            .node_id("a")
            .kwargs(r#"{"value": 5}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_double")
            .node_id("b")
            .args_from("input_result", a),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    // B should have result 10 (5 * 2).
    let b_result = get_wf_task_result(&pool, &wf_id, 1).await.unwrap();
    let tr: TaskResult<serde_json::Value> = serde_json::from_str(&b_result).unwrap();
    assert!(tr.is_ok());
    assert_eq!(tr.unwrap(), serde_json::json!(10));
}

// ---------------------------------------------------------------------------
// T5.2: args_from multiple dependencies (A=10, B=20 → C sums to 30)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_args_from_multiple_dependencies() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_args_from_multi");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_produce_int")
            .node_id("a")
            .kwargs(r#"{"value": 10}"#.to_owned()),
    );
    let bnode = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_produce_int")
            .node_id("b")
            .kwargs(r#"{"value": 20}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_sum_two")
            .node_id("c")
            .args_from("first", a)
            .args_from("second", bnode),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    // C should have result 30 (10 + 20).
    let c_result = get_wf_task_result(&pool, &wf_id, 2).await.unwrap();
    let tr: TaskResult<serde_json::Value> = serde_json::from_str(&c_result).unwrap();
    assert!(tr.is_ok());
    assert_eq!(tr.unwrap(), serde_json::json!(30));
}

// ---------------------------------------------------------------------------
// T5.5: Failed dep propagation via args_from
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_failed_propagation_via_args_from() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // A fails → B receives error via args_from (allow_failed_deps=true).
    let mut b = WorkflowSpecBuilder::new("e2e_fail_propagation");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_fail")
            .node_id("a")
            .kwargs(r#"{"error_code": "UPSTREAM_FAIL"}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_double")
            .node_id("b")
            .args_from("input_result", a)
            .allow_failed_deps(true),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "FAILED");

    // A is FAILED.
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 0).await, "FAILED");
    // B receives the error via args_from and propagates it → also FAILED.
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 1).await, "FAILED");
}

// ---------------------------------------------------------------------------
// T5.6: Skipped propagation (A fails → B skipped, no allow_failed_deps)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_skipped_propagation() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_skip_propagation");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_fail")
            .node_id("a")
            .kwargs(r#"{"error_code": "DELIBERATE"}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
            .node_id("b")
            .kwargs(r#"{"step": "B"}"#.to_owned())
            .waits_for(a),
        // allow_failed_deps defaults to false
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(10)).await;
    assert_eq!(status, "FAILED");

    assert_eq!(get_workflow_task_status(&pool, &wf_id, 0).await, "FAILED");
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 1).await, "SKIPPED");
}

// ---------------------------------------------------------------------------
// T5.9: args_from transitive chain (A → B → C, each doubles)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_args_from_transitive_chain() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // A produces 3, B doubles to 6, C doubles to 12.
    let mut b = WorkflowSpecBuilder::new("e2e_transitive_chain");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_produce_int")
            .node_id("a")
            .kwargs(r#"{"value": 3}"#.to_owned()),
    );
    let bnode = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_double")
            .node_id("b")
            .args_from("input_result", a),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_double")
            .node_id("c")
            .args_from("input_result", bnode),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    // C should be 12 (3 * 2 * 2).
    let c_result = get_wf_task_result(&pool, &wf_id, 2).await.unwrap();
    let tr: TaskResult<serde_json::Value> = serde_json::from_str(&c_result).unwrap();
    assert!(tr.is_ok());
    assert_eq!(tr.unwrap(), serde_json::json!(12));
}

// ---------------------------------------------------------------------------
// T5.10: args_from failed dep skips downstream and fails workflow
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_args_from_failed_dep_skips_and_fails() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // A fails → B (args_from A, no allow_failed_deps) → SKIPPED
    let mut b = WorkflowSpecBuilder::new("e2e_args_from_fail_skip");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_fail")
            .node_id("a")
            .kwargs(r#"{"error_code": "FAIL_A"}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_double")
            .node_id("b")
            .args_from("input_result", a),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(10)).await;
    assert_eq!(status, "FAILED");

    assert_eq!(get_workflow_task_status(&pool, &wf_id, 0).await, "FAILED");
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 1).await, "SKIPPED");
}

// ---------------------------------------------------------------------------
// T5.12: Dual injection same node (two args_from on one task)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_dual_injection_same_node() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A=7, B=3 → C sums to 10.
    let mut b = WorkflowSpecBuilder::new("e2e_dual_injection");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_produce_int")
            .node_id("a")
            .kwargs(r#"{"value": 7}"#.to_owned()),
    );
    let bnode = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_produce_int")
            .node_id("b")
            .kwargs(r#"{"value": 3}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_sum_two")
            .node_id("c")
            .args_from("first", a)
            .args_from("second", bnode),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    let c_result = get_wf_task_result(&pool, &wf_id, 2).await.unwrap();
    let tr: TaskResult<serde_json::Value> = serde_json::from_str(&c_result).unwrap();
    assert!(tr.is_ok());
    assert_eq!(tr.unwrap(), serde_json::json!(10));
}

// ---------------------------------------------------------------------------
// T5.3: workflow_ctx single dependency
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_workflow_ctx_single_dependency() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // A produces 42 → B reads A's result via workflow_ctx.
    let mut b = WorkflowSpecBuilder::new("e2e_ctx_single");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_produce_int")
            .node_id("a")
            .kwargs(r#"{"value": 42}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_ctx_reader")
            .node_id("b")
            .waits_for(a)
            .workflow_ctx_from(vec!["a".to_owned()]),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    // B should have received context with A's result.
    let b_result = get_wf_task_result(&pool, &wf_id, 1).await.unwrap();
    let tr: TaskResult<serde_json::Value> = serde_json::from_str(&b_result).unwrap();
    assert!(tr.is_ok(), "B should succeed, got: {:?}", tr);

    let output = tr.unwrap();
    assert_eq!(
        output.get("result_count").and_then(|v| v.as_i64()),
        Some(1),
        "should have 1 result in ctx"
    );
    // The result for node "a" should contain the value 42.
    let results = output.get("results").unwrap().as_object().unwrap();
    assert!(results.contains_key("a"), "ctx should contain node 'a'");
}

// ---------------------------------------------------------------------------
// T5.4: workflow_ctx multiple dependencies
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_workflow_ctx_multiple_dependencies() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A=10, B=20 → C sums via workflow_ctx.
    let mut b = WorkflowSpecBuilder::new("e2e_ctx_multi");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_produce_int")
            .node_id("a")
            .kwargs(r#"{"value": 10}"#.to_owned()),
    );
    let bnode = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_produce_int")
            .node_id("b")
            .kwargs(r#"{"value": 20}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_ctx_sum")
            .node_id("c")
            .waits_for(a)
            .waits_for(bnode)
            .workflow_ctx_from(vec!["a".to_owned(), "b".to_owned()]),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    let c_result = get_wf_task_result(&pool, &wf_id, 2).await.unwrap();
    let tr: TaskResult<serde_json::Value> = serde_json::from_str(&c_result).unwrap();
    assert!(tr.is_ok());
    assert_eq!(
        tr.unwrap(),
        serde_json::json!(30),
        "should sum 10 + 20 = 30"
    );
}

// ---------------------------------------------------------------------------
// T5.8: Mixed args_from and workflow_ctx on same node
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_mixed_args_from_and_ctx() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A produces 5 → B receives both args_from(A) and workflow_ctx_from(A).
    let mut b = WorkflowSpecBuilder::new("e2e_mixed");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_produce_int")
            .node_id("a")
            .kwargs(r#"{"value": 5}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_mixed")
            .node_id("b")
            .args_from("input_result", a)
            .workflow_ctx_from(vec!["a".to_owned()]),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    let b_result = get_wf_task_result(&pool, &wf_id, 1).await.unwrap();
    let tr: TaskResult<serde_json::Value> = serde_json::from_str(&b_result).unwrap();
    assert!(tr.is_ok());

    let output = tr.unwrap();
    assert_eq!(
        output.get("has_args_from").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(output.get("has_ctx").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(
        output.get("args_from_value").and_then(|v| v.as_i64()),
        Some(5)
    );
}

// ---------------------------------------------------------------------------
// T5.11: args_from dict serialization roundtrip
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_args_from_dict_serialization() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // A produces a nested dict → B receives it via args_from and adds a field.
    let mut b = WorkflowSpecBuilder::new("e2e_dict_roundtrip");
    let a = b.task(TaskNode::<serde_json::Value>::new("e2e_wf_produce_dict").node_id("a"));
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_read_dict")
            .node_id("b")
            .args_from("input_result", a),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    let b_result = get_wf_task_result(&pool, &wf_id, 1).await.unwrap();
    let tr: TaskResult<serde_json::Value> = serde_json::from_str(&b_result).unwrap();
    assert!(tr.is_ok());

    let output = tr.unwrap();
    // Should have the original fields plus "received": true.
    assert_eq!(output.get("name").and_then(|v| v.as_str()), Some("Alice"));
    assert_eq!(
        output
            .get("scores")
            .and_then(|v| v.as_array())
            .map(|a| a.len()),
        Some(3)
    );
    assert!(output.get("nested").is_some());
    assert_eq!(output.get("received").and_then(|v| v.as_bool()), Some(true));
}

// ---------------------------------------------------------------------------
// T5.7: Join=any + ctx gating — C must wait for ctx dep B even though A finishes first
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_join_ctx_gating() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A (100ms fast), B (300ms slow). C has join=any + workflow_ctx_from=[B].
    // A will complete first, but C must wait for B because it needs B's result in ctx.
    let mut b = WorkflowSpecBuilder::new("e2e_join_ctx_gating");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("a")
            .kwargs(r#"{"step": "A", "delay_ms": 100}"#.to_owned()),
    );
    let bnode = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("b")
            .kwargs(r#"{"step": "B", "delay_ms": 300}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_ctx_reader")
            .node_id("c")
            .waits_for(a)
            .waits_for(bnode)
            .workflow_ctx_from(vec!["b".to_owned()])
            .join_any(),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    let tasks = horsies_test_support::e2e::workflow::get_workflow_tasks(&pool, &wf_id).await;
    assert_eq!(tasks.len(), 3);
    for t in &tasks {
        assert_eq!(t.status, "COMPLETED", "all tasks should complete");
    }

    let b_task = &tasks[1]; // B (slow, 300ms)
    let c_task = &tasks[2]; // C (ctx reader, join=any)

    // Critical invariant: C must start AFTER B completes,
    // even though join=any would normally fire when A completes first.
    assert!(
        c_task.started_at.unwrap() > b_task.completed_at.unwrap(),
        "C should start only after ctx dep B is terminal: \
         C.started_at={:?}, B.completed_at={:?}",
        c_task.started_at,
        b_task.completed_at,
    );

    // Verify C actually received B's result via ctx.
    let c_result = get_wf_task_result(&pool, &wf_id, 2).await.unwrap();
    let tr: TaskResult<serde_json::Value> = serde_json::from_str(&c_result).unwrap();
    assert!(tr.is_ok());

    let output = tr.unwrap();
    let results = output.get("results").and_then(|v| v.as_object()).unwrap();
    assert!(
        results.contains_key("b"),
        "C's workflow context should contain node 'b' result, got keys: {:?}",
        results.keys().collect::<Vec<_>>(),
    );
}
