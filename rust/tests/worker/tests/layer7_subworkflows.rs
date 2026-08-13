#![allow(clippy::unwrap_used)]

//! Layer 7 e2e tests: sub-workflow read-surface matrix.
//!
//! Mirrors Python's `tests/e2e/test_layer7_subworkflows.py` (horsies PR #64),
//! driving the real worker -> DB -> handle lifecycle.
//!
//! Rust adaptation: Python distinguishes sub-workflow failures by `isinstance`
//! on a `SubWorkflowError` subclass. Rust's `TaskResult<T>` err slot is the
//! concrete `TaskError` (no inheritance), so failures are discriminated by
//! `error_code == "SUBWORKFLOW_FAILED"` and the typed detail
//! (`sub_workflow_id` / `sub_workflow_summary`) is recovered via
//! `TaskError::sub_workflow_details()`. The same structured summary is also
//! reachable through `summary_for` / `tasks()`.
//!
//! Run with:
//!   cargo test -p horsies-test-worker --test layer7_subworkflows -- --test-threads=1

use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serial_test::serial;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use horsies::{
    resume_workflow, Horsies, OnError, PostgresBroker, SubWorkflowNode, SuccessCase, SuccessPolicy,
    TaskResult, Worker, WorkerConfig, WorkflowHandle, WorkflowSpec, WorkflowSpecBuilder,
    WorkflowSpecRegistry,
};
use horsies_test_support::{db, e2e::db_poll::wait_for_workflow_terminal, fixtures};
use horsies_test_worker::tasks::{
    wf_fail_int, wf_produce_int, ChildLabelInput, FailingChildInput, NestedParentInput,
    ProduceIntInput,
};

// ---------------------------------------------------------------------------
// Harness helpers
// ---------------------------------------------------------------------------

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

/// Spawn an in-process worker with the full test registry (so child workflow
/// definitions resolve in-process). Returns the cancel token + join handle.
async fn spawn_worker(pool: &PgPool) -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let mut app = Horsies::with_broker(fixtures::default_app_config(), broker).unwrap();
    horsies_test_worker::tasks::register(&mut app).unwrap();
    let (app_config, registry, wf_registry, broker) = app.into_parts().await.unwrap();
    let worker = Worker::new(
        broker,
        Arc::new(registry),
        Arc::new(wf_registry),
        app_config,
        WorkerConfig {
            concurrency: 4,
            ..WorkerConfig::default()
        },
    )
    .unwrap();
    let cancel = worker.cancel_token();
    let handle = tokio::spawn(async move {
        let _ = worker.run().await;
    });
    tokio::time::sleep(Duration::from_millis(250)).await;
    (cancel, handle)
}

async fn start_registered_wf<T>(pool: &PgPool, spec: &WorkflowSpec) -> WorkflowHandle<T>
where
    T: DeserializeOwned + Clone + Send + Sync + 'static,
{
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let mut app = Horsies::with_broker(fixtures::default_app_config(), broker).unwrap();
    horsies_test_worker::tasks::register(&mut app).unwrap();
    app.start(spec.clone())
        .await
        .unwrap_or_else(|e| panic!("app.start failed: {}", e))
}

async fn task_status(pool: &PgPool, wf_id: &Uuid, index: i32) -> String {
    sqlx::query_scalar(
        "SELECT status FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = $2",
    )
    .bind(wf_id)
    .bind(index)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn task_result_json(pool: &PgPool, wf_id: &Uuid, index: i32) -> Option<String> {
    sqlx::query_scalar(
        "SELECT result FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = $2",
    )
    .bind(wf_id)
    .bind(index)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn task_subwf_summary(pool: &PgPool, wf_id: &Uuid, index: i32) -> Option<String> {
    sqlx::query_scalar(
        "SELECT sub_workflow_summary FROM horsies_workflow_tasks \
         WHERE workflow_id = $1 AND task_index = $2",
    )
    .bind(wf_id)
    .bind(index)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Decode the Ok value of a stored `TaskResult<Value>` for a task index.
fn ok_value(result_json: &str) -> serde_json::Value {
    match serde_json::from_str::<TaskResult<serde_json::Value>>(result_json).unwrap() {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => panic!("expected Ok task result, got Err: {}", e),
    }
}

fn error_code_str(err: &horsies::TaskError) -> String {
    err.error_code
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// T7.1: child success -> every read surface agrees
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subworkflow_success_full_surface_matrix() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let (cancel, worker) = spawn_worker(&pool).await;

    // produce(0) -> child sub-workflow(1) -> reader(2, ctx_from child). Output = child.
    let mut b = WorkflowSpecBuilder::new("e2e_l7_success_matrix");
    let produce = b.task(
        wf_produce_int::node()
            .unwrap()
            .node_id("produce")
            .set_input(ProduceIntInput { value: 21 })
            .unwrap(),
    );
    let child = b.sub_workflow(
        SubWorkflowNode::<ChildLabelInput, String>::typed("e2e_child_label_pipeline")
            .node_id("child")
            .queue("default")
            .set(ChildLabelInput::field_label(), "count".to_owned())
            .unwrap()
            .arg_from(ChildLabelInput::field_input_result(), produce),
    );
    b.task(
        horsies_test_worker::tasks::wf_subwf_ctx_reader::node()
            .unwrap()
            .node_id("reader")
            .waits_for(child)
            .kwargs(r#"{}"#.to_owned())
            .workflow_ctx_from([child]),
    );
    b.output(child);
    let spec = b.build().unwrap();

    let handle: WorkflowHandle<String> = start_registered_wf(&pool, &spec).await;
    let result = handle.get(Some(Duration::from_secs(20))).await;

    // 1) handle.get() -> child output
    match &result {
        TaskResult::Ok(v) => assert_eq!(v, "count=21"),
        TaskResult::Err(e) => panic!("workflow failed: {}", e),
    }

    // 2) parent sub-workflow node COMPLETED + populated summary
    assert_eq!(
        task_status(&pool, &handle.workflow_id(), 1).await,
        "COMPLETED"
    );
    let summary_json = task_subwf_summary(&pool, &handle.workflow_id(), 1)
        .await
        .expect("summary column populated");
    assert!(summary_json.contains("COMPLETED"));

    // 3) reader observed the child through workflow_ctx (summary_for/output_for/result_for)
    let reader_json = ok_value(
        &task_result_json(&pool, &handle.workflow_id(), 2)
            .await
            .expect("reader produced a result"),
    );
    assert_eq!(reader_json["summary_status"], "COMPLETED");
    assert_eq!(reader_json["summary_is_success"], true);
    assert_eq!(reader_json["output"], "count=21");
    assert_eq!(reader_json["result_is_ok"], true);

    cancel.cancel();
    let _ = worker.await;
}

// ---------------------------------------------------------------------------
// T7.2: on_error=fail surfaces the sub-workflow error everywhere
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subworkflow_failure_fail_policy_preserves_error_everywhere() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let (cancel, worker) = spawn_worker(&pool).await;

    // child(0) is an intrinsically-failing sub-workflow → the only/first failure,
    // so the top error is SUBWORKFLOW_FAILED. on_error=fail.
    let mut b = WorkflowSpecBuilder::new("e2e_l7_fail_policy");
    b.on_error(OnError::Fail);
    let child = b.sub_workflow(
        SubWorkflowNode::<FailingChildInput, serde_json::Value>::typed(
            "e2e_child_failing_pipeline",
        )
        .node_id("child")
        .queue("default")
        .set(
            FailingChildInput::field_error_code(),
            "INNER_FAIL".to_owned(),
        )
        .unwrap(),
    );
    b.output(child);
    let spec = b.build().unwrap();

    let handle: WorkflowHandle<serde_json::Value> = start_registered_wf(&pool, &spec).await;
    let result = handle.get(Some(Duration::from_secs(20))).await;

    // handle.get().err discriminates + carries the full typed detail.
    let err = match result {
        TaskResult::Err(e) => e,
        TaskResult::Ok(v) => panic!("expected failure, got Ok: {:?}", v),
    };
    assert_eq!(error_code_str(&err), "SUBWORKFLOW_FAILED");
    let details = err
        .sub_workflow_details()
        .expect("sub-workflow detail recoverable from handle error");
    assert_eq!(details.sub_workflow_summary.status, "FAILED");
    assert!(details.sub_workflow_summary.failed_tasks >= 1);
    assert_ne!(details.sub_workflow_id, Uuid::nil());

    // Parent node FAILED with the same code; summary column FAILED.
    assert_eq!(task_status(&pool, &handle.workflow_id(), 0).await, "FAILED");
    assert!(task_subwf_summary(&pool, &handle.workflow_id(), 0)
        .await
        .unwrap()
        .contains("FAILED"));

    cancel.cancel();
    let _ = worker.await;
}

// ---------------------------------------------------------------------------
// T7.3: on_error=pause preserves the error across resume
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subworkflow_failure_pause_policy_preserves_error_after_resume() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let (cancel, worker) = spawn_worker(&pool).await;

    let mut b = WorkflowSpecBuilder::new("e2e_l7_pause_policy");
    b.on_error(OnError::Pause);
    let child = b.sub_workflow(
        SubWorkflowNode::<FailingChildInput, serde_json::Value>::typed(
            "e2e_child_failing_pipeline",
        )
        .node_id("child")
        .queue("default")
        .set(
            FailingChildInput::field_error_code(),
            "INNER_FAIL".to_owned(),
        )
        .unwrap(),
    );
    b.output(child);
    let spec = b.build().unwrap();

    let handle: WorkflowHandle<serde_json::Value> = start_registered_wf(&pool, &spec).await;

    // Wait for the pause triggered by the sub-workflow failure.
    let wf_id = handle.workflow_id().to_owned();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(&wf_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        if status == "PAUSED" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "workflow never paused (status={status})"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Resume; workflow finalizes FAILED, error preserved with detail.
    // The failed child is already terminal, so an empty registry suffices.
    let reg = WorkflowSpecRegistry::new();
    resume_workflow(
        &pool,
        wf_id,
        &reg,
        &horsies::PayloadPolicy::default(),
        &horsies::RetentionConfig::default(),
    )
    .await
    .unwrap();
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(20)).await;
    assert_eq!(status, "FAILED");

    let result = handle.get(Some(Duration::from_secs(5))).await;
    let err = match result {
        TaskResult::Err(e) => e,
        TaskResult::Ok(v) => panic!("expected failure, got Ok: {:?}", v),
    };
    assert_eq!(error_code_str(&err), "SUBWORKFLOW_FAILED");
    let details = err.sub_workflow_details().expect("detail recoverable");
    assert_eq!(details.sub_workflow_summary.status, "FAILED");

    cancel.cancel();
    let _ = worker.await;
}

// ---------------------------------------------------------------------------
// T7.4: successful sub-workflow visible through workflow_ctx
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn successful_subworkflow_is_available_through_workflow_ctx() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let (cancel, worker) = spawn_worker(&pool).await;

    // produce(0) -> child(1) -> reader(2, ctx_from child). Output = reader.
    let mut b = WorkflowSpecBuilder::new("e2e_l7_ctx_success");
    let produce = b.task(
        wf_produce_int::node()
            .unwrap()
            .node_id("produce")
            .set_input(ProduceIntInput { value: 7 })
            .unwrap(),
    );
    let child = b.sub_workflow(
        SubWorkflowNode::<ChildLabelInput, String>::typed("e2e_child_label_pipeline")
            .node_id("child")
            .queue("default")
            .set(ChildLabelInput::field_label(), "v".to_owned())
            .unwrap()
            .arg_from(ChildLabelInput::field_input_result(), produce),
    );
    let reader = b.task(
        horsies_test_worker::tasks::wf_subwf_ctx_reader::node()
            .unwrap()
            .node_id("reader")
            .waits_for(child)
            .kwargs(r#"{}"#.to_owned())
            .workflow_ctx_from([child]),
    );
    b.output(reader);
    let spec = b.build().unwrap();

    let handle: WorkflowHandle<serde_json::Value> = start_registered_wf(&pool, &spec).await;
    let result = handle.get(Some(Duration::from_secs(20))).await;
    let value = match result {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => panic!("workflow failed: {}", e),
    };
    assert_eq!(value["summary_status"], "COMPLETED");
    assert_eq!(value["output"], "v=7");
    assert_eq!(value["result_is_ok"], true);

    cancel.cancel();
    let _ = worker.await;
}

// ---------------------------------------------------------------------------
// T7.5: failed sub-workflow flows through workflow_ctx
//
// (The args_from half of the Python test — a failed result delivered into a
// downstream — is covered by T7.11; Rust's typed `arg_from` cannot bridge the
// child's `String` output into a downstream's differently-typed field, so the
// ctx surface is exercised here and build_with delivery in T7.11.)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn failed_subworkflow_flows_through_args_from_and_workflow_ctx() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let (cancel, worker) = spawn_worker(&pool).await;

    // fail(0) -> child(1, fails) -> reader(2: ctx_from child, allow_failed_deps).
    let mut b = WorkflowSpecBuilder::new("e2e_l7_failed_ctx");
    let failing = b.task(
        wf_fail_int::node()
            .unwrap()
            .node_id("fail")
            .kwargs(r#"{"error_code":"INNER_FAIL"}"#.to_owned()),
    );
    let child = b.sub_workflow(
        SubWorkflowNode::<ChildLabelInput, String>::typed("e2e_child_label_pipeline")
            .node_id("child")
            .queue("default")
            .set(ChildLabelInput::field_label(), "x".to_owned())
            .unwrap()
            .arg_from(ChildLabelInput::field_input_result(), failing)
            .allow_failed_deps(true),
    );
    let reader = b.task(
        horsies_test_worker::tasks::wf_subwf_ctx_reader::node()
            .unwrap()
            .node_id("reader")
            .waits_for(child)
            .kwargs(r#"{}"#.to_owned())
            .workflow_ctx_from([child])
            .allow_failed_deps(true),
    );
    b.output(reader);
    let spec = b.build().unwrap();

    let handle: WorkflowHandle<serde_json::Value> = start_registered_wf(&pool, &spec).await;
    let wf_id = handle.workflow_id().to_owned();
    // The upstream failure makes the workflow terminate FAILED; the reader still
    // runs (allow_failed_deps) and observed the failed child via workflow_ctx.
    wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(20)).await;
    assert_eq!(task_status(&pool, &wf_id, 2).await, "COMPLETED");

    let reader_json = ok_value(
        &task_result_json(&pool, &wf_id, 2)
            .await
            .expect("reader produced a result"),
    );
    assert_eq!(reader_json["summary_status"], "FAILED");
    assert_eq!(reader_json["summary_is_success"], false);
    assert_eq!(reader_json["result_is_ok"], false);
    assert!(task_subwf_summary(&pool, &wf_id, 1)
        .await
        .unwrap()
        .contains("FAILED"));

    cancel.cancel();
    let _ = worker.await;
}

// ---------------------------------------------------------------------------
// T7.6: outputless success policy keeps the failed sub-workflow result
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn outputless_success_policy_keeps_failed_subworkflow_result() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let (cancel, worker) = spawn_worker(&pool).await;

    // produce(0) ok, fail(1) -> child(2, fails, optional). Success policy: produce required,
    // child optional. No output index -> outputless terminal map.
    let mut b = WorkflowSpecBuilder::new("e2e_l7_outputless");
    b.task(
        wf_produce_int::node()
            .unwrap()
            .node_id("produce")
            .set_input(ProduceIntInput { value: 1 })
            .unwrap(),
    );
    let failing = b.task(
        wf_fail_int::node()
            .unwrap()
            .node_id("fail")
            .kwargs(r#"{"error_code":"INNER_FAIL"}"#.to_owned()),
    );
    b.sub_workflow(
        SubWorkflowNode::<ChildLabelInput, String>::typed("e2e_child_label_pipeline")
            .node_id("child")
            .queue("default")
            .set(ChildLabelInput::field_label(), "x".to_owned())
            .unwrap()
            .arg_from(ChildLabelInput::field_input_result(), failing)
            .allow_failed_deps(true),
    );
    b.success_policy(SuccessPolicy {
        cases: vec![SuccessCase {
            name: None,
            required_indices: vec![0],
        }],
        optional_indices: Some(vec![1, 2]),
    });
    let spec = b.build().unwrap();

    let wf_id = start_registered_wf::<serde_json::Value>(&pool, &spec)
        .await
        .workflow_id()
        .to_owned();
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(20)).await;
    assert_eq!(status, "COMPLETED"); // required produce satisfied; child optional

    // The failed sub-workflow's result is still recorded on its node.
    assert_eq!(task_status(&pool, &wf_id, 2).await, "FAILED");
    let child_result = task_result_json(&pool, &wf_id, 2)
        .await
        .expect("failed child result retained");
    assert!(child_result.contains("SUBWORKFLOW_FAILED"));
    assert!(task_subwf_summary(&pool, &wf_id, 2)
        .await
        .unwrap()
        .contains("FAILED"));

    cancel.cancel();
    let _ = worker.await;
}

// ---------------------------------------------------------------------------
// T7.7: parallel sub-workflows preserve independent success/failure
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn parallel_subworkflows_preserve_independent_success_and_failure() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let (cancel, worker) = spawn_worker(&pool).await;

    // produce_ok(0), fail(1). good child(2 <- produce_ok), bad child(3 <- fail). on_error=fail
    // but both children run independently (no deps between them).
    let mut b = WorkflowSpecBuilder::new("e2e_l7_parallel");
    b.on_error(OnError::Fail);
    let produce = b.task(
        wf_produce_int::node()
            .unwrap()
            .node_id("produce")
            .set_input(ProduceIntInput { value: 5 })
            .unwrap(),
    );
    let failing = b.task(
        wf_fail_int::node()
            .unwrap()
            .node_id("fail")
            .kwargs(r#"{"error_code":"INNER_FAIL"}"#.to_owned()),
    );
    b.sub_workflow(
        SubWorkflowNode::<ChildLabelInput, String>::typed("e2e_child_label_pipeline")
            .node_id("good")
            .queue("default")
            .set(ChildLabelInput::field_label(), "ok".to_owned())
            .unwrap()
            .arg_from(ChildLabelInput::field_input_result(), produce),
    );
    b.sub_workflow(
        SubWorkflowNode::<ChildLabelInput, String>::typed("e2e_child_label_pipeline")
            .node_id("bad")
            .queue("default")
            .set(ChildLabelInput::field_label(), "no".to_owned())
            .unwrap()
            .arg_from(ChildLabelInput::field_input_result(), failing)
            .allow_failed_deps(true),
    );
    let spec = b.build().unwrap();

    let wf_id = start_registered_wf::<serde_json::Value>(&pool, &spec)
        .await
        .workflow_id()
        .to_owned();
    wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(20)).await;

    // good child(2) COMPLETED with "ok=5"; bad child(3) FAILED.
    assert_eq!(task_status(&pool, &wf_id, 2).await, "COMPLETED");
    assert_eq!(
        ok_value(&task_result_json(&pool, &wf_id, 2).await.unwrap()),
        serde_json::json!("ok=5")
    );
    assert_eq!(task_status(&pool, &wf_id, 3).await, "FAILED");
    assert!(task_subwf_summary(&pool, &wf_id, 2)
        .await
        .unwrap()
        .contains("COMPLETED"));
    assert!(task_subwf_summary(&pool, &wf_id, 3)
        .await
        .unwrap()
        .contains("FAILED"));

    cancel.cancel();
    let _ = worker.await;
}

// ---------------------------------------------------------------------------
// T7.8: nested sub-workflow success records depth/root and result
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn nested_subworkflow_success_records_depth_root_and_result() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let (cancel, worker) = spawn_worker(&pool).await;

    // top -> nested_parent(0) which internally runs produce -> grandchild label pipeline.
    let mut b = WorkflowSpecBuilder::new("e2e_l7_nested_success");
    let nested = b.sub_workflow(
        SubWorkflowNode::<NestedParentInput, String>::typed("e2e_nested_parent_pipeline")
            .node_id("nested")
            .queue("default")
            .set(NestedParentInput::field_value(), 21i64)
            .unwrap()
            .set(NestedParentInput::field_label(), "lbl".to_owned())
            .unwrap(),
    );
    b.output(nested);
    let spec = b.build().unwrap();

    let handle: WorkflowHandle<String> = start_registered_wf(&pool, &spec).await;
    let result = handle.get(Some(Duration::from_secs(25))).await;
    match result {
        TaskResult::Ok(v) => assert_eq!(v, "lbl=21"),
        TaskResult::Err(e) => panic!("nested workflow failed: {}", e),
    }

    // Descendants share the top's root_workflow_id at depths 1 and 2.
    let depths: Vec<i32> = sqlx::query_scalar(
        "SELECT depth FROM horsies_workflows WHERE root_workflow_id = $1 ORDER BY depth",
    )
    .bind(handle.workflow_id())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        depths.contains(&1),
        "expected a depth-1 descendant, got {depths:?}"
    );
    assert!(
        depths.contains(&2),
        "expected a depth-2 descendant, got {depths:?}"
    );

    cancel.cancel();
    let _ = worker.await;
}

// ---------------------------------------------------------------------------
// T7.9: nested sub-workflow failure surfaces at each level
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn nested_subworkflow_failure_surfaces_each_child_error() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let (cancel, worker) = spawn_worker(&pool).await;

    // top -> nested_failing(0): produce fails inside -> grandchild fails -> nested fails.
    let mut b = WorkflowSpecBuilder::new("e2e_l7_nested_failure");
    b.on_error(OnError::Fail);
    let nested = b.sub_workflow(
        SubWorkflowNode::<NestedParentInput, String>::typed("e2e_nested_failing_pipeline")
            .node_id("nested")
            .queue("default")
            .set(NestedParentInput::field_value(), 1i64)
            .unwrap()
            .set(NestedParentInput::field_label(), "lbl".to_owned())
            .unwrap(),
    );
    b.output(nested);
    let spec = b.build().unwrap();

    let handle: WorkflowHandle<String> = start_registered_wf(&pool, &spec).await;
    let result = handle.get(Some(Duration::from_secs(25))).await;
    let err = match result {
        TaskResult::Err(e) => e,
        TaskResult::Ok(v) => panic!("expected failure, got Ok: {:?}", v),
    };
    // Top sees a SUBWORKFLOW_FAILED with recoverable detail.
    assert_eq!(error_code_str(&err), "SUBWORKFLOW_FAILED");
    let details = err.sub_workflow_details().expect("detail recoverable");
    assert_eq!(details.sub_workflow_summary.status, "FAILED");

    // The top sub-workflow node is FAILED, and a descendant workflow is FAILED too.
    assert_eq!(task_status(&pool, &handle.workflow_id(), 0).await, "FAILED");
    let failed_descendants: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM horsies_workflows WHERE root_workflow_id = $1 AND status = 'FAILED'",
    )
    .bind(handle.workflow_id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        failed_descendants >= 1,
        "expected a failed descendant workflow"
    );

    cancel.cancel();
    let _ = worker.await;
}

// ---------------------------------------------------------------------------
// T7.10: args_from delivers a typed TaskResult into build_with
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subworkflow_build_with_args_from_receives_typed_task_result() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let (cancel, worker) = spawn_worker(&pool).await;

    let mut b = WorkflowSpecBuilder::new("e2e_l7_args_from_typed");
    let produce = b.task(
        wf_produce_int::node()
            .unwrap()
            .node_id("produce")
            .set_input(ProduceIntInput { value: 99 })
            .unwrap(),
    );
    let child = b.sub_workflow(
        SubWorkflowNode::<ChildLabelInput, String>::typed("e2e_child_label_pipeline")
            .node_id("child")
            .queue("default")
            .set(ChildLabelInput::field_label(), "n".to_owned())
            .unwrap()
            .arg_from(ChildLabelInput::field_input_result(), produce),
    );
    b.output(child);
    let spec = b.build().unwrap();

    let handle: WorkflowHandle<String> = start_registered_wf(&pool, &spec).await;
    // "n=99" proves the typed TaskResult<i64> Ok(99) reached the child's build_with.
    match handle.get(Some(Duration::from_secs(20))).await {
        TaskResult::Ok(v) => assert_eq!(v, "n=99"),
        TaskResult::Err(e) => panic!("workflow failed: {}", e),
    }

    cancel.cancel();
    let _ = worker.await;
}

// ---------------------------------------------------------------------------
// T7.11: allow_failed_deps passes a failed TaskResult into build_with
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subworkflow_allow_failed_deps_passes_failed_task_result_to_build_with() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let (cancel, worker) = spawn_worker(&pool).await;

    let mut b = WorkflowSpecBuilder::new("e2e_l7_allow_failed");
    b.on_error(OnError::Fail);
    let failing = b.task(
        wf_fail_int::node()
            .unwrap()
            .node_id("fail")
            .kwargs(r#"{"error_code":"INNER_FAIL"}"#.to_owned()),
    );
    let child = b.sub_workflow(
        SubWorkflowNode::<ChildLabelInput, String>::typed("e2e_child_label_pipeline")
            .node_id("child")
            .queue("default")
            .set(ChildLabelInput::field_label(), "x".to_owned())
            .unwrap()
            .arg_from(ChildLabelInput::field_input_result(), failing)
            .allow_failed_deps(true),
    );
    b.output(child);
    let spec = b.build().unwrap();

    let handle: WorkflowHandle<String> = start_registered_wf(&pool, &spec).await;
    let _ = handle.get(Some(Duration::from_secs(20))).await;

    // Child STARTED (received the failed TaskResult in build_with) then FAILED inside —
    // it must not have been SKIPPED, which is what proves delivery.
    assert_eq!(task_status(&pool, &handle.workflow_id(), 1).await, "FAILED");
    assert!(task_subwf_summary(&pool, &handle.workflow_id(), 1)
        .await
        .unwrap()
        .contains("FAILED"));

    cancel.cancel();
    let _ = worker.await;
}

// ---------------------------------------------------------------------------
// T7.12: typed static kwargs reach build_with
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subworkflow_static_kwargs_preserve_build_with_types() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let (cancel, worker) = spawn_worker(&pool).await;

    // Static label kwarg + args_from int: "label=value" proves both reached build_with typed.
    let mut b = WorkflowSpecBuilder::new("e2e_l7_static_kwargs");
    let produce = b.task(
        wf_produce_int::node()
            .unwrap()
            .node_id("produce")
            .set_input(ProduceIntInput { value: 8 })
            .unwrap(),
    );
    let child = b.sub_workflow(
        SubWorkflowNode::<ChildLabelInput, String>::typed("e2e_child_label_pipeline")
            .node_id("child")
            .queue("default")
            .set(ChildLabelInput::field_label(), "static_label".to_owned())
            .unwrap()
            .arg_from(ChildLabelInput::field_input_result(), produce),
    );
    b.output(child);
    let spec = b.build().unwrap();

    let handle: WorkflowHandle<String> = start_registered_wf(&pool, &spec).await;
    match handle.get(Some(Duration::from_secs(20))).await {
        TaskResult::Ok(v) => assert_eq!(v, "static_label=8"),
        TaskResult::Err(e) => panic!("workflow failed: {}", e),
    }

    cancel.cancel();
    let _ = worker.await;
}
