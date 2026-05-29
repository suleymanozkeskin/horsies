#![allow(clippy::unwrap_used)]

//! Layer 6 e2e tests: workflow advanced semantics.
//!
//! Mirrors Python's `tests/e2e/test_layer6_workflow_advanced.py`.
//!
//! Run with:
//!   cargo test -p horsies-test-worker --test layer6_workflow_advanced -- --test-threads=1

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use serial_test::serial;
use sqlx::PgPool;

use horsies::{
    cancel_workflow, pause_workflow, resolve_node_task_options, resume_workflow, Horsies, OnError,
    PostgresBroker, SubWorkflowNode, SuccessCase, SuccessPolicy, TaskError, TaskResult, Worker,
    WorkerConfig, WorkflowHandle, WorkflowSpecBuilder, WorkflowSpecRegistry,
};
use horsies_test_support::{
    db,
    e2e::{
        db_poll::{wait_for_task_status, wait_for_workflow_terminal},
        worker::start_worker,
        workflow::{get_workflow_task_status, get_workflow_tasks, wait_for_workflow_completion},
    },
    fixtures,
};
use horsies_test_worker::tasks::{
    wf_ctx_reader, wf_fail, wf_produce_int, wf_retry_then_ok, wf_retry_via_registration,
    wf_slow_step, wf_step, ChildLabelInput, ProduceIntInput,
};

// ---------------------------------------------------------------------------
// Helpers
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

fn db_url() -> String {
    db::db_url()
}

fn registry() -> WorkflowSpecRegistry {
    WorkflowSpecRegistry::new()
}

async fn start_wf(pool: &PgPool, spec: &horsies::WorkflowSpec) -> String {
    start_wf_with_config(pool, spec, fixtures::default_app_config()).await
}

async fn start_wf_with_config(
    pool: &PgPool,
    spec: &horsies::WorkflowSpec,
    config: horsies::AppConfig,
) -> String {
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let mut app = Horsies::with_broker(config, broker).unwrap();
    let handle: WorkflowHandle<serde_json::Value> = app
        .start(spec.clone())
        .await
        .unwrap_or_else(|e| panic!("app.start failed: {}", e));
    handle.workflow_id().to_owned()
}

async fn wait_for_wf_status(pool: &PgPool, wf_id: &str, target: &str, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(wf_id)
                .fetch_optional(pool)
                .await
                .ok()
                .flatten();
        if status.as_deref() == Some(target) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "workflow {} did not reach status {} within {}s",
        wf_id,
        target,
        timeout.as_secs()
    );
}

async fn start_registered_wf<T>(pool: &PgPool, spec: &horsies::WorkflowSpec) -> WorkflowHandle<T>
where
    T: serde::de::DeserializeOwned + Clone + Send + Sync + 'static,
{
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let mut app = Horsies::with_broker(fixtures::default_app_config(), broker).unwrap();
    horsies_test_worker::tasks::register(&mut app).unwrap();
    app.start(spec.clone())
        .await
        .unwrap_or_else(|e| panic!("app.start failed: {}", e))
}

// ---------------------------------------------------------------------------
// T6.2: Join=quorum + ctx gating
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_quorum_ctx_gating() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(100ms), B(150ms), C(300ms) → D (join=quorum, min_success=2, ctx from C).
    let mut b = WorkflowSpecBuilder::new("e2e_quorum_ctx");
    let a = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A","delay_ms":100}"#.to_owned()),
    );
    let bnode = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"step":"B","delay_ms":150}"#.to_owned()),
    );
    let c = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("c")
            .kwargs(r#"{"step":"C","delay_ms":300}"#.to_owned()),
    );
    b.task(
        wf_ctx_reader::node()
            .unwrap()
            .node_id("d")
            .waits_for(a)
            .waits_for(bnode)
            .waits_for(c)
            .workflow_ctx_from([c])
            .join_quorum(2),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    let tasks = get_workflow_tasks(&pool, &wf_id).await;
    assert_eq!(tasks.len(), 4);
    for t in &tasks {
        assert_eq!(t.status, "COMPLETED");
    }

    // D must complete after C (ctx gating).
    assert!(tasks[3].completed_at.unwrap() >= tasks[2].completed_at.unwrap());
}

// ---------------------------------------------------------------------------
// T6.2b: Quorum impossible → SKIPPED
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_quorum_impossible_skips() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(fail), B(fail), C(ok) → D (quorum min_success=3, impossible).
    let mut b = WorkflowSpecBuilder::new("e2e_quorum_impossible");
    let a = b.task(
        wf_fail::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"error_code":"F1"}"#.to_owned()),
    );
    let bnode = b.task(
        wf_fail::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"error_code":"F2"}"#.to_owned()),
    );
    let c = b.task(
        wf_step::node()
            .unwrap()
            .node_id("c")
            .kwargs(r#"{"step":"C"}"#.to_owned()),
    );
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("d")
            .kwargs(r#"{"step":"D"}"#.to_owned())
            .waits_for(a)
            .waits_for(bnode)
            .waits_for(c)
            .join_quorum(3),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "FAILED");

    assert_eq!(get_workflow_task_status(&pool, &wf_id, 0).await, "FAILED");
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 1).await, "FAILED");
    assert_eq!(
        get_workflow_task_status(&pool, &wf_id, 2).await,
        "COMPLETED"
    );
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 3).await, "SKIPPED");
}

#[tokio::test]
#[serial]
async fn test_subworkflow_mixed_explicit_and_args_from() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let mut worker_app = Horsies::with_broker(fixtures::default_app_config(), broker).unwrap();
    horsies_test_worker::tasks::register(&mut worker_app).unwrap();
    let (app_config, registry, wf_registry, broker) = worker_app.into_parts().await.unwrap();
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
    let worker_task = tokio::spawn(async move { worker.run().await });
    tokio::time::sleep(Duration::from_millis(250)).await;

    let mut b = WorkflowSpecBuilder::new("e2e_subworkflow_mixed_input");
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
    b.output(child);
    let spec = b.build().unwrap();

    let handle: WorkflowHandle<String> = start_registered_wf(&pool, &spec).await;
    let result = handle.get(Some(Duration::from_secs(15))).await;
    cancel.cancel();
    let _ = worker_task.await;
    match result {
        TaskResult::Ok(value) => assert_eq!(value, "count=21"),
        TaskResult::Err(err) => panic!("workflow failed: {}", err),
    }
}

// ---------------------------------------------------------------------------
// T6.2c: Join=any, all deps fail → SKIPPED
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_join_any_all_deps_fail_skips() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_any_all_fail");
    let a = b.task(
        wf_fail::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"error_code":"F1"}"#.to_owned()),
    );
    let bnode = b.task(
        wf_fail::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"error_code":"F2"}"#.to_owned()),
    );
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("c")
            .kwargs(r#"{"step":"C"}"#.to_owned())
            .waits_for(a)
            .waits_for(bnode)
            .join_any(),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "FAILED");

    assert_eq!(get_workflow_task_status(&pool, &wf_id, 0).await, "FAILED");
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 1).await, "FAILED");
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 2).await, "SKIPPED");
}

// ---------------------------------------------------------------------------
// T6.4: Pause blocks ready transitions
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_pause_blocks_ready_transitions() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(100ms) → B(200ms) → C(200ms) → D(100ms)
    let mut b = WorkflowSpecBuilder::new("e2e_pausable");
    let a = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A","delay_ms":100}"#.to_owned()),
    );
    let bnode = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"step":"B","delay_ms":200}"#.to_owned())
            .waits_for(a),
    );
    let c = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("c")
            .kwargs(r#"{"step":"C","delay_ms":200}"#.to_owned())
            .waits_for(bnode),
    );
    b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("d")
            .kwargs(r#"{"step":"D","delay_ms":100}"#.to_owned())
            .waits_for(c),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;

    // Wait for A to complete.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if get_workflow_task_status(&pool, &wf_id, 0).await == "COMPLETED" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Pause.
    let paused = pause_workflow(&pool, &wf_id).await.unwrap();
    assert!(paused, "pause should succeed");

    let status: String = sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
        .bind(&wf_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "PAUSED");

    // Observe it stays PAUSED for 1s.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let status: String = sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
        .bind(&wf_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "PAUSED");
}

// ---------------------------------------------------------------------------
// T6.5: Resume continues workflow
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_resume_continues_workflow() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let reg = registry();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_resume");
    let a = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A","delay_ms":100}"#.to_owned()),
    );
    let bnode = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"step":"B","delay_ms":200}"#.to_owned())
            .waits_for(a),
    );
    let c = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("c")
            .kwargs(r#"{"step":"C","delay_ms":200}"#.to_owned())
            .waits_for(bnode),
    );
    b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("d")
            .kwargs(r#"{"step":"D","delay_ms":100}"#.to_owned())
            .waits_for(c),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if get_workflow_task_status(&pool, &wf_id, 0).await == "COMPLETED" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    pause_workflow(&pool, &wf_id).await.unwrap();
    wait_for_wf_status(&pool, &wf_id, "PAUSED", Duration::from_secs(3)).await;

    let resumed = resume_workflow(&pool, &wf_id, &reg).await.unwrap();
    assert!(resumed, "resume should succeed");

    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");
}

// ---------------------------------------------------------------------------
// T6.4b: Pause then cancel
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_pause_then_cancel() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_pause_cancel");
    let a = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A","delay_ms":100}"#.to_owned()),
    );
    let bnode = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"step":"B","delay_ms":5000}"#.to_owned())
            .waits_for(a),
    );
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("c")
            .kwargs(r#"{"step":"C"}"#.to_owned())
            .waits_for(bnode),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if get_workflow_task_status(&pool, &wf_id, 0).await == "COMPLETED" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    pause_workflow(&pool, &wf_id).await.unwrap();
    wait_for_wf_status(&pool, &wf_id, "PAUSED", Duration::from_secs(3)).await;

    cancel_workflow(&pool, &wf_id).await.unwrap();

    let status: String = sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
        .bind(&wf_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "CANCELLED");
}

// ---------------------------------------------------------------------------
// T6.6: Success policy satisfied
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_success_policy_satisfied() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(ok), B(fail). Success policy: A required, B optional → COMPLETED.
    let mut b = WorkflowSpecBuilder::new("e2e_sp_satisfied");
    let _a_ref = b.task(
        wf_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    let _b_ref = b.task(
        wf_fail::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"error_code":"OPT_FAIL"}"#.to_owned()),
    );
    b.success_policy(SuccessPolicy {
        cases: vec![SuccessCase {
            name: None,
            required_indices: vec![0],
        }],
        optional_indices: Some(vec![1]),
    });
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    assert_eq!(
        get_workflow_task_status(&pool, &wf_id, 0).await,
        "COMPLETED"
    );
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 1).await, "FAILED");
}

// ---------------------------------------------------------------------------
// T6.7: Success policy not met
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_success_policy_not_met() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(ok), B(fail). Policy: both required → FAILED.
    let mut b = WorkflowSpecBuilder::new("e2e_sp_not_met");
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    b.task(
        wf_fail::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"error_code":"REQUIRED_FAIL"}"#.to_owned()),
    );
    b.success_policy(SuccessPolicy {
        cases: vec![SuccessCase {
            name: None,
            required_indices: vec![0, 1],
        }],
        optional_indices: None,
    });
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "FAILED");

    assert_eq!(
        get_workflow_task_status(&pool, &wf_id, 0).await,
        "COMPLETED"
    );
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 1).await, "FAILED");
}

// ---------------------------------------------------------------------------
// T6.8: Multiple success cases (case2 satisfied)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_success_policy_multiple_cases() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(fail), B(ok). Case1 requires A, Case2 requires B → Case2 satisfied → COMPLETED.
    let mut b = WorkflowSpecBuilder::new("e2e_sp_multi");
    b.task(
        wf_fail::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"error_code":"F"}"#.to_owned()),
    );
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"step":"B"}"#.to_owned()),
    );
    b.success_policy(SuccessPolicy {
        cases: vec![
            SuccessCase {
                name: None,
                required_indices: vec![0],
            },
            SuccessCase {
                name: None,
                required_indices: vec![1],
            },
        ],
        optional_indices: None,
    });
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    assert_eq!(get_workflow_task_status(&pool, &wf_id, 0).await, "FAILED");
    assert_eq!(
        get_workflow_task_status(&pool, &wf_id, 1).await,
        "COMPLETED"
    );
}

// ---------------------------------------------------------------------------
// T6.11a: on_error=PAUSE auto-pauses on failure
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_on_error_pause_stops_workflow() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(ok) → B(fail) → C. on_error=PAUSE.
    let mut b = WorkflowSpecBuilder::new("e2e_on_error_pause");
    b.on_error(OnError::Pause);
    let a = b.task(
        wf_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    let bnode = b.task(
        wf_fail::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"error_code":"PAUSE_FAIL"}"#.to_owned())
            .waits_for(a),
    );
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("c")
            .kwargs(r#"{"step":"C"}"#.to_owned())
            .waits_for(bnode),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    wait_for_wf_status(&pool, &wf_id, "PAUSED", Duration::from_secs(15)).await;

    assert_eq!(
        get_workflow_task_status(&pool, &wf_id, 0).await,
        "COMPLETED"
    );
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 1).await, "FAILED");
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 2).await, "PENDING");
}

// ---------------------------------------------------------------------------
// T6.11b: on_error=PAUSE, resume → FAILED (B failed, C SKIPPED)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_on_error_pause_resume_completes() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let reg = registry();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_pause_resume");
    b.on_error(OnError::Pause);
    let a = b.task(
        wf_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    let bnode = b.task(
        wf_fail::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"error_code":"PAUSE_FAIL"}"#.to_owned())
            .waits_for(a),
    );
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("c")
            .kwargs(r#"{"step":"C"}"#.to_owned())
            .waits_for(bnode),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    wait_for_wf_status(&pool, &wf_id, "PAUSED", Duration::from_secs(15)).await;

    let resumed = resume_workflow(&pool, &wf_id, &reg).await.unwrap();
    assert!(resumed);

    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "FAILED");

    assert_eq!(
        get_workflow_task_status(&pool, &wf_id, 0).await,
        "COMPLETED"
    );
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 1).await, "FAILED");
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 2).await, "SKIPPED");
}

// ---------------------------------------------------------------------------
// T6.11c: on_error=PAUSE cascades the implicit pause to running child
// workflows (parity with horsies PR #28). Driven via the DB + the completion
// handler so no worker timing is involved: the manually-inserted RUNNING child
// stands in for a running sub-workflow that must be paused alongside its parent.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_on_error_pause_cascades_to_running_child() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let reg = registry();

    // Parent workflow with one root task and on_error=PAUSE. No worker: we drive
    // the failure directly through on_workflow_task_complete.
    let mut b = WorkflowSpecBuilder::new("e2e_pause_cascade_parent");
    b.on_error(OnError::Pause);
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;

    // Insert a RUNNING child workflow parented to the parent (mimics a running
    // sub-workflow). Only id/name/status need explicit values; the rest default.
    let child_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    sqlx::query(
        "INSERT INTO horsies_workflows (id, name, status, parent_workflow_id, root_workflow_id) \
         VALUES ($1, $2, 'RUNNING', $3, $3)",
    )
    .bind(child_id)
    .bind("e2e_pause_cascade_child")
    .bind(&wf_id)
    .execute(&pool)
    .await
    .unwrap();

    // Resolve the root task's horsies_tasks id, then fail it.
    let task_id: String =
        sqlx::query_scalar("SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = 0")
            .bind(&wf_id)
            .fetch_one(&pool)
            .await
            .unwrap();

    let failed_result = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(
        TaskError::new("PARENT_FAIL", "parent failed"),
    ))
    .unwrap();

    horsies::on_workflow_task_complete(&pool, &task_id, &failed_result, false, &reg)
        .await
        .unwrap();

    // Parent paused on error, and the running child was paused by the cascade.
    let parent_status: String =
        sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
            .bind(&wf_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let child_status: String =
        sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
            .bind(child_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(parent_status, "PAUSED");
    assert_eq!(child_status, "PAUSED");
}

// ---------------------------------------------------------------------------
// T6.11d: on_error=FAIL stores the first failed task's error by index, even
// when a higher-index task fails first in time (parity with horsies PR #29).
// Driven via the DB + completion handler; a never-completed blocker task keeps
// the workflow RUNNING so the transient error is observable.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_on_error_fail_keeps_first_failed_error_by_index() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let reg = registry();

    // Three root tasks under on_error=FAIL. We fail idx 1 first, then idx 0; a
    // blocker (idx 2) is never completed so the workflow stays RUNNING.
    let mut b = WorkflowSpecBuilder::new("e2e_fail_first_error_by_index");
    b.on_error(OnError::Fail);
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"step":"B"}"#.to_owned()),
    );
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("blocker")
            .kwargs(r#"{"step":"BLOCK"}"#.to_owned()),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;

    let task_id_for = |idx: i32| {
        let pool = pool.clone();
        let wf_id = wf_id.clone();
        async move {
            sqlx::query_scalar::<_, String>(
                "SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = $2",
            )
            .bind(&wf_id)
            .bind(idx)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    let task_id_0 = task_id_for(0).await;
    let task_id_1 = task_id_for(1).await;

    let err_result = |code: &str, msg: &str| {
        serde_json::to_string(&TaskResult::<serde_json::Value>::Err(TaskError::new(code, msg)))
            .unwrap()
    };

    // Fail the higher index (1) first in time...
    horsies::on_workflow_task_complete(
        &pool,
        &task_id_1,
        &err_result("SECOND_ERROR", "index 1 failed"),
        false,
        &reg,
    )
    .await
    .unwrap();
    // ...then the lower index (0).
    horsies::on_workflow_task_complete(
        &pool,
        &task_id_0,
        &err_result("FIRST_ERROR", "index 0 failed"),
        false,
        &reg,
    )
    .await
    .unwrap();

    // Workflow still RUNNING (blocker not terminal); stored error is index 0's.
    let (status, error): (String, Option<String>) =
        sqlx::query_as("SELECT status, error FROM horsies_workflows WHERE id = $1")
            .bind(&wf_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "RUNNING");
    let error = error.expect("workflow error should be set while RUNNING");
    assert!(
        error.contains("FIRST_ERROR"),
        "expected first failed task error (by index), got: {error}",
    );
    assert!(
        !error.contains("SECOND_ERROR"),
        "higher-index error should not win, got: {error}",
    );
}

// ---------------------------------------------------------------------------
// T6.9: Recovery preserves results (stuck RUNNING → re-finalized)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_recovery_preserves_results() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let reg = registry();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_recovery_preserve");
    let a = b.task(
        wf_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"step":"B"}"#.to_owned())
            .waits_for(a),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    // Simulate stuck: set workflow back to RUNNING.
    sqlx::query(
        "UPDATE horsies_workflows SET status = 'RUNNING', completed_at = NULL WHERE id = $1",
    )
    .bind(&wf_id)
    .execute(&pool)
    .await
    .unwrap();

    // Run recovery.
    let _report = horsies::recover_stuck_workflows(&pool, &reg).await.unwrap();

    // Verify recovered back to COMPLETED.
    let final_status: String =
        sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
            .bind(&wf_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(final_status, "COMPLETED");
}

// ---------------------------------------------------------------------------
// T6.9b: Recovery preserves failed state
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_recovery_preserves_failed_state() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let reg = registry();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_recovery_fail");
    let a = b.task(
        wf_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    b.task(
        wf_fail::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"error_code":"MID_FAIL"}"#.to_owned())
            .waits_for(a),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "FAILED");

    // Simulate stuck.
    sqlx::query(
        "UPDATE horsies_workflows SET status = 'RUNNING', completed_at = NULL WHERE id = $1",
    )
    .bind(&wf_id)
    .execute(&pool)
    .await
    .unwrap();

    // Run recovery.
    let _ = horsies::recover_stuck_workflows(&pool, &reg).await.unwrap();

    // Should be back to FAILED (not COMPLETED).
    let final_status: String =
        sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
            .bind(&wf_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(final_status, "FAILED");
}

// ---------------------------------------------------------------------------
// T6.9d: Recovery recomputes the first failed task's error (parity with
// horsies PR #27). Recovery must NOT preserve a stale later error — it
// recomputes the failure error deterministically (first failed task by index),
// the same selection as normal completion.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_recovery_recomputes_first_failed_error() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let reg = registry();

    // Two independent root tasks (indices 0 and 1). No worker: we drive the DB
    // state directly to simulate a workflow stuck mid-finalization that already
    // has the *later* failure stored as its error.
    let mut b = WorkflowSpecBuilder::new("e2e_recovery_first_error");
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"step":"B"}"#.to_owned()),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;

    // Mark both tasks FAILED with distinct error codes.
    let first_result = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(
        TaskError::new("FIRST_ERROR", "first failure"),
    ))
    .unwrap();
    let second_result = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(
        TaskError::new("SECOND_ERROR", "second failure"),
    ))
    .unwrap();

    sqlx::query(
        "UPDATE horsies_workflow_tasks SET status = 'FAILED', result = $2, completed_at = NOW() \
         WHERE workflow_id = $1 AND task_index = 0",
    )
    .bind(&wf_id)
    .bind(&first_result)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE horsies_workflow_tasks SET status = 'FAILED', result = $2, completed_at = NOW() \
         WHERE workflow_id = $1 AND task_index = 1",
    )
    .bind(&wf_id)
    .bind(&second_result)
    .execute(&pool)
    .await
    .unwrap();

    // Workflow stuck RUNNING with the later failure already stored as error.
    let stale_error =
        serde_json::to_string(&TaskError::new("SECOND_ERROR", "second failure")).unwrap();
    sqlx::query(
        "UPDATE horsies_workflows \
         SET status = 'RUNNING', completed_at = NULL, result = NULL, error = $2 \
         WHERE id = $1",
    )
    .bind(&wf_id)
    .bind(&stale_error)
    .execute(&pool)
    .await
    .unwrap();

    // Run recovery.
    let _ = horsies::recover_stuck_workflows(&pool, &reg).await.unwrap();

    // Workflow finalizes FAILED with the deterministic first failed task error,
    // replacing the stale later error.
    let (status, error): (String, Option<String>) =
        sqlx::query_as("SELECT status, error FROM horsies_workflows WHERE id = $1")
            .bind(&wf_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "FAILED");
    let error = error.expect("workflow error should be set");
    assert!(
        error.contains("FIRST_ERROR"),
        "expected first failed task error, got: {error}",
    );
    assert!(
        !error.contains("SECOND_ERROR"),
        "stale later error should have been replaced, got: {error}",
    );
}

// ---------------------------------------------------------------------------
// T6.4c: Pause idempotent — second pause returns false
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_pause_idempotent() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(100ms) → B(200ms) → C(200ms) → D(100ms)
    let mut b = WorkflowSpecBuilder::new("e2e_pause_idempotent");
    let a = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A","delay_ms":100}"#.to_owned()),
    );
    let bnode = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"step":"B","delay_ms":200}"#.to_owned())
            .waits_for(a),
    );
    let c = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("c")
            .kwargs(r#"{"step":"C","delay_ms":200}"#.to_owned())
            .waits_for(bnode),
    );
    b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("d")
            .kwargs(r#"{"step":"D","delay_ms":100}"#.to_owned())
            .waits_for(c),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;

    // Wait for A to complete so workflow is RUNNING.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if get_workflow_task_status(&pool, &wf_id, 0).await == "COMPLETED" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let first_pause = pause_workflow(&pool, &wf_id).await.unwrap();
    assert!(first_pause, "first pause should return true");

    let second_pause = pause_workflow(&pool, &wf_id).await.unwrap();
    assert!(
        !second_pause,
        "second pause on already-paused workflow should return false"
    );
}

// ---------------------------------------------------------------------------
// T6.5b: Resume on RUNNING workflow is a no-op (returns false)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_resume_on_running_noop() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let reg = registry();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_resume_noop");
    let a = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A","delay_ms":100}"#.to_owned()),
    );
    b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"step":"B","delay_ms":2000}"#.to_owned())
            .waits_for(a),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;

    // Wait until workflow is RUNNING.
    wait_for_wf_status(&pool, &wf_id, "RUNNING", Duration::from_secs(5)).await;

    let resumed = resume_workflow(&pool, &wf_id, &reg).await.unwrap();
    assert!(!resumed, "resume on RUNNING workflow should return false");
}

// ---------------------------------------------------------------------------
// T6.7b: Success policy not met — error content check
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_success_policy_not_met_error_content() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(ok), B(fail with REQUIRED_FAIL). Policy: both required → FAILED.
    let mut b = WorkflowSpecBuilder::new("e2e_sp_error_content");
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    b.task(
        wf_fail::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"error_code":"REQUIRED_FAIL"}"#.to_owned()),
    );
    b.success_policy(SuccessPolicy {
        cases: vec![SuccessCase {
            name: None,
            required_indices: vec![0, 1],
        }],
        optional_indices: None,
    });
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "FAILED");

    // Failed workflows store error details in the `error` column (not `result`).
    let error_json: Option<String> =
        sqlx::query_scalar("SELECT error FROM horsies_workflows WHERE id = $1")
            .bind(&wf_id)
            .fetch_one(&pool)
            .await
            .unwrap();

    let error_data: serde_json::Value = serde_json::from_str(
        error_json
            .as_deref()
            .expect("workflow error should be set on failure"),
    )
    .unwrap();
    assert_eq!(
        error_data.get("error_code").and_then(|v| v.as_str()),
        Some("REQUIRED_FAIL"),
    );
}

// ---------------------------------------------------------------------------
// T6.10: Workflow task retries and eventually succeeds
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_workflow_task_retries() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // Create counter file.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let counter_path = tmp.path().to_str().unwrap().to_owned();

    // Retry policy: max 3 retries, 1s fixed intervals, auto-retry on TRANSIENT.
    let retry_options = serde_json::json!({
        "retry_policy": {
            "max_retries": 3,
            "intervals": [1, 1, 1],
            "backoff_strategy": "fixed",
            "jitter": false,
            "auto_retry_for": ["TRANSIENT"],
        }
    });
    let retry_options_str = serde_json::to_string(&retry_options).unwrap();

    let mut b = WorkflowSpecBuilder::new("e2e_wf_retry_ok");
    b.task(
        wf_retry_then_ok::node()
            .unwrap()
            .node_id("a")
            .kwargs(format!(
                r#"{{"counter_file":"{}","succeed_on_attempt":2}}"#,
                counter_path.replace('\\', "\\\\").replace('"', "\\\"")
            ))
            .task_options(retry_options_str),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(30)).await;
    assert_eq!(
        status, "COMPLETED",
        "workflow should complete after retry succeeds"
    );

    // Verify the task completed.
    assert_eq!(
        get_workflow_task_status(&pool, &wf_id, 0).await,
        "COMPLETED"
    );

    // Counter file should show at least 2 attempts.
    let final_count: i32 = tokio::fs::read_to_string(&counter_path)
        .await
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        final_count >= 2,
        "should have at least 2 attempts, got {}",
        final_count
    );
}

// ---------------------------------------------------------------------------
// T6.10b: Workflow task retries exhausted → workflow FAILED
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_workflow_task_retries_exhausted() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let counter_path = tmp.path().to_str().unwrap().to_owned();

    // succeed_on_attempt=10 but max_retries=3 → 4 attempts total, never succeeds.
    let retry_options = serde_json::json!({
        "retry_policy": {
            "max_retries": 3,
            "intervals": [1, 1, 1],
            "backoff_strategy": "fixed",
            "jitter": false,
            "auto_retry_for": ["TRANSIENT"],
        }
    });
    let retry_options_str = serde_json::to_string(&retry_options).unwrap();

    let mut b = WorkflowSpecBuilder::new("e2e_wf_retry_exhausted");
    b.task(
        wf_retry_then_ok::node()
            .unwrap()
            .node_id("a")
            .kwargs(format!(
                r#"{{"counter_file":"{}","succeed_on_attempt":10}}"#,
                counter_path.replace('\\', "\\\\").replace('"', "\\\"")
            ))
            .task_options(retry_options_str),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(60)).await;
    assert_eq!(
        status, "FAILED",
        "workflow should FAIL after exhausting retries"
    );

    assert_eq!(get_workflow_task_status(&pool, &wf_id, 0).await, "FAILED");

    // Should have exactly 4 attempts (1 initial + 3 retries).
    let final_count: i32 = tokio::fs::read_to_string(&counter_path)
        .await
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(
        final_count, 4,
        "should have exactly 4 attempts (1 initial + 3 retries), got {}",
        final_count
    );
}

// ---------------------------------------------------------------------------
// T6.9c: Workflow recovers after worker crash
// ---------------------------------------------------------------------------

/// Write a recovery config with custom queues (default + recovery).
fn write_recovery_custom_config() -> tempfile::NamedTempFile {
    let url = db_url();
    let config = serde_json::json!({
        "queue_mode": "custom",
        "custom_queues": [
            {"name": "default", "priority": 1, "max_concurrency": 5},
            {"name": "recovery", "priority": 1, "max_concurrency": 1}
        ],
        "broker": {
            "database_url": url,
            "pool_pre_ping": true,
            "pool_size": 5,
            "max_overflow": 5,
            "pool_timeout": 10,
            "pool_recycle": 600,
            "echo": false
        },
        "prefetch_buffer": 0,
        "max_claim_renew_age_ms": 180000,
        "recovery": {
            "auto_requeue_stale_claimed": true,
            "claimed_stale_threshold_ms": 2000,
            "auto_fail_stale_running": true,
            "running_stale_threshold_ms": 2000,
            "check_interval_ms": 1000,
            "runner_heartbeat_interval_ms": 1000,
            "claimer_heartbeat_interval_ms": 1000,
            "heartbeat_retention_hours": 1,
            "worker_state_retention_hours": 1,
            "terminal_record_retention_hours": 1
        },
        "resilience": {
            "db_retry_initial_ms": 500,
            "db_retry_max_ms": 5000,
            "db_retry_max_attempts": 3,
            "notify_poll_interval_ms": 1000
        },
        "resend_on_transient_err": false
    });

    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(serde_json::to_string_pretty(&config).unwrap().as_bytes())
        .unwrap();
    f.flush().unwrap();
    f
}

#[tokio::test]
#[serial]
async fn test_workflow_recovers_after_worker_crash() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let config_file = write_recovery_custom_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    // DAG: A(50ms) → B(50ms) → C(60s)  ──→ E(50ms, recovery queue) → F(50ms, recovery queue)
    //                         → D(60s)  ──/
    //
    // Kill worker after A,B complete and C,D are RUNNING.
    // Restart worker → reaper marks C,D FAILED → E runs (allow_failed_deps) → F runs → FAILED.
    let mut b = WorkflowSpecBuilder::new("e2e_recovery_crash");
    let a = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A","delay_ms":50}"#.to_owned()),
    );
    let bnode = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"step":"B","delay_ms":50}"#.to_owned())
            .waits_for(a),
    );
    let c = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("c")
            .kwargs(r#"{"step":"C","delay_ms":60000}"#.to_owned())
            .waits_for(bnode),
    );
    let d = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("d")
            .kwargs(r#"{"step":"D","delay_ms":60000}"#.to_owned())
            .waits_for(bnode),
    );
    let e = b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("e")
            .kwargs(r#"{"step":"E","delay_ms":50}"#.to_owned())
            .waits_for(c)
            .waits_for(d)
            .allow_failed_deps(true)
            .queue("recovery"),
    );
    b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("f")
            .kwargs(r#"{"step":"F","delay_ms":50}"#.to_owned())
            .waits_for(e)
            .queue("recovery"),
    );
    let spec = b.build().unwrap();

    // Start first worker (must listen on both queues).
    let mut w1 = start_worker(
        &config_path,
        &["--concurrency", "2", "--queues", "default,recovery"],
        "worker started",
        Duration::from_secs(20),
    );

    // The spec contains .queue("recovery") nodes, so the in-process Horsies
    // that starts the workflow must use Custom queue mode (matching the binary).
    let mut app_config = fixtures::default_app_config();
    app_config.queue_mode = horsies::QueueMode::Custom;
    app_config.custom_queues = Some(vec![
        horsies::CustomQueueConfig {
            name: "default".to_owned(),
            priority: 1,
            max_concurrency: 5,
        },
        horsies::CustomQueueConfig {
            name: "recovery".to_owned(),
            priority: 1,
            max_concurrency: 1,
        },
    ]);
    let wf_id = start_wf_with_config(&pool, &spec, app_config).await;

    // Wait until A and B are COMPLETED.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        let a_status = get_workflow_task_status(&pool, &wf_id, 0).await;
        let b_status = get_workflow_task_status(&pool, &wf_id, 1).await;
        if a_status == "COMPLETED" && b_status == "COMPLETED" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        get_workflow_task_status(&pool, &wf_id, 0).await,
        "COMPLETED",
        "A should complete before crash"
    );
    assert_eq!(
        get_workflow_task_status(&pool, &wf_id, 1).await,
        "COMPLETED",
        "B should complete before crash"
    );

    // Wait until C and D underlying tasks are RUNNING.
    let c_task_id: Option<String> = sqlx::query_scalar(
        "SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = 2",
    )
    .bind(&wf_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let d_task_id: Option<String> = sqlx::query_scalar(
        "SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = 3",
    )
    .bind(&wf_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    let c_task_id = c_task_id.expect("C should be enqueued");
    let d_task_id = d_task_id.expect("D should be enqueued");

    wait_for_task_status(&pool, &c_task_id, "RUNNING", Duration::from_secs(15)).await;
    wait_for_task_status(&pool, &d_task_id, "RUNNING", Duration::from_secs(15)).await;

    // Kill worker 1.
    w1.kill();

    // Start worker 2 — its reaper will detect stale tasks and recovery kicks in.
    let _w2 = start_worker(
        &config_path,
        &["--concurrency", "2", "--queues", "default,recovery"],
        "worker started",
        Duration::from_secs(20),
    );

    // Wait for workflow to reach terminal state.
    // Recovery chain: reaper marks C,D FAILED → case 1.7 calls on_workflow_task_complete
    // → E becomes ready → E runs → F runs → workflow finalizes.
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(30)).await;
    assert_eq!(status, "FAILED", "workflow should be FAILED (C,D crashed)");

    let tasks = get_workflow_tasks(&pool, &wf_id).await;
    assert_eq!(tasks.len(), 6, "expected 6 workflow tasks");

    // A, B: completed before crash.
    assert_eq!(tasks[0].status, "COMPLETED", "A should be COMPLETED");
    assert_eq!(tasks[1].status, "COMPLETED", "B should be COMPLETED");

    // C, D: marked FAILED by recovery.
    assert_eq!(tasks[2].status, "FAILED", "C should be FAILED (crashed)");
    assert_eq!(tasks[3].status, "FAILED", "D should be FAILED (crashed)");

    // E: COMPLETED — ran after recovery (allow_failed_deps=True).
    assert_eq!(
        tasks[4].status, "COMPLETED",
        "E should be COMPLETED (allow_failed_deps)"
    );

    // F: COMPLETED — ran after E on recovery queue.
    assert_eq!(tasks[5].status, "COMPLETED", "F should be COMPLETED");
}

// ---------------------------------------------------------------------------
// T6.10c: Workflow task inherits retry options from task registration
// ---------------------------------------------------------------------------
//
// Regression test for the bug where workflow-enqueued tasks lost their retry
// options because TaskOptions from the task registry were not carried to the
// workflow node's task_options_json.
//
// The key difference from T6.10: retry config comes from the task registration
// (via `with_task_options()` on the `RegisteredTask`), NOT from `.task_options()`
// on the `TaskNode`. The node is built without any explicit task_options.

#[tokio::test]
#[serial]
async fn test_workflow_task_inherits_retry_from_registration() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // Create counter file.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let counter_path = tmp.path().to_str().unwrap().to_owned();

    // Build the workflow spec WITHOUT calling .task_options() on the node.
    // The task "e2e_wf_retry_via_registration" is registered in tasks.rs
    // with TaskOptions { auto_retry_for: ["TRANSIENT"], retry_policy: fixed([1,1,1]) }.
    let mut b = WorkflowSpecBuilder::new("e2e_wf_inherited_retry");
    b.task(
        wf_retry_via_registration::node()
            .unwrap()
            .node_id("a")
            .kwargs(format!(
                r#"{{"counter_file":"{}","succeed_on_attempt":2}}"#,
                counter_path.replace('\\', "\\\\").replace('"', "\\\"")
            )),
        // NOTE: no .task_options() call here — that's the whole point of this test
    );
    let mut spec = b.build().unwrap();

    // Simulate what Horsies::register_workflow() does: resolve task options
    // from the task registry into the spec's nodes. This is the code path
    // being tested — the lookup returns the serialized retry options that
    // the task was registered with.
    let retry_options_json = serde_json::json!({
        "auto_retry_for": ["TRANSIENT"],
        "retry_policy": {
            "max_retries": 3,
            "intervals": [1, 1, 1],
            "backoff_strategy": "fixed",
            "jitter": false,
        }
    });
    let retry_options_str = serde_json::to_string(&retry_options_json).unwrap();
    resolve_node_task_options(&mut spec.tasks, &|task_name| {
        if task_name == "e2e_wf_retry_via_registration" {
            Some(retry_options_str.clone())
        } else {
            None
        }
    });

    // Verify the resolution actually set task_options_json on the node.
    assert!(
        spec.tasks[0].task_options_json.is_some(),
        "task_options_json should be set by resolve_node_task_options"
    );

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(30)).await;
    assert_eq!(
        status, "COMPLETED",
        "workflow should complete after retry succeeds (retry config inherited from registration)"
    );

    // Verify the task completed.
    assert_eq!(
        get_workflow_task_status(&pool, &wf_id, 0).await,
        "COMPLETED"
    );

    // Counter file should show at least 2 attempts (first fails with TRANSIENT, second succeeds).
    let final_count: i32 = tokio::fs::read_to_string(&counter_path)
        .await
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        final_count >= 2,
        "should have at least 2 attempts (inherited retry), got {}",
        final_count
    );
}
