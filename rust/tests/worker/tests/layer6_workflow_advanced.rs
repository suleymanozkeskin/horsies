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
    cancel_workflow, pause_workflow, resolve_node_task_options, resume_workflow, Horsies,
    OnError, PostgresBroker, SuccessCase, SuccessPolicy, TaskNode, WorkflowHandle,
    WorkflowSpecBuilder, WorkflowSpecRegistry,
};
use horsies_test_support::{
    db, fixtures,
    e2e::{
        db_poll::{wait_for_task_status, wait_for_workflow_terminal},
        worker::start_worker,
        workflow::{get_workflow_task_status, get_workflow_tasks, wait_for_workflow_completion},
    },
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

fn registry() -> WorkflowSpecRegistry {
    WorkflowSpecRegistry::new()
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

// ---------------------------------------------------------------------------
// T6.2: Join=quorum + ctx gating
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_quorum_ctx_gating() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let reg = registry();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(100ms), B(150ms), C(300ms) → D (join=quorum, min_success=2, ctx from C).
    let mut b = WorkflowSpecBuilder::new("e2e_quorum_ctx");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("a")
            .kwargs(r#"{"step":"A","delay_ms":100}"#.to_owned()),
    );
    let bnode = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("b")
            .kwargs(r#"{"step":"B","delay_ms":150}"#.to_owned()),
    );
    let c = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("c")
            .kwargs(r#"{"step":"C","delay_ms":300}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_ctx_reader")
            .node_id("d")
            .waits_for(a)
            .waits_for(bnode)
            .waits_for(c)
            .workflow_ctx_from(vec!["c".to_owned()])
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
    let reg = registry();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(fail), B(fail), C(ok) → D (quorum min_success=3, impossible).
    let mut b = WorkflowSpecBuilder::new("e2e_quorum_impossible");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_fail")
            .node_id("a")
            .kwargs(r#"{"error_code":"F1"}"#.to_owned()),
    );
    let bnode = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_fail")
            .node_id("b")
            .kwargs(r#"{"error_code":"F2"}"#.to_owned()),
    );
    let c = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
            .node_id("c")
            .kwargs(r#"{"step":"C"}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
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

// ---------------------------------------------------------------------------
// T6.2c: Join=any, all deps fail → SKIPPED
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_join_any_all_deps_fail_skips() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let reg = registry();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_any_all_fail");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_fail")
            .node_id("a")
            .kwargs(r#"{"error_code":"F1"}"#.to_owned()),
    );
    let bnode = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_fail")
            .node_id("b")
            .kwargs(r#"{"error_code":"F2"}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
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
    let reg = registry();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(100ms) → B(200ms) → C(200ms) → D(100ms)
    let mut b = WorkflowSpecBuilder::new("e2e_pausable");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("a")
            .kwargs(r#"{"step":"A","delay_ms":100}"#.to_owned()),
    );
    let bnode = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("b")
            .kwargs(r#"{"step":"B","delay_ms":200}"#.to_owned())
            .waits_for(a),
    );
    let c = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("c")
            .kwargs(r#"{"step":"C","delay_ms":200}"#.to_owned())
            .waits_for(bnode),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
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
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("a")
            .kwargs(r#"{"step":"A","delay_ms":100}"#.to_owned()),
    );
    let bnode = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("b")
            .kwargs(r#"{"step":"B","delay_ms":200}"#.to_owned())
            .waits_for(a),
    );
    let c = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("c")
            .kwargs(r#"{"step":"C","delay_ms":200}"#.to_owned())
            .waits_for(bnode),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
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
    let reg = registry();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_pause_cancel");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("a")
            .kwargs(r#"{"step":"A","delay_ms":100}"#.to_owned()),
    );
    let bnode = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("b")
            .kwargs(r#"{"step":"B","delay_ms":5000}"#.to_owned())
            .waits_for(a),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
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
    let reg = registry();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(ok), B(fail). Success policy: A required, B optional → COMPLETED.
    let mut b = WorkflowSpecBuilder::new("e2e_sp_satisfied");
    let _a_ref = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    let _b_ref = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_fail")
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
    let reg = registry();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(ok), B(fail). Policy: both required → FAILED.
    let mut b = WorkflowSpecBuilder::new("e2e_sp_not_met");
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_fail")
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
    let reg = registry();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(fail), B(ok). Case1 requires A, Case2 requires B → Case2 satisfied → COMPLETED.
    let mut b = WorkflowSpecBuilder::new("e2e_sp_multi");
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_fail")
            .node_id("a")
            .kwargs(r#"{"error_code":"F"}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
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
    let reg = registry();

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
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    let bnode = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_fail")
            .node_id("b")
            .kwargs(r#"{"error_code":"PAUSE_FAIL"}"#.to_owned())
            .waits_for(a),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
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
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    let bnode = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_fail")
            .node_id("b")
            .kwargs(r#"{"error_code":"PAUSE_FAIL"}"#.to_owned())
            .waits_for(a),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
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
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
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
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_fail")
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
// T6.4c: Pause idempotent — second pause returns false
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_pause_idempotent() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let reg = registry();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(100ms) → B(200ms) → C(200ms) → D(100ms)
    let mut b = WorkflowSpecBuilder::new("e2e_pause_idempotent");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("a")
            .kwargs(r#"{"step":"A","delay_ms":100}"#.to_owned()),
    );
    let bnode = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("b")
            .kwargs(r#"{"step":"B","delay_ms":200}"#.to_owned())
            .waits_for(a),
    );
    let c = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("c")
            .kwargs(r#"{"step":"C","delay_ms":200}"#.to_owned())
            .waits_for(bnode),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
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
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("a")
            .kwargs(r#"{"step":"A","delay_ms":100}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
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
    let reg = registry();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // A(ok), B(fail with REQUIRED_FAIL). Policy: both required → FAILED.
    let mut b = WorkflowSpecBuilder::new("e2e_sp_error_content");
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_step")
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_fail")
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
    let reg = registry();

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
        TaskNode::<serde_json::Value>::new("e2e_wf_retry_then_ok")
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
    let reg = registry();

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
        TaskNode::<serde_json::Value>::new("e2e_wf_retry_then_ok")
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
    let reg = registry();

    let config_file = write_recovery_custom_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    // DAG: A(50ms) → B(50ms) → C(60s)  ──→ E(50ms, recovery queue) → F(50ms, recovery queue)
    //                         → D(60s)  ──/
    //
    // Kill worker after A,B complete and C,D are RUNNING.
    // Restart worker → reaper marks C,D FAILED → E runs (allow_failed_deps) → F runs → FAILED.
    let mut b = WorkflowSpecBuilder::new("e2e_recovery_crash");
    let a = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("a")
            .kwargs(r#"{"step":"A","delay_ms":50}"#.to_owned()),
    );
    let bnode = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("b")
            .kwargs(r#"{"step":"B","delay_ms":50}"#.to_owned())
            .waits_for(a),
    );
    let c = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("c")
            .kwargs(r#"{"step":"C","delay_ms":60000}"#.to_owned())
            .waits_for(bnode),
    );
    let d = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("d")
            .kwargs(r#"{"step":"D","delay_ms":60000}"#.to_owned())
            .waits_for(bnode),
    );
    let e = b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
            .node_id("e")
            .kwargs(r#"{"step":"E","delay_ms":50}"#.to_owned())
            .waits_for(c)
            .waits_for(d)
            .allow_failed_deps(true)
            .queue("recovery"),
    );
    b.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_slow_step")
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

    let wf_id = start_wf(&pool, &spec).await;

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
    let reg = registry();

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
        TaskNode::<serde_json::Value>::new("e2e_wf_retry_via_registration")
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
