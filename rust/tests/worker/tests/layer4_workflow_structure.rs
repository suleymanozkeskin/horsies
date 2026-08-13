#![allow(clippy::unwrap_used)]

//! Layer 4 e2e tests: workflow structure (DAG patterns).
//!
//! Mirrors Python's `tests/e2e/test_layer4_workflow_structure.py`.
//!
//! Run with:
//!   cargo test -p horsies-test-worker --test layer4_workflow_structure -- --test-threads=1

use std::time::Duration;

use serial_test::serial;
use sqlx::PgPool;

use std::sync::Arc;

use horsies::{Horsies, PostgresBroker, WorkflowHandle, WorkflowSpecBuilder};
use horsies_test_support::{
    db,
    e2e::{
        db_poll::{wait_for_task_status, wait_for_workflow_terminal},
        worker::start_worker,
        workflow::{get_workflow_task_status, get_workflow_tasks, wait_for_workflow_completion},
    },
    fixtures,
};
use horsies_test_worker::tasks::{wf_fail, wf_slow_step, wf_step};

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

fn write_prefetch_config() -> tempfile::NamedTempFile {
    use std::io::Write;

    let url = db_url();
    let config = serde_json::json!({
        "queue_mode": "default",
        "broker": {
            "database_url": url,
            "pool_pre_ping": true,
            "pool_size": 5,
            "max_overflow": 5,
            "pool_timeout": 10,
            "pool_recycle": 600,
            "echo": false
        },
        "prefetch_buffer": 2,
        "claim_lease_ms": 5000,
        "max_claim_renew_age_ms": 180000,
        "recovery": {
            "auto_requeue_stale_claimed": true,
            "claimed_stale_threshold_ms": 120000,
            "auto_fail_stale_running": true,
            "running_stale_threshold_ms": 300000,
            "check_interval_ms": 30000,
            "runner_heartbeat_interval_ms": 30000,
            "claimer_heartbeat_interval_ms": 1000
        },
        "resend_on_transient_err": false
    });

    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(serde_json::to_string_pretty(&config).unwrap().as_bytes())
        .unwrap();
    f.flush().unwrap();
    f
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

async fn enqueue_blocker_task(pool: &PgPool, duration_ms: i64) -> String {
    let broker = PostgresBroker::from_pool(pool.clone());
    broker
        .enqueue(
            "e2e_slow",
            None,
            Some(&format!(r#"{{"duration_ms":{duration_ms}}}"#)),
            "default",
            100,
            None,
            None,
            None,
            None,
            &format!("test-{}", uuid::Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap()
        .to_string()
}

// ---------------------------------------------------------------------------
// L4.1: Linear Chain (A → B → C)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_linear_workflow() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_linear");
    let a = b.task(
        wf_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    let bnode = b.task(
        wf_step::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"step":"B"}"#.to_owned())
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
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    let tasks = get_workflow_tasks(&pool, &wf_id).await;
    assert_eq!(tasks.len(), 3);
    for t in &tasks {
        assert_eq!(t.status, "COMPLETED");
    }

    // Verify sequential: B started after A completed, C after B.
    assert!(
        tasks[1].started_at.unwrap() > tasks[0].completed_at.unwrap(),
        "B should start after A completes"
    );
    assert!(
        tasks[2].started_at.unwrap() > tasks[1].completed_at.unwrap(),
        "C should start after B completes"
    );
}

// ---------------------------------------------------------------------------
// L4.2: Fan-Out (root → B, C, D)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_fanout_parallel_execution() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_fanout");
    let root = b.task(
        wf_step::node()
            .unwrap()
            .node_id("root")
            .kwargs(r#"{"step":"root"}"#.to_owned()),
    );
    b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"step":"B","delay_ms":500}"#.to_owned())
            .waits_for(root),
    );
    b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("c")
            .kwargs(r#"{"step":"C","delay_ms":500}"#.to_owned())
            .waits_for(root),
    );
    b.task(
        wf_slow_step::node()
            .unwrap()
            .node_id("d")
            .kwargs(r#"{"step":"D","delay_ms":500}"#.to_owned())
            .waits_for(root),
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

    // Parallel tasks started after root completed.
    let root_completed = tasks[0].completed_at.unwrap();
    for t in &tasks[1..4] {
        assert!(
            t.started_at.unwrap() > root_completed,
            "parallel task should start after root"
        );
    }
}

// ---------------------------------------------------------------------------
// L4.3: Fan-In (A, B, C → AGG)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_fanin_waits_for_all() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_fanin");
    let a = b.task(
        wf_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    let bnode = b.task(
        wf_step::node()
            .unwrap()
            .node_id("b")
            .kwargs(r#"{"step":"B"}"#.to_owned()),
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
            .node_id("agg")
            .kwargs(r#"{"step":"AGG"}"#.to_owned())
            .waits_for(a)
            .waits_for(bnode)
            .waits_for(c),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "COMPLETED");

    let tasks = get_workflow_tasks(&pool, &wf_id).await;
    assert_eq!(tasks.len(), 4);

    let agg = &tasks[3];
    let latest_root = tasks[..3]
        .iter()
        .filter_map(|t| t.completed_at)
        .max()
        .unwrap();
    assert!(
        agg.started_at.unwrap() > latest_root,
        "AGG should start after all roots complete"
    );
}

// ---------------------------------------------------------------------------
// L4.4: Diamond (A → B, C → D)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_diamond_structure() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_diamond");
    let a = b.task(
        wf_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
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
            .waits_for(a),
    );
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("d")
            .kwargs(r#"{"step":"D"}"#.to_owned())
            .waits_for(bnode)
            .waits_for(c),
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

    // D started after both B and C completed.
    let d = &tasks[3];
    let b_completed = tasks[1].completed_at.unwrap();
    let c_completed = tasks[2].completed_at.unwrap();
    let latest = b_completed.max(c_completed);
    assert!(
        d.started_at.unwrap() > latest,
        "D should start after both B and C"
    );
}

// ---------------------------------------------------------------------------
// L4.9: Single Node Workflow
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_single_node_workflow() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_single");
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("only")
            .kwargs(r#"{"step":"only"}"#.to_owned()),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_completion(&pool, &wf_id, Duration::from_secs(10)).await;
    assert_eq!(status, "COMPLETED");

    let tasks = get_workflow_tasks(&pool, &wf_id).await;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].status, "COMPLETED");
}

// ---------------------------------------------------------------------------
// L4.10: Linear Failure Cascades to Skip
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_linear_failure_cascades_to_skip() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_fail_cascade");
    let a = b.task(
        wf_fail::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"error_code":"DELIBERATE"}"#.to_owned()),
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
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(10)).await;
    assert_eq!(status, "FAILED");

    let a_status = get_workflow_task_status(&pool, &wf_id, 0).await;
    let b_status = get_workflow_task_status(&pool, &wf_id, 1).await;
    assert_eq!(a_status, "FAILED");
    assert_eq!(b_status, "SKIPPED", "B should be SKIPPED when A fails");
}

// ---------------------------------------------------------------------------
// L4.11: Handle.get() Returns Error on Failed Workflow
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_workflow_fails_with_error() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_fail_wf");
    b.task(
        wf_fail::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"error_code":"BOOM"}"#.to_owned()),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(10)).await;
    assert_eq!(status, "FAILED");

    // Verify error is stored.
    let error: Option<String> =
        sqlx::query_scalar("SELECT error FROM horsies_workflows WHERE id = $1")
            .bind(&wf_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(error.is_some(), "workflow error should be set");
}

// ---------------------------------------------------------------------------
// L4.12: Fan-Out One Branch Fails
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_fanout_one_branch_fails() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_fanout_fail");
    let root = b.task(
        wf_step::node()
            .unwrap()
            .node_id("root")
            .kwargs(r#"{"step":"root"}"#.to_owned()),
    );
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("ok1")
            .kwargs(r#"{"step":"ok1"}"#.to_owned())
            .waits_for(root),
    );
    b.task(
        wf_fail::node()
            .unwrap()
            .node_id("fail")
            .kwargs(r#"{"error_code":"BRANCH_FAIL"}"#.to_owned())
            .waits_for(root),
    );
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("ok2")
            .kwargs(r#"{"step":"ok2"}"#.to_owned())
            .waits_for(root),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(15)).await;
    assert_eq!(status, "FAILED");

    // Root and ok branches completed, fail branch failed.
    assert_eq!(
        get_workflow_task_status(&pool, &wf_id, 0).await,
        "COMPLETED"
    ); // root
    assert_eq!(
        get_workflow_task_status(&pool, &wf_id, 1).await,
        "COMPLETED"
    ); // ok1
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 2).await, "FAILED"); // fail
    assert_eq!(
        get_workflow_task_status(&pool, &wf_id, 3).await,
        "COMPLETED"
    ); // ok2
}

// ---------------------------------------------------------------------------
// L4.13: Fan-In One Dep Fails → Aggregator Skipped
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_fanin_one_dep_fails_skips_aggregator() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut b = WorkflowSpecBuilder::new("e2e_fanin_fail");
    let a = b.task(
        wf_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
    );
    let fail = b.task(
        wf_fail::node()
            .unwrap()
            .node_id("fail")
            .kwargs(r#"{"error_code":"FAIL"}"#.to_owned()),
    );
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("agg")
            .kwargs(r#"{"step":"AGG"}"#.to_owned())
            .waits_for(a)
            .waits_for(fail),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;
    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(10)).await;
    assert_eq!(status, "FAILED");

    assert_eq!(
        get_workflow_task_status(&pool, &wf_id, 0).await,
        "COMPLETED"
    ); // a
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 1).await, "FAILED"); // fail
    assert_eq!(get_workflow_task_status(&pool, &wf_id, 2).await, "SKIPPED"); // agg
}

// ---------------------------------------------------------------------------
// L4.14: Workflow Cancellation
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_workflow_cancellation() {
    let pool = pool().await;
    db::clean_tables(&pool).await;
    // Build a chain where B is slow — we cancel while B is enqueued/running.
    let mut b = WorkflowSpecBuilder::new("e2e_cancel");
    let a = b.task(
        wf_step::node()
            .unwrap()
            .node_id("a")
            .kwargs(r#"{"step":"A"}"#.to_owned()),
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

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let wf_id = start_wf(&pool, &spec).await;

    // Wait for A to complete.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let s = get_workflow_task_status(&pool, &wf_id, 0).await;
        if s == "COMPLETED" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Cancel the workflow.
    horsies::cancel_workflow(&pool, &wf_id).await.unwrap();

    let status: String = sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
        .bind(&wf_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "CANCELLED");
}

#[tokio::test]
#[serial]
async fn test_workflow_task_expired_while_claimed_before_start() {
    let pool = pool().await;
    db::clean_tables(&pool).await;

    let config_file = write_prefetch_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let _worker = start_worker(
        &config_path,
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(10),
    );

    let blocker_id = enqueue_blocker_task(&pool, 1500).await;

    let mut b = WorkflowSpecBuilder::new("e2e_claimed_expiry");
    b.task(
        wf_step::node()
            .unwrap()
            .node_id("expires")
            .kwargs(r#"{"step":"EXP"}"#.to_owned())
            .good_until(chrono::Utc::now() + chrono::Duration::milliseconds(500)),
    );
    let spec = b.build().unwrap();

    let wf_id = start_wf(&pool, &spec).await;

    let claim_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < claim_deadline {
        let row: (Option<String>, Option<String>) = sqlx::query_as(
            "SELECT wt.task_id, t.status \
             FROM horsies_workflow_tasks wt \
             LEFT JOIN horsies_tasks t ON t.id = wt.task_id \
             WHERE wt.workflow_id = $1 AND wt.task_index = 0",
        )
        .bind(&wf_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        if row.1.as_deref() == Some("CLAIMED") {
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let claimed_status: String = sqlx::query_scalar(
        "SELECT t.status \
         FROM horsies_workflow_tasks wt \
         JOIN horsies_tasks t ON t.id = wt.task_id \
         WHERE wt.workflow_id = $1 AND wt.task_index = 0",
    )
    .bind(&wf_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        claimed_status, "CLAIMED",
        "workflow task should be buffered as CLAIMED before its deadline passes",
    );

    let status = wait_for_workflow_terminal(&pool, &wf_id, Duration::from_secs(20)).await;
    assert_eq!(
        status, "FAILED",
        "workflow should fail on expired root task",
    );

    wait_for_task_status(&pool, &blocker_id, "COMPLETED", Duration::from_secs(15)).await;
    assert_eq!(
        get_workflow_task_status(&pool, &wf_id, 0).await,
        "FAILED",
        "expired task should be reported as failed to the workflow engine",
    );

    let row: (
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
        Option<bool>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT t.status, t.started_at, t.claimed_by_worker_id, t.claimed, t.error_code, wt.status \
         FROM horsies_workflow_tasks wt \
         JOIN horsies_tasks t ON t.id = wt.task_id \
         WHERE wt.workflow_id = $1 AND wt.task_index = 0",
    )
    .bind(&wf_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(row.0, "EXPIRED");
    assert!(
        row.1.is_none(),
        "underlying task should never transition to RUNNING",
    );
    assert!(
        row.2.is_some(),
        "claimed_by_worker_id should be preserved on claimed expiry",
    );
    assert_eq!(row.3, Some(false));
    assert_eq!(row.4.as_deref(), Some("TASK_EXPIRED"));
    assert_eq!(row.5.as_deref(), Some("FAILED"));

    let attempts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) \
         FROM horsies_task_attempts ta \
         JOIN horsies_workflow_tasks wt ON wt.task_id = ta.task_id \
         WHERE wt.workflow_id = $1 AND wt.task_index = 0",
    )
    .bind(&wf_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        attempts, 0,
        "workflow expired-before-start should record no attempts"
    );
}
