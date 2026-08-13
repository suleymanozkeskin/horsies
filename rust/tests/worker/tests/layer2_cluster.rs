#![allow(clippy::unwrap_used)]

//! Layer 2 e2e tests: Multi-Worker / Cluster behaviors.
//!
//! Mirrors Python's `tests/e2e/test_layer2_cluster.py`.
//!
//! Run with:
//!   cargo test -p horsies-test-worker --test layer2_cluster -- --test-threads=1

use std::io::Write;
use std::time::Duration;

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use horsies::{PostgresBroker, TaskResult};
use horsies_test_support::{
    db,
    e2e::{
        db_poll::{wait_for_all_terminal, wait_for_task_status},
        worker::start_worker,
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

async fn broker() -> PostgresBroker {
    let url = db::db_url();
    let b = PostgresBroker::connect(&url).await.unwrap();
    b.migrate().await.unwrap();
    b
}

fn db_url() -> String {
    db::db_url()
}

async fn enqueue_task(broker: &PostgresBroker, task_name: &str, kwargs: &str) -> Uuid {
    broker
        .enqueue(
            task_name,
            None,
            Some(kwargs),
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
}

async fn enqueue_no_args(broker: &PostgresBroker, task_name: &str) -> Uuid {
    enqueue_task(broker, task_name, "{}").await
}

/// Enqueue a task with retry policy in task_options.
async fn enqueue_with_retry(
    broker: &PostgresBroker,
    task_name: &str,
    kwargs: &str,
    max_retries: i32,
    intervals: &[i32],
    auto_retry_for: &[&str],
) -> Uuid {
    let task_options = serde_json::json!({
        "retry_policy": {
            "max_retries": max_retries,
            "intervals": intervals,
            "backoff_strategy": "fixed",
            "jitter": false,
            "auto_retry_for": auto_retry_for,
        }
    });
    let task_options_str = serde_json::to_string(&task_options).unwrap();

    broker
        .enqueue(
            task_name,
            None,
            Some(kwargs),
            "default",
            100,
            None,
            None,
            None,
            Some(&task_options_str),
            &format!("test-{}", uuid::Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap()
}

/// Write a fast-recovery AppConfig to a temp JSON file. Returns the path.
fn write_fast_recovery_config() -> tempfile::NamedTempFile {
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
        "prefetch_buffer": 0,
        "max_claim_renew_age_ms": 180000,
        "recovery": {
            "auto_requeue_stale_claimed": true,
            "claimed_stale_threshold_ms": 3000,
            "auto_fail_stale_running": true,
            "running_stale_threshold_ms": 3000,
            "check_interval_ms": 1000,
            "runner_heartbeat_interval_ms": 1000,
            "claimer_heartbeat_interval_ms": 1000
        },
        "retention": {
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

// ---------------------------------------------------------------------------
// L2.1: Multi-Worker Distribution
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_multi_worker_distribution() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    // Start 2 workers.
    let _w1 = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );
    let _w2 = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // Enqueue 10 slow tasks to force overlap.
    let mut task_ids = Vec::new();
    for _ in 0..10 {
        let id = enqueue_task(&broker, "e2e_slow", r#"{"duration_ms": 500}"#).await;
        task_ids.push(id);
    }

    wait_for_all_terminal(&pool, &task_ids, Duration::from_secs(30)).await;

    // Verify at least 2 distinct workers claimed tasks.
    let worker_ids: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT claimed_by_worker_id FROM horsies_tasks \
         WHERE id = ANY($1) AND claimed_by_worker_id IS NOT NULL",
    )
    .bind(&task_ids)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert!(
        worker_ids.len() >= 2,
        "expected at least 2 distinct worker IDs, got {}: {:?}",
        worker_ids.len(),
        worker_ids,
    );

    // Verify all completed.
    for task_id in &task_ids {
        let status: String = sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "COMPLETED", "task {} should be COMPLETED", task_id);
    }
}

// ---------------------------------------------------------------------------
// L2.5: No Double Execution
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_multi_worker_no_double_execution() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _w1 = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );
    let _w2 = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // Enqueue 20 fast tasks.
    let mut task_ids = Vec::new();
    for i in 0..20 {
        let id = enqueue_task(&broker, "e2e_simple", &format!(r#"{{"x": {}}}"#, i)).await;
        task_ids.push(id);
    }

    wait_for_all_terminal(&pool, &task_ids, Duration::from_secs(30)).await;

    // Verify each task has exactly one completion — no RUNNING or CLAIMED leftovers.
    let non_terminal: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM horsies_tasks WHERE id = ANY($1) AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')",
    )
    .bind(&task_ids)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(non_terminal, 0, "all tasks should be in terminal state");

    // Verify all completed (not failed).
    let completed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM horsies_tasks WHERE id = ANY($1) AND status = 'COMPLETED'",
    )
    .bind(&task_ids)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(completed, 20, "all 20 tasks should be COMPLETED");
}

// ---------------------------------------------------------------------------
// L2.6: Stale RUNNING → FAILED on Crash
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_stale_running_marked_failed_on_crash() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let config_file = write_fast_recovery_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    // Start a worker with fast recovery (3s stale threshold).
    let mut worker = start_worker(
        &config_path,
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(10),
    );

    // Enqueue a 60s task (will be RUNNING when we kill the worker).
    let task_id = enqueue_task(&broker, "e2e_slow", r#"{"duration_ms": 60000}"#).await;
    wait_for_task_status(&pool, &task_id, "RUNNING", Duration::from_secs(15)).await;

    // Kill the worker hard.
    worker.kill();

    // Start a NEW worker with same fast recovery — its reaper detects the stale task.
    let _w2 = start_worker(
        &config_path,
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(10),
    );

    // Wait for task to be marked FAILED by the reaper (within ~3s + check_interval).
    wait_for_task_status(&pool, &task_id, "FAILED", Duration::from_secs(15)).await;

    // Verify the error code is WORKER_CRASHED.
    let result_json: Option<String> =
        sqlx::query_scalar("SELECT result FROM horsies_tasks WHERE id = $1")
            .bind(&task_id)
            .fetch_one(&pool)
            .await
            .unwrap();

    let result: TaskResult<serde_json::Value> =
        serde_json::from_str(&result_json.unwrap()).unwrap();
    assert!(result.is_err());

    let err = result.unwrap_err();
    let code_str = err
        .error_code
        .as_ref()
        .map(|c| c.to_string())
        .unwrap_or_default();
    assert_eq!(
        code_str, "WORKER_CRASHED",
        "expected WORKER_CRASHED error code"
    );
}

// ---------------------------------------------------------------------------
// L2.8: Cluster Cap Not Leaked on Failure
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_cluster_cap_not_leaked_on_failure() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // Enqueue tasks: mix of success and failure.
    let mut task_ids = Vec::new();
    for i in 0..6 {
        let id = if i % 2 == 0 {
            enqueue_task(&broker, "e2e_simple", &format!(r#"{{"x": {}}}"#, i)).await
        } else {
            enqueue_no_args(&broker, "e2e_error").await
        };
        task_ids.push(id);
    }

    wait_for_all_terminal(&pool, &task_ids, Duration::from_secs(15)).await;

    // After all tasks complete/fail, in-flight count should be 0.
    let in_flight: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM horsies_tasks WHERE status IN ('RUNNING', 'CLAIMED')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        in_flight, 0,
        "in-flight count should be 0 after all tasks finish"
    );
}

// ---------------------------------------------------------------------------
// L2.9: Retry Works Across Multiple Workers
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_retry_works_across_multiple_workers() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _w1 = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );
    let _w2 = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // Enqueue several tasks with a retry policy (3 retries at 1s intervals for
    // TRANSIENT). Multiple tasks across two workers make participation robust.
    let mut task_ids = Vec::new();
    for _ in 0..8 {
        let id = enqueue_with_retry(
            &broker,
            "e2e_retry_exhausted",
            "{}",
            3,
            &[1, 1, 1],
            &["TRANSIENT"],
        )
        .await;
        task_ids.push(id);
    }

    // Wait for all to reach terminal (each retries 3 times then fails).
    wait_for_all_terminal(&pool, &task_ids, Duration::from_secs(40)).await;

    // Each task: exactly 3 retries, and exactly 4 recorded attempts
    // (initial attempt + 3 retries). Assert via the immutable
    // horsies_task_attempts history rather than the mutable task-row owner
    // (parity with horsies PR #38).
    for task_id in &task_ids {
        let retry_count: i32 =
            sqlx::query_scalar("SELECT retry_count FROM horsies_tasks WHERE id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            retry_count, 3,
            "task {task_id}: expected 3 retries before final failure",
        );

        let attempt_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM horsies_task_attempts WHERE task_id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            attempt_count, 4,
            "task {task_id}: expected 4 attempts (initial + 3 retries)",
        );
    }

    // Prove multiple workers participated, using the attempt history's worker_id
    // (the task row's claimed_by_worker_id is only the last owner).
    let worker_ids: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT worker_id FROM horsies_task_attempts \
         WHERE task_id = ANY($1) AND worker_id IS NOT NULL",
    )
    .bind(&task_ids)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert!(
        worker_ids.len() >= 2,
        "expected at least 2 distinct workers across attempts, got {}: {:?}",
        worker_ids.len(),
        worker_ids,
    );
}

// ---------------------------------------------------------------------------
// L2.10: Concurrent Enqueue During Processing
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_concurrent_enqueue_during_processing() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // First batch.
    let mut batch1_ids = Vec::new();
    for _ in 0..5 {
        let id = enqueue_task(&broker, "e2e_slow", r#"{"duration_ms": 200}"#).await;
        batch1_ids.push(id);
    }

    // Wait a bit, then enqueue second batch while first is processing.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut batch2_ids = Vec::new();
    for i in 0..5 {
        let id = enqueue_task(&broker, "e2e_simple", &format!(r#"{{"x": {}}}"#, i)).await;
        batch2_ids.push(id);
    }

    let mut all_ids = batch1_ids.clone();
    all_ids.extend(batch2_ids.clone());

    wait_for_all_terminal(&pool, &all_ids, Duration::from_secs(30)).await;

    // All should be completed.
    let completed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM horsies_tasks WHERE id = ANY($1) AND status = 'COMPLETED'",
    )
    .bind(&all_ids)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(completed, 10, "all 10 tasks should be COMPLETED");
}

// ---------------------------------------------------------------------------
// L2.2: Cluster-Wide Cap Enforcement
// ---------------------------------------------------------------------------

/// Write a cluster_wide_cap=2 config.
fn write_cluster_cap_config() -> tempfile::NamedTempFile {
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
        "cluster_wide_cap": 2,
        "prefetch_buffer": 0,
        "max_claim_renew_age_ms": 180000,
        "recovery": {
            "auto_requeue_stale_claimed": true,
            "claimed_stale_threshold_ms": 120000,
            "auto_fail_stale_running": true,
            "running_stale_threshold_ms": 300000,
            "check_interval_ms": 30000,
            "runner_heartbeat_interval_ms": 30000,
            "claimer_heartbeat_interval_ms": 30000
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
async fn test_cluster_wide_cap() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let config_file = write_cluster_cap_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let _w1 = start_worker(
        &config_path,
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );
    let _w2 = start_worker(
        &config_path,
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // Enqueue 6 slow tasks.
    let mut task_ids = Vec::new();
    for _ in 0..6 {
        let id = enqueue_task(&broker, "e2e_slow", r#"{"duration_ms": 500}"#).await;
        task_ids.push(id);
    }

    // Poll DB during execution to find max RUNNING count.
    let poll_end = tokio::time::Instant::now() + Duration::from_secs(4);
    let mut max_running: i64 = 0;
    while tokio::time::Instant::now() < poll_end {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM horsies_tasks WHERE status = 'RUNNING'")
                .fetch_one(&pool)
                .await
                .unwrap();
        max_running = max_running.max(count);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    wait_for_all_terminal(&pool, &task_ids, Duration::from_secs(30)).await;

    assert!(
        max_running > 0,
        "polling observed 0 RUNNING tasks — test may be vacuous"
    );
    assert!(
        max_running <= 2,
        "RUNNING count {} exceeded cluster_wide_cap 2",
        max_running,
    );

    // Verify all completed.
    let completed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM horsies_tasks WHERE id = ANY($1) AND status = 'COMPLETED'",
    )
    .bind(&task_ids)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(completed, 6, "all 6 tasks should be COMPLETED");
}

// ---------------------------------------------------------------------------
// L2.7: Stale CLAIMED → Requeued → COMPLETED
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_stale_claimed_requeued_and_completed() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let config_file = write_fast_recovery_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let dead_worker_id = "dead_worker_00000000";

    // Enqueue a simple task.
    let task_id = enqueue_no_args(&broker, "e2e_healthcheck").await;

    // Manually set to CLAIMED with stale timestamp (simulates dead worker).
    sqlx::query(
        "UPDATE horsies_tasks SET status = 'CLAIMED', claimed = TRUE, \
         claimed_at = NOW() - INTERVAL '10 seconds', \
         claimed_by_worker_id = $2 WHERE id = $1",
    )
    .bind(&task_id)
    .bind(dead_worker_id)
    .execute(&pool)
    .await
    .unwrap();

    // Start a worker with fast recovery — reaper detects stale CLAIMED and requeues.
    let _worker = start_worker(
        &config_path,
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(10),
    );

    wait_for_task_status(&pool, &task_id, "COMPLETED", Duration::from_secs(15)).await;

    // Verify reclaimed by a different worker.
    let new_owner: Option<String> =
        sqlx::query_scalar("SELECT claimed_by_worker_id FROM horsies_tasks WHERE id = $1")
            .bind(&task_id)
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_ne!(
        new_owner.as_deref(),
        Some(dead_worker_id),
        "task should have been reclaimed by a live worker",
    );
}

// ---------------------------------------------------------------------------
// L2.12: Single Worker Crash — Remaining Continue
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_single_worker_crash_remaining_continue() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let mut w1 = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );
    let _w2 = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );
    let _w3 = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // Enqueue 20 slow tasks.
    let mut task_ids = Vec::new();
    for _ in 0..20 {
        let id = enqueue_task(&broker, "e2e_slow", r#"{"duration_ms": 300}"#).await;
        task_ids.push(id);
    }

    // Wait until at least one task is RUNNING.
    let poll_end = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < poll_end {
        let running: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_tasks WHERE id = ANY($1) AND status = 'RUNNING'",
        )
        .bind(&task_ids)
        .fetch_one(&pool)
        .await
        .unwrap();
        if running > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Kill worker 1.
    w1.kill();

    // Give remaining workers time to process.
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Query completed tasks.
    let completed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM horsies_tasks WHERE id = ANY($1) AND status = 'COMPLETED'",
    )
    .bind(&task_ids)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert!(
        completed >= 15,
        "expected at least 15 completed tasks, got {}",
        completed,
    );

    // At least 2 distinct workers contributed.
    let worker_ids: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT claimed_by_worker_id FROM horsies_tasks \
         WHERE id = ANY($1) AND status = 'COMPLETED' AND claimed_by_worker_id IS NOT NULL",
    )
    .bind(&task_ids)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert!(
        worker_ids.len() >= 2,
        "expected at least 2 distinct workers among completed, got {}: {:?}",
        worker_ids.len(),
        worker_ids,
    );
}

// ---------------------------------------------------------------------------
// Config helpers for remaining tests
// ---------------------------------------------------------------------------

fn write_custom_queues_config() -> tempfile::NamedTempFile {
    let url = db_url();
    let config = serde_json::json!({
        "queue_mode": "custom",
        "custom_queues": [
            {"name": "high", "priority": 1, "max_concurrency": 5},
            {"name": "normal", "priority": 50, "max_concurrency": 10},
            {"name": "low", "priority": 100, "max_concurrency": 20}
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
        "resend_on_transient_err": false
    });
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(serde_json::to_string_pretty(&config).unwrap().as_bytes())
        .unwrap();
    f.flush().unwrap();
    f
}

fn write_softcap_config() -> tempfile::NamedTempFile {
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
        "claim_lease_ms": 2000,
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

/// Enqueue a task to a specific queue.
async fn enqueue_to_queue(
    broker: &PostgresBroker,
    task_name: &str,
    kwargs: &str,
    queue_name: &str,
) -> Uuid {
    broker
        .enqueue(
            task_name,
            None,
            Some(kwargs),
            queue_name,
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
}

// ---------------------------------------------------------------------------
// L2.3: Per-Queue Concurrency Cap
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_per_queue_concurrency_cap() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let config_file = write_custom_queues_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    // high queue has max_concurrency=5
    let _worker = start_worker(
        &config_path,
        &["--concurrency", "8", "--queues", "high,normal,low"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut task_ids = Vec::new();
    for _ in 0..10 {
        let id = enqueue_to_queue(
            &broker,
            "e2e_custom_slow",
            r#"{"duration_ms": 400}"#,
            "high",
        )
        .await;
        task_ids.push(id);
    }

    // Poll max RUNNING in high queue.
    let poll_end = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut max_running: i64 = 0;
    while tokio::time::Instant::now() < poll_end {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_tasks WHERE status = 'RUNNING' AND queue_name = 'high'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        max_running = max_running.max(count);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    wait_for_all_terminal(&pool, &task_ids, Duration::from_secs(30)).await;

    assert!(
        max_running > 0,
        "polling observed 0 RUNNING tasks — vacuous"
    );
    assert!(
        max_running <= 5,
        "RUNNING count {} in high queue exceeded max_concurrency 5",
        max_running,
    );

    let completed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM horsies_tasks WHERE id = ANY($1) AND status = 'COMPLETED'",
    )
    .bind(&task_ids)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(completed, 10);
}

// ---------------------------------------------------------------------------
// L2.10: Per-Queue Caps Independent Across Queues
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_per_queue_caps_independent() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let config_file = write_custom_queues_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let _worker = start_worker(
        &config_path,
        &["--concurrency", "8", "--queues", "high,normal,low"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut high_ids = Vec::new();
    let mut normal_ids = Vec::new();
    for _ in 0..10 {
        high_ids.push(
            enqueue_to_queue(
                &broker,
                "e2e_custom_slow",
                r#"{"duration_ms": 400}"#,
                "high",
            )
            .await,
        );
        normal_ids.push(
            enqueue_to_queue(
                &broker,
                "e2e_custom_slow",
                r#"{"duration_ms": 400}"#,
                "normal",
            )
            .await,
        );
    }

    let poll_end = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut max_high: i64 = 0;
    let mut max_normal: i64 = 0;
    while tokio::time::Instant::now() < poll_end {
        let h: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_tasks WHERE status = 'RUNNING' AND queue_name = 'high'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_tasks WHERE status = 'RUNNING' AND queue_name = 'normal'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        max_high = max_high.max(h);
        max_normal = max_normal.max(n);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let mut all_ids = high_ids.clone();
    all_ids.extend(normal_ids.clone());
    wait_for_all_terminal(&pool, &all_ids, Duration::from_secs(30)).await;

    assert!(max_high > 0, "0 RUNNING in high queue");
    assert!(max_normal > 0, "0 RUNNING in normal queue");
    assert!(
        max_high <= 5,
        "high queue RUNNING {} exceeded cap 5",
        max_high
    );
    assert!(
        max_normal <= 10,
        "normal queue RUNNING {} exceeded cap 10",
        max_normal
    );
}

// ---------------------------------------------------------------------------
// L2.11: Queue Priority Ordering
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_queue_priority_ordering() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    // Enqueue low-priority tasks FIRST.
    let mut low_ids = Vec::new();
    for _ in 0..5 {
        low_ids.push(
            enqueue_to_queue(&broker, "e2e_custom_slow", r#"{"duration_ms": 100}"#, "low").await,
        );
    }

    // Enqueue high-priority tasks SECOND.
    let mut high_ids = Vec::new();
    for _ in 0..5 {
        high_ids.push(
            enqueue_to_queue(
                &broker,
                "e2e_custom_slow",
                r#"{"duration_ms": 100}"#,
                "high",
            )
            .await,
        );
    }

    let mut all_ids = low_ids.clone();
    all_ids.extend(high_ids.clone());

    let config_file = write_custom_queues_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    // Start worker AFTER all tasks enqueued — 1 concurrency for serial execution.
    let _worker = start_worker(
        &config_path,
        &["--concurrency", "1", "--queues", "high,normal,low"],
        "worker started",
        Duration::from_secs(10),
    );

    wait_for_all_terminal(&pool, &all_ids, Duration::from_secs(30)).await;

    // Query completion order.
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, queue_name FROM horsies_tasks WHERE id = ANY($1) \
         ORDER BY completed_at ASC",
    )
    .bind(&all_ids)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(rows.len(), 10);

    // First 5 completed should be high-priority (priority=1 < priority=100).
    let high_in_first_5 = rows[..5].iter().filter(|(_, q)| q == "high").count();
    assert!(
        high_in_first_5 >= 4,
        "expected at least 4 high-priority tasks in first 5 completions, got {}. Order: {:?}",
        high_in_first_5,
        rows.iter()
            .map(|(id, q)| (&id[..8], q.as_str()))
            .collect::<Vec<_>>(),
    );
}

// ---------------------------------------------------------------------------
// L2.14: Softcap Lease — No Double Execution
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_softcap_lease_no_double_execution() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let config_file = write_softcap_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let log_dir = tempfile::tempdir().unwrap();
    std::env::set_var("E2E_IDEMPOTENT_LOG_DIR", log_dir.path().to_str().unwrap());

    let _w1 = start_worker(
        &config_path,
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(10),
    );
    let _w2 = start_worker(
        &config_path,
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(10),
    );

    let mut task_ids = Vec::new();
    for i in 0..6 {
        let kwargs =
            serde_json::json!({"token": format!("softcap_task_{}", i), "duration_ms": 1500});
        let id = enqueue_task(&broker, "e2e_softcap_slow_idempotent", &kwargs.to_string()).await;
        task_ids.push(id);
    }

    wait_for_all_terminal(&pool, &task_ids, Duration::from_secs(60)).await;

    // All should succeed (no DOUBLE_EXECUTION).
    let completed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM horsies_tasks WHERE id = ANY($1) AND status = 'COMPLETED'",
    )
    .bind(&task_ids)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(completed, 6, "all 6 softcap tasks should be COMPLETED");

    // Each token file should exist exactly once.
    let file_count = std::fs::read_dir(log_dir.path()).unwrap().count();
    assert_eq!(file_count, 6, "expected 6 token files, got {}", file_count);

    std::env::remove_var("E2E_IDEMPOTENT_LOG_DIR");
}

// ---------------------------------------------------------------------------
// L2.15: Softcap Expired Claim Requeued
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_softcap_expired_claim_requeued() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let config_file = write_softcap_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let log_dir = tempfile::tempdir().unwrap();
    std::env::set_var("E2E_IDEMPOTENT_LOG_DIR", log_dir.path().to_str().unwrap());

    let dead_worker_id = "dead_softcap_worker";
    let token = "softcap_expired_claim";

    let kwargs = serde_json::json!({"token": token, "duration_ms": 100});
    let task_id = enqueue_task(&broker, "e2e_softcap_slow_idempotent", &kwargs.to_string()).await;

    // Manually set to CLAIMED with expired lease (simulates dead worker).
    sqlx::query(
        "UPDATE horsies_tasks SET status = 'CLAIMED', claimed = TRUE, \
         claimed_at = NOW() - INTERVAL '10 seconds', \
         claimed_by_worker_id = $2, \
         claim_expires_at = NOW() - INTERVAL '5 seconds' \
         WHERE id = $1",
    )
    .bind(&task_id)
    .bind(dead_worker_id)
    .execute(&pool)
    .await
    .unwrap();

    // Start worker — claim loop picks up expired claim.
    let _worker = start_worker(
        &config_path,
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(10),
    );

    wait_for_task_status(&pool, &task_id, "COMPLETED", Duration::from_secs(15)).await;

    // Verify reclaimed by live worker.
    let new_owner: Option<String> =
        sqlx::query_scalar("SELECT claimed_by_worker_id FROM horsies_tasks WHERE id = $1")
            .bind(&task_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_ne!(
        new_owner.as_deref(),
        Some(dead_worker_id),
        "task should be reclaimed"
    );

    // Token file exists exactly once.
    let token_file = log_dir.path().join(token);
    assert!(token_file.exists(), "token file should exist");
    let file_count = std::fs::read_dir(log_dir.path()).unwrap().count();
    assert_eq!(file_count, 1, "expected exactly 1 token file");

    std::env::remove_var("E2E_IDEMPOTENT_LOG_DIR");
}

// ---------------------------------------------------------------------------
// L2.16: Softcap DB Ledger Race — Single Execution Proof
// ---------------------------------------------------------------------------

/// Helper: create and clear DB-ledger tables for the race test.
async fn prepare_execution_ledger_tables(pool: &PgPool) {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS e2e_execution_attempts (
            id BIGSERIAL PRIMARY KEY,
            token TEXT NOT NULL,
            worker_identity TEXT NOT NULL,
            worker_pid INTEGER NOT NULL,
            entered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
    )
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS e2e_execution_winner (
            token TEXT PRIMARY KEY,
            worker_identity TEXT NOT NULL,
            worker_pid INTEGER NOT NULL,
            won_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
    )
    .execute(pool)
    .await
    .unwrap();

    sqlx::query("TRUNCATE e2e_execution_attempts, e2e_execution_winner")
        .execute(pool)
        .await
        .unwrap();
}

/// Helper: wait until task is CLAIMED and return claimed_by_worker_id.
async fn wait_for_claimed_owner(pool: &PgPool, task_id: &Uuid, timeout: Duration) -> String {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        let row: Option<(String, Option<String>)> =
            sqlx::query_as("SELECT status, claimed_by_worker_id FROM horsies_tasks WHERE id = $1")
                .bind(task_id)
                .fetch_optional(pool)
                .await
                .unwrap();

        if let Some((status, Some(owner))) = row {
            if status == "CLAIMED" {
                return owner;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "task {} was not CLAIMED within {}s",
        task_id,
        timeout.as_secs()
    );
}

#[tokio::test]
#[serial]
async fn test_softcap_db_ledger_race_single_execution() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;
    prepare_execution_ledger_tables(&pool).await;

    let config_file = write_softcap_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let blocker_duration_ms = 4000;
    let token = "softcap_db_ledger_race_token";

    let _w1 = start_worker(
        &config_path,
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(20),
    );

    let blocker_id = enqueue_task(
        &broker,
        "e2e_softcap_blocker",
        &format!(r#"{{"duration_ms": {}}}"#, blocker_duration_ms),
    )
    .await;
    wait_for_task_status(&pool, &blocker_id, "RUNNING", Duration::from_secs(10)).await;

    let ledger_id = enqueue_task(
        &broker,
        "e2e_softcap_db_ledger",
        &format!(r#"{{"token": "{}"}}"#, token),
    )
    .await;

    let _first_owner = wait_for_claimed_owner(&pool, &ledger_id, Duration::from_secs(10)).await;
    tokio::time::sleep(Duration::from_millis(2500)).await;

    let _w2 = start_worker(
        &config_path,
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(20),
    );

    wait_for_all_terminal(&pool, &[blocker_id, ledger_id], Duration::from_secs(40)).await;

    let attempts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM e2e_execution_attempts WHERE token = $1")
            .bind(token)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(attempts, 1, "expected exactly 1 body-entry attempt");

    let winners: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM e2e_execution_winner WHERE token = $1")
            .bind(token)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(winners, 1, "expected exactly 1 winner row");
}

// ---------------------------------------------------------------------------
// L2.17: Softcap Owner Transition After Worker Crash
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_softcap_owner_transition_after_worker_crash() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let config_file = write_softcap_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let log_dir = tempfile::tempdir().unwrap();
    std::env::set_var("E2E_IDEMPOTENT_LOG_DIR", log_dir.path().to_str().unwrap());

    let blocker_duration_ms = 6000;
    let token = "softcap_owner_transition";

    let mut w1 = start_worker(
        &config_path,
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(20),
    );

    let blocker_id = enqueue_task(
        &broker,
        "e2e_softcap_blocker",
        &format!(r#"{{"duration_ms": {}}}"#, blocker_duration_ms),
    )
    .await;
    wait_for_task_status(&pool, &blocker_id, "RUNNING", Duration::from_secs(10)).await;

    let kwargs = serde_json::json!({"token": token, "duration_ms": 100});
    let task_id = enqueue_task(&broker, "e2e_softcap_slow_idempotent", &kwargs.to_string()).await;

    let first_owner = wait_for_claimed_owner(&pool, &task_id, Duration::from_secs(10)).await;
    tokio::time::sleep(Duration::from_millis(2500)).await;

    w1.kill();

    let _w2 = start_worker(
        &config_path,
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(20),
    );

    wait_for_task_status(&pool, &task_id, "COMPLETED", Duration::from_secs(30)).await;

    let final_owner: Option<String> =
        sqlx::query_scalar("SELECT claimed_by_worker_id FROM horsies_tasks WHERE id = $1")
            .bind(&task_id)
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_ne!(
        final_owner.as_deref(),
        Some(first_owner.as_str()),
        "expected owner transition after crash",
    );

    let token_file = log_dir.path().join(token);
    assert!(token_file.exists(), "token file should exist");
    let file_count = std::fs::read_dir(log_dir.path()).unwrap().count();
    assert_eq!(file_count, 1, "expected exactly 1 token file");

    std::env::remove_var("E2E_IDEMPOTENT_LOG_DIR");
}

// ---------------------------------------------------------------------------
// L2.18: Requeue DB Error — Age Guard Recovery
// ---------------------------------------------------------------------------

/// Write a requeue-guard config: short claim lease (2s) and short max_claim_renew_age (5s).
fn write_requeue_guard_config() -> tempfile::NamedTempFile {
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
        "prefetch_buffer": 0,
        "claim_lease_ms": 2000,
        "max_claim_renew_age_ms": 5000,
        "recovery": {
            "auto_requeue_stale_claimed": true,
            "claimed_stale_threshold_ms": 3000,
            "auto_fail_stale_running": true,
            "running_stale_threshold_ms": 3000,
            "check_interval_ms": 1000,
            "runner_heartbeat_interval_ms": 1000,
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

#[tokio::test]
#[serial]
async fn test_requeue_db_error_age_guard_recovery() {
    // Simplified version: simulate a stuck CLAIMED task that would result from
    // a dispatch failure + requeue DB error. The age guard stops lease renewal
    // after max_claim_renew_age_ms (5s), then the claim_lease_ms (2s) expires,
    // and a normal worker reclaims the task.
    //
    // We simulate step 1-4 by manually setting the task to CLAIMED with a
    // lease and a stale claimed_at that will exceed the age guard.

    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let log_dir = tempfile::tempdir().unwrap();
    std::env::set_var(
        "E2E_REQUEUE_GUARD_LOG_DIR",
        log_dir.path().to_str().unwrap(),
    );

    let config_file = write_requeue_guard_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let token = "requeue_guard_token_l218";
    let kwargs = serde_json::json!({"token": token});
    let task_id = enqueue_task(&broker, "e2e_requeue_guard", &kwargs.to_string()).await;

    let dead_worker_id = "dead_requeue_guard_worker";

    // Simulate a stuck CLAIMED task: claimed >5s ago (exceeds max_claim_renew_age_ms),
    // with a lease that expired (claim_expires_at in the past).
    sqlx::query(
        "UPDATE horsies_tasks SET status = 'CLAIMED', claimed = TRUE, \
         claimed_at = NOW() - INTERVAL '10 seconds', \
         claimed_by_worker_id = $2, \
         claim_expires_at = NOW() - INTERVAL '3 seconds' \
         WHERE id = $1",
    )
    .bind(&task_id)
    .bind(dead_worker_id)
    .execute(&pool)
    .await
    .unwrap();

    // Start a normal worker — it reclaims the expired task.
    let _worker = start_worker(
        &config_path,
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(10),
    );

    wait_for_task_status(&pool, &task_id, "COMPLETED", Duration::from_secs(15)).await;

    // Verify owner transition.
    let final_owner: Option<String> =
        sqlx::query_scalar("SELECT claimed_by_worker_id FROM horsies_tasks WHERE id = $1")
            .bind(&task_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_ne!(
        final_owner.as_deref(),
        Some(dead_worker_id),
        "task should be reclaimed by live worker",
    );

    // Verify result.
    let result_json: Option<String> =
        sqlx::query_scalar("SELECT result FROM horsies_tasks WHERE id = $1")
            .bind(&task_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let result: horsies::TaskResult<serde_json::Value> =
        serde_json::from_str(&result_json.unwrap()).unwrap();
    assert!(result.is_ok());

    // Verify single execution via token file.
    let token_file = log_dir.path().join(token);
    assert!(token_file.exists(), "token file should exist");
    let file_count = std::fs::read_dir(log_dir.path()).unwrap().count();
    assert_eq!(file_count, 1, "expected exactly 1 execution marker");

    std::env::remove_var("E2E_REQUEUE_GUARD_LOG_DIR");
}
