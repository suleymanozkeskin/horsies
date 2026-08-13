#![allow(clippy::unwrap_used)]

//! Layer 1 e2e tests: task fundamentals.
//!
//! Mirrors Python's `tests/e2e/test_layer1_tasks.py`.
//!
//! Run with:
//!   cargo test -p horsies-test-worker --test layer1_tasks -- --test-threads=1
//!
//! Requires: DATABASE_URL env var or local test DB, built `horsies-test-worker` binary.

use std::time::Duration;

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use horsies::{OperationalErrorCode, PostgresBroker, TaskResult};
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
    let broker = PostgresBroker::connect(&url).await.unwrap();
    broker.migrate().await.unwrap();
    broker
}

/// Enqueue a task by name with kwargs JSON, return the task ID.
async fn enqueue_task(broker: &PostgresBroker, task_name: &str, kwargs_json: &str) -> Uuid {
    broker
        .enqueue(
            task_name,
            None,
            Some(kwargs_json),
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

/// Enqueue a task with no args.
async fn enqueue_no_args(broker: &PostgresBroker, task_name: &str) -> Uuid {
    enqueue_task(broker, task_name, "{}").await
}

/// Wait for a task to complete and return the result JSON.
async fn get_result_json(pool: &PgPool, task_id: &Uuid) -> Option<String> {
    let info = PostgresBroker::from_pool(pool.clone())
        .get_task_info(*task_id, true, false)
        .await
        .unwrap()?;
    info.result
        .map(|result| serde_json::to_string(&result).unwrap())
}

/// Get task status from DB.
async fn get_task_status(pool: &PgPool, task_id: &Uuid) -> String {
    PostgresBroker::from_pool(pool.clone())
        .get_task_info(*task_id, false, false)
        .await
        .unwrap()
        .unwrap()
        .status
        .to_string()
}

/// Get task retry_count from DB.
async fn get_retry_count(pool: &PgPool, task_id: &Uuid) -> i32 {
    PostgresBroker::from_pool(pool.clone())
        .get_task_info(*task_id, false, false)
        .await
        .unwrap()
        .unwrap()
        .retry_count as i32
}

/// Enqueue with an explicit task_options JSON string. The low-level broker
/// enqueue does not apply registry-declared options, so options the worker must
/// read (timeout_ms, retry_policy) are supplied here — same as `enqueue_with_retry`.
async fn enqueue_with_options(
    broker: &PostgresBroker,
    task_name: &str,
    kwargs: &str,
    task_options_json: &str,
) -> Uuid {
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
            Some(task_options_json),
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

/// Get the latest recorded attempt's error_code for a task.
async fn get_latest_attempt_error_code(pool: &PgPool, task_id: &Uuid) -> Option<String> {
    PostgresBroker::from_pool(pool.clone())
        .get_task_attempts(*task_id)
        .await
        .unwrap()
        .into_iter()
        .next()
        .and_then(|attempt| attempt.error_code)
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_simple_task_lifecycle() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let task_id = enqueue_task(&broker, "e2e_simple", r#"{"x": 5}"#).await;
    wait_for_task_status(&pool, &task_id, "COMPLETED", Duration::from_secs(10)).await;

    let result_json = get_result_json(&pool, &task_id).await.unwrap();
    let result: TaskResult<serde_json::Value> = serde_json::from_str(&result_json).unwrap();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), serde_json::json!(10));
}

#[tokio::test]
#[serial]
async fn test_status_transitions() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let task_id = enqueue_task(&broker, "e2e_simple", r#"{"x": 7}"#).await;
    wait_for_task_status(&pool, &task_id, "COMPLETED", Duration::from_secs(10)).await;

    let status = get_task_status(&pool, &task_id).await;
    assert_eq!(status, "COMPLETED");

    // Verify archived provenance is exposed through the public info surface.
    let info = broker
        .get_task_info(task_id, false, false)
        .await
        .unwrap()
        .unwrap();
    assert!(info.started_at.is_some(), "started_at should be set");
    assert!(info.completed_at.is_some(), "completed_at should be set");
    assert!(info.claimed_at.is_some(), "claimed_at should be set");
}

#[tokio::test]
#[serial]
async fn test_kwargs_with_defaults() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // required=5, defaults for optional and multiplier
    let t1 = enqueue_task(&broker, "e2e_kwargs", r#"{"required": 5}"#).await;
    wait_for_task_status(&pool, &t1, "COMPLETED", Duration::from_secs(10)).await;
    let r1: TaskResult<serde_json::Value> =
        serde_json::from_str(&get_result_json(&pool, &t1).await.unwrap()).unwrap();
    assert_eq!(r1.unwrap(), serde_json::json!("5_default"));

    // required=5, optional="custom"
    let t2 = enqueue_task(
        &broker,
        "e2e_kwargs",
        r#"{"required": 5, "optional": "custom"}"#,
    )
    .await;
    wait_for_task_status(&pool, &t2, "COMPLETED", Duration::from_secs(10)).await;
    let r2: TaskResult<serde_json::Value> =
        serde_json::from_str(&get_result_json(&pool, &t2).await.unwrap()).unwrap();
    assert_eq!(r2.unwrap(), serde_json::json!("5_custom"));

    // required=5, optional="x", multiplier=10
    let t3 = enqueue_task(
        &broker,
        "e2e_kwargs",
        r#"{"required": 5, "optional": "x", "multiplier": 10}"#,
    )
    .await;
    wait_for_task_status(&pool, &t3, "COMPLETED", Duration::from_secs(10)).await;
    let r3: TaskResult<serde_json::Value> =
        serde_json::from_str(&get_result_json(&pool, &t3).await.unwrap()).unwrap();
    assert_eq!(r3.unwrap(), serde_json::json!("50_x"));
}

#[tokio::test]
#[serial]
async fn test_explicit_error_result() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let task_id = enqueue_no_args(&broker, "e2e_error").await;
    wait_for_task_status(&pool, &task_id, "FAILED", Duration::from_secs(10)).await;

    let result_json = get_result_json(&pool, &task_id).await.unwrap();
    let result: TaskResult<serde_json::Value> = serde_json::from_str(&result_json).unwrap();
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert_eq!(
        err.error_code.as_ref().map(|c| c.to_string()).as_deref(),
        Some("DELIBERATE_ERROR")
    );
    assert_eq!(err.message.as_deref(), Some("This is intentional"));
}

#[tokio::test]
#[serial]
async fn test_no_retry_for_non_matching() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let task_id = enqueue_no_args(&broker, "e2e_no_retry").await;
    wait_for_task_status(&pool, &task_id, "FAILED", Duration::from_secs(10)).await;

    let retry_count = get_retry_count(&pool, &task_id).await;
    assert_eq!(
        retry_count, 0,
        "no retry should happen for non-matching error code"
    );
}

// Parity with horsies PR #102: per-task execution timeout (timeout_ms).

#[tokio::test]
#[serial]
async fn test_task_timeout_fails_with_task_timeout() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // timeout_ms 1000 supplied via task_options; body sleeps 5000 ms.
    let task_id = enqueue_with_options(
        &broker,
        "e2e_timeout_sleeper",
        r#"{"duration_ms": 5000}"#,
        r#"{"timeout_ms": 1000}"#,
    )
    .await;
    wait_for_task_status(&pool, &task_id, "FAILED", Duration::from_secs(10)).await;

    let result_json = get_result_json(&pool, &task_id).await.unwrap();
    let result: TaskResult<serde_json::Value> = serde_json::from_str(&result_json).unwrap();
    let err = result.unwrap_err();
    assert_eq!(
        err.error_code.as_ref().map(|c| c.to_string()).as_deref(),
        Some("TASK_TIMEOUT"),
    );

    // Attempt row recorded with TASK_TIMEOUT; no retry (not opted in).
    assert_eq!(
        get_latest_attempt_error_code(&pool, &task_id)
            .await
            .as_deref(),
        Some("TASK_TIMEOUT"),
    );
    assert_eq!(get_retry_count(&pool, &task_id).await, 0);
}

#[tokio::test]
#[serial]
async fn test_task_timeout_retry_opt_in() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // Opted into retry on TASK_TIMEOUT (one retry, 1s delay); times out every
    // attempt, so it retries once then exhausts to FAILED.
    let task_id = enqueue_with_options(
        &broker,
        "e2e_timeout_retry",
        r#"{"duration_ms": 5000}"#,
        r#"{"timeout_ms": 1000, "retry_policy": {"max_retries": 1, "intervals": [1], "backoff_strategy": "fixed", "jitter": false, "auto_retry_for": ["TASK_TIMEOUT"]}}"#,
    )
    .await;
    wait_for_task_status(&pool, &task_id, "FAILED", Duration::from_secs(30)).await;

    assert!(
        get_retry_count(&pool, &task_id).await >= 1,
        "timeout should have been retried before exhausting",
    );
    let result_json = get_result_json(&pool, &task_id).await.unwrap();
    let result: TaskResult<serde_json::Value> = serde_json::from_str(&result_json).unwrap();
    assert_eq!(
        result
            .unwrap_err()
            .error_code
            .as_ref()
            .map(|c| c.to_string())
            .as_deref(),
        Some("TASK_TIMEOUT"),
    );
}

#[tokio::test]
#[serial]
async fn test_slow_task_completes() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let task_id = enqueue_task(&broker, "e2e_slow", r#"{"duration_ms": 200}"#).await;
    wait_for_task_status(&pool, &task_id, "COMPLETED", Duration::from_secs(10)).await;

    let result_json = get_result_json(&pool, &task_id).await.unwrap();
    let result: TaskResult<serde_json::Value> = serde_json::from_str(&result_json).unwrap();
    assert_eq!(result.unwrap(), serde_json::json!("slept_200"));
}

#[tokio::test]
#[serial]
async fn test_multiple_tasks_concurrent() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // Enqueue 5 tasks.
    let mut task_ids = Vec::new();
    for i in 0..5 {
        let id = enqueue_task(&broker, "e2e_simple", &format!(r#"{{"x": {}}}"#, i)).await;
        task_ids.push(id);
    }

    // Wait for all to complete.
    wait_for_all_terminal(&pool, &task_ids, Duration::from_secs(15)).await;

    // Verify all completed.
    for task_id in &task_ids {
        let status = get_task_status(&pool, task_id).await;
        assert_eq!(status, "COMPLETED", "task {} should be COMPLETED", task_id);
    }
}

#[tokio::test]
#[serial]
async fn test_healthcheck() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let task_id = enqueue_no_args(&broker, "e2e_healthcheck").await;
    wait_for_task_status(&pool, &task_id, "COMPLETED", Duration::from_secs(10)).await;

    let result_json = get_result_json(&pool, &task_id).await.unwrap();
    let result: TaskResult<serde_json::Value> = serde_json::from_str(&result_json).unwrap();
    assert_eq!(result.unwrap(), serde_json::json!("ready"));
}

// ---------------------------------------------------------------------------
// L1.5: Retry tests
// ---------------------------------------------------------------------------

/// Enqueue with retry policy in task_options.
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

#[tokio::test]
#[serial]
async fn test_retry_exhausted() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let task_id = enqueue_with_retry(
        &broker,
        "e2e_retry_exhausted",
        "{}",
        3,
        &[1, 1, 1],
        &["TRANSIENT"],
    )
    .await;

    wait_for_task_status(&pool, &task_id, "FAILED", Duration::from_secs(30)).await;

    let retry_count = get_retry_count(&pool, &task_id).await;
    assert_eq!(retry_count, 3, "expected 3 retries before final failure");

    let result_json = get_result_json(&pool, &task_id).await.unwrap();
    let result: TaskResult<serde_json::Value> = serde_json::from_str(&result_json).unwrap();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(
        err.error_code.as_ref().map(|c| c.to_string()).as_deref(),
        Some("TRANSIENT")
    );
}

#[tokio::test]
#[serial]
async fn test_retry_eventual_success() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    // Create state file for retry_success task.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), "0").unwrap();
    std::env::set_var("E2E_RETRY_SUCCESS_PATH", tmp.path().to_str().unwrap());

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(10),
    );

    let task_id = enqueue_with_retry(
        &broker,
        "e2e_retry_success",
        "{}",
        3,
        &[1, 1, 1],
        &["TRANSIENT"],
    )
    .await;

    wait_for_task_status(&pool, &task_id, "COMPLETED", Duration::from_secs(30)).await;

    let result_json = get_result_json(&pool, &task_id).await.unwrap();
    let result: TaskResult<serde_json::Value> = serde_json::from_str(&result_json).unwrap();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), serde_json::json!("succeeded_on_attempt_3"));

    std::env::remove_var("E2E_RETRY_SUCCESS_PATH");
}

// ---------------------------------------------------------------------------
// L1.6: good_until expiry
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_good_until_already_past() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let past = chrono::Utc::now() - chrono::Duration::seconds(10);
    let task_id = broker
        .enqueue(
            "e2e_simple",
            None,
            Some(r#"{"x": 5}"#),
            "default",
            100,
            None,
            None,
            Some(past),
            None,
            &format!("test-{}", uuid::Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // Give worker time to potentially claim — it should NOT.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let status = get_task_status(&pool, &task_id).await;
    assert_eq!(status, "PENDING", "expired task should remain PENDING");
}

#[tokio::test]
#[serial]
async fn test_good_until_future_completes() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let future = chrono::Utc::now() + chrono::Duration::seconds(60);
    let task_id = broker
        .enqueue(
            "e2e_simple",
            None,
            Some(r#"{"x": 5}"#),
            "default",
            100,
            None,
            None,
            Some(future),
            None,
            &format!("test-{}", uuid::Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    wait_for_task_status(&pool, &task_id, "COMPLETED", Duration::from_secs(10)).await;

    let result_json = get_result_json(&pool, &task_id).await.unwrap();
    let result: TaskResult<serde_json::Value> = serde_json::from_str(&result_json).unwrap();
    assert_eq!(result.unwrap(), serde_json::json!(10));
}

#[tokio::test]
#[serial]
async fn test_retry_blocked_by_good_until_expiry() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let good_until = chrono::Utc::now() + chrono::Duration::seconds(10);
    let task_options = serde_json::json!({
        "retry_policy": {
            "max_retries": 3,
            "intervals": [60],
            "backoff_strategy": "fixed",
            "jitter": false,
            "auto_retry_for": ["TRANSIENT"],
        },
        "good_until": good_until.to_rfc3339(),
    });
    let task_options_str = serde_json::to_string(&task_options).unwrap();

    let task_id = broker
        .enqueue(
            "e2e_retry_exhausted",
            None,
            Some("{}"),
            "default",
            100,
            None,
            None,
            Some(good_until),
            Some(&task_options_str),
            &format!("test-{}", uuid::Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(10),
    );

    wait_for_task_status(&pool, &task_id, "FAILED", Duration::from_secs(15)).await;

    // Retry interval is 60s but good_until is 10s away — retry should be blocked.
    let retry_count = get_retry_count(&pool, &task_id).await;
    assert_eq!(
        retry_count, 0,
        "retry should be blocked by good_until expiry"
    );
}

// ---------------------------------------------------------------------------
// L1.7: Error handling edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_worker_resolution_error() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let task_id = enqueue_task(&broker, "nonexistent_task_xyz", "{}").await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    wait_for_task_status(&pool, &task_id, "FAILED", Duration::from_secs(10)).await;

    // Task-level failures write to `result` (serialized TaskResult::Err), not
    // `failed_reason` (which is reserved for worker crashes — see FAIL_TASK_SQL
    // vs FAIL_WORKER_SQL in horsies/src/broker/postgres.rs).
    let result_json = get_result_json(&pool, &task_id).await.unwrap();
    let parsed: TaskResult<serde_json::Value> =
        serde_json::from_str(&result_json).expect("result must deserialize as TaskResult");
    let TaskResult::Err(err) = parsed else {
        panic!("expected TaskResult::Err for resolution error, got Ok");
    };
    assert_eq!(
        err.error_code,
        Some(OperationalErrorCode::WorkerResolutionError.into()),
        "expected WORKER_RESOLUTION_ERROR, got: {:?}",
        err.error_code,
    );
    assert!(
        err.message
            .as_deref()
            .map(|m| m.contains("not registered"))
            .unwrap_or(false),
        "expected 'not registered' in message, got: {:?}",
        err.message,
    );
}

#[tokio::test]
#[serial]
async fn test_argument_deserialization_error() {
    let pool = pool().await;
    db::clean_tables(&pool).await;

    // Insert task with corrupted args directly.
    let task_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_tasks (id, task_name, queue_name, priority, args, kwargs, \
         status, sent_at, enqueued_at, enqueue_sha, created_at, updated_at,
         command_fingerprint_version, command_fingerprint, retention_class_key,
         retain_rerun_input, prepared_rerun_input_disposition) \
         VALUES ($1, 'e2e_simple', 'default', 100, '\"not_valid\"', '{}', \
         'PENDING', NOW(), NOW(), 'test-sha', NOW(), NOW(),
         1, decode(repeat('00', 32), 'hex'), 'standard_30d', FALSE, 'NEVER_ELIGIBLE')",
    )
    .bind(&task_id)
    .execute(&pool)
    .await
    .unwrap();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    wait_for_task_status(&pool, &task_id, "FAILED", Duration::from_secs(10)).await;

    let error_code = PostgresBroker::from_pool(pool.clone())
        .get_task_info(task_id, false, false)
        .await
        .unwrap()
        .unwrap()
        .error_code;

    assert_eq!(
        error_code.as_deref(),
        Some("ARGUMENT_TYPE_MISMATCH"),
        "expected ARGUMENT_TYPE_MISMATCH, got: {:?}",
        error_code,
    );
}

#[tokio::test]
#[serial]
async fn test_task_cancelled() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let task_id = enqueue_task(&broker, "e2e_simple", r#"{"x": 5}"#).await;

    assert!(broker
        .cancel(
            task_id,
            &[
                horsies::TaskStatus::Pending,
                horsies::TaskStatus::Claimed,
                horsies::TaskStatus::Running,
            ],
        )
        .await
        .unwrap());

    let status = get_task_status(&pool, &task_id).await;
    assert_eq!(status, "CANCELLED");
}

#[tokio::test]
#[serial]
async fn test_task_info_after_completion() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let task_id = enqueue_task(&broker, "e2e_simple", r#"{"x": 21}"#).await;
    wait_for_task_status(&pool, &task_id, "COMPLETED", Duration::from_secs(10)).await;

    let info = broker.get_task_info(task_id, true, true).await.unwrap();
    let info = info.expect("task info should exist");

    assert_eq!(info.task_id, task_id);
    assert_eq!(info.task_name, "e2e_simple");
    assert_eq!(info.status, horsies::TaskStatus::Completed);
    assert_eq!(info.queue_name, "default");
    assert!(info.started_at.is_some(), "started_at should be set");
    assert!(info.completed_at.is_some(), "completed_at should be set");

    // Result should be included (include_result=true).
    assert!(info.result.is_some(), "result should be included");
}

// ---------------------------------------------------------------------------
// L1.4: Exponential backoff state — retry_count advances with next_retry_at
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_exponential_backoff_state() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // Enqueue with exponential backoff: base=1s, max_retries=3.
    let task_options = serde_json::json!({
        "retry_policy": {
            "max_retries": 3,
            "intervals": [1],
            "backoff_strategy": "exponential",
            "jitter": false,
            "auto_retry_for": ["TRANSIENT"],
        }
    });
    let task_options_str = serde_json::to_string(&task_options).unwrap();
    let task_id = broker
        .enqueue(
            "e2e_retry_exhausted",
            None,
            Some("{}"),
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
        .unwrap();

    // Poll until retry_count >= 1 and next_retry_at is set.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut found = false;
    while tokio::time::Instant::now() < deadline {
        let row: Option<(i32, Option<chrono::DateTime<chrono::Utc>>)> =
            sqlx::query_as("SELECT retry_count, next_retry_at FROM horsies_tasks WHERE id = $1")
                .bind(&task_id)
                .fetch_optional(&pool)
                .await
                .unwrap();
        if let Some((rc, nra)) = row {
            if rc >= 1 && nra.is_some() {
                found = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        found,
        "retry_count should advance to >= 1 with next_retry_at set"
    );
}

// ---------------------------------------------------------------------------
// L1.4b: Exponential backoff intervals — interval_2 > interval_1
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_exponential_backoff_intervals() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let task_options = serde_json::json!({
        "retry_policy": {
            "max_retries": 3,
            "intervals": [1],
            "backoff_strategy": "exponential",
            "jitter": false,
            "auto_retry_for": ["TRANSIENT"],
        }
    });
    let task_options_str = serde_json::to_string(&task_options).unwrap();
    let task_id = broker
        .enqueue(
            "e2e_retry_exhausted",
            None,
            Some("{}"),
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
        .unwrap();

    // Capture enqueued_at snapshots at each retry_count.
    let mut snapshots: std::collections::HashMap<i32, chrono::DateTime<chrono::Utc>> =
        std::collections::HashMap::new();

    // Capture initial enqueued_at.
    let initial: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT enqueued_at FROM horsies_tasks WHERE id = $1")
            .bind(&task_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    snapshots.insert(0, initial);

    // Poll until retry_count reaches 2.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let row: Option<(i32, chrono::DateTime<chrono::Utc>)> =
            sqlx::query_as("SELECT retry_count, enqueued_at FROM horsies_tasks WHERE id = $1")
                .bind(&task_id)
                .fetch_optional(&pool)
                .await
                .unwrap();
        if let Some((rc, ea)) = row {
            snapshots.entry(rc).or_insert(ea);
            if rc >= 2 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    assert!(
        snapshots.contains_key(&0) && snapshots.contains_key(&1) && snapshots.contains_key(&2),
        "should have snapshots for retry_count 0, 1, 2; got: {:?}",
        snapshots.keys().collect::<Vec<_>>(),
    );

    let interval_1 = (snapshots[&1] - snapshots[&0]).num_milliseconds() as f64 / 1000.0;
    let interval_2 = (snapshots[&2] - snapshots[&1]).num_milliseconds() as f64 / 1000.0;
    assert!(
        interval_2 > interval_1,
        "exponential backoff: interval_2 ({:.2}s) should exceed interval_1 ({:.2}s)",
        interval_2,
        interval_1,
    );
}

// ---------------------------------------------------------------------------
// L1.4c: Auto-retry by error code
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_auto_retry_by_error_code() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // Task returns error code "TRANSIENT". Retry policy auto_retry_for includes "TRANSIENT".
    let task_options = serde_json::json!({
        "retry_policy": {
            "max_retries": 1,
            "intervals": [1],
            "backoff_strategy": "fixed",
            "jitter": false,
            "auto_retry_for": ["TRANSIENT"],
        }
    });
    let task_options_str = serde_json::to_string(&task_options).unwrap();
    let task_id = broker
        .enqueue(
            "e2e_retry_exhausted",
            None,
            Some("{}"),
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
        .unwrap();

    wait_for_task_status(&pool, &task_id, "FAILED", Duration::from_secs(15)).await;

    let retry_count = get_retry_count(&pool, &task_id).await;
    assert_eq!(
        retry_count, 1,
        "should have retried once via error code match"
    );
}

// ---------------------------------------------------------------------------
// L1.6.1: Task expires before claim (500ms expiry timing)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_task_expires_before_claim() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    // Submit task that expires in 500ms.
    let expiry = chrono::Utc::now() + chrono::Duration::milliseconds(500);
    let task_id = broker
        .enqueue(
            "e2e_simple",
            None,
            Some(r#"{"x": 5}"#),
            "default",
            100,
            None,
            None,
            Some(expiry),
            None,
            &format!("test-{}", uuid::Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Wait until task is definitely expired.
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Start worker AFTER expiry.
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // Observe for 2s — expired task should never be claimed.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let status = get_task_status(&pool, &task_id).await;
    assert_eq!(status, "PENDING", "expired task should remain PENDING");
}

// ---------------------------------------------------------------------------
// L1.6.5: Retry succeeds within good_until
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_retry_succeeds_within_good_until() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), "0").unwrap();
    std::env::set_var("E2E_RETRY_SUCCESS_PATH", tmp.path().to_str().unwrap());

    let good_until = chrono::Utc::now() + chrono::Duration::seconds(30);
    let task_options = serde_json::json!({
        "retry_policy": {
            "max_retries": 3,
            "intervals": [1, 1, 1],
            "backoff_strategy": "fixed",
            "jitter": false,
            "auto_retry_for": ["TRANSIENT"],
        },
        "good_until": good_until.to_rfc3339(),
    });
    let task_options_str = serde_json::to_string(&task_options).unwrap();

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(10),
    );

    let task_id = broker
        .enqueue(
            "e2e_retry_success",
            None,
            Some("{}"),
            "default",
            100,
            None,
            None,
            Some(good_until),
            Some(&task_options_str),
            &format!("test-{}", uuid::Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    wait_for_task_status(&pool, &task_id, "COMPLETED", Duration::from_secs(30)).await;

    let result_json = get_result_json(&pool, &task_id).await.unwrap();
    let result: TaskResult<serde_json::Value> = serde_json::from_str(&result_json).unwrap();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), serde_json::json!("succeeded_on_attempt_3"));

    let retry_count = get_retry_count(&pool, &task_id).await;
    assert_eq!(retry_count, 2, "should have 2 retries (3 attempts total)");

    std::env::remove_var("E2E_RETRY_SUCCESS_PATH");
}

#[tokio::test]
#[serial]
async fn test_good_until_expires_while_claimed_before_start() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let config_file = write_prefetch_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let _worker = start_worker(
        &config_path,
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(10),
    );

    let blocker_id = enqueue_task(&broker, "e2e_slow", r#"{"duration_ms":1500}"#).await;

    let good_until = chrono::Utc::now() + chrono::Duration::milliseconds(500);
    let target_id = broker
        .enqueue(
            "e2e_simple",
            None,
            Some(r#"{"x": 5}"#),
            "default",
            100,
            None,
            None,
            Some(good_until),
            None,
            &format!("test-{}", uuid::Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let claim_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < claim_deadline {
        let status = get_task_status(&pool, &target_id).await;
        if status == "CLAIMED" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        get_task_status(&pool, &target_id).await,
        "CLAIMED",
        "target task should be buffered as CLAIMED before it expires",
    );

    wait_for_task_status(&pool, &target_id, "EXPIRED", Duration::from_secs(15)).await;
    wait_for_task_status(&pool, &blocker_id, "COMPLETED", Duration::from_secs(15)).await;

    let row: (
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT (detail.task_row).status,
                (detail.task_row).started_at,
                (detail.task_row).last_claimed_worker_id,
                (detail.task_row).error_code
         FROM horsies_task_detail_staged($1) AS detail
         WHERE detail.location = 'HISTORY'",
    )
    .bind(&target_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(row.0, "EXPIRED");
    assert!(
        row.1.is_none(),
        "started_at must stay NULL when task expires before user code starts",
    );
    assert!(
        row.2.is_some(),
        "claimed_by_worker_id should be preserved for forensics",
    );
    assert_eq!(row.3.as_deref(), Some("TASK_EXPIRED"));

    let live_exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM horsies_tasks WHERE id = $1)")
            .bind(target_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!live_exists, "expired claim must leave the live table");

    let attempts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM horsies_task_attempts WHERE task_id = $1")
            .bind(&target_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(attempts, 0, "no attempt row should be written");
}

// ---------------------------------------------------------------------------
// L1.5.4: Priority ordering — high priority claimed before low
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_priority_ordering() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let enqueue_base = chrono::Utc::now();

    // Low priority (100) submitted first.
    let task_id_low = broker
        .enqueue(
            "e2e_slow",
            None,
            Some(r#"{"duration_ms": 100}"#),
            "default",
            100,
            None,
            Some(enqueue_base),
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
        .unwrap();

    // High priority (1) submitted second.
    let task_id_high = broker
        .enqueue(
            "e2e_slow",
            None,
            Some(r#"{"duration_ms": 100}"#),
            "default",
            1,
            None,
            Some(enqueue_base + chrono::Duration::milliseconds(50)),
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
        .unwrap();

    // Start worker with concurrency=1 to force serial execution.
    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "1"],
        "worker started",
        Duration::from_secs(10),
    );

    wait_for_all_terminal(
        &pool,
        &[task_id_low.clone(), task_id_high.clone()],
        Duration::from_secs(15),
    )
    .await;

    // High priority should have started before low.
    let started_high = broker
        .get_task_info(task_id_high, false, false)
        .await
        .unwrap()
        .unwrap()
        .started_at;
    let started_low = broker
        .get_task_info(task_id_low, false, false)
        .await
        .unwrap()
        .unwrap()
        .started_at;

    assert!(
        started_high.unwrap() < started_low.unwrap(),
        "high priority should start before low: high={:?}, low={:?}",
        started_high,
        started_low,
    );
}

// ---------------------------------------------------------------------------
// L1.8.2: SKIP LOCKED prevents double claim
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_skip_locked_prevents_double_claim() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let log_dir = tempfile::tempdir().unwrap();
    std::env::set_var("E2E_IDEMPOTENT_LOG_DIR", log_dir.path().to_str().unwrap());

    // Start two workers.
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

    // Submit 20 idempotent tasks.
    let mut task_ids = Vec::new();
    for i in 0..20 {
        let token = format!("skip_locked_{}", i);
        let kwargs = serde_json::json!({"token": token});
        let id = enqueue_task(&broker, "e2e_idempotent", &kwargs.to_string()).await;
        task_ids.push(id);
    }

    wait_for_all_terminal(&pool, &task_ids, Duration::from_secs(30)).await;

    // All tasks should succeed — no DOUBLE_EXECUTION errors.
    for task_id in &task_ids {
        let status = get_task_status(&pool, task_id).await;
        assert_eq!(status, "COMPLETED", "task {} should COMPLETE", task_id);
    }

    // Each token file should exist exactly once.
    let file_count = std::fs::read_dir(log_dir.path()).unwrap().count();
    assert_eq!(
        file_count, 20,
        "expected 20 token files, got {}",
        file_count
    );

    std::env::remove_var("E2E_IDEMPOTENT_LOG_DIR");
}

// ---------------------------------------------------------------------------
// L1.8.3: max_claim_batch respected
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_max_claim_batch() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "1", "--max-claim-batch", "1"],
        "worker started",
        Duration::from_secs(10),
    );

    // Enqueue 6 slow tasks.
    let mut task_ids = Vec::new();
    for _ in 0..6 {
        let id = enqueue_task(&broker, "e2e_slow", r#"{"duration_ms": 300}"#).await;
        task_ids.push(id);
    }

    // Poll DB while tasks are running — CLAIMED count should never exceed 1.
    let mut max_claimed_seen = 0i64;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        let claimed: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM horsies_tasks WHERE status = 'CLAIMED'")
                .fetch_one(&pool)
                .await
                .unwrap();
        if claimed > max_claimed_seen {
            max_claimed_seen = claimed;
        }

        // Check if all done.
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_tasks WHERE status IN ('PENDING', 'CLAIMED', 'RUNNING')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if pending == 0 {
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        max_claimed_seen <= 1,
        "CLAIMED count {} exceeded max_claim_batch 1",
        max_claimed_seen,
    );

    // All tasks should complete.
    wait_for_all_terminal(&pool, &task_ids, Duration::from_secs(30)).await;
    for task_id in &task_ids {
        let status = get_task_status(&pool, task_id).await;
        assert_eq!(status, "COMPLETED", "task {} should COMPLETE", task_id);
    }
}

// ---------------------------------------------------------------------------
// L1.8.1 upgrade: Multiple tasks concurrent — DB polling proof
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_multiple_tasks_concurrent_db_proof() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "4"],
        "worker started",
        Duration::from_secs(10),
    );

    // Enqueue 4 tasks that take 800ms each.
    let mut task_ids = Vec::new();
    for _ in 0..4 {
        let id = enqueue_task(&broker, "e2e_slow", r#"{"duration_ms": 800}"#).await;
        task_ids.push(id);
    }

    // Poll DB to observe max concurrent RUNNING tasks.
    let mut max_running = 0i64;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        let running: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM horsies_tasks WHERE status = 'RUNNING'")
                .fetch_one(&pool)
                .await
                .unwrap();
        if running > max_running {
            max_running = running;
        }
        if max_running >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    wait_for_all_terminal(&pool, &task_ids, Duration::from_secs(15)).await;

    assert!(max_running > 0, "should have observed RUNNING tasks");
    assert!(
        max_running >= 2,
        "should prove parallelism with >= 2 concurrent RUNNING, got {}",
        max_running
    );
}

// ---------------------------------------------------------------------------
// L1.7.3: Task not found — non-existent task_id returns TaskNotFound
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_task_not_found() {
    let broker = broker().await;

    let result = broker
        .get_result::<serde_json::Value>(uuid::Uuid::nil(), Some(Duration::from_millis(500)))
        .await
        .unwrap();

    assert!(
        result.is_err() && result.is_transient(),
        "expected TaskNotFound (transient error), got: {:?}",
        result,
    );
}

// ---------------------------------------------------------------------------
// L1.7.4: Wait timeout — task never completes within timeout
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_wait_timeout() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    // Enqueue but DON'T start a worker.
    let task_id = enqueue_task(&broker, "e2e_simple", r#"{"x": 5}"#).await;

    let result = broker
        .get_result::<serde_json::Value>(task_id, Some(Duration::from_millis(500)))
        .await
        .unwrap();

    assert!(
        result.is_err() && result.is_transient(),
        "expected WaitTimeout (transient error), got: {:?}",
        result,
    );
}

// ---------------------------------------------------------------------------
// L1.7.6: Library error code in user task — OperationalErrorCode preserved
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_library_error_code_in_user_task() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let task_id = enqueue_no_args(&broker, "e2e_error_code").await;
    wait_for_task_status(&pool, &task_id, "FAILED", Duration::from_secs(10)).await;

    let result_json = get_result_json(&pool, &task_id).await.unwrap();
    let result: TaskResult<serde_json::Value> = serde_json::from_str(&result_json).unwrap();
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert_eq!(
        err.error_code.as_ref().map(|c| c.to_string()).as_deref(),
        Some("TASK_ERROR"),
    );
    assert_eq!(err.message.as_deref(), Some("boom"));
}

// ---------------------------------------------------------------------------
// L1.7.9 upgrade: Cancelled task — get_result returns TaskCancelled
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_task_cancelled_get_result() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let task_id = enqueue_task(&broker, "e2e_simple", r#"{"x": 5}"#).await;

    assert!(broker
        .cancel(
            task_id,
            &[
                horsies::TaskStatus::Pending,
                horsies::TaskStatus::Claimed,
                horsies::TaskStatus::Running,
            ],
        )
        .await
        .unwrap());

    let result = broker
        .get_result::<serde_json::Value>(task_id, Some(Duration::from_millis(1000)))
        .await
        .unwrap();

    assert!(
        result.is_err(),
        "expected TaskCancelled error, got: {:?}",
        result,
    );

    // Verify the error message contains the task_id.
    let err = result.unwrap_err();
    assert!(
        err.message
            .as_deref()
            .unwrap_or("")
            .contains(&task_id.to_string()),
        "expected task_id in error message",
    );
}

// ---------------------------------------------------------------------------
// L1.5.1: Complex result serialization — nested struct roundtrip
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_complex_result_serialization() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let task_id = enqueue_no_args(&broker, "e2e_complex_result").await;
    wait_for_task_status(&pool, &task_id, "COMPLETED", Duration::from_secs(10)).await;

    let result_json = get_result_json(&pool, &task_id).await.unwrap();
    let result: TaskResult<serde_json::Value> = serde_json::from_str(&result_json).unwrap();
    assert!(result.is_ok());

    let output = result.unwrap();
    assert_eq!(output.get("value").and_then(|v| v.as_i64()), Some(42));
    let nested = output.get("nested").unwrap().as_object().unwrap();
    assert_eq!(nested.get("a").unwrap().as_array().unwrap().len(), 3);
    assert_eq!(nested.get("b").unwrap().as_array().unwrap().len(), 2);
}

// ---------------------------------------------------------------------------
// L1.5.2: Custom mode queue routing — tasks route to correct queues
// ---------------------------------------------------------------------------

fn write_custom_queues_config_l1() -> tempfile::NamedTempFile {
    use std::io::Write;
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
        "recovery": {
            "auto_requeue_stale_claimed": false,
            "auto_fail_stale_running": false,
            "check_interval_ms": 60000
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
async fn test_custom_mode_queue_routing() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let config_file = write_custom_queues_config_l1();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let _worker = start_worker(
        &config_path,
        &["--concurrency", "4", "--queues", "high,normal,low"],
        "worker started",
        Duration::from_secs(10),
    );

    // Enqueue to each queue.
    let high_id = broker
        .enqueue(
            "e2e_high",
            None,
            Some("{}"),
            "high",
            1,
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
        .unwrap();
    let normal_id = broker
        .enqueue(
            "e2e_normal",
            None,
            Some("{}"),
            "normal",
            50,
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
        .unwrap();
    let low_id = broker
        .enqueue(
            "e2e_low",
            None,
            Some("{}"),
            "low",
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
        .unwrap();

    wait_for_all_terminal(
        &pool,
        &[high_id.clone(), normal_id.clone(), low_id.clone()],
        Duration::from_secs(10),
    )
    .await;

    // All should complete.
    assert_eq!(get_task_status(&pool, &high_id).await, "COMPLETED");
    assert_eq!(get_task_status(&pool, &normal_id).await, "COMPLETED");
    assert_eq!(get_task_status(&pool, &low_id).await, "COMPLETED");
}

// ---------------------------------------------------------------------------
// L1.5.3: Custom mode queue names stored in DB
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_custom_mode_queue_names_stored() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let config_file = write_custom_queues_config_l1();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let _worker = start_worker(
        &config_path,
        &["--concurrency", "4", "--queues", "high,normal,low"],
        "worker started",
        Duration::from_secs(10),
    );

    let high_id = broker
        .enqueue(
            "e2e_high",
            None,
            Some("{}"),
            "high",
            1,
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
        .unwrap();

    wait_for_task_status(&pool, &high_id, "COMPLETED", Duration::from_secs(10)).await;

    let queue_name = broker
        .get_task_info(high_id, false, false)
        .await
        .unwrap()
        .unwrap()
        .queue_name;

    assert_eq!(queue_name, "high", "queue_name should be stored as 'high'");
}

// ---------------------------------------------------------------------------
// L1.9 Task attempt history
// ---------------------------------------------------------------------------

async fn get_attempts(pool: &PgPool, task_id: &Uuid) -> Vec<horsies::TaskAttemptRow> {
    let mut attempts = PostgresBroker::from_pool(pool.clone())
        .get_task_attempts(*task_id)
        .await
        .unwrap();
    attempts.reverse();
    attempts
}

/// Successful task writes a COMPLETED attempt record.
#[tokio::test]
#[serial]
async fn test_attempt_recorded_on_success() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let task_id = enqueue_no_args(&broker, "e2e_healthcheck").await;
    wait_for_task_status(&pool, &task_id, "COMPLETED", Duration::from_secs(10)).await;

    let attempts = get_attempts(&pool, &task_id).await;
    assert_eq!(attempts.len(), 1, "should have exactly 1 attempt");
    assert_eq!(attempts[0].attempt, 1);
    assert_eq!(attempts[0].outcome, "COMPLETED");
    assert!(!attempts[0].will_retry);
    assert!(attempts[0].error_code.is_none());
    assert!(attempts[0].worker_id.is_some());
}

/// Failed task (no retry) writes a FAILED attempt record.
#[tokio::test]
#[serial]
async fn test_attempt_recorded_on_failure() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let task_id = enqueue_no_args(&broker, "e2e_error").await;
    wait_for_task_status(&pool, &task_id, "FAILED", Duration::from_secs(10)).await;

    let attempts = get_attempts(&pool, &task_id).await;
    assert_eq!(attempts.len(), 1, "should have exactly 1 attempt");
    assert_eq!(attempts[0].attempt, 1);
    assert_eq!(attempts[0].outcome, "FAILED");
    assert!(!attempts[0].will_retry);
    assert!(attempts[0].error_code.is_some());
}

/// Retry-exhausted task writes multiple FAILED attempt records.
#[tokio::test]
#[serial]
async fn test_attempt_recorded_on_retry_exhausted() {
    let pool = pool().await;
    let broker = broker().await;
    db::clean_tables(&pool).await;

    let _worker = start_worker(
        &db_url(),
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // e2e_retry_exhausted always returns TRANSIENT error, configured with 3 max retries.
    // Enqueue with task_options that configure auto_retry_for + retry_policy.
    let task_options = serde_json::json!({
        "task_name": "e2e_retry_exhausted",
        "auto_retry_for": ["TRANSIENT"],
        "retry_policy": {
            "max_retries": 2,
            "intervals": [1, 1],
            "backoff_strategy": "Fixed",
            "jitter": false,
        }
    });
    let task_id = broker
        .enqueue(
            "e2e_retry_exhausted",
            None,
            Some("{}"),
            "default",
            100,
            None,
            None,
            None,
            Some(&serde_json::to_string(&task_options).unwrap()),
            &format!("test-{}", uuid::Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    wait_for_task_status(&pool, &task_id, "FAILED", Duration::from_secs(30)).await;

    let attempts = get_attempts(&pool, &task_id).await;
    assert!(
        attempts.len() >= 2,
        "should have at least 2 attempts (initial + retries), got {}",
        attempts.len(),
    );

    // All non-final attempts should have will_retry=true
    for a in &attempts[..attempts.len() - 1] {
        assert_eq!(a.outcome, "FAILED", "non-final attempt should be FAILED");
        assert!(
            a.will_retry,
            "non-final attempt should have will_retry=true"
        );
    }

    // Final attempt should be FAILED with will_retry=false (exhausted)
    let last = attempts.last().unwrap();
    assert_eq!(last.outcome, "FAILED");
    assert!(
        !last.will_retry,
        "final attempt should have will_retry=false (exhausted)"
    );
}

// ---------------------------------------------------------------------------
// L1.10 enqueue_sha idempotency verification
// ---------------------------------------------------------------------------

/// Same task_id + same SHA = idempotent success.
#[tokio::test]
#[serial]
async fn test_enqueue_sha_idempotent_same_sha() {
    let broker = broker().await;
    let task_id = uuid::Uuid::new_v4();
    let sha = "deadbeef1234";

    let id1 = broker
        .enqueue(
            "e2e_healthcheck",
            None,
            Some("{}"),
            "default",
            100,
            None,
            None,
            None,
            None,
            sha,
            Some(task_id),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(id1, task_id);

    // Second enqueue with same SHA — should succeed idempotently.
    let id2 = broker
        .enqueue(
            "e2e_healthcheck",
            None,
            Some("{}"),
            "default",
            100,
            None,
            None,
            None,
            None,
            sha,
            Some(task_id),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        id2, task_id,
        "idempotent re-enqueue should return same task_id"
    );
}

/// Same task_id + different SHA = PayloadMismatch error.
#[tokio::test]
#[serial]
async fn test_enqueue_sha_payload_mismatch() {
    let broker = broker().await;
    let task_id = uuid::Uuid::new_v4();

    let _ = broker
        .enqueue(
            "e2e_healthcheck",
            None,
            Some("{}"),
            "default",
            100,
            None,
            None,
            None,
            None,
            "sha_original",
            Some(task_id),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Second enqueue with DIFFERENT SHA — should fail.
    let result = broker
        .enqueue(
            "e2e_healthcheck",
            None,
            Some(r#"{"different": true}"#),
            "default",
            100,
            None,
            None,
            None,
            None,
            "sha_different",
            Some(task_id),
            None,
            None,
            None,
            None,
        )
        .await;

    assert!(result.is_err(), "different SHA should return an error");
    let err = result.unwrap_err();
    assert!(
        matches!(err, horsies::BrokerError::PayloadMismatch { .. }),
        "error should be PayloadMismatch, got: {:?}",
        err,
    );
}
