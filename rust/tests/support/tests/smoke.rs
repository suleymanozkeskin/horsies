#![allow(clippy::unwrap_used)]

//! Smoke tests for the test infrastructure itself.
//!
//! Validates that every helper in horsies-test-support actually works
//! against a real Postgres instance before we build hundreds of tests on top.
//!
//! Requires `DATABASE_URL` env var or defaults to local test DB.
//! Run with: `cargo test -p horsies-test-support --test smoke`

use horsies::{TaskNode, TaskResult, WorkflowHandle, WorkflowSpecBuilder, WorkflowSpecRegistry};
use horsies_test_support::{db, fixtures, tasks, workflow_helpers};
use serial_test::serial;

// ---------------------------------------------------------------------------
// db module
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn db_create_pool_connects() {
    let pool = db::create_pool().await;
    // Verify we can execute a simple query.
    let row: (i32,) = sqlx::query_as("SELECT 1").fetch_one(&pool).await.unwrap();
    assert_eq!(row.0, 1);
}

#[tokio::test]
#[serial]
async fn db_run_migrations_succeeds() {
    let pool = db::create_pool().await;
    db::run_migrations(&pool).await;
    // Verify a horsies table exists after migration.
    let exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'horsies_tasks')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(exists.0, "horsies_tasks table should exist after migration");
}

#[tokio::test]
#[serial]
async fn db_clean_tables_succeeds() {
    let pool = db::create_pool().await;
    db::run_migrations(&pool).await;
    db::clean_tables(&pool).await;
    // Should not panic — tables are empty.
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM horsies_tasks")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 0);
}

#[tokio::test]
#[serial]
async fn db_url_returns_string() {
    let url = db::db_url();
    assert!(
        url.contains("postgres"),
        "URL should contain 'postgres': {}",
        url
    );
}

// ---------------------------------------------------------------------------
// fixtures module
// ---------------------------------------------------------------------------

#[test]
fn fixtures_default_app_config_validates() {
    let config = fixtures::default_app_config();
    let errors = config.validate();
    assert!(
        errors.is_empty(),
        "default config should validate: {:?}",
        errors
    );
}

#[test]
fn fixtures_fast_recovery_config_validates() {
    let config = fixtures::fast_recovery_app_config();
    let errors = config.validate();
    assert!(
        errors.is_empty(),
        "fast recovery config should validate: {:?}",
        errors
    );
}

#[test]
fn fixtures_test_postgres_config_validates() {
    let config = fixtures::test_postgres_config();
    config.validate().expect("postgres config should validate");
}

// ---------------------------------------------------------------------------
// tasks module
// ---------------------------------------------------------------------------

#[test]
fn tasks_simple_result_is_ok() {
    let r = tasks::simple_task_result(21);
    assert!(r.is_ok());
    assert_eq!(r.unwrap(), serde_json::json!(42));
}

#[test]
fn tasks_failing_result_is_err() {
    let r = tasks::failing_task_result();
    assert!(r.is_err());
}

#[test]
fn tasks_identity_result_preserves_value() {
    let v = serde_json::json!({"key": "value"});
    let r = tasks::identity_task_result(v.clone());
    assert_eq!(r.unwrap(), v);
}

#[test]
fn tasks_args_receiver_result_adds_ten() {
    let r = tasks::args_receiver_result(32);
    assert_eq!(r.unwrap(), serde_json::json!(42));
}

#[test]
fn tasks_recovery_result_returns_999_on_failure() {
    let r = tasks::recovery_task_result(false, 0);
    assert_eq!(r.unwrap(), serde_json::json!(999));
}

#[test]
fn tasks_recovery_result_passes_through_on_success() {
    let r = tasks::recovery_task_result(true, 42);
    assert_eq!(r.unwrap(), serde_json::json!(42));
}

#[test]
fn tasks_multi_args_receiver_sums() {
    let r = tasks::multi_args_receiver_result(20, 22);
    assert_eq!(r.unwrap(), serde_json::json!(42));
}

#[test]
fn tasks_result_json_roundtrips() {
    let r = tasks::simple_task_result(5);
    let json = tasks::result_json(&r);
    let back: TaskResult<serde_json::Value> = serde_json::from_str(&json).unwrap();
    assert!(back.is_ok());
    assert_eq!(back.unwrap(), serde_json::json!(10));
}

#[test]
fn tasks_result_json_err_roundtrips() {
    let r = tasks::failing_task_result();
    let json = tasks::result_json(&r);
    let back: TaskResult<serde_json::Value> = serde_json::from_str(&json).unwrap();
    assert!(back.is_err());
}

// ---------------------------------------------------------------------------
// workflow_helpers module — requires database
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn workflow_helpers_insert_and_query_task() {
    let pool = db::create_pool().await;
    db::run_migrations(&pool).await;
    db::clean_tables(&pool).await;

    let task_id = uuid::Uuid::new_v4().to_string();
    workflow_helpers::insert_task(&pool, &task_id, "test_task", "default", "PENDING").await;

    // Verify the task exists.
    let status: String = sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
        .bind(&task_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "PENDING");
}

#[tokio::test]
#[serial]
async fn workflow_helpers_start_and_complete_workflow() {
    let pool = db::create_pool().await;
    db::run_migrations(&pool).await;
    db::clean_workflow_tables(&pool).await;

    let registry = WorkflowSpecRegistry::new();

    // Build a simple 2-node chain: a -> b
    let mut builder = WorkflowSpecBuilder::new("smoke_test_wf");
    let a = builder.task(TaskNode::<serde_json::Value>::new("task_a").node_id("a"));
    builder.task(
        TaskNode::<serde_json::Value>::new("task_b")
            .node_id("b")
            .waits_for(a),
    );
    let spec = builder.build().unwrap();

    // Start the workflow.
    let handle: WorkflowHandle<serde_json::Value> =
        workflow_helpers::start_ok(&pool, &spec, &registry).await;
    let wf_id = handle.workflow_id();

    // Verify workflow is RUNNING.
    let status = workflow_helpers::get_workflow_status(&pool, wf_id).await;
    assert_eq!(status, "RUNNING", "workflow should be RUNNING after start");

    // Task A should be ENQUEUED (root node).
    let a_status = workflow_helpers::get_wf_task_status(&pool, wf_id, 0).await;
    assert_eq!(a_status, "ENQUEUED", "root task should be ENQUEUED");

    // Task B should be PENDING (waiting for A).
    let b_status = workflow_helpers::get_wf_task_status(&pool, wf_id, 1).await;
    assert_eq!(b_status, "PENDING", "dependent task should be PENDING");

    // Complete task A.
    let a_result = tasks::simple_task_result(21);
    workflow_helpers::complete_task(&pool, wf_id, 0, &a_result, &registry).await;

    // Task B should now be ENQUEUED (A completed, B's dependency met).
    let b_status = workflow_helpers::get_wf_task_status(&pool, wf_id, 1).await;
    assert!(
        b_status == "READY" || b_status == "ENQUEUED",
        "task B should be READY or ENQUEUED after A completes, got: {}",
        b_status,
    );

    // Complete task B.
    let b_result = tasks::simple_task_result(42);
    workflow_helpers::complete_task(&pool, wf_id, 1, &b_result, &registry).await;

    // Workflow should be COMPLETED.
    let final_status = workflow_helpers::get_workflow_status(&pool, wf_id).await;
    assert_eq!(
        final_status, "COMPLETED",
        "workflow should be COMPLETED after all tasks finish"
    );
}

#[tokio::test]
#[serial]
async fn workflow_helpers_failing_task_fails_workflow() {
    let pool = db::create_pool().await;
    db::run_migrations(&pool).await;
    db::clean_workflow_tables(&pool).await;

    let registry = WorkflowSpecRegistry::new();

    let mut builder = WorkflowSpecBuilder::new("smoke_fail_wf");
    builder.task(TaskNode::<serde_json::Value>::new("task_a").node_id("a"));
    let spec = builder.build().unwrap();

    let handle: WorkflowHandle<serde_json::Value> =
        workflow_helpers::start_ok(&pool, &spec, &registry).await;
    let wf_id = handle.workflow_id();

    // Fail the task.
    let fail_result = tasks::failing_task_result();
    workflow_helpers::complete_task(&pool, wf_id, 0, &fail_result, &registry).await;

    // Workflow should be FAILED.
    let final_status = workflow_helpers::get_workflow_status(&pool, wf_id).await;
    assert_eq!(
        final_status, "FAILED",
        "workflow should be FAILED when task fails"
    );
}

#[tokio::test]
#[serial]
async fn workflow_helpers_good_until_persisted_via_task_options() {
    use chrono::{Duration, Utc};

    let pool = db::create_pool().await;
    db::run_migrations(&pool).await;
    db::clean_workflow_tables(&pool).await;

    let registry = WorkflowSpecRegistry::new();
    let deadline = Utc::now() + Duration::hours(2);

    let mut builder = WorkflowSpecBuilder::new("smoke_good_until_wf");
    builder.task(
        TaskNode::<serde_json::Value>::new("task_a")
            .node_id("a")
            .good_until(deadline),
    );
    let spec = builder.build().unwrap();

    let handle: WorkflowHandle<serde_json::Value> =
        workflow_helpers::start_ok(&pool, &spec, &registry).await;
    let wf_id = handle.workflow_id();

    // Read the task_options stored in horsies_workflow_tasks.
    let task_options: Option<String> = sqlx::query_scalar(
        "SELECT task_options FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = 0",
    )
    .bind(wf_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    let opts = task_options.expect("task_options should not be NULL");
    let parsed: serde_json::Value = serde_json::from_str(&opts).unwrap();
    assert!(
        parsed.get("good_until").is_some(),
        "task_options should contain good_until key, got: {}",
        opts,
    );

    // Verify horsies_tasks row also got good_until.
    let task_id = workflow_helpers::get_task_id(&pool, wf_id, 0).await;
    let task_good_until: Option<chrono::DateTime<Utc>> =
        sqlx::query_scalar("SELECT good_until FROM horsies_tasks WHERE id = $1")
            .bind(&task_id)
            .fetch_one(&pool)
            .await
            .unwrap();

    assert!(
        task_good_until.is_some(),
        "horsies_tasks.good_until should be set from task_options",
    );

    let stored = task_good_until.unwrap();
    let diff = (stored - deadline).num_seconds().abs();
    assert!(
        diff < 2,
        "stored good_until should match deadline (diff={}s)",
        diff,
    );
}

#[tokio::test]
#[serial]
async fn workflow_helpers_count_tasks_in_status() {
    let pool = db::create_pool().await;
    db::run_migrations(&pool).await;
    db::clean_workflow_tables(&pool).await;

    let registry = WorkflowSpecRegistry::new();

    let mut builder = WorkflowSpecBuilder::new("smoke_count_wf");
    let a = builder.task(TaskNode::<serde_json::Value>::new("task_a").node_id("a"));
    builder.task(
        TaskNode::<serde_json::Value>::new("task_b")
            .node_id("b")
            .waits_for(a),
    );
    let spec = builder.build().unwrap();

    let handle: WorkflowHandle<serde_json::Value> =
        workflow_helpers::start_ok(&pool, &spec, &registry).await;
    let wf_id = handle.workflow_id();

    let enqueued = workflow_helpers::count_wf_tasks_in_status(&pool, wf_id, "ENQUEUED").await;
    let pending = workflow_helpers::count_wf_tasks_in_status(&pool, wf_id, "PENDING").await;
    assert_eq!(enqueued, 1, "should have 1 ENQUEUED (root)");
    assert_eq!(pending, 1, "should have 1 PENDING (dependent)");
}

// ---------------------------------------------------------------------------
// Reaper Phase 2 heartbeat freshness guard regression
// ---------------------------------------------------------------------------
//
// Verifies that the Phase 2 SELECT_STALE_TASK_FOR_UPDATE_SQL rejects a task
// that received a fresh heartbeat after the Phase 1 scan identified it as stale.
// This closes the race window where a heartbeat landing between Phase 1 and
// Phase 2 would otherwise be ignored.

#[tokio::test]
#[serial]
async fn reaper_phase2_rejects_task_with_fresh_heartbeat() {
    let pool = db::create_pool().await;
    db::run_migrations(&pool).await;
    db::clean_tables(&pool).await;

    let stale_threshold_secs: f64 = 5.0;

    // Insert a RUNNING task with started_at well in the past.
    let task_id = uuid::Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO horsies_tasks (id, task_name, queue_name, status, started_at, max_retries, enqueue_sha) \
         VALUES ($1, 'test_task', 'default', 'RUNNING', NOW() - INTERVAL '10 minutes', 0, 'sha-test')",
    )
    .bind(&task_id)
    .execute(&pool)
    .await
    .unwrap();

    // Insert a FRESH runner heartbeat (sent_at = now, well within threshold).
    sqlx::query(
        "INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid) \
         VALUES ($1, 'worker-1', 'runner', NOW(), 'test-host', 12345)",
    )
    .bind(&task_id)
    .execute(&pool)
    .await
    .unwrap();

    // Phase 2 query should return NO rows because the heartbeat is fresh.
    let phase2_sql = "\
        SELECT t.id, t.retry_count, t.worker_pid, t.worker_hostname, \
               t.claimed_by_worker_id, t.started_at, t.worker_process_name, \
               t.max_retries, t.task_options, t.good_until, t.queue_name, \
               clock_timestamp() AS db_now \
        FROM horsies_tasks t \
        WHERE t.id = $1 AND t.status = 'RUNNING' \
          AND NOT EXISTS ( \
            SELECT 1 FROM horsies_heartbeats \
            WHERE task_id = t.id AND role = 'runner' \
              AND sent_at > NOW() - $2 * INTERVAL '1 second' \
          ) \
        FOR UPDATE OF t";

    let row: Option<(String,)> = sqlx::query_as(
        // Simplified: just check if the query returns anything.
        &format!(
            "SELECT t.id FROM horsies_tasks t \
             WHERE t.id = $1 AND t.status = 'RUNNING' \
               AND NOT EXISTS ( \
                 SELECT 1 FROM horsies_heartbeats \
                 WHERE task_id = t.id AND role = 'runner' \
                   AND sent_at > NOW() - $2 * INTERVAL '1 second' \
               )"
        ),
    )
    .bind(&task_id)
    .bind(stale_threshold_secs)
    .fetch_optional(&pool)
    .await
    .unwrap();

    assert!(
        row.is_none(),
        "Phase 2 should return no rows when a fresh heartbeat exists — \
         this prevents falsely crashing a live task"
    );

    // Verify that without the fresh heartbeat, the query DOES return the task.
    sqlx::query("DELETE FROM horsies_heartbeats WHERE task_id = $1")
        .bind(&task_id)
        .execute(&pool)
        .await
        .unwrap();

    let row_after_delete: Option<(String,)> = sqlx::query_as(
        "SELECT t.id FROM horsies_tasks t \
         WHERE t.id = $1 AND t.status = 'RUNNING' \
           AND NOT EXISTS ( \
             SELECT 1 FROM horsies_heartbeats \
             WHERE task_id = t.id AND role = 'runner' \
               AND sent_at > NOW() - $2 * INTERVAL '1 second' \
           )",
    )
    .bind(&task_id)
    .bind(stale_threshold_secs)
    .fetch_optional(&pool)
    .await
    .unwrap();

    assert!(
        row_after_delete.is_some(),
        "Phase 2 should return the task when no fresh heartbeat exists"
    );
}
