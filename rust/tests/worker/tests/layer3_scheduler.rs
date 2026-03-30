#![allow(clippy::unwrap_used)]

//! Layer 3 e2e tests: scheduler functionality.
//!
//! Mirrors Python's `tests/e2e/test_layer3_scheduler.py`.
//!
//! Run with:
//!   cargo test -p horsies-test-worker --test layer3_scheduler -- --test-threads=1

use std::io::Write;
use std::time::Duration;

use serial_test::serial;
use sqlx::PgPool;

use horsies_test_support::{
    db,
    e2e::worker::{start_scheduler, start_worker},
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

/// Write a scheduler config with a 2s interval schedule.
fn write_scheduler_config() -> tempfile::NamedTempFile {
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
        "schedule": {
            "enabled": true,
            "check_interval_seconds": 1,
            "schedules": [
                {
                    "name": "e2e_scheduled_simple_interval",
                    "task_name": "e2e_scheduled_simple",
                    "pattern": {"type": "interval", "seconds": 2},
                    "timezone": "UTC",
                    "catch_up_missed": false,
                    "max_catch_up_runs": 100
                },
                {
                    "name": "e2e_disabled_schedule",
                    "task_name": "e2e_scheduled_simple",
                    "pattern": {"type": "interval", "seconds": 1},
                    "enabled": false,
                    "max_catch_up_runs": 100
                },
                {
                    "name": "e2e_schedule_with_args",
                    "task_name": "e2e_scheduled_with_args",
                    "pattern": {"type": "interval", "seconds": 2},
                    "kwargs": {"x": 42},
                    "max_catch_up_runs": 100
                },
                {
                    "name": "e2e_schedule_catch_up",
                    "task_name": "e2e_catch_up_task",
                    "pattern": {"type": "interval", "seconds": 1},
                    "catch_up_missed": true,
                    "max_catch_up_runs": 100
                }
            ]
        },
        "resend_on_transient_err": false
    });
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(serde_json::to_string_pretty(&config).unwrap().as_bytes())
        .unwrap();
    f.flush().unwrap();
    f
}

/// Wait for a schedule_state row to appear with the given schedule name.
async fn wait_for_schedule_state(pool: &PgPool, schedule_name: &str, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM horsies_schedule_state WHERE schedule_name = $1)",
        )
        .bind(schedule_name)
        .fetch_one(pool)
        .await
        .unwrap_or(false);
        if exists {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Wait for run_count to reach at least `min_count` for a schedule.
async fn wait_for_run_count(
    pool: &PgPool,
    schedule_name: &str,
    min_count: i32,
    timeout: Duration,
) -> i32 {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_count = 0;
    while tokio::time::Instant::now() < deadline {
        let count: i32 = sqlx::query_scalar(
            "SELECT run_count FROM horsies_schedule_state WHERE schedule_name = $1",
        )
        .bind(schedule_name)
        .fetch_optional(pool)
        .await
        .unwrap()
        .unwrap_or(0);
        last_count = count;
        if count >= min_count {
            return count;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    last_count
}

// ---------------------------------------------------------------------------
// T3.1: Schedule Persistence
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_schedule_persistence() {
    let pool = pool().await;
    db::clean_tables(&pool).await;

    let config_file = write_scheduler_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let _scheduler = start_scheduler(&config_path, "scheduler started", Duration::from_secs(10));

    let found = wait_for_schedule_state(
        &pool,
        "e2e_scheduled_simple_interval",
        Duration::from_secs(5),
    )
    .await;
    assert!(found, "schedule state should exist in DB");

    let (next_run_at, run_count): (Option<chrono::DateTime<chrono::Utc>>, i32) = sqlx::query_as(
        "SELECT next_run_at, run_count FROM horsies_schedule_state WHERE schedule_name = $1",
    )
    .bind("e2e_scheduled_simple_interval")
    .fetch_one(&pool)
    .await
    .unwrap();

    assert!(next_run_at.is_some(), "next_run_at should be set");
    assert!(
        next_run_at.unwrap() > chrono::Utc::now() - chrono::Duration::seconds(1),
        "next_run_at should be recent/future"
    );
    assert_eq!(run_count, 0, "run_count should be 0 initially");
}

// ---------------------------------------------------------------------------
// T3.2: Schedule Executes After Due Time
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_schedule_executes_after_due_time() {
    let pool = pool().await;
    db::clean_tables(&pool).await;

    let config_file = write_scheduler_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let start_time = chrono::Utc::now();

    let _scheduler = start_scheduler(&config_path, "scheduler started", Duration::from_secs(10));
    let _worker = start_worker(
        &config_path,
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // Wait for a task to appear.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut task_found = false;
    while tokio::time::Instant::now() < deadline {
        let created: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT created_at FROM horsies_tasks WHERE task_name = 'e2e_scheduled_simple' ORDER BY created_at ASC LIMIT 1",
        )
        .fetch_optional(&pool)
        .await
        .unwrap()
        .flatten();

        if let Some(created_at) = created {
            assert!(
                created_at >= start_time - chrono::Duration::seconds(1),
                "task created before start"
            );
            task_found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(task_found, "scheduled task should have been created");
}

// ---------------------------------------------------------------------------
// T3.3: Schedule Does Not Execute Early
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_schedule_does_not_execute_early() {
    let pool = pool().await;
    db::clean_tables(&pool).await;

    let config_file = write_scheduler_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let _scheduler = start_scheduler(&config_path, "scheduler started", Duration::from_secs(10));

    // Wait for scheduler init.
    let found = wait_for_schedule_state(
        &pool,
        "e2e_scheduled_simple_interval",
        Duration::from_secs(5),
    )
    .await;
    assert!(found, "scheduler should have initialized state");

    // No tasks should exist yet (interval is 2s, we just started).
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM horsies_tasks WHERE task_name = 'e2e_scheduled_simple'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 0, "no tasks should be created before next_run_at");
}

// ---------------------------------------------------------------------------
// T3.4: Repeating Schedule
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_repeating_schedule() {
    let pool = pool().await;
    db::clean_tables(&pool).await;

    let config_file = write_scheduler_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let _scheduler = start_scheduler(&config_path, "scheduler started", Duration::from_secs(10));
    let _worker = start_worker(
        &config_path,
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    let run_count = wait_for_run_count(
        &pool,
        "e2e_scheduled_simple_interval",
        2,
        Duration::from_secs(15),
    )
    .await;
    assert!(
        run_count >= 2,
        "run_count should be >= 2, got {}",
        run_count
    );

    let task_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM horsies_tasks WHERE task_name = 'e2e_scheduled_simple'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        task_count >= 2,
        "expected at least 2 scheduled tasks, got {}",
        task_count
    );
}

// ---------------------------------------------------------------------------
// T3.7: Disabled Schedule Does Not Fire
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_disabled_schedule_does_not_fire() {
    let pool = pool().await;
    db::clean_tables(&pool).await;

    let config_file = write_scheduler_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let _scheduler = start_scheduler(&config_path, "scheduler started", Duration::from_secs(10));

    // Wait for an enabled schedule to confirm scheduler is running.
    let found = wait_for_schedule_state(
        &pool,
        "e2e_scheduled_simple_interval",
        Duration::from_secs(5),
    )
    .await;
    assert!(found, "scheduler should be running");

    // Disabled schedule should NOT have a state row.
    let disabled_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM horsies_schedule_state WHERE schedule_name = 'e2e_disabled_schedule')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        !disabled_exists,
        "disabled schedule should not have a state row"
    );
}

// ---------------------------------------------------------------------------
// T3.8: Schedule With Args Passes Arguments
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_schedule_with_args_passes_arguments() {
    let pool = pool().await;
    db::clean_tables(&pool).await;

    let config_file = write_scheduler_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    let _scheduler = start_scheduler(&config_path, "scheduler started", Duration::from_secs(10));
    let _worker = start_worker(
        &config_path,
        &["--concurrency", "2"],
        "worker started",
        Duration::from_secs(10),
    );

    // Wait for at least 1 run.
    let run_count =
        wait_for_run_count(&pool, "e2e_schedule_with_args", 1, Duration::from_secs(15)).await;
    assert!(run_count >= 1, "schedule should have run at least once");

    // Find completed task with result containing 84 (42 * 2).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut found = false;
    while tokio::time::Instant::now() < deadline {
        let result: Option<String> = sqlx::query_scalar(
            "SELECT result FROM horsies_tasks WHERE task_name = 'e2e_scheduled_with_args' AND status = 'COMPLETED' LIMIT 1",
        )
        .fetch_optional(&pool)
        .await
        .unwrap()
        .flatten();

        if let Some(r) = result {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&r) {
                if v.get("value").and_then(|v| v.as_i64()) == Some(84) {
                    found = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(
        found,
        "scheduled task with args should complete with result containing 84"
    );
}

// ---------------------------------------------------------------------------
// T3.9: Config Hash Change Recalculates Next Run
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_config_hash_change_recalculates_next_run() {
    let pool = pool().await;
    db::clean_tables(&pool).await;

    let config_file = write_scheduler_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    // Phase 1: Start scheduler, record state.
    {
        let _scheduler =
            start_scheduler(&config_path, "scheduler started", Duration::from_secs(10));
        let found = wait_for_schedule_state(
            &pool,
            "e2e_scheduled_simple_interval",
            Duration::from_secs(5),
        )
        .await;
        assert!(found);
    }

    let original_hash: Option<String> = sqlx::query_scalar(
        "SELECT config_hash FROM horsies_schedule_state WHERE schedule_name = 'e2e_scheduled_simple_interval'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // Phase 2: Corrupt the config_hash.
    sqlx::query("UPDATE horsies_schedule_state SET config_hash = 'corrupted' WHERE schedule_name = 'e2e_scheduled_simple_interval'")
        .execute(&pool)
        .await
        .unwrap();

    // Phase 3: Restart scheduler and verify hash is corrected.
    {
        let _scheduler =
            start_scheduler(&config_path, "scheduler started", Duration::from_secs(10));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut corrected = false;
        while tokio::time::Instant::now() < deadline {
            let hash: Option<String> = sqlx::query_scalar(
                "SELECT config_hash FROM horsies_schedule_state WHERE schedule_name = 'e2e_scheduled_simple_interval'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();

            if hash.as_deref() != Some("corrupted") {
                assert_eq!(hash, original_hash, "hash should be restored to original");
                corrected = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            corrected,
            "scheduler should recalculate config_hash on restart"
        );
    }
}

// ---------------------------------------------------------------------------
// T3.10: Catch-Up Missed Runs
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_catch_up_missed_enqueues_missed_runs() {
    let pool = pool().await;
    db::clean_tables(&pool).await;

    let config_file = write_scheduler_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    // Phase 1: Start scheduler to initialize state.
    {
        let _scheduler =
            start_scheduler(&config_path, "scheduler started", Duration::from_secs(10));
        let found =
            wait_for_schedule_state(&pool, "e2e_schedule_catch_up", Duration::from_secs(5)).await;
        assert!(found, "catch-up schedule state should be initialized");
    }

    // Phase 2: Backdate next_run_at by 5 seconds to simulate downtime.
    let past_time = chrono::Utc::now() - chrono::Duration::seconds(5);
    sqlx::query("UPDATE horsies_schedule_state SET next_run_at = $1 WHERE schedule_name = 'e2e_schedule_catch_up'")
        .bind(past_time)
        .execute(&pool)
        .await
        .unwrap();

    let tasks_before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM horsies_tasks WHERE task_name = 'e2e_catch_up_task'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // Phase 3: Restart with worker — should catch up.
    {
        let _scheduler =
            start_scheduler(&config_path, "scheduler started", Duration::from_secs(10));
        let _worker = start_worker(
            &config_path,
            &["--concurrency", "2"],
            "worker started",
            Duration::from_secs(10),
        );

        // Wait for next_run_at to advance past the backdated value.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut advanced = false;
        while tokio::time::Instant::now() < deadline {
            let next: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
                "SELECT next_run_at FROM horsies_schedule_state WHERE schedule_name = 'e2e_schedule_catch_up'",
            )
            .fetch_optional(&pool)
            .await
            .unwrap()
            .flatten();

            if let Some(n) = next {
                if n > past_time {
                    advanced = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        assert!(advanced, "scheduler should advance next_run_at");

        let tasks_after: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_tasks WHERE task_name = 'e2e_catch_up_task'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let new_tasks = tasks_after - tasks_before;
        assert!(
            new_tasks >= 2,
            "catch-up should enqueue at least 2 tasks, got {}",
            new_tasks
        );
    }
}

// ---------------------------------------------------------------------------
// T3.11: Scheduler Restart Reuses Existing State
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_scheduler_restart_reuses_existing_state() {
    let pool = pool().await;
    db::clean_tables(&pool).await;

    let config_file = write_scheduler_config();
    let config_path = config_file.path().to_str().unwrap().to_owned();

    // Phase 1: Start scheduler + worker, let it run at least once.
    let run_count_before;
    let hash_before;
    {
        let _scheduler =
            start_scheduler(&config_path, "scheduler started", Duration::from_secs(10));
        let _worker = start_worker(
            &config_path,
            &["--concurrency", "2"],
            "worker started",
            Duration::from_secs(10),
        );

        let count = wait_for_run_count(
            &pool,
            "e2e_scheduled_simple_interval",
            1,
            Duration::from_secs(8),
        )
        .await;
        assert!(
            count >= 1,
            "schedule should have run at least once, got {}",
            count
        );
        run_count_before = count;

        hash_before = sqlx::query_scalar::<_, Option<String>>(
            "SELECT config_hash FROM horsies_schedule_state WHERE schedule_name = 'e2e_scheduled_simple_interval'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
    }

    // Phase 2: Restart scheduler and verify state preserved.
    {
        let _scheduler =
            start_scheduler(&config_path, "scheduler started", Duration::from_secs(10));
        tokio::time::sleep(Duration::from_secs(1)).await;

        let (run_count_after, hash_after): (i32, Option<String>) = sqlx::query_as(
            "SELECT run_count, config_hash FROM horsies_schedule_state WHERE schedule_name = 'e2e_scheduled_simple_interval'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(
            run_count_after >= run_count_before,
            "run_count should not decrease: before={}, after={}",
            run_count_before,
            run_count_after,
        );
        assert_eq!(
            hash_after, hash_before,
            "config_hash should be unchanged after restart"
        );
    }
}
