use super::{mark_task_finalizing, FinalizingMark, MARK_FINALIZING_SQL};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

/// A RUNNING plain task held by `current-worker`, with every column of the
/// baseline task row named. Returns the id and the claim generation.
async fn seed(pool: &PgPool) -> (Uuid, DateTime<Utc>) {
    let id = Uuid::new_v4();
    let generation = sqlx::query_scalar(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution,
            retention_class_key, sent_at, created_at,
            command_fingerprint_version, command_fingerprint, idempotency_key_digest,
            status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
            started_at, finalizing_at, finalizing_by_worker_id,
            worker_pid, worker_hostname, worker_process_name,
            retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
            updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
         ) VALUES (
            $1, 'finalizing_test', 'default', 100, '{}', NULL, 'ISOLATED',
            'forever', NOW() - interval '1 hour', NOW() - interval '1 hour',
            1, decode(repeat('00', 32), 'hex'), NULL,
            'RUNNING', NOW() - interval '1 hour', NOW() - interval '1 hour',
            'current-worker', NULL,
            NOW() - interval '1 hour', NOW() - interval '1 hour', 'current-worker',
            4242, 'finalizing-host', 'finalizing-process',
            0, 0, NULL, NULL, FALSE,
            NOW() - interval '1 hour', FALSE, NULL, NULL
         ) RETURNING claimed_at",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .unwrap();
    (id, generation)
}

async fn snapshot(pool: &PgPool, id: Uuid) -> serde_json::Value {
    sqlx::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn remove(pool: &PgPool, id: Uuid) {
    sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
#[serial]
async fn finalizing_handoff_rejects_stale_owner_and_generation() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let (id, generation) = seed(&pool).await;
    let before = snapshot(&pool, id).await;
    for (worker, claimed_at) in [
        ("stale-worker", generation),
        ("current-worker", generation - ChronoDuration::seconds(1)),
    ] {
        let mark = mark_task_finalizing(&pool, id, worker, claimed_at)
            .await
            .unwrap();
        assert_eq!(
            mark,
            FinalizingMark::NotHeld,
            "stale handoff must not refresh recovery grace"
        );
        assert_eq!(snapshot(&pool, id).await, before);
    }
    remove(&pool, id).await;
}

#[tokio::test]
#[serial]
async fn finalizing_handoff_commits_for_current_claim_and_blocks_stale_recovery() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let (id, generation) = seed(&pool).await;
    let before = snapshot(&pool, id).await;
    assert_eq!(
        mark_task_finalizing(&pool, id, "current-worker", generation)
            .await
            .unwrap(),
        FinalizingMark::Marked
    );
    let marked = snapshot(&pool, id).await;
    assert_ne!(marked["finalizing_at"], before["finalizing_at"]);
    assert_eq!(marked["finalizing_by_worker_id"], "current-worker");
    assert_eq!(marked["claimed_at"], before["claimed_at"]);

    let mut transaction = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM horsies_tasks WHERE id = $1 FOR UPDATE")
        .bind(id)
        .fetch_one(transaction.as_mut())
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    assert_eq!(snapshot(&pool, id).await, marked);

    let outcome: (String, String) = sqlx::query_as(
        "SELECT outcome, guard_kind FROM horsies_fail_stale_task(
            $1, 300000, 300000, '{}', 'WORKER_CRASHED', 'test'
         )",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        outcome,
        ("SOURCE_STATE_CONFLICT".into(), "STALENESS".into())
    );
    assert_eq!(snapshot(&pool, id).await, marked);
    remove(&pool, id).await;
}

/// Python `test_finalize_status_guard.py::test_the_finalizing_marker_has_no_null_fence`:
/// the marker compares the claim time with no NULL branch, so a NULL claim
/// time matches no row and nothing can turn off the generation half.
#[tokio::test]
#[serial]
async fn the_finalizing_marker_has_no_null_fence() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let (id, _) = seed(&pool).await;
    let before = snapshot(&pool, id).await;
    let changed = sqlx::query(MARK_FINALIZING_SQL)
        .bind(id)
        .bind("current-worker")
        .bind(None::<DateTime<Utc>>)
        .execute(&pool)
        .await
        .unwrap()
        .rows_affected();
    assert_eq!(changed, 0, "a NULL generation must match no row");
    assert_eq!(snapshot(&pool, id).await, before);
    remove(&pool, id).await;
}

/// The marker reads its row through the primary key whatever the
/// statistics say, as Python's
/// `test_task_row_plans.py::test_the_finalizing_marker_reads_its_row_through_the_primary_key`.
#[tokio::test]
#[serial]
async fn the_finalizing_marker_reads_its_row_through_the_primary_key() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let (id, generation) = seed(&pool).await;
    let mut transaction = pool.begin().await.unwrap();
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(transaction.as_mut())
        .await
        .unwrap();
    let plan: serde_json::Value = sqlx::query_scalar(&format!(
        "EXPLAIN (FORMAT JSON) {}",
        MARK_FINALIZING_SQL
            .replace("$1", &format!("'{id}'::uuid"))
            .replace("$2", "'current-worker'")
            .replace("$3", &format!("'{}'::timestamptz", generation.to_rfc3339()))
    ))
    .fetch_one(transaction.as_mut())
    .await
    .unwrap();
    transaction.rollback().await.unwrap();
    let plan_text = plan.to_string();
    assert!(
        plan_text.contains("horsies_tasks_pkey"),
        "the marker must read its row through the primary key: {plan_text}"
    );
    remove(&pool, id).await;
}

#[tokio::test]
#[serial]
async fn finalizing_handoff_ignores_non_running_and_missing_tasks() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let (id, generation) = seed(&pool).await;
    for status in ["PENDING", "CLAIMED"] {
        sqlx::query(
            "UPDATE horsies_tasks
             SET status = $2,
                 claim_expires_at = CASE WHEN $2 = 'CLAIMED'
                     THEN NOW() + interval '1 minute' END
             WHERE id = $1",
        )
        .bind(id)
        .bind(status)
        .execute(&pool)
        .await
        .unwrap();
        let before = snapshot(&pool, id).await;
        assert_eq!(
            mark_task_finalizing(&pool, id, "current-worker", generation)
                .await
                .unwrap(),
            FinalizingMark::NotHeld
        );
        assert_eq!(snapshot(&pool, id).await, before);
    }
    remove(&pool, id).await;
    assert_eq!(
        mark_task_finalizing(&pool, id, "current-worker", generation)
            .await
            .unwrap(),
        FinalizingMark::NotHeld
    );
}

#[tokio::test]
#[serial]
async fn finalizing_handoff_rechecks_owner_and_generation_after_lock_wait() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let waiting_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let waiting_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&waiting_pool)
        .await
        .unwrap();

    for (replacement_worker, generation_delta) in [("new-worker", 0), ("current-worker", 1)] {
        let (id, generation) = seed(&pool).await;
        let mut owner = pool.begin().await.unwrap();
        let owner_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(owner.as_mut())
            .await
            .unwrap();
        sqlx::query(
            "UPDATE horsies_tasks SET claimed_by_worker_id = $2,
                claimed_at = $3,
                finalizing_at = NOW(), finalizing_by_worker_id = $2
             WHERE id = $1",
        )
        .bind(id)
        .bind(replacement_worker)
        .bind(generation + ChronoDuration::seconds(generation_delta))
        .execute(owner.as_mut())
        .await
        .unwrap();
        let replacement: serde_json::Value =
            sqlx::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
                .bind(id)
                .fetch_one(owner.as_mut())
                .await
                .unwrap();

        let waiter_pool = waiting_pool.clone();
        let waiter = tokio::spawn(async move {
            mark_task_finalizing(&waiter_pool, id, "current-worker", generation).await
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let blocked: bool = sqlx::query_scalar("SELECT $1 = ANY(pg_blocking_pids($2))")
                    .bind(owner_pid)
                    .bind(waiting_pid)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                if blocked {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("marker must wait for the ownership-changing transaction");
        owner.commit().await.unwrap();
        let changed = tokio::time::timeout(Duration::from_secs(10), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(changed, FinalizingMark::NotHeld);
        assert_eq!(snapshot(&pool, id).await, replacement);
        remove(&pool, id).await;
    }
    waiting_pool.close().await;
}
