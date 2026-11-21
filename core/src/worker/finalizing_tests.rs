use super::{mark_task_finalizing, FinalizingMark, MARK_FINALIZING_SQL};
use crate::broker::PostgresBroker;
use crate::test_support::sql::{self, TestPool};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serial_test::serial;
use std::time::Duration;
use uuid::Uuid;

/// A RUNNING plain task held by `current-worker`, with every column of the
/// baseline task row named. Returns the id and the claim generation.
async fn seed(pool: &TestPool) -> (Uuid, DateTime<Utc>) {
    let id = Uuid::new_v4();
    let generation = sql::query_scalar(
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

async fn snapshot(pool: &TestPool, id: Uuid) -> serde_json::Value {
    sql::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// A broker on the test database: the marker runs on its main core pool.
fn broker(pool: &TestPool) -> PostgresBroker {
    PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust)
}

async fn remove(pool: &TestPool, id: Uuid) {
    sql::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
#[serial]
async fn finalizing_handoff_rejects_stale_owner_and_generation() {
    let pool = crate::test_support::migrated_pool().await;
    let (id, generation) = seed(&pool).await;
    let before = snapshot(&pool, id).await;
    for (worker, claimed_at) in [
        ("stale-worker", generation),
        ("current-worker", generation - ChronoDuration::seconds(1)),
    ] {
        let mark = mark_task_finalizing(&broker(&pool), id, worker, claimed_at)
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
    let pool = crate::test_support::migrated_pool().await;
    let (id, generation) = seed(&pool).await;
    let before = snapshot(&pool, id).await;
    assert_eq!(
        mark_task_finalizing(&broker(&pool), id, "current-worker", generation)
            .await
            .unwrap(),
        FinalizingMark::Marked
    );
    let marked = snapshot(&pool, id).await;
    assert_ne!(marked["finalizing_at"], before["finalizing_at"]);
    assert_eq!(marked["finalizing_by_worker_id"], "current-worker");
    assert_eq!(marked["claimed_at"], before["claimed_at"]);

    let mut transaction = pool.begin().await.unwrap();
    sql::query("SELECT id FROM horsies_tasks WHERE id = $1 FOR UPDATE")
        .bind(id)
        .fetch_one(&mut *transaction)
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    assert_eq!(snapshot(&pool, id).await, marked);

    let outcome: (String, String) = sql::query_as(
        "SELECT outcome, guard_kind FROM horsies_finish_stale_attempt(
            $1, 'BEAT_AGE', 300000, 300000, NULL, NULL, 'NONE', NULL, NULL, '{}', 'WORKER_CRASHED', 'test', 'test', 'test'
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

/// `horsies_mark_finalizing` requires the claim time, so a NULL claim time
/// is refused with 22023 and nothing can turn off the generation half.
#[tokio::test]
#[serial]
async fn the_finalizing_marker_refuses_a_null_claim_generation() {
    let pool = crate::test_support::migrated_pool().await;
    let (id, _) = seed(&pool).await;
    let before = snapshot(&pool, id).await;
    let refused = sql::query(MARK_FINALIZING_SQL)
        .bind(id)
        .bind("current-worker")
        .bind(None::<DateTime<Utc>>)
        .fetch_all(&pool)
        .await
        .expect_err("a NULL claim generation is refused");
    let sql::Error::Database(database_error) = refused else {
        panic!("a NULL claim generation is a database refusal: {refused:?}");
    };
    assert_eq!(database_error.code().code(), "22023");
    assert_eq!(snapshot(&pool, id).await, before);
    remove(&pool, id).await;
}

/// The installed marker, as `regprocedure` text.
const MARKER_FUNCTION: &str = "horsies_mark_finalizing(uuid,text,timestamp with time zone)";
/// First and last words of the marker's row read in its body.
const ROW_READ_START: &str = "SELECT (";
const ROW_READ_END: &str = "FOR NO KEY UPDATE";
/// The plpgsql target of the row read, which plain SQL does not take.
const ROW_READ_TARGET: &str = "INTO v_held";

/// The row read of the installed marker: the statement from
/// [`ROW_READ_START`] to [`ROW_READ_END`], without its plpgsql target, with
/// the arguments as literals.
fn marker_row_read(definition: &str, id: Uuid, generation: DateTime<Utc>) -> String {
    let start = definition
        .find(ROW_READ_START)
        .expect("the marker reads its task row");
    let length = definition[start..]
        .find(ROW_READ_END)
        .expect("the marker locks its task row")
        + ROW_READ_END.len();
    let statement = &definition[start..start + length];
    assert!(
        statement.contains(ROW_READ_TARGET),
        "the row read of the marker has no {ROW_READ_TARGET:?}: {statement}"
    );
    statement
        .replace(ROW_READ_TARGET, "")
        .replace("p_task_id", &format!("'{id}'::uuid"))
        .replace("p_worker_id", "'current-worker'")
        .replace(
            "p_claimed_at",
            &format!("'{}'::timestamptz", generation.to_rfc3339()),
        )
}

/// The marker reads its row through the primary key whatever the
/// statistics say, as Python's
/// `test_task_row_plans.py::test_the_finalizing_marker_reads_its_row_through_the_primary_key`:
/// the row read of the installed `horsies_mark_finalizing`.
#[tokio::test]
#[serial]
async fn the_finalizing_marker_reads_its_row_through_the_primary_key() {
    let pool = crate::test_support::migrated_pool().await;
    let (id, generation) = seed(&pool).await;
    let definition: String = sql::query_scalar("SELECT pg_get_functiondef($1::regprocedure)")
        .bind(MARKER_FUNCTION)
        .fetch_one(&pool)
        .await
        .unwrap();
    let statement = marker_row_read(&definition, id, generation);
    let mut transaction = pool.begin().await.unwrap();
    sql::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *transaction)
        .await
        .unwrap();
    let plan: serde_json::Value = sql::query_scalar(&format!("EXPLAIN (FORMAT JSON) {statement}"))
        .fetch_one(&mut *transaction)
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
    let pool = crate::test_support::migrated_pool().await;
    let (id, generation) = seed(&pool).await;
    for status in ["PENDING", "CLAIMED"] {
        sql::query(
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
            mark_task_finalizing(&broker(&pool), id, "current-worker", generation)
                .await
                .unwrap(),
            FinalizingMark::NotHeld
        );
        assert_eq!(snapshot(&pool, id).await, before);
    }
    remove(&pool, id).await;
    assert_eq!(
        mark_task_finalizing(&broker(&pool), id, "current-worker", generation)
            .await
            .unwrap(),
        FinalizingMark::NotHeld
    );
}

#[tokio::test]
#[serial]
async fn finalizing_handoff_rechecks_owner_and_generation_after_lock_wait() {
    let pool = crate::test_support::migrated_pool().await;
    let waiting_broker = std::sync::Arc::new(broker(&pool));

    for (replacement_worker, generation_delta) in [("new-worker", 0), ("current-worker", 1)] {
        let (id, generation) = seed(&pool).await;
        let mut owner = pool.begin().await.unwrap();
        let owner_pid: i32 = sql::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *owner)
            .await
            .unwrap();
        sql::query(
            "UPDATE horsies_tasks SET claimed_by_worker_id = $2,
                claimed_at = $3,
                finalizing_at = NOW(), finalizing_by_worker_id = $2
             WHERE id = $1",
        )
        .bind(id)
        .bind(replacement_worker)
        .bind(generation + ChronoDuration::seconds(generation_delta))
        .execute(&mut *owner)
        .await
        .unwrap();
        let replacement: serde_json::Value =
            sql::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
                .bind(id)
                .fetch_one(&mut *owner)
                .await
                .unwrap();

        let waiter_broker = std::sync::Arc::clone(&waiting_broker);
        let waiter = tokio::spawn(async move {
            mark_task_finalizing(&waiter_broker, id, "current-worker", generation).await
        });
        // The tests are serial: the only session that the owner can block
        // is the marker's.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let blocked: bool = sql::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
                     WHERE $1 = ANY(pg_blocking_pids(pid)))",
                )
                .bind(owner_pid)
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
}
