use std::time::Duration;

use serial_test::serial;
use tokio::time::timeout;
use uuid::Uuid;

use crate::core::lifecycle::{TerminalizationKind, TerminalizationOutcome};
use crate::test_support::sql::{self, TestListener, TestPool};
use crate::test_support::terminal_writes;

use crate::test_support::migrated_pool;

const TASK_STATUS_CHANNEL: &str = "horsies_task_status";
const TASK_DONE_CHANNEL: &str = "task_done";
const WORKFLOW_STATUS_CHANNEL: &str = "horsies_workflow_status";
const WORKER_STATE_CHANNEL: &str = "horsies_worker_state";

async fn next_notification(listener: &mut TestListener) -> tokio_postgres::Notification {
    timeout(Duration::from_secs(3), listener.recv())
        .await
        .expect("monitoring notification timed out")
        .expect("monitoring listener failed")
}

async fn assert_notification(listener: &mut TestListener, channel: &str, payload: &str) {
    let notification = next_notification(listener).await;
    assert_eq!(notification.channel(), channel);
    assert_eq!(notification.payload(), payload);
}

async fn seed_task(pool: &TestPool, task_id: Uuid, task_name: &str) {
    sql::query(
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
            $1, $2, 'default', 100, 'null', NULL, 'ISOLATED',
            'forever', NOW(), NOW(),
            1, decode(repeat('07', 32), 'hex'), NULL,
            'PENDING', NOW(), NULL, NULL, NULL,
            NULL, NULL, NULL,
            NULL, NULL, NULL,
            0, 0, NULL, NULL, FALSE,
            NOW(), FALSE, NULL, NULL
        )",
    )
    .bind(task_id)
    .bind(task_name)
    .execute(pool)
    .await
    .expect("seed W1 task");
}

async fn seed_workflow(pool: &TestPool, workflow_id: Uuid) {
    sql::query(
        "INSERT INTO horsies_workflows (
            id, name, status, on_error, output_task_index,
            definition_key, depth, root_workflow_id,
            sent_at, created_at, started_at, updated_at
        ) VALUES (
            $1, 'w1_trigger_workflow', 'RUNNING', 'fail', NULL,
            'test.w1.v1', 0, $1, NOW(), NOW(), NOW(), NOW()
        )",
    )
    .bind(workflow_id)
    .execute(pool)
    .await
    .expect("seed W1 workflow");
}

/// One snapshot with every NOT NULL column of the baseline
/// `horsies_worker_states` named: no slot down, no class budget.
async fn seed_worker_state(pool: &TestPool, worker_id: &str) {
    sql::query(
        "INSERT INTO horsies_worker_states (
            worker_id, snapshot_at, hostname, pid, processes,
            max_claim_batch, max_claim_per_worker, cluster_wide_cap, queues,
            queue_priorities, queue_max_concurrency, recovery_config,
            tasks_running, tasks_claimed, memory_usage_mb, memory_percent,
            cpu_percent, children_memory_mb, worker_started_at,
            slots_down, class_capacity
        ) VALUES (
            $1, NOW(), 'w1-host', 1, 1, 1, 1, NULL,
            ARRAY['default']::text[], '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
            0, 0, NULL, NULL,
            NULL, NULL, NOW(),
            '[]'::jsonb, '{}'::jsonb
        )",
    )
    .bind(worker_id)
    .execute(pool)
    .await
    .expect("seed W1 worker state");
}

async fn explain(pool: &TestPool, statement: &str) -> String {
    let mut transaction = pool.begin().await.expect("begin EXPLAIN transaction");
    sql::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *transaction)
        .await
        .expect("disable sequential scans");
    let rows = sql::query(&format!("EXPLAIN {statement}"))
        .fetch_all(&mut *transaction)
        .await
        .expect("EXPLAIN monitoring statement");
    transaction
        .rollback()
        .await
        .expect("rollback EXPLAIN transaction");
    rows.into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
#[serial]
async fn monitoring_indexes_match_v16_and_serve_reads_without_stealing_claims() {
    let pool = migrated_pool().await;

    // The baseline keeps one monitoring index on the live table. It drops
    // idx_horsies_tasks_task_name: its only reader was the facet statement of
    // a plan test, which production does not call.
    for (index_name, expected_column) in [("idx_horsies_tasks_enqueued_at", "enqueued_at")] {
        let shape: (String, bool, bool, bool, bool, i32, i32, String) = sql::query_as(
            "SELECT am.amname, i.indisvalid, i.indisready, i.indisunique,
                        i.indpred IS NULL, i.indnkeyatts::int, i.indnatts::int,
                        a.attname
                 FROM pg_index AS i
                 JOIN pg_class AS ic ON ic.oid = i.indexrelid
                 JOIN pg_am AS am ON am.oid = ic.relam
                 JOIN pg_attribute AS a
                   ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
                 WHERE ic.oid = to_regclass($1)",
        )
        .bind(index_name)
        .fetch_one(&pool)
        .await
        .expect("read W1 index shape");
        assert_eq!(
            shape,
            (
                "btree".to_owned(),
                true,
                true,
                false,
                true,
                1,
                1,
                expected_column.to_owned(),
            ),
            "unexpected catalog shape for {index_name}"
        );
    }

    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, execution, status,
            sent_at, enqueued_at, is_workflow_task,
            retry_count, max_retries,
            command_fingerprint_version, command_fingerprint,
            retention_class_key, retain_rerun_input, created_at, updated_at
        )
        SELECT gen_random_uuid(), 'w1_plan_task_' || (g % 5), 'default',
               100, 'null', 'ISOLATED', 'PENDING', NOW(),
               NOW() - (g || ' seconds')::interval, FALSE,
               0, 0, 1,
               decode(repeat('08', 32), 'hex'), 'forever', FALSE, NOW(), NOW()
        FROM generate_series(1, 2500) AS g",
    )
    .execute(&pool)
    .await
    .expect("seed W1 plan population");
    sql::query("ANALYZE horsies_tasks")
        .execute(&pool)
        .await
        .expect("analyze W1 plan population");

    let list_plan = explain(
        &pool,
        "SELECT id, task_name, enqueued_at
         FROM horsies_tasks
         ORDER BY enqueued_at DESC
         LIMIT 50",
    )
    .await;
    assert!(
        list_plan.contains("idx_horsies_tasks_enqueued_at"),
        "{list_plan}"
    );
    assert!(!list_plan.contains("Sort"), "{list_plan}");

    // The pending arm of horsies_claim_candidates
    // (0004_lifecycle_functions.sql): one queue and one execution class.
    let claim_plan = explain(
        &pool,
        "SELECT id
         FROM horsies_tasks
         WHERE queue_name = 'default'
           AND execution = 'ISOLATED'
           AND status = 'PENDING'
           AND enqueued_at <= now()
           AND (next_retry_at IS NULL OR next_retry_at <= now())
           AND (good_until IS NULL OR good_until > now())
         ORDER BY priority, enqueued_at, id
         FOR UPDATE SKIP LOCKED
         LIMIT 20",
    )
    .await;
    assert!(
        claim_plan.contains("horsies_tasks_claim_pending"),
        "{claim_plan}"
    );
    assert!(
        !claim_plan.contains("idx_horsies_tasks_enqueued_at"),
        "{claim_plan}"
    );

    sql::query("DELETE FROM horsies_tasks WHERE task_name LIKE 'w1_plan_task_%'")
        .execute(&pool)
        .await
        .expect("clean W1 plan population");
}

#[tokio::test]
#[serial]
async fn migration_0038_monitoring_triggers_deliver_each_channel() {
    let pool = migrated_pool().await;

    let task_id = Uuid::new_v4();
    let mut task_listener = TestListener::connect_with(&pool)
        .await
        .expect("connect task listener");
    task_listener
        .listen(TASK_STATUS_CHANNEL)
        .await
        .expect("listen task channel");
    seed_task(&pool, task_id, "w1_trigger_task").await;
    assert_notification(
        &mut task_listener,
        TASK_STATUS_CHANNEL,
        &task_id.to_string(),
    )
    .await;
    // A CLAIMED row carries its lease (horsies_tasks_status_live).
    sql::query(
        "UPDATE horsies_tasks
         SET status = 'CLAIMED', claim_expires_at = NOW() + interval '1 minute'
         WHERE id = $1",
    )
    .bind(task_id)
    .execute(&pool)
    .await
    .expect("update task status");
    assert_notification(
        &mut task_listener,
        TASK_STATUS_CHANNEL,
        &task_id.to_string(),
    )
    .await;

    let workflow_id = Uuid::new_v4();
    let mut workflow_listener = TestListener::connect_with(&pool)
        .await
        .expect("connect workflow listener");
    workflow_listener
        .listen(WORKFLOW_STATUS_CHANNEL)
        .await
        .expect("listen workflow channel");
    seed_workflow(&pool, workflow_id).await;
    assert_notification(
        &mut workflow_listener,
        WORKFLOW_STATUS_CHANNEL,
        &workflow_id.to_string(),
    )
    .await;
    sql::query("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = $1")
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect("update workflow status");
    assert_notification(
        &mut workflow_listener,
        WORKFLOW_STATUS_CHANNEL,
        &workflow_id.to_string(),
    )
    .await;

    let worker_id = format!("w1-worker-{}", Uuid::new_v4());
    let mut worker_listener = TestListener::connect_with(&pool)
        .await
        .expect("connect worker listener");
    worker_listener
        .listen(WORKER_STATE_CHANNEL)
        .await
        .expect("listen worker channel");
    seed_worker_state(&pool, &worker_id).await;
    assert_notification(&mut worker_listener, WORKER_STATE_CHANNEL, &worker_id).await;
    // The worker-state trigger fires on INSERT only (0001_tables.sql): an
    // update of a snapshot sends nothing. A marker sent after the update
    // commits is the next event on the channel; an event of the update
    // would arrive before it.
    sql::query(
        "UPDATE horsies_worker_states
         SET tasks_running = 1
         WHERE worker_id = $1",
    )
    .bind(&worker_id)
    .execute(&pool)
    .await
    .expect("update worker state");
    let marker = format!("{worker_id}-after-update");
    sql::query("SELECT pg_notify($1, $2)")
        .bind(WORKER_STATE_CHANNEL)
        .bind(&marker)
        .execute(&pool)
        .await
        .expect("send the marker event");
    assert_notification(&mut worker_listener, WORKER_STATE_CHANNEL, &marker).await;

    sql::query("DELETE FROM horsies_worker_states WHERE worker_id = $1")
        .bind(&worker_id)
        .execute(&pool)
        .await
        .expect("clean W1 worker state");
    sql::query("DELETE FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect("clean W1 workflow");
    sql::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("clean W1 task");
}

/// Python `tests/integration/test_status_events.py`: a status change other
/// than the start sends a `horsies_task_status` event; the start (to
/// RUNNING) sends none.
#[tokio::test]
#[serial]
async fn a_start_sends_no_status_event() {
    let pool = migrated_pool().await;
    let task_id = Uuid::new_v4();
    let mut listener = TestListener::connect_with(&pool)
        .await
        .expect("connect status listener");
    listener
        .listen(TASK_STATUS_CHANNEL)
        .await
        .expect("listen status channel");

    seed_task(&pool, task_id, "status_event_task").await;
    assert_notification(&mut listener, TASK_STATUS_CHANNEL, &task_id.to_string()).await;

    for (status, sends_event) in [("CLAIMED", true), ("RUNNING", false), ("PENDING", true)] {
        sql::query(
            "UPDATE horsies_tasks
             SET status = $2,
                 claim_expires_at = CASE WHEN $2 = 'CLAIMED'
                     THEN NOW() + interval '1 minute' END
             WHERE id = $1",
        )
        .bind(task_id)
        .bind(status)
        .execute(&pool)
        .await
        .expect("change status");
        match sends_event {
            true => {
                assert_notification(&mut listener, TASK_STATUS_CHANNEL, &task_id.to_string()).await
            }
            false => assert!(
                timeout(Duration::from_millis(250), listener.recv())
                    .await
                    .is_err(),
                "the start must send no status event"
            ),
        }
    }

    sql::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("clean status event task");
}

/// Python `tests/integration/test_status_events.py::test_a_completion_sends_the_status_event`
/// and the move notifications (`core/history/terminalization/move.py`): a
/// terminal move sends `task_done`, then the `horsies_task_status` event, so
/// the web app does not show a finished task as still live.
#[tokio::test]
#[serial]
async fn a_terminal_move_sends_task_done_and_the_status_event() {
    let pool = migrated_pool().await;
    let task_id = Uuid::new_v4();
    let mut listener = TestListener::connect_with(&pool)
        .await
        .expect("connect terminal-move listener");
    listener
        .listen(TASK_STATUS_CHANNEL)
        .await
        .expect("listen terminal-move channel");
    listener
        .listen(TASK_DONE_CHANNEL)
        .await
        .expect("listen task-done channel");

    seed_task(&pool, task_id, "w1_terminal_move_task").await;
    assert_notification(&mut listener, TASK_STATUS_CHANNEL, &task_id.to_string()).await;

    let outcomes = terminal_writes::cancel_live_task(&pool, task_id, false)
        .await
        .expect("move task to history");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied {
            kind: TerminalizationKind::CancelAdmin,
            ..
        }]
    ));
    assert_notification(&mut listener, TASK_DONE_CHANNEL, &task_id.to_string()).await;
    assert_notification(&mut listener, TASK_STATUS_CHANNEL, &task_id.to_string()).await;

    let live_count: i64 = sql::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .expect("count moved live task");
    let history_status: String =
        sql::query_scalar("SELECT status FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("read moved history task");
    assert_eq!(live_count, 0);
    assert_eq!(history_status, "CANCELLED");

    sql::query("DELETE FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("clean W1 history task");
}
