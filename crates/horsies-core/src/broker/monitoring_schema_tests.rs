use std::time::Duration;

use serial_test::serial;
use sqlx::postgres::{PgListener, PgNotification};
use sqlx::{PgPool, Row};
use tokio::time::timeout;
use uuid::Uuid;

use crate::broker::terminalization::terminalize;
use crate::core::lifecycle::{
    CallerHoldsRowLock, TerminalizationCommand, TerminalizationKind, TerminalizationOutcome,
};
use crate::core::types::status::TaskStatus;

use super::terminalization_matrix::migrated_pool;

const TASK_STATUS_CHANNEL: &str = "horsies_task_status";
const TASK_DONE_CHANNEL: &str = "task_done";
const WORKFLOW_STATUS_CHANNEL: &str = "horsies_workflow_status";
const WORKER_STATE_CHANNEL: &str = "horsies_worker_state";

async fn next_notification(listener: &mut PgListener) -> PgNotification {
    timeout(Duration::from_secs(3), listener.recv())
        .await
        .expect("monitoring notification timed out")
        .expect("monitoring listener failed")
}

async fn assert_notification(listener: &mut PgListener, channel: &str, payload: &str) {
    let notification = next_notification(listener).await;
    assert_eq!(notification.channel(), channel);
    assert_eq!(notification.payload(), payload);
}

async fn seed_task(pool: &PgPool, task_id: Uuid, task_name: &str) {
    sqlx::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, args, kwargs, status,
            sent_at, enqueued_at, claimed, is_workflow_task,
            retry_count, max_retries, enqueue_sha,
            command_fingerprint_version, command_fingerprint,
            retention_class_key, retain_rerun_input,
            prepared_rerun_input_disposition, created_at, updated_at
        ) VALUES (
            $1, $2, 'default', 100, '[]', '{}', 'PENDING',
            NOW(), NOW(), FALSE, FALSE, 0, 0, $1::text,
            1, decode(repeat('07', 32), 'hex'), 'forever', FALSE,
            'DECLINED_BY_POLICY', NOW(), NOW()
        )",
    )
    .bind(task_id)
    .bind(task_name)
    .execute(pool)
    .await
    .expect("seed W1 task");
}

async fn seed_workflow(pool: &PgPool, workflow_id: Uuid) {
    sqlx::query(
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

async fn seed_worker_state(pool: &PgPool, worker_id: &str) {
    sqlx::query(
        "INSERT INTO horsies_worker_states (
            worker_id, snapshot_at, hostname, pid, processes,
            max_claim_batch, max_claim_per_worker, queues,
            tasks_running, tasks_claimed, worker_started_at
        ) VALUES (
            $1, NOW(), 'w1-host', 1, 1, 1, 1,
            ARRAY['default']::text[], 0, 0, NOW()
        )",
    )
    .bind(worker_id)
    .execute(pool)
    .await
    .expect("seed W1 worker state");
}

async fn explain(pool: &PgPool, statement: &str) -> String {
    let mut transaction = pool.begin().await.expect("begin EXPLAIN transaction");
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *transaction)
        .await
        .expect("disable sequential scans");
    let rows = sqlx::query(&format!("EXPLAIN {statement}"))
        .fetch_all(&mut *transaction)
        .await
        .expect("EXPLAIN monitoring statement");
    transaction
        .rollback()
        .await
        .expect("rollback EXPLAIN transaction");
    rows.into_iter()
        .map(|row| row.get::<String, _>(0))
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
#[serial]
async fn monitoring_indexes_match_v16_and_serve_reads_without_stealing_claims() {
    let pool = migrated_pool().await;

    for (index_name, expected_column) in [
        ("idx_horsies_tasks_enqueued_at", "enqueued_at"),
        ("idx_horsies_tasks_task_name", "task_name"),
    ] {
        let shape: (String, bool, bool, bool, bool, i32, i32, String) = sqlx::query_as(
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

    sqlx::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, args, kwargs, status,
            sent_at, enqueued_at, claimed, is_workflow_task,
            retry_count, max_retries, enqueue_sha,
            command_fingerprint_version, command_fingerprint,
            retention_class_key, retain_rerun_input,
            prepared_rerun_input_disposition, created_at, updated_at
        )
        SELECT gen_random_uuid(), 'w1_plan_task_' || (g % 5), 'default',
               100, '[]', '{}', 'PENDING', NOW(),
               NOW() - (g || ' seconds')::interval, FALSE, FALSE,
               0, 0, gen_random_uuid()::text, 1,
               decode(repeat('08', 32), 'hex'), 'forever', FALSE,
               'DECLINED_BY_POLICY', NOW(), NOW()
        FROM generate_series(1, 2500) AS g",
    )
    .execute(&pool)
    .await
    .expect("seed W1 plan population");
    sqlx::query("ANALYZE horsies_tasks")
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

    let facet_plan = explain(
        &pool,
        "SELECT task_name, count(*)
         FROM horsies_tasks
         GROUP BY task_name
         ORDER BY task_name",
    )
    .await;
    assert!(
        facet_plan.contains("Index Only Scan using idx_horsies_tasks_task_name"),
        "{facet_plan}"
    );

    let claim_plan = explain(
        &pool,
        "SELECT id
         FROM horsies_tasks
         WHERE queue_name = 'default'
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
        claim_plan.contains("idx_horsies_tasks_claim_pending"),
        "{claim_plan}"
    );
    assert!(
        !claim_plan.contains("idx_horsies_tasks_enqueued_at")
            && !claim_plan.contains("idx_horsies_tasks_task_name"),
        "{claim_plan}"
    );

    sqlx::query("DELETE FROM horsies_tasks WHERE task_name LIKE 'w1_plan_task_%'")
        .execute(&pool)
        .await
        .expect("clean W1 plan population");
}

#[tokio::test]
#[serial]
async fn migration_0038_monitoring_triggers_deliver_each_channel() {
    let pool = migrated_pool().await;

    let task_id = Uuid::new_v4();
    let mut task_listener = PgListener::connect_with(&pool)
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
    sqlx::query("UPDATE horsies_tasks SET status = 'CLAIMED' WHERE id = $1")
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
    let mut workflow_listener = PgListener::connect_with(&pool)
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
    sqlx::query("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = $1")
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
    let mut worker_listener = PgListener::connect_with(&pool)
        .await
        .expect("connect worker listener");
    worker_listener
        .listen(WORKER_STATE_CHANNEL)
        .await
        .expect("listen worker channel");
    seed_worker_state(&pool, &worker_id).await;
    assert_notification(&mut worker_listener, WORKER_STATE_CHANNEL, &worker_id).await;
    sqlx::query(
        "UPDATE horsies_worker_states
         SET tasks_running = 1
         WHERE worker_id = $1",
    )
    .bind(&worker_id)
    .execute(&pool)
    .await
    .expect("update worker state");
    assert_notification(&mut worker_listener, WORKER_STATE_CHANNEL, &worker_id).await;

    sqlx::query("DELETE FROM horsies_worker_states WHERE worker_id = $1")
        .bind(&worker_id)
        .execute(&pool)
        .await
        .expect("clean W1 worker state");
    sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect("clean W1 workflow");
    sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("clean W1 task");
}

#[tokio::test]
#[serial]
async fn terminal_move_emits_task_done_without_a_monitoring_status_event() {
    let pool = migrated_pool().await;
    let task_id = Uuid::new_v4();
    let mut listener = PgListener::connect_with(&pool)
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

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::CancelLockedTask {
            task_id,
            fence: CallerHoldsRowLock,
            permitted_source_statuses: vec![TaskStatus::Pending],
        },
    )
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
    assert!(
        timeout(Duration::from_millis(250), listener.recv())
            .await
            .is_err(),
        "pinned Python emits no horsies_task_status event for a live-row delete"
    );

    let live_count: i64 = sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .expect("count moved live task");
    let history_status: String =
        sqlx::query_scalar("SELECT status FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("read moved history task");
    assert_eq!(live_count, 0);
    assert_eq!(history_status, "CANCELLED");

    sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("clean W1 history task");
}
