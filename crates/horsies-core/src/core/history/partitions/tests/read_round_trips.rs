//! Ports of the Python round-trip tests of the result read: a result read is
//! one statement, for a moved task and for a live one.
//!
//! Python specification: `tests/integration/test_task_result_history_fallback.py`
//! (`TestStagedResultReader`). The tests live here for the statement-counting
//! proxy of this module.

use super::*;
use crate::broker::terminalization::terminalize;
use crate::core::history::reads::result::{read_task_result, TaskResultRead};
use crate::core::lifecycle::{PriorLockedRead, TerminalizationCommand, TerminalizationOutcome};

const PORT_WORKER: &str = "port-round-trip-worker";

/// A plain task of `status`, claimed by `PORT_WORKER` when RUNNING. Every
/// baseline column is named.
async fn seed_task(pool: &PgPool, task_id: Uuid, status: &str) {
    sqlx::query(
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
             $1, 'port.round_trip', 'default', 100, '{}', NULL, 'ISOLATED',
             'standard_30d', NOW(), NOW(),
             1, decode(repeat('0c', 32), 'hex'), NULL,
             $2, NOW(),
             CASE WHEN $2 = 'RUNNING' THEN NOW() END,
             CASE WHEN $2 = 'RUNNING' THEN $3 END,
             NULL,
             CASE WHEN $2 = 'RUNNING' THEN NOW() END, NULL, NULL,
             NULL, NULL, NULL,
             0, 0, NULL, NULL, FALSE,
             NOW(), FALSE, NULL, NULL
         )",
    )
    .bind(task_id)
    .bind(status)
    .bind(PORT_WORKER)
    .execute(pool)
    .await
    .expect("insert task");
}

async fn published_database() -> TestDatabase {
    let database = TestDatabase::create_with_connections(3).await;
    let coverage =
        ensure_partition_coverage_in_pool(&database.pool, 2, 2, &[], &StagedLoaderPublisher)
            .await
            .expect("publish the readers");
    assert!(
        matches!(coverage, CoverageOutcome::Ensured(_)),
        "{coverage:?}"
    );
    database
}

/// `TestStagedResultReader.test_a_history_result_is_one_statement`.
#[tokio::test]
#[serial]
async fn a_history_result_read_is_one_statement() {
    let database = published_database().await;
    let task_id = crate::core::history::identity::uuid7::mint_task_id().expect("mint a task id");
    seed_task(&database.pool, task_id, "RUNNING").await;
    let outcomes = terminalize(
        &database.pool,
        &TerminalizationCommand::CompleteLockedTask {
            task_id,
            fence: PriorLockedRead {
                worker_id: PORT_WORKER.to_owned(),
            },
            result_json: "{\"Ok\":true}".to_owned(),
        },
    )
    .await
    .expect("complete the task");
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{outcomes:?}"
    );
    let (proxy, pool) = proxied_pool(&database, 0, 1).await;
    let mut connection = pool.acquire().await.expect("acquire reader");
    proxy.reset();
    let read = read_task_result(&mut connection, task_id)
        .await
        .expect("read the result");
    assert!(matches!(read, TaskResultRead::History(_)), "{read:?}");
    assert_eq!(proxy.statement_count(), 1, "{:?}", proxy.sql());
    drop(connection);
    pool.close().await;
    proxy.stop().await;
    database.drop().await;
}

/// `TestStagedResultReader.test_a_live_task_is_one_statement`.
#[tokio::test]
#[serial]
async fn a_live_result_read_is_one_statement() {
    let database = published_database().await;
    let task_id = Uuid::new_v4();
    seed_task(&database.pool, task_id, "PENDING").await;
    let (proxy, pool) = proxied_pool(&database, 0, 1).await;
    let mut connection = pool.acquire().await.expect("acquire reader");
    proxy.reset();
    let read = read_task_result(&mut connection, task_id)
        .await
        .expect("read the result");
    assert!(matches!(read, TaskResultRead::Live(_)), "{read:?}");
    assert_eq!(proxy.statement_count(), 1, "{:?}", proxy.sql());
    drop(connection);
    pool.close().await;
    proxy.stop().await;
    database.drop().await;
}
