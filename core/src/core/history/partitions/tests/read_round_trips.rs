//! Ports of the Python round-trip tests of the result read: a result read is
//! one statement, for a moved task and for a live one.
//!
//! Python specification: `clients/python/tests/integration/test_task_result_history_fallback.py`
//! (`TestStagedResultReader`). The tests live here for the statement-counting
//! proxy of this module. The read is the broker's statement-layer reader
//! (`broker::history_reads::read_task_result`) on a core pool of one
//! connection through the proxy.

use super::*;
use crate::broker::core_rows::StatementLog;
use crate::broker::history_reads::{read_task_result, ResultReaderSeen};
use crate::broker::pools::unpinged_single_connection_core_pool_for_url;
use crate::core::history::reads::result::TaskResultRead;
use crate::core::lifecycle::TerminalizationOutcome;
use crate::test_support::sql::{self, TestPool};
use crate::test_support::terminal_writes;

const PORT_WORKER: &str = "port-round-trip-worker";

/// A plain task of `status`, claimed by `PORT_WORKER` with the process facts
/// of its start when RUNNING. Every baseline column is named.
async fn seed_task(pool: &TestPool, task_id: Uuid, status: &str) {
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
             $1, 'port.round_trip', 'default', 100, '{}', NULL, 'ISOLATED',
             'standard_30d', NOW(), NOW(),
             1, decode(repeat('0c', 32), 'hex'), NULL,
             $2, NOW(),
             CASE WHEN $2 = 'RUNNING' THEN NOW() END,
             CASE WHEN $2 = 'RUNNING' THEN $3 END,
             NULL,
             CASE WHEN $2 = 'RUNNING' THEN NOW() END, NULL, NULL,
             CASE WHEN $2 = 'RUNNING' THEN 4242 END,
             CASE WHEN $2 = 'RUNNING' THEN 'port-host' END,
             CASE WHEN $2 = 'RUNNING' THEN 'port-process' END,
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

/// The result of `task_id`, read twice on `reader` with one reader state:
/// the first read opens the connection and sees the published reader, so
/// its session setup and publication probe are not counted; the proxy
/// counts only the second read.
async fn counted_result_read(
    proxy: &SqlStatementProxy,
    reader: &crate::db::CorePool,
    task_id: Uuid,
) -> TaskResultRead {
    let reader_state = ResultReaderSeen::default();
    let _opened = read_task_result(reader, StatementLog::Off, &reader_state, task_id)
        .await
        .expect("open the reader connection with a first read");
    proxy.reset();
    read_task_result(reader, StatementLog::Off, &reader_state, task_id)
        .await
        .expect("read the result")
}

async fn published_database() -> TestDatabase {
    let database = TestDatabase::create_with_connections(3).await;
    let coverage = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
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
    let outcomes =
        terminal_writes::complete_plain_task(&database.pool, task_id, PORT_WORKER, "{\"Ok\":true}")
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
    let reader = unpinged_single_connection_core_pool_for_url(pool.url());
    let read = counted_result_read(&proxy, &reader, task_id).await;
    assert!(matches!(read, TaskResultRead::History(_)), "{read:?}");
    assert_eq!(proxy.statement_count(), 1, "{:?}", proxy.sql());
    reader.close();
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
    let reader = unpinged_single_connection_core_pool_for_url(pool.url());
    let read = counted_result_read(&proxy, &reader, task_id).await;
    assert!(matches!(read, TaskResultRead::Live(_)), "{read:?}");
    assert_eq!(proxy.statement_count(), 1, "{:?}", proxy.sql());
    reader.close();
    pool.close().await;
    proxy.stop().await;
    database.drop().await;
}
