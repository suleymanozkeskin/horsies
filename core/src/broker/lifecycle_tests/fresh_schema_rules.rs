//! Rules of the baseline schema that no Python baseline test checks (#881).
//!
//! The core installs the same SQL text as Python. Objects that Python's
//! `clients/python/tests/integration/test_baseline_install.py` already checks are not tested
//! again here. These tests cover the rules of the fresh-schema rows that no
//! committed test checks: the live rerun lineage pair, the two history CHECKs,
//! and the absence of a DEFAULT partition.

use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::support::{assert_refused, cleanup_tasks, pool, seed_task, Constraint, TaskSeed};
use crate::core::config::retention::RetentionConfig;
use crate::core::history::archive::attempts::encode_attempt_snapshot;
use crate::core::history::maintenance::coverage::CoverageOutcome;
use crate::test_support::sql::{self, TestConnection};

/// Levels below a partition root that the tree walk reads: root, class
/// parent, leaf.
const MAX_PARTITION_DEPTH: i32 = 3;

/// How a history row stores its rerun input.
#[derive(Debug, Clone, Copy)]
enum RerunInput {
    /// `NEVER_ELIGIBLE`, no envelope.
    NeverEligible,
    /// `INLINE` with a version-1 envelope of `{}`.
    Inline,
}

/// One history row that a test inserts. Every baseline column is named.
#[derive(Debug, Clone, Copy)]
struct HistoryRow {
    status: &'static str,
    kind: &'static str,
    is_workflow_task: bool,
    result: Option<&'static [u8]>,
    rerun_input: RerunInput,
}

/// Insert `row` into the static forever leaf on `connection`. Returns the
/// database's answer, so a test can assert a refusal.
async fn insert_history_row(
    connection: &mut TestConnection,
    row: HistoryRow,
) -> Result<u64, sql::Error> {
    let snapshot = encode_attempt_snapshot(&[]).expect("encode the empty attempt snapshot");
    let (disposition, version, digest, inline): (
        &str,
        Option<i16>,
        Option<Vec<u8>>,
        Option<&[u8]>,
    ) = match row.rerun_input {
        RerunInput::NeverEligible => ("NEVER_ELIGIBLE", None, None, None),
        RerunInput::Inline => (
            "INLINE",
            Some(1),
            Some(Sha256::digest(b"{}").to_vec()),
            Some(b"{}".as_slice()),
        ),
    };
    let result_digest = row.result.map(|payload| Sha256::digest(payload).to_vec());
    let inserted = sql::query(
        "INSERT INTO horsies_task_history (
            task_id, task_name, queue_name, priority,
            command_fingerprint_version, command_fingerprint,
            retention_class_key, is_workflow_task, workflow_id,
            rerun_of_task_id, rerun_root_task_id,
            sent_at, created_at, enqueued_at, claimed_at, started_at, good_until,
            terminal_at, status, terminalization_kind, retry_count, max_retries,
            last_claimed_worker_id, last_worker_hostname, last_worker_pid,
            last_worker_process_name, error_code, final_failed_reason,
            result_envelope_version, result_payload, result_digest,
            history_schema_version, attempt_archive_version, attempt_snapshot,
            attempt_snapshot_digest, rerun_input_disposition, rerun_input_version,
            rerun_input_digest, rerun_input_inline
        ) VALUES (
            $1, 'fresh.schema', 'default', 100,
            NULL, NULL,
            'forever', $2, NULL,
            NULL, NULL,
            NOW(), NOW(), NOW(), NULL, NULL, NULL,
            NOW(), $3, $4, 0, 0,
            NULL, NULL, NULL,
            NULL, NULL, NULL,
            1, $5, $6,
            1, $7, $8,
            $9, $10, $11,
            $12, $13
        )",
    )
    .bind(Uuid::new_v4())
    .bind(row.is_workflow_task)
    .bind(row.status)
    .bind(row.kind)
    .bind(row.result)
    .bind(result_digest)
    .bind(snapshot.version)
    .bind(snapshot.payload)
    .bind(snapshot.digest.to_vec())
    .bind(disposition)
    .bind(version)
    .bind(digest)
    .bind(inline)
    .execute(connection)
    .await?;
    Ok(inserted.rows_affected())
}

/// `test_schema_upgrade_paths.py::TestFreshWorldParity::test_fresh_database_carries_the_tightened_task_shape`
/// (the lineage half): a live row carries both rerun ids or neither (CHECK
/// `horsies_tasks_rerun_lineage_pair`). The live-status half is
/// `terminalization_functions.rs::status_and_terminal_at_must_agree`.
#[tokio::test]
#[serial_test::serial]
async fn a_live_row_carries_both_rerun_ids_or_neither() {
    let pool = pool().await;
    let task_id = seed_task(&pool, &TaskSeed::pending()).await;

    for column in ["rerun_of_task_id", "rerun_root_task_id"] {
        let refused = sql::query(&format!(
            "UPDATE horsies_tasks SET {column} = $2 WHERE id = $1"
        ))
        .bind(task_id)
        .bind(Uuid::new_v4())
        .execute(&pool)
        .await
        .map(|done| done.rows_affected());
        assert_refused(
            refused,
            "23514",
            Constraint::Named("horsies_tasks_rerun_lineage_pair"),
        );
    }
    let paired = sql::query(
        "UPDATE horsies_tasks SET rerun_of_task_id = $2, rerun_root_task_id = $2 WHERE id = $1",
    )
    .bind(task_id)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .expect("both ids together");
    assert_eq!(paired.rows_affected(), 1);

    cleanup_tasks(&pool, &[task_id]).await;
}

/// `test_task_history_archive_prototypes.py::test_completed_and_workflow_rows_reject_rerun_input`:
/// a COMPLETED row and a workflow row store no rerun input (CHECK
/// `horsies_task_history_rerun_input_shape`). A FAILED plain row may.
#[tokio::test]
#[serial_test::serial]
async fn completed_and_workflow_history_rows_refuse_rerun_input() {
    let pool = pool().await;
    let mut connection = pool.acquire().await.expect("acquire");
    for (status, kind, is_workflow_task) in [
        ("COMPLETED", "COMPLETE_LOCKED", false),
        ("FAILED", "FAIL_RUNNING", true),
    ] {
        let refused = insert_history_row(
            &mut *connection,
            HistoryRow {
                status,
                kind,
                is_workflow_task,
                result: Some(b"{}".as_slice()),
                rerun_input: RerunInput::Inline,
            },
        )
        .await;
        assert_refused(
            refused,
            "23514",
            Constraint::Named("horsies_task_history_rerun_input_shape"),
        );
    }

    let mut transaction = pool.begin().await.expect("begin the control insert");
    let accepted = insert_history_row(
        &mut *transaction,
        HistoryRow {
            status: "FAILED",
            kind: "FAIL_RUNNING",
            is_workflow_task: false,
            result: None,
            rerun_input: RerunInput::Inline,
        },
    )
    .await
    .expect("a FAILED plain row stores its rerun input");
    assert_eq!(accepted, 1);
    transaction
        .rollback()
        .await
        .expect("roll back the control row");
}

/// `test_task_history_archive_prototypes.py::test_administrative_cancel_cannot_present_prior_result_as_disposition`:
/// an administrative cancel stores no result (CHECK
/// `horsies_task_history_result_domain`).
#[tokio::test]
#[serial_test::serial]
async fn an_administrative_cancel_refuses_a_stored_result() {
    let pool = pool().await;
    let mut connection = pool.acquire().await.expect("acquire");
    let refused = insert_history_row(
        &mut *connection,
        HistoryRow {
            status: "CANCELLED",
            kind: "CANCEL_ADMIN",
            is_workflow_task: false,
            result: Some(b"{\"old\":\"success\"}".as_slice()),
            rerun_input: RerunInput::NeverEligible,
        },
    )
    .await;
    assert_refused(
        refused,
        "23514",
        Constraint::Named("horsies_task_history_result_domain"),
    );
}

/// `test_task_history_archive_prototypes.py::test_catalog_has_no_default_partition_and_aligned_attempt_leaves`
/// (the DEFAULT half): after a coverage pass, no partitioned table of the
/// history tree or the heartbeat tree has a DEFAULT partition, so partition
/// routing refuses a row whose class or time no leaf covers. The aligned
/// attempt leaves are a prototype layout; attempts are not partitioned.
#[tokio::test]
#[serial_test::serial]
async fn the_history_and_heartbeat_trees_have_no_default_partition() {
    let pool = pool().await;
    let config = RetentionConfig::default();
    let core = crate::broker::pools::core_pool_for_test_url(pool.url());
    let coverage = crate::core::history::maintenance::coverage::ensure_partition_coverage_in_pool(
        &core,
        crate::broker::core_rows::StatementLog::Off,
        config.history_leaf_horizon_days,
        config.heartbeat_leaf_horizon_hours,
        &config.registrable_periods(),
    )
    .await
    .expect("ensure coverage");
    core.close();
    assert!(
        matches!(coverage, CoverageOutcome::Ensured(_)),
        "{coverage:?}"
    );

    let (members, leaves, defaults): (i64, i64, Vec<String>) = sql::query_as(
        "WITH RECURSIVE tree AS (
             SELECT root.oid AS relid, 0 AS depth
             FROM pg_class AS root
             WHERE root.oid IN (
                 to_regclass('horsies_task_history'), to_regclass('horsies_heartbeats')
             )
             UNION ALL
             SELECT inheritance.inhrelid, tree.depth + 1
             FROM pg_inherits AS inheritance
             JOIN tree ON inheritance.inhparent = tree.relid
             WHERE tree.depth < $1
         )
         SELECT count(*),
                count(*) FILTER (WHERE member.relkind = 'r'),
                COALESCE(
                    array_agg(member.relname::text ORDER BY member.relname) FILTER (
                        WHERE pg_get_expr(member.relpartbound, member.oid) = 'DEFAULT'
                           OR partitioned.partdefid <> 0
                    ),
                    '{}'
                )
         FROM tree
         JOIN pg_class AS member ON member.oid = tree.relid
         LEFT JOIN pg_partitioned_table AS partitioned ON partitioned.partrelid = member.oid",
    )
    .bind(MAX_PARTITION_DEPTH)
    .fetch_one(&pool)
    .await
    .expect("read the partition trees");

    assert!(members > 2, "the walk reached the partition trees");
    assert!(leaves > 0, "the walk reached the leaves");
    assert!(defaults.is_empty(), "DEFAULT partitions: {defaults:?}");
}
