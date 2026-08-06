//! Persistence adapter for the terminalization vocabulary.
//!
//! Renders each [`TerminalizationCommand`] as `SELECT * FROM horsies_<fn>(…)`
//! with positional binds, decodes every returned row through the one shared
//! decoder, and enforces the cardinality contract per command shape:
//!
//! - single-task commands return exactly one row;
//! - id-keyed batches (`AbandonOwnedNodes`, `CancelOwnedNodes`) return the
//!   exact ordinal set `1..=n`, which is verified and used to restore caller
//!   input order — SQL result order is never trusted;
//! - discovery and workflow-scoped batches return one row per transition
//!   they made; zero rows is a valid answer, and inventing an outcome for
//!   work that did not happen is what the row-per-transition contract exists
//!   to prevent.
//!
//! The transaction stays the caller's: the functions never commit, so a
//! coupled workflow-node write belongs in the same transaction as the
//! transition it proves.

use chrono::{DateTime, Utc};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Transaction};

use crate::core::lifecycle::operations::{equivalence_class_of, function_name_of};
use crate::core::lifecycle::outcomes::decode_outcome_row;
use crate::core::lifecycle::{TerminalizationCommand, TerminalizationKind, TerminalizationOutcome};

use super::error::BrokerError;

const COMPLETE_LOCKED_TASK_SQL: &str =
    "SELECT * FROM horsies_complete_locked_task($1::varchar, $2, $3)";
const COMPLETE_TASK_FUSED_SQL: &str =
    "SELECT * FROM horsies_complete_task_fused($1::varchar, $2, $3::timestamptz, $4, $5, $6)";
const FAIL_LOCKED_TASK_SQL: &str =
    "SELECT * FROM horsies_fail_locked_task($1::varchar, $2, $3, $4, $5)";
const FAIL_STALE_TASK_SQL: &str =
    "SELECT * FROM horsies_fail_stale_task($1::varchar, $2::integer, $3::integer, $4, $5, $6)";
const EXPIRE_OWNED_CLAIM_SQL: &str =
    "SELECT * FROM horsies_expire_owned_claim($1::varchar, $2, $3, $4)";
const EXPIRE_PENDING_TASKS_SQL: &str =
    "SELECT * FROM horsies_expire_pending_tasks($1::integer, $2, $3)";
const CANCEL_LOCKED_TASK_SQL: &str =
    "SELECT * FROM horsies_cancel_locked_task($1::varchar, $2::text[])";
const CANCEL_OWNED_ORPHAN_SQL: &str =
    "SELECT * FROM horsies_cancel_owned_orphan($1::varchar, $2, $3::timestamptz)";
const CANCEL_ORPHANED_TASKS_SQL: &str =
    "SELECT * FROM horsies_cancel_orphaned_tasks($1::integer)";
const ABANDON_OWNED_NODE_SQL: &str =
    "SELECT * FROM horsies_abandon_owned_node($1::varchar, $2, $3::timestamptz)";
const ABANDON_OWNED_NODES_SQL: &str =
    "SELECT * FROM horsies_abandon_owned_nodes($1::varchar[], $2::timestamptz[], $3)";
const ABANDON_NODES_OF_PAUSED_WORKFLOWS_SQL: &str =
    "SELECT * FROM horsies_abandon_nodes_of_paused_workflows($1::varchar[])";
const CANCEL_OWNED_NODE_SQL: &str =
    "SELECT * FROM horsies_cancel_owned_node($1::varchar, $2, $3::timestamptz, $4::boolean)";
const CANCEL_OWNED_NODES_SQL: &str =
    "SELECT * FROM horsies_cancel_owned_nodes($1::varchar[], $2::timestamptz[], $3)";
const CANCEL_NODES_OF_CANCELLED_WORKFLOW_SQL: &str =
    "SELECT * FROM horsies_cancel_nodes_of_cancelled_workflow($1::varchar[])";
const LOCKED_READ_MISS_SQL: &str =
    "SELECT * FROM horsies_terminalization_miss($1::varchar, $2::text[], $3, $4::timestamptz)";

/// How many rows the command's function must report, per the wire contract.
#[derive(Clone, Copy)]
enum Cardinality {
    ExactlyOne,
    IdKeyedBatch { expected: usize },
    PerTransition,
}

fn fetch_query(command: &TerminalizationCommand) -> (sqlx::query::Query<'_, Postgres, sqlx::postgres::PgArguments>, Cardinality) {
    match command {
        TerminalizationCommand::CompleteLockedTask { task_id, fence, result_json } => (
            sqlx::query(COMPLETE_LOCKED_TASK_SQL)
                .bind(task_id)
                .bind(&fence.worker_id)
                .bind(result_json),
            Cardinality::ExactlyOne,
        ),
        TerminalizationCommand::CompleteTaskFused {
            task_id,
            fence,
            result_json,
            notify_channel,
            notify_payload,
        } => (
            sqlx::query(COMPLETE_TASK_FUSED_SQL)
                .bind(task_id)
                .bind(&fence.worker_id)
                .bind(fence.claimed_at)
                .bind(result_json)
                .bind(notify_channel)
                .bind(notify_payload),
            Cardinality::ExactlyOne,
        ),
        TerminalizationCommand::FailLockedTask {
            task_id,
            fence,
            result_json,
            error_code,
            failed_reason,
        } => (
            sqlx::query(FAIL_LOCKED_TASK_SQL)
                .bind(task_id)
                .bind(&fence.worker_id)
                .bind(result_json)
                .bind(error_code.as_deref())
                .bind(failed_reason.as_deref()),
            Cardinality::ExactlyOne,
        ),
        TerminalizationCommand::FailStaleTask {
            task_id,
            stale_after_ms,
            finalizing_stale_after_ms,
            result_json,
            error_code,
            failed_reason,
        } => (
            sqlx::query(FAIL_STALE_TASK_SQL)
                .bind(task_id)
                .bind(stale_after_ms)
                .bind(finalizing_stale_after_ms)
                .bind(result_json)
                .bind(error_code)
                .bind(failed_reason),
            Cardinality::ExactlyOne,
        ),
        TerminalizationCommand::ExpireOwnedClaim { task_id, fence, result_json, error_code } => (
            sqlx::query(EXPIRE_OWNED_CLAIM_SQL)
                .bind(task_id)
                .bind(&fence.worker_id)
                .bind(result_json)
                .bind(error_code),
            Cardinality::ExactlyOne,
        ),
        TerminalizationCommand::ExpirePendingTasks { batch_size, result_json, error_code } => (
            sqlx::query(EXPIRE_PENDING_TASKS_SQL)
                .bind(batch_size.get())
                .bind(result_json)
                .bind(error_code),
            Cardinality::PerTransition,
        ),
        TerminalizationCommand::CancelLockedTask {
            task_id,
            fence: _,
            permitted_source_statuses,
        } => (
            sqlx::query(CANCEL_LOCKED_TASK_SQL).bind(task_id).bind(
                permitted_source_statuses
                    .iter()
                    .map(|status| status.to_string())
                    .collect::<Vec<String>>(),
            ),
            Cardinality::ExactlyOne,
        ),
        TerminalizationCommand::CancelOwnedOrphan { task_id, fence } => (
            sqlx::query(CANCEL_OWNED_ORPHAN_SQL)
                .bind(task_id)
                .bind(&fence.worker_id)
                .bind(fence.claimed_at),
            Cardinality::ExactlyOne,
        ),
        TerminalizationCommand::CancelOrphanedTasks { batch_size } => (
            sqlx::query(CANCEL_ORPHANED_TASKS_SQL).bind(batch_size.get()),
            Cardinality::PerTransition,
        ),
        TerminalizationCommand::AbandonOwnedNode { task_id, fence } => (
            sqlx::query(ABANDON_OWNED_NODE_SQL)
                .bind(task_id)
                .bind(&fence.worker_id)
                .bind(fence.claimed_at),
            Cardinality::ExactlyOne,
        ),
        TerminalizationCommand::AbandonOwnedNodes { fence } => (
            sqlx::query(ABANDON_OWNED_NODES_SQL)
                .bind(fence.task_ids())
                .bind(fence.generations())
                .bind(fence.worker_id().to_owned()),
            Cardinality::IdKeyedBatch { expected: fence.len() },
        ),
        TerminalizationCommand::AbandonNodesOfPausedWorkflows { workflow_ids } => (
            sqlx::query(ABANDON_NODES_OF_PAUSED_WORKFLOWS_SQL).bind(workflow_ids),
            Cardinality::PerTransition,
        ),
        TerminalizationCommand::CancelOwnedNode {
            task_id,
            fence,
            accepts_requeued_pending,
        } => (
            sqlx::query(CANCEL_OWNED_NODE_SQL)
                .bind(task_id)
                .bind(&fence.worker_id)
                .bind(fence.claimed_at)
                .bind(accepts_requeued_pending),
            Cardinality::ExactlyOne,
        ),
        TerminalizationCommand::CancelOwnedNodes { fence } => (
            sqlx::query(CANCEL_OWNED_NODES_SQL)
                .bind(fence.task_ids())
                .bind(fence.generations())
                .bind(fence.worker_id().to_owned()),
            Cardinality::IdKeyedBatch { expected: fence.len() },
        ),
        TerminalizationCommand::CancelNodesOfCancelledWorkflow { workflow_ids } => (
            sqlx::query(CANCEL_NODES_OF_CANCELLED_WORKFLOW_SQL).bind(workflow_ids),
            Cardinality::PerTransition,
        ),
    }
}

fn decode_all(
    rows: &[PgRow],
    operation: &str,
) -> Result<Vec<TerminalizationOutcome>, BrokerError> {
    rows.iter()
        .map(|row| {
            decode_outcome_row(row)
                .map_err(|e| BrokerError::TerminalizationContract(format!("{operation}: {e}")))
        })
        .collect()
}

fn enforce_cardinality(
    outcomes: Vec<TerminalizationOutcome>,
    cardinality: Cardinality,
    operation: &str,
) -> Result<Vec<TerminalizationOutcome>, BrokerError> {
    match cardinality {
        Cardinality::ExactlyOne => {
            if outcomes.len() != 1 {
                return Err(BrokerError::TerminalizationContract(format!(
                    "{operation}: expected exactly one outcome row, got {}",
                    outcomes.len()
                )));
            }
            Ok(outcomes)
        }
        Cardinality::IdKeyedBatch { expected } => {
            reorder_by_ordinal(outcomes, expected, operation)
        }
        Cardinality::PerTransition => Ok(outcomes),
    }
}

/// Verify the ordinal contract and restore caller input order.
///
/// The exact ordinal set proves there is one answer per input and no answer
/// for anything else; result order is then irrelevant.
fn reorder_by_ordinal(
    outcomes: Vec<TerminalizationOutcome>,
    expected: usize,
    operation: &str,
) -> Result<Vec<TerminalizationOutcome>, BrokerError> {
    let mut by_ordinal: Vec<Option<TerminalizationOutcome>> = vec![None; expected];
    for outcome in outcomes {
        let Some(ordinal) = outcome.ordinality() else {
            return Err(BrokerError::TerminalizationContract(format!(
                "{operation}: returned a row without ordinality"
            )));
        };
        if ordinal < 1 || ordinal as usize > expected {
            return Err(BrokerError::TerminalizationContract(format!(
                "{operation}: ordinal {ordinal} outside expected set 1..={expected}"
            )));
        }
        let slot = &mut by_ordinal[(ordinal - 1) as usize];
        if slot.is_some() {
            return Err(BrokerError::TerminalizationContract(format!(
                "{operation}: returned duplicate ordinality {ordinal}"
            )));
        }
        *slot = Some(outcome);
    }
    let mut ordered = Vec::with_capacity(expected);
    for (index, slot) in by_ordinal.into_iter().enumerate() {
        match slot {
            Some(outcome) => ordered.push(outcome),
            None => {
                return Err(BrokerError::TerminalizationContract(format!(
                    "{operation}: ordinal set does not match its input: \
                     missing ordinal {}",
                    index + 1
                )));
            }
        }
    }
    Ok(ordered)
}

/// Emit the operation and what it reported through one tracing boundary.
///
/// Applied transitions are debug-level steady-state traffic. Every refusal
/// or replay is warning-level because its evidence is the only race
/// diagnosis that will ever exist for that moment.
pub fn log_terminalization_outcome(operation: &str, outcome: &TerminalizationOutcome) {
    match outcome {
        TerminalizationOutcome::Applied { task_id, ordinality, terminal_at, kind, observed } => {
            tracing::debug!(
                operation,
                task_id,
                ?ordinality,
                %terminal_at,
                kind = kind.as_str(),
                ?observed,
                "terminalization applied"
            );
        }
        TerminalizationOutcome::AlreadyApplied {
            task_id,
            ordinality,
            terminal_at,
            kind,
            observed,
        } => {
            tracing::warn!(
                operation,
                task_id,
                ?ordinality,
                %terminal_at,
                kind = kind.as_str(),
                ?observed,
                "terminalization already applied"
            );
        }
        TerminalizationOutcome::LostClaim { task_id, ordinality, observed } => {
            tracing::warn!(operation, task_id, ?ordinality, ?observed, "terminalization lost claim");
        }
        TerminalizationOutcome::SourceStateConflict { task_id, ordinality, observed, evidence } => {
            tracing::warn!(
                operation,
                task_id,
                ?ordinality,
                ?observed,
                ?evidence,
                "terminalization source-state conflict"
            );
        }
        TerminalizationOutcome::TaskAbsent { task_id, ordinality } => {
            tracing::warn!(operation, task_id, ?ordinality, "terminalization task absent");
        }
    }
}

/// Execute a command on the pool and decode everything it reports.
pub async fn terminalize(
    pool: &PgPool,
    command: &TerminalizationCommand,
) -> Result<Vec<TerminalizationOutcome>, BrokerError> {
    let operation = function_name_of(command);
    let (query, cardinality) = fetch_query(command);
    let rows = query.fetch_all(pool).await.map_err(BrokerError::Database)?;
    let outcomes = enforce_cardinality(decode_all(&rows, operation)?, cardinality, operation)?;
    for outcome in &outcomes {
        log_terminalization_outcome(operation, outcome);
    }
    Ok(outcomes)
}

/// Execute a command inside the caller's transaction.
///
/// The functions never commit; a coupled workflow-node write belongs in the
/// same transaction as the transition it proves.
pub async fn terminalize_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    command: &TerminalizationCommand,
) -> Result<Vec<TerminalizationOutcome>, BrokerError> {
    let operation = function_name_of(command);
    let (query, cardinality) = fetch_query(command);
    let rows = query
        .fetch_all(tx.as_mut())
        .await
        .map_err(BrokerError::Database)?;
    let outcomes = enforce_cardinality(decode_all(&rows, operation)?, cardinality, operation)?;
    for outcome in &outcomes {
        log_terminalization_outcome(operation, outcome);
    }
    Ok(outcomes)
}

/// Classify a failed generation-fenced locking read without mutating.
///
/// The locked-shape commands carry only the worker half of their fence: the
/// generation was already checked by the caller's `SELECT … FOR UPDATE`. If
/// that read matched nothing, invoking the operation function would be
/// unsafe — the same worker may already own a newer generation. The shared
/// miss classifier accepts the full dispatched generation and distinguishes
/// an idempotent replay from that lost claim while keeping
/// terminal-before-fence ordering identical to every operation function.
pub async fn classify_locked_read_miss_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    task_id: &str,
    requested: TerminalizationKind,
    worker_id: &str,
    claimed_at: Option<DateTime<Utc>>,
) -> Result<TerminalizationOutcome, BrokerError> {
    let operation = "horsies_terminalization_miss";
    let mut equivalent_kinds: Vec<String> = equivalence_class_of(requested)
        .iter()
        .map(|kind| kind.as_str().to_owned())
        .collect();
    equivalent_kinds.sort();
    let rows = sqlx::query(LOCKED_READ_MISS_SQL)
        .bind(task_id)
        .bind(equivalent_kinds)
        .bind(worker_id)
        .bind(claimed_at)
        .fetch_all(tx.as_mut())
        .await
        .map_err(BrokerError::Database)?;
    let mut outcomes = enforce_cardinality(
        decode_all(&rows, operation)?,
        Cardinality::ExactlyOne,
        operation,
    )?;
    let outcome = outcomes.remove(0);
    log_terminalization_outcome(operation, &outcome);
    Ok(outcome)
}

#[cfg(test)]
mod catalog_tests {
    //! Catalog conformance: the installed database program must match the
    //! vocabulary exactly — signatures, return shape, language, and the
    //! composite type's column list. A stale overload left behind by a
    //! changed argument list appears here as a difference.
    use super::*;
    use serial_test::serial;
    use sqlx::Row;

    fn test_db_url() -> String {
        if let Ok(url) = std::env::var("DATABASE_URL") {
            return url;
        }
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let root = std::path::Path::new(manifest_dir)
            .ancestors()
            .find(|p| p.join(".env").exists());
        let pw = root
            .and_then(|r| std::fs::read_to_string(r.join(".env")).ok())
            .and_then(|c| {
                c.lines()
                    .filter_map(|l| l.trim().split_once('='))
                    .find(|(k, _)| k.trim() == "DB_PASSWORD")
                    .map(|(_, v)| v.trim().to_owned())
            })
            .unwrap_or_else(|| "W0rklane".to_owned());
        format!("postgresql://postgres:{pw}@localhost:5432/horsies-rust-port")
    }

    async fn migrated_pool() -> PgPool {
        let broker = crate::broker::postgres::PostgresBroker::connect(&test_db_url())
            .await
            .expect("connect");
        broker.ensure_schema_initialized().await.expect("schema");
        broker.pool().clone()
    }

    /// (function, rendered argument list, rendered result type).
    const EXPECTED_SIGNATURES: [(&str, &str, &str); 16] = [
        (
            "horsies_terminalization_miss",
            "p_task_id character varying, p_equivalent_kinds text[], p_worker_id text, p_claimed_at timestamp with time zone",
            "SETOF horsies_terminalization_outcome",
        ),
        (
            "horsies_complete_locked_task",
            "p_task_id character varying, p_worker_id text, p_result text",
            "SETOF horsies_terminalization_outcome",
        ),
        (
            "horsies_complete_task_fused",
            "p_task_id character varying, p_worker_id text, p_claimed_at timestamp with time zone, p_result text, p_notify_channel text, p_notify_payload text",
            "SETOF horsies_terminalization_outcome",
        ),
        (
            "horsies_fail_locked_task",
            "p_task_id character varying, p_worker_id text, p_result text, p_error_code text, p_failed_reason text",
            "SETOF horsies_terminalization_outcome",
        ),
        (
            "horsies_fail_stale_task",
            "p_task_id character varying, p_stale_after_ms integer, p_finalizing_stale_after_ms integer, p_result text, p_error_code text, p_failed_reason text",
            "SETOF horsies_terminalization_outcome",
        ),
        (
            "horsies_expire_owned_claim",
            "p_task_id character varying, p_worker_id text, p_result text, p_error_code text",
            "SETOF horsies_terminalization_outcome",
        ),
        (
            "horsies_expire_pending_tasks",
            "p_batch_size integer, p_result text, p_error_code text",
            "SETOF horsies_terminalization_outcome",
        ),
        (
            "horsies_cancel_locked_task",
            "p_task_id character varying, p_permitted_source_statuses text[]",
            "SETOF horsies_terminalization_outcome",
        ),
        (
            "horsies_cancel_owned_orphan",
            "p_task_id character varying, p_worker_id text, p_claimed_at timestamp with time zone",
            "SETOF horsies_terminalization_outcome",
        ),
        (
            "horsies_cancel_orphaned_tasks",
            "p_batch_size integer",
            "SETOF horsies_terminalization_outcome",
        ),
        (
            "horsies_abandon_owned_node",
            "p_task_id character varying, p_worker_id text, p_claimed_at timestamp with time zone",
            "SETOF horsies_terminalization_outcome",
        ),
        (
            "horsies_abandon_owned_nodes",
            "p_ids character varying[], p_claimed_ats timestamp with time zone[], p_worker_id text",
            "SETOF horsies_terminalization_outcome",
        ),
        (
            "horsies_abandon_nodes_of_paused_workflows",
            "p_workflow_ids character varying[]",
            "SETOF horsies_terminalization_outcome",
        ),
        (
            "horsies_cancel_owned_node",
            "p_task_id character varying, p_worker_id text, p_claimed_at timestamp with time zone, p_accepts_requeued_pending boolean",
            "SETOF horsies_terminalization_outcome",
        ),
        (
            "horsies_cancel_owned_nodes",
            "p_ids character varying[], p_claimed_ats timestamp with time zone[], p_worker_id text",
            "SETOF horsies_terminalization_outcome",
        ),
        (
            "horsies_cancel_nodes_of_cancelled_workflow",
            "p_workflow_ids character varying[]",
            "SETOF horsies_terminalization_outcome",
        ),
    ];

    #[tokio::test]
    #[serial]
    async fn installed_functions_match_vocabulary_signatures() {
        let pool = migrated_pool().await;
        let names: Vec<String> = EXPECTED_SIGNATURES
            .iter()
            .map(|(name, _, _)| (*name).to_owned())
            .collect();
        let rows = sqlx::query(
            "SELECT p.proname,
                    pg_get_function_arguments(p.oid) AS args,
                    pg_get_function_result(p.oid) AS result,
                    p.prokind::text AS prokind,
                    l.lanname
             FROM pg_proc p
             JOIN pg_language l ON l.oid = p.prolang
             WHERE p.proname = ANY($1)
             ORDER BY p.proname",
        )
        .bind(&names)
        .fetch_all(&pool)
        .await
        .expect("catalog query");

        assert_eq!(
            rows.len(),
            EXPECTED_SIGNATURES.len(),
            "one installed definition per function, no stale overloads"
        );
        let mut expected: Vec<(&str, &str, &str)> = EXPECTED_SIGNATURES.to_vec();
        expected.sort_by_key(|(name, _, _)| *name);
        for (row, (name, args, result)) in rows.iter().zip(expected) {
            assert_eq!(row.get::<String, _>("proname"), name);
            assert_eq!(row.get::<String, _>("args"), args, "{name} arguments");
            assert_eq!(row.get::<String, _>("result"), result, "{name} result");
            assert_eq!(row.get::<String, _>("prokind"), "f", "{name} prokind");
            assert_eq!(row.get::<String, _>("lanname"), "plpgsql", "{name} language");
        }
    }

    #[tokio::test]
    #[serial]
    async fn outcome_type_matches_wire_contract() {
        let pool = migrated_pool().await;
        let rows = sqlx::query(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS type
             FROM pg_attribute a
             JOIN pg_type t ON t.typrelid = a.attrelid
             WHERE t.typname = 'horsies_terminalization_outcome'
               AND a.attnum > 0 AND NOT a.attisdropped
             ORDER BY a.attnum",
        )
        .fetch_all(&pool)
        .await
        .expect("type query");
        let columns: Vec<(String, String)> = rows
            .iter()
            .map(|r| (r.get("attname"), r.get("type")))
            .collect();
        let expected: Vec<(String, String)> = [
            ("task_id", "character varying"),
            ("ordinality", "bigint"),
            ("outcome", "text"),
            ("terminal_at", "timestamp with time zone"),
            ("terminalization_kind", "text"),
            ("observed_status", "text"),
            ("observed_worker_id", "character varying"),
            ("observed_claimed_at", "timestamp with time zone"),
            ("guard_kind", "text"),
            ("observed_guard", "jsonb"),
        ]
        .iter()
        .map(|(n, t)| ((*n).to_owned(), (*t).to_owned()))
        .collect();
        assert_eq!(columns, expected);
    }

    #[tokio::test]
    #[serial]
    async fn body_canary_fragments_present() {
        let pool = migrated_pool().await;
        let fused: String = sqlx::query_scalar(
            "SELECT pg_get_functiondef(
                'horsies_complete_task_fused(varchar, text, timestamptz, text, text, text)'::regprocedure
             )",
        )
        .fetch_one(&pool)
        .await
        .expect("fused def");
        for fragment in [
            "'COMPLETE_FUSED'",
            "claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)",
            "(p_claimed_at IS NULL OR claimed_at = p_claimed_at)",
            "ARRAY['COMPLETE_FUSED', 'COMPLETE_LOCKED']::text[]",
            "pg_notify(p_notify_channel, p_notify_payload)",
        ] {
            assert!(fused.contains(fragment), "fused body must contain {fragment:?}");
        }

        let miss: String = sqlx::query_scalar(
            "SELECT pg_get_functiondef(
                'horsies_terminalization_miss(varchar, text[], text, timestamptz)'::regprocedure
             )",
        )
        .fetch_one(&pool)
        .await
        .expect("miss def");
        for fragment in [
            "terminalization_kind = ANY(p_equivalent_kinds)",
            "'ALREADY_APPLIED'",
            "'FOREIGN_TERMINALIZATION'",
            "'TASK_ABSENT'",
            "'LOST_CLAIM'",
        ] {
            assert!(miss.contains(fragment), "miss body must contain {fragment:?}");
        }
    }

    #[tokio::test]
    #[serial]
    async fn kind_domain_check_rejects_unknown_kinds() {
        let pool = migrated_pool().await;
        let id = uuid::Uuid::new_v4().to_string();
        let insert = "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, created_at, updated_at, completed_at, terminal_at,
                terminalization_kind, retry_count, max_retries, enqueue_sha
            ) VALUES (
                $1, 'kind_domain_task', 'default', 100, '[]', '{}', 'COMPLETED',
                NOW(), NOW(), NOW(), NOW(), NOW(), $2, 0, 0, $1
            )";

        let err = sqlx::query(insert)
            .bind(&id)
            .bind(Some("NOT_A_KIND"))
            .execute(&pool)
            .await
            .expect_err("unknown kind must be rejected");
        let sqlx::Error::Database(db_err) = err else {
            panic!("expected database error, got {err:?}");
        };
        assert_eq!(db_err.code().as_deref(), Some("23514"));

        for kind in [None, Some("COMPLETE_FUSED")] {
            sqlx::query(insert)
                .bind(&id)
                .bind(kind)
                .execute(&pool)
                .await
                .expect("NULL and known kinds pass the domain check");
            sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
                .bind(&id)
                .execute(&pool)
                .await
                .expect("cleanup");
        }
    }

    #[tokio::test]
    #[serial]
    async fn decoder_rejects_wrong_row_shape() {
        let pool = migrated_pool().await;
        let row = sqlx::query("SELECT 'x'::varchar AS task_id, 'APPLIED'::text AS outcome")
            .fetch_one(&pool)
            .await
            .expect("query");
        let err = decode_outcome_row(&row).unwrap_err();
        assert!(err.to_string().contains("row shape"), "{err}");
    }
}
