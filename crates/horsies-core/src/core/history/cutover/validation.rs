//! Six-family structural validation and committing attestation ownership.

use sqlx::PgConnection;

use crate::core::history::errors::HistoryError;
use crate::core::history::names::{
    HEARTBEATS_TABLE, LIVE_ATTEMPTS, LIVE_TASKS, TASK_HISTORY_FOREVER, TASK_HISTORY_PARENT,
};

use super::relocation::RELOCATION_LEDGER;
use super::state::{clear_complete, mark_complete};
use super::tighten::frozen_foreign_key_violations;

const REQUIRED_NOT_NULL_COLUMNS: [&str; 5] = [
    "command_fingerprint_version",
    "command_fingerprint",
    "retention_class_key",
    "retain_rerun_input",
    "prepared_rerun_input_disposition",
];

const UUID_COLUMNS: [(&str, &str); 10] = [
    (LIVE_TASKS, "id"),
    (LIVE_ATTEMPTS, "task_id"),
    ("horsies_workflows", "id"),
    ("horsies_workflows", "parent_workflow_id"),
    ("horsies_workflows", "root_workflow_id"),
    ("horsies_workflow_tasks", "id"),
    ("horsies_workflow_tasks", "workflow_id"),
    ("horsies_workflow_tasks", "task_id"),
    ("horsies_workflow_tasks", "sub_workflow_id"),
    (HEARTBEATS_TABLE, "task_id"),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationOutcome {
    Validated { history_rows: i64, ledger_rows: i64 },
    Invalid { violations: Vec<String> },
}

pub async fn validate_cutover(
    connection: &mut PgConnection,
) -> Result<ValidationOutcome, HistoryError> {
    let outcome = validate_cutover_structure(connection).await?;
    match &outcome {
        ValidationOutcome::Validated { .. } => mark_complete(connection).await?,
        ValidationOutcome::Invalid { .. } => clear_complete(connection).await?,
    }
    Ok(outcome)
}

pub async fn validate_cutover_structure(
    connection: &mut PgConnection,
) -> Result<ValidationOutcome, HistoryError> {
    let mut violations = Vec::new();
    let terminal_rows: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {LIVE_TASKS}
         WHERE status NOT IN ('PENDING', 'CLAIMED', 'RUNNING')"
    ))
    .fetch_one(&mut *connection)
    .await?;
    if terminal_rows != 0 {
        violations.push(format!("{terminal_rows} terminal rows remain live"));
    }

    let status_domain: bool = sqlx::query_scalar(&format!(
        "SELECT EXISTS (
             SELECT 1 FROM pg_constraint
             WHERE conrelid = CAST($1 AS regclass)
               AND conname = '{LIVE_TASKS}_live_status_only'
         )"
    ))
    .bind(LIVE_TASKS)
    .fetch_one(&mut *connection)
    .await?;
    if !status_domain {
        violations.push("the live-only status domain is absent".to_owned());
    }

    for column in REQUIRED_NOT_NULL_COLUMNS {
        let not_null: Option<bool> = sqlx::query_scalar(
            "SELECT attnotnull FROM pg_attribute
             WHERE attrelid = CAST($1 AS regclass) AND attname = $2",
        )
        .bind(LIVE_TASKS)
        .bind(column)
        .fetch_optional(&mut *connection)
        .await?;
        if not_null != Some(true) {
            violations.push(format!("declared not-null column {column} is nullable"));
        }
    }

    for (relation, column) in UUID_COLUMNS {
        let is_uuid: Option<bool> = sqlx::query_scalar(
            "SELECT atttypid = 'uuid'::regtype FROM pg_attribute
             WHERE attrelid = CAST($1 AS regclass) AND attname = $2",
        )
        .bind(relation)
        .bind(column)
        .fetch_optional(&mut *connection)
        .await?;
        if is_uuid != Some(true) {
            violations.push(format!("{relation}.{column} is not uuid"));
        }
    }
    violations.extend(frozen_foreign_key_violations(connection).await?);

    let heartbeats_partitioned: bool = sqlx::query_scalar(
        "SELECT COALESCE((SELECT relkind = 'p' FROM pg_class
                          WHERE oid = to_regclass($1)), FALSE)",
    )
    .bind(HEARTBEATS_TABLE)
    .fetch_one(&mut *connection)
    .await?;
    if !heartbeats_partitioned {
        violations.push("the heartbeat shape is not partitioned".to_owned());
    }

    let forever_partitioned: bool = sqlx::query_scalar(
        "SELECT COALESCE((SELECT relkind = 'p' FROM pg_class
                          WHERE oid = to_regclass($1)), FALSE)",
    )
    .bind(TASK_HISTORY_FOREVER)
    .fetch_one(&mut *connection)
    .await?;
    if !forever_partitioned {
        violations.push("the forever history class is not RANGE-partitioned".to_owned());
    } else {
        let uncataloged: i64 = sqlx::query_scalar(
            "SELECT count(*)
             FROM pg_partition_tree(CAST($1 AS regclass)) AS tree
             JOIN pg_class AS child ON child.oid = tree.relid
             LEFT JOIN horsies_task_history_leaf_catalog AS catalog
               ON catalog.leaf_name = child.relname
              AND catalog.detached_at IS NULL
              AND catalog.dropped_at IS NULL
             WHERE tree.isleaf AND catalog.leaf_name IS NULL",
        )
        .bind(TASK_HISTORY_FOREVER)
        .fetch_one(&mut *connection)
        .await?;
        if uncataloged != 0 {
            violations.push(format!(
                "{uncataloged} forever history leaves are absent from the leaf catalog"
            ));
        }
    }

    let (history_rows, ledger_rows): (i64, i64) = sqlx::query_as(&format!(
        "SELECT (SELECT count(*) FROM {TASK_HISTORY_PARENT}),
                (SELECT COALESCE(sum(rows_relocated), 0) FROM {RELOCATION_LEDGER})"
    ))
    .fetch_one(connection)
    .await?;
    if history_rows < ledger_rows {
        violations.push(format!(
            "history holds {history_rows} rows but the ledger recorded {ledger_rows} relocations"
        ));
    }

    if violations.is_empty() {
        Ok(ValidationOutcome::Validated {
            history_rows,
            ledger_rows,
        })
    } else {
        Ok(ValidationOutcome::Invalid { violations })
    }
}
