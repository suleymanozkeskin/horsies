//! Point-of-no-return conversion from the transitional to the frozen shape.

use sqlx::{FromRow, PgConnection};

use crate::core::history::errors::HistoryError;
use crate::core::history::names::{LIVE_ATTEMPTS, LIVE_TASKS};

use super::identity::attempts_identity_is_uuid;
use super::program::{
    rendered_statement_starting, tightening_statements, EXPECTED_TIGHTENING_STATEMENTS,
};

const UUID_TEXT_PATTERN: &str = concat!(
    "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}",
    "-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
);

const CONVERTED_IDENTITY_COLUMNS: [(&str, &str); 8] = [
    (LIVE_TASKS, "id"),
    ("horsies_workflows", "id"),
    ("horsies_workflows", "parent_workflow_id"),
    ("horsies_workflows", "root_workflow_id"),
    ("horsies_workflow_tasks", "id"),
    ("horsies_workflow_tasks", "workflow_id"),
    ("horsies_workflow_tasks", "task_id"),
    ("horsies_workflow_tasks", "sub_workflow_id"),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TightenOutcome {
    Refused { reasons: Vec<String> },
    Complete { statements_executed: usize },
}

#[derive(Debug, FromRow)]
struct EntryCounts {
    terminal_rows: i64,
    in_flight_rows: i64,
    unprepared_rows: i64,
    unfingerprinted_rows: i64,
    unclassified_live_rows: i64,
}

#[derive(Debug, FromRow)]
struct ForeignKey {
    table_name: String,
    constraint_name: String,
    column_name: String,
    definition: String,
    delete_action: String,
    canonical_target: bool,
}

const WORKFLOW_FOREIGN_KEYS: [(&str, &str, &str, &str); 3] = [
    (
        "horsies_workflow_tasks",
        "horsies_workflow_tasks_sub_workflow_id_fkey",
        "sub_workflow_id",
        "n",
    ),
    (
        "horsies_workflow_tasks",
        "horsies_workflow_tasks_workflow_id_fkey",
        "workflow_id",
        "c",
    ),
    (
        "horsies_workflows",
        "horsies_workflows_parent_workflow_id_fkey",
        "parent_workflow_id",
        "c",
    ),
];

pub fn confirmation_phrase(backup_label: &str) -> String {
    format!("point-of-no-return: {backup_label}")
}

async fn entry_violations(connection: &mut PgConnection) -> Result<Vec<String>, HistoryError> {
    let counts: EntryCounts = sqlx::query_as(&format!(
        "SELECT
             count(*) FILTER (
                 WHERE status NOT IN ('PENDING', 'CLAIMED', 'RUNNING')
             ) AS terminal_rows,
             count(*) FILTER (
                 WHERE status IN ('CLAIMED', 'RUNNING')
             ) AS in_flight_rows,
             count(*) FILTER (
                 WHERE prepared_rerun_input_disposition IS NULL
             ) AS unprepared_rows,
             count(*) FILTER (
                 WHERE command_fingerprint IS NULL
             ) AS unfingerprinted_rows,
             count(*) FILTER (
                 WHERE retention_class_key IS NULL
             ) AS unclassified_live_rows
         FROM {LIVE_TASKS}"
    ))
    .fetch_one(&mut *connection)
    .await?;
    let mut violations = Vec::new();
    if counts.terminal_rows != 0 {
        violations.push(format!(
            "{} terminal rows remain live (relocation incomplete)",
            counts.terminal_rows
        ));
    }
    if counts.in_flight_rows != 0 {
        violations.push(format!(
            "{} rows are in flight (the fleet is not drained)",
            counts.in_flight_rows
        ));
    }
    if counts.unprepared_rows != 0 {
        violations.push(format!(
            "{} rows lack a prepared disposition (preparation incomplete)",
            counts.unprepared_rows
        ));
    }
    if counts.unfingerprinted_rows != 0 {
        violations.push(format!(
            "{} rows lack a command fingerprint (preparation incomplete)",
            counts.unfingerprinted_rows
        ));
    }
    if counts.unclassified_live_rows != 0 {
        violations.push(format!(
            "{} live rows carry no retention class (backfill a class before tightening)",
            counts.unclassified_live_rows
        ));
    }
    if !attempts_identity_is_uuid(connection).await? {
        violations.push(
            "the attempts identity is not uuid (identity normalization has not run)".to_owned(),
        );
    }
    Ok(violations)
}

async fn identity_parse_violations(
    connection: &mut PgConnection,
) -> Result<Vec<String>, HistoryError> {
    let mut violations = Vec::new();
    for (table, column) in CONVERTED_IDENTITY_COLUMNS {
        let bad: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM {table}
             WHERE {column} IS NOT NULL AND {column}::text !~ $1"
        ))
        .bind(UUID_TEXT_PATTERN)
        .fetch_one(&mut *connection)
        .await?;
        if bad != 0 {
            violations.push(format!(
                "{bad} rows in {table}.{column} do not parse as uuid"
            ));
        }
    }
    Ok(violations)
}

async fn status_check_constraints(
    connection: &mut PgConnection,
) -> Result<Vec<String>, HistoryError> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT con.conname
         FROM pg_constraint AS con
         WHERE con.conrelid = CAST($1 AS regclass)
           AND con.contype = 'c'
           AND (
               SELECT att.attnum FROM pg_attribute AS att
               WHERE att.attrelid = con.conrelid AND att.attname = 'status'
           ) = ANY(con.conkey)
           AND con.conname <> '{LIVE_TASKS}_live_status_only'
         ORDER BY con.conname"
    ))
    .bind(LIVE_TASKS)
    .fetch_all(connection)
    .await?)
}

async fn workflow_foreign_keys(
    connection: &mut PgConnection,
) -> Result<Vec<ForeignKey>, HistoryError> {
    Ok(sqlx::query_as(
        "SELECT con.conrelid::regclass::text AS table_name,
                con.conname AS constraint_name,
                (SELECT att.attname FROM pg_attribute AS att
                 WHERE att.attrelid = con.conrelid
                   AND att.attnum = con.conkey[1]) AS column_name,
                pg_get_constraintdef(con.oid) AS definition,
                con.confdeltype::text AS delete_action,
                cardinality(con.conkey) = 1
                AND con.confkey = ARRAY[(
                    SELECT attnum FROM pg_attribute
                    WHERE attrelid = con.confrelid AND attname = 'id'
                )]::smallint[] AS canonical_target
         FROM pg_constraint AS con
         WHERE con.confrelid = 'horsies_workflows'::regclass
           AND con.contype = 'f'
         ORDER BY con.conname",
    )
    .fetch_all(connection)
    .await?)
}

fn workflow_foreign_key_violations(rows: &[ForeignKey]) -> Vec<String> {
    let actual: Vec<(&str, &str, &str, &str, bool)> = rows
        .iter()
        .map(|row| {
            (
                row.table_name.as_str(),
                row.constraint_name.as_str(),
                row.column_name.as_str(),
                row.delete_action.as_str(),
                row.canonical_target,
            )
        })
        .collect();
    let expected: Vec<(&str, &str, &str, &str, bool)> = WORKFLOW_FOREIGN_KEYS
        .iter()
        .map(|(table, name, column, delete)| (*table, *name, *column, *delete, true))
        .collect();
    (actual != expected)
        .then(|| {
            format!(
                "workflow foreign-key topology drifted: expected {expected:?}, found {actual:?}"
            )
        })
        .into_iter()
        .collect()
}

pub(crate) async fn frozen_foreign_key_violations(
    connection: &mut PgConnection,
) -> Result<Vec<String>, HistoryError> {
    let mut violations = workflow_foreign_key_violations(&workflow_foreign_keys(connection).await?);
    let attempts_ok: bool = sqlx::query_scalar(
        "SELECT count(*) = 1 AND bool_and(
             conname = 'horsies_task_attempts_task_id_fkey'
             AND confrelid = 'horsies_tasks'::regclass
             AND confdeltype = 'c'
             AND conkey = ARRAY[(SELECT attnum FROM pg_attribute
                                 WHERE attrelid = conrelid AND attname = 'task_id')]::smallint[]
             AND confkey = ARRAY[(SELECT attnum FROM pg_attribute
                                  WHERE attrelid = confrelid AND attname = 'id')]::smallint[]
         ) FROM pg_constraint
         WHERE conrelid = 'horsies_task_attempts'::regclass AND contype = 'f'",
    )
    .fetch_one(&mut *connection)
    .await?;
    if !attempts_ok {
        violations.push("the attempts-to-task CASCADE foreign key is absent or drifted".to_owned());
    }
    let pending_ok: bool = sqlx::query_scalar(
        "SELECT count(*) = 1 AND bool_and(
             confrelid = 'horsies_workflow_tasks'::regclass
             AND confdeltype = 'c'
             AND (SELECT array_agg(attname ORDER BY ordinality)
                  FROM unnest(conkey) WITH ORDINALITY AS key(attnum, ordinality)
                  JOIN pg_attribute ON attrelid = conrelid
                                   AND pg_attribute.attnum = key.attnum)
                 = ARRAY['workflow_node_row_id', 'workflow_id']::name[]
             AND (SELECT array_agg(attname ORDER BY ordinality)
                  FROM unnest(confkey) WITH ORDINALITY AS key(attnum, ordinality)
                  JOIN pg_attribute ON attrelid = confrelid
                                   AND pg_attribute.attnum = key.attnum)
                 = ARRAY['id', 'workflow_id']::name[]
         ) FROM pg_constraint
         WHERE conrelid = 'horsies_workflow_phase2_pending'::regclass
           AND conname = 'horsies_workflow_phase2_pending_node_fkey'
           AND contype = 'f'",
    )
    .fetch_one(connection)
    .await?;
    if !pending_ok {
        violations
            .push("the phase-2 pending composite CASCADE locator is absent or drifted".to_owned());
    }
    Ok(violations)
}

pub async fn tighten_to_frozen(
    connection: &mut PgConnection,
    backup_label: &str,
    operator_confirmation: &str,
) -> Result<TightenOutcome, HistoryError> {
    let mut reasons = Vec::new();
    if backup_label.is_empty() {
        reasons.push("backup label must be non-empty".to_owned());
    }
    if operator_confirmation != confirmation_phrase(backup_label) {
        reasons.push(format!(
            "operator confirmation does not name the backup (expected the exact phrase for {backup_label:?})"
        ));
    }
    reasons.extend(entry_violations(connection).await?);
    reasons.extend(identity_parse_violations(connection).await?);
    let workflow_keys = workflow_foreign_keys(connection).await?;
    reasons.extend(workflow_foreign_key_violations(&workflow_keys));
    if !reasons.is_empty() {
        return Ok(TightenOutcome::Refused { reasons });
    }

    let mut executed = 0;
    let tightening = tightening_statements()?;
    debug_assert_eq!(tightening.len(), EXPECTED_TIGHTENING_STATEMENTS);
    for statement in tightening {
        sqlx::raw_sql(statement).execute(&mut *connection).await?;
        executed += 1;
    }
    for constraint in status_check_constraints(connection).await? {
        sqlx::query(&format!(
            "ALTER TABLE {LIVE_TASKS} DROP CONSTRAINT \"{constraint}\""
        ))
        .execute(&mut *connection)
        .await?;
        executed += 1;
    }
    sqlx::raw_sql(rendered_statement_starting(
        "ALTER TABLE horsies_tasks\n    ADD CONSTRAINT horsies_tasks_live_status_only",
    )?)
    .execute(&mut *connection)
    .await?;
    executed += 1;

    sqlx::query(&format!(
        "ALTER TABLE {LIVE_TASKS} ALTER COLUMN id TYPE uuid USING id::uuid"
    ))
    .execute(&mut *connection)
    .await?;
    executed += 1;
    sqlx::query(&format!(
        "ALTER TABLE {LIVE_ATTEMPTS} DROP CONSTRAINT IF EXISTS \
         horsies_task_attempts_task_id_fkey"
    ))
    .execute(&mut *connection)
    .await?;
    sqlx::query(&format!(
        "ALTER TABLE {LIVE_ATTEMPTS}
         ADD CONSTRAINT horsies_task_attempts_task_id_fkey
         FOREIGN KEY (task_id) REFERENCES {LIVE_TASKS}(id) ON DELETE CASCADE"
    ))
    .execute(&mut *connection)
    .await?;
    executed += 3;

    for key in &workflow_keys {
        sqlx::query(&format!(
            "ALTER TABLE {} DROP CONSTRAINT \"{}\"",
            key.table_name, key.constraint_name
        ))
        .execute(&mut *connection)
        .await?;
        executed += 1;
    }
    sqlx::query("ALTER TABLE horsies_workflows ALTER COLUMN id TYPE uuid USING id::uuid")
        .execute(&mut *connection)
        .await?;
    executed += 1;
    for (table, column) in [
        ("horsies_workflows", "parent_workflow_id"),
        ("horsies_workflows", "root_workflow_id"),
        ("horsies_workflow_tasks", "id"),
        ("horsies_workflow_tasks", "workflow_id"),
        ("horsies_workflow_tasks", "task_id"),
        ("horsies_workflow_tasks", "sub_workflow_id"),
    ] {
        sqlx::query(&format!(
            "ALTER TABLE {table} ALTER COLUMN {column} TYPE uuid USING {column}::uuid"
        ))
        .execute(&mut *connection)
        .await?;
        executed += 1;
    }
    for key in &workflow_keys {
        sqlx::query(&format!(
            "ALTER TABLE {} ADD CONSTRAINT \"{}\" {}",
            key.table_name, key.constraint_name, key.definition
        ))
        .execute(&mut *connection)
        .await?;
        executed += 1;
    }

    for prefix in [
        "ALTER TABLE horsies_workflow_tasks\n            ADD CONSTRAINT horsies_workflow_tasks_node_workflow_key",
        "ALTER TABLE horsies_workflow_phase2_pending\n            ADD CONSTRAINT horsies_workflow_phase2_pending_node_fkey",
    ] {
        sqlx::raw_sql(rendered_statement_starting(prefix)?)
            .execute(&mut *connection)
            .await?;
        executed += 1;
    }
    sqlx::query("DROP TABLE horsies_heartbeats")
        .execute(&mut *connection)
        .await?;
    sqlx::raw_sql(rendered_statement_starting(
        "CREATE TABLE horsies_heartbeats",
    )?)
    .execute(connection)
    .await?;
    executed += 2;

    Ok(TightenOutcome::Complete {
        statements_executed: executed,
    })
}
