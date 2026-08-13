//! Exact rendered move-program installation and the named R2 rollback.

use sqlx::PgConnection;

use crate::core::history::errors::HistoryError;

use super::drain::{verify_drained, DrainOutcome};
use super::identity::{
    attempts_identity_is_uuid, restore_attempt_identity, AttemptIdentityRestoration,
};
use super::state::cutover_complete;

const FRESH_CUTOVER_MIGRATION: &str =
    include_str!("../../../../migrations/0041_task_history_fresh_cutover.sql");
const IN_PLACE_PROGRAM_MIGRATION: &str =
    include_str!("../../../../migrations/0032_terminalization_operations.sql");
const IN_PLACE_PROGRAM_FIRST: &str =
    "DO $$\nBEGIN\n    IF NOT EXISTS (\n        SELECT 1\n        FROM pg_type";
const RENDER_TAG: &str = "$horsies_p1_sql$";
const TEARDOWN_FIRST: &str = "DROP TYPE IF EXISTS horsies_terminalization_outcome CASCADE";
const TEARDOWN_LAST: &str = "DROP TABLE IF EXISTS horsies_cutover_relocation_ledger";
const INSTALL_FIRST: &str = "CREATE TYPE horsies_terminalization_outcome AS";
const INSTALL_LAST: &str = "CREATE TABLE IF NOT EXISTS horsies_cutover_relocation_ledger";
pub const EXPECTED_TEARDOWN_STATEMENTS: usize = 12;
pub const EXPECTED_INSTALLATION_STATEMENTS: usize = 29;
pub const EXPECTED_TIGHTENING_STATEMENTS: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProgramInstallation {
    Installed { statements_executed: usize },
    Refused { reasons: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProgramRollback {
    RolledBack {
        teardown_statements_executed: usize,
        attempt_identity: AttemptIdentityRestoration,
    },
    Refused {
        reasons: Vec<String>,
    },
}

fn tagged_statements() -> Vec<&'static str> {
    FRESH_CUTOVER_MIGRATION
        .split(RENDER_TAG)
        .enumerate()
        .filter_map(|(index, part)| (index % 2 == 1).then_some(part))
        .collect()
}

fn rendered_group(
    first_prefix: &str,
    last_prefix: &str,
    expected: usize,
) -> Result<Vec<&'static str>, HistoryError> {
    let statements = tagged_statements();
    let first = statements
        .iter()
        .position(|statement| statement.starts_with(first_prefix))
        .ok_or_else(|| HistoryError::contract(format!("rendered group lacks {first_prefix:?}")))?;
    let last = statements[first..]
        .iter()
        .position(|statement| statement.starts_with(last_prefix))
        .map(|offset| first + offset)
        .ok_or_else(|| HistoryError::contract(format!("rendered group lacks {last_prefix:?}")))?;
    let group = statements[first..=last].to_vec();
    if group.len() != expected {
        return Err(HistoryError::contract(format!(
            "rendered group from {first_prefix:?} has {} statements, expected {expected}",
            group.len()
        )));
    }
    Ok(group)
}

pub(crate) fn teardown_statements() -> Result<Vec<&'static str>, HistoryError> {
    rendered_group(TEARDOWN_FIRST, TEARDOWN_LAST, EXPECTED_TEARDOWN_STATEMENTS)
}

pub(crate) fn installation_statements() -> Result<Vec<&'static str>, HistoryError> {
    rendered_group(
        INSTALL_FIRST,
        INSTALL_LAST,
        EXPECTED_INSTALLATION_STATEMENTS,
    )
}

pub(crate) fn tightening_statements() -> Result<Vec<&'static str>, HistoryError> {
    rendered_group(
        "ALTER TABLE horsies_tasks\n    ALTER COLUMN command_fingerprint_version SET NOT NULL",
        "ALTER TABLE horsies_tasks\n    ADD CONSTRAINT horsies_tasks_rerun_lineage_pair",
        EXPECTED_TIGHTENING_STATEMENTS,
    )
}

pub(crate) fn rendered_statement_starting(prefix: &str) -> Result<&'static str, HistoryError> {
    let matches: Vec<&str> = tagged_statements()
        .into_iter()
        .filter(|statement| statement.starts_with(prefix))
        .collect();
    match matches.as_slice() {
        [statement] => Ok(*statement),
        _ => Err(HistoryError::contract(format!(
            "rendered statement prefix {prefix:?} matched {} statements",
            matches.len()
        ))),
    }
}

fn in_place_program_sql() -> Result<&'static str, HistoryError> {
    IN_PLACE_PROGRAM_MIGRATION
        .find(IN_PLACE_PROGRAM_FIRST)
        .map(|offset| &IN_PLACE_PROGRAM_MIGRATION[offset..])
        .ok_or_else(|| HistoryError::contract("migration 0032 lacks its in-place program"))
}

pub async fn install_programs(
    connection: &mut PgConnection,
) -> Result<ProgramInstallation, HistoryError> {
    if !attempts_identity_is_uuid(connection).await? {
        return Ok(ProgramInstallation::Refused {
            reasons: vec![
                "the attempts identity is not uuid (identity normalization has not run)".to_owned(),
            ],
        });
    }
    let teardown = teardown_statements()?;
    let installation = installation_statements()?;
    for statement in teardown.iter().chain(installation.iter()) {
        sqlx::raw_sql(statement).execute(&mut *connection).await?;
    }
    Ok(ProgramInstallation::Installed {
        statements_executed: teardown.len() + installation.len(),
    })
}

pub async fn uninstall_programs(
    connection: &mut PgConnection,
) -> Result<ProgramRollback, HistoryError> {
    let mut reasons = Vec::new();
    if cutover_complete(connection).await? {
        reasons.push(
            "the cutover is attested; after tighten only a named backup restore is valid"
                .to_owned(),
        );
    }
    let live_identity_type: String = sqlx::query_scalar(
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute
         WHERE attrelid = 'horsies_tasks'::regclass AND attname = 'id'",
    )
    .fetch_one(&mut *connection)
    .await?;
    match live_identity_type.as_str() {
        "character varying(36)" => {}
        "uuid" => reasons
            .push("the live identity is uuid; the point of no return has been crossed".to_owned()),
        actual => reasons.push(format!(
            "the live identity has unexpected type {actual}; R2 requires character varying(36)"
        )),
    }
    if let DrainOutcome::Blocked {
        claimed_rows,
        running_rows,
        finalizing_rows,
        recent_heartbeats,
    } = verify_drained(connection, 60.0).await?
    {
        reasons.push(format!(
            "the fleet is not drained: claimed={claimed_rows}, running={running_rows}, finalizing={finalizing_rows}, recent_heartbeats={recent_heartbeats}"
        ));
    }
    if !reasons.is_empty() {
        return Ok(ProgramRollback::Refused { reasons });
    }
    let attempt_identity = match restore_attempt_identity(&mut *connection).await? {
        AttemptIdentityRestoration::Refused { reasons } => {
            return Ok(ProgramRollback::Refused { reasons });
        }
        outcome => outcome,
    };
    let teardown = teardown_statements()?;
    for statement in &teardown {
        sqlx::raw_sql(statement).execute(&mut *connection).await?;
    }
    sqlx::raw_sql(in_place_program_sql()?)
        .execute(connection)
        .await?;
    Ok(ProgramRollback::RolledBack {
        teardown_statements_executed: teardown.len(),
        attempt_identity,
    })
}

#[cfg(test)]
mod rendered_tests {
    use super::*;

    #[test]
    fn migration_embeds_the_exact_closed_program_groups() {
        let teardown = teardown_statements().unwrap();
        let installation = installation_statements().unwrap();
        assert_eq!(teardown.len(), EXPECTED_TEARDOWN_STATEMENTS);
        assert_eq!(installation.len(), EXPECTED_INSTALLATION_STATEMENTS);
        assert_eq!(
            tightening_statements().unwrap().len(),
            EXPECTED_TIGHTENING_STATEMENTS
        );
        assert!(installation[0].starts_with(INSTALL_FIRST));
        assert!(installation.last().unwrap().starts_with(INSTALL_LAST));
        assert!(rendered_statement_starting("CREATE TABLE horsies_heartbeats").is_ok());
        let in_place = in_place_program_sql().unwrap();
        assert!(!in_place.contains("ALTER TABLE horsies_tasks"));
        assert_eq!(in_place.matches("CREATE OR REPLACE FUNCTION").count(), 16);
    }
}
