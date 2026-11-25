//! The pure functions of the addon: no I/O, and none of them starts the
//! core. After each one, `coreState()` is still what it was.

use napi::bindgen_prelude::BigInt;
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;
use crate::process_core::runtime::{core_status, pure_call_refusal, CoreStatus};
use crate::values::boundary::{bigint_of_i64, text_of_uuid};

/// The state of the core in this process.
#[napi(string_enum = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoreState {
    NotStarted,
    Running,
    Stopped,
    /// The core started in a parent process and this process is its fork.
    Inherited,
}

impl From<CoreStatus> for CoreState {
    fn from(status: CoreStatus) -> Self {
        match status {
            CoreStatus::NotStarted => Self::NotStarted,
            CoreStatus::Running => Self::Running,
            CoreStatus::Stopped => Self::Stopped,
            CoreStatus::Inherited => Self::Inherited,
        }
    }
}

/// The state of the core in this process. Starts nothing.
#[napi(ts_return_type = "NativeEnvelope<CoreState>")]
pub fn core_state() -> Envelope<CoreState> {
    sync_call("core_state", || Envelope::Value(core_status().into()))
}

/// A minted task id, or the refusal of the core's UUIDv7 source.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MintOutcome {
    /// The id as lower-case hyphenated text.
    Minted {
        task_id: String,
    },
    Refused {
        cause: String,
    },
}

/// Mint a new task id (UUIDv7). The fork and stop rules apply, as for every
/// core call. Starts nothing.
#[napi(ts_return_type = "NativeEnvelope<MintOutcome>")]
pub fn mint_task_id() -> Envelope<MintOutcome> {
    sync_call("mint_task_id", || {
        if let Some(refusal) = pure_call_refusal() {
            return Envelope::Thrown(refusal.thrown());
        }
        Envelope::Value(match horsies_core::mint_task_id() {
            Ok(task_id) => MintOutcome::Minted {
                task_id: text_of_uuid(task_id),
            },
            Err(error) => MintOutcome::Refused {
                cause: error.to_string(),
            },
        })
    })
}

/// One SQL file of the migration chain.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationSqlFile {
    /// The migration version of the file.
    pub version: BigInt,
    pub name: String,
    pub text: String,
}

/// Every SQL file of the migration chain, in the order they run: the files
/// of migration 1000, then one file per later migration. A test harness
/// installs the schema of this build by running each text in this order.
/// Starts nothing.
#[napi(ts_return_type = "NativeEnvelope<Array<MigrationSqlFile>>")]
pub fn migration_sql_files() -> Envelope<Vec<MigrationSqlFile>> {
    sync_call("migration_sql_files", || {
        Envelope::Value(
            horsies_core::migration_sql_file_texts()
                .into_iter()
                .map(|(version, name, text)| MigrationSqlFile {
                    version: bigint_of_i64(version),
                    name: name.to_owned(),
                    text: text.to_owned(),
                })
                .collect(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_core_status_has_its_state() {
        assert_eq!(
            CoreState::from(CoreStatus::NotStarted),
            CoreState::NotStarted
        );
        assert_eq!(CoreState::from(CoreStatus::Running), CoreState::Running);
        assert_eq!(CoreState::from(CoreStatus::Stopped), CoreState::Stopped);
        assert_eq!(CoreState::from(CoreStatus::Inherited), CoreState::Inherited);
    }

    #[test]
    fn the_migration_files_start_with_the_baseline_in_order() {
        let Envelope::Value(files) = migration_sql_files() else {
            panic!("the migration files are a value");
        };
        assert_eq!(
            files.first().map(|file| file.version.clone()),
            Some(bigint_of_i64(1000))
        );
        let versions: Vec<i128> = files.iter().map(|file| file.version.get_i128().0).collect();
        let mut sorted = versions.clone();
        sorted.sort_unstable();
        assert_eq!(versions, sorted);
    }

    #[test]
    fn a_minted_task_id_is_lower_case_hyphenated_text() {
        let Envelope::Value(MintOutcome::Minted { task_id }) = mint_task_id() else {
            panic!("a fresh test process mints an id");
        };
        assert_eq!(
            crate::values::boundary::uuid_of_text("taskId", &task_id).map(text_of_uuid),
            Ok(task_id.clone())
        );
    }
}
