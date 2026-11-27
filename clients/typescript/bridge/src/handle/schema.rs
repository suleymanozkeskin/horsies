//! Schema install and schema status.
//!
//! - Install applies the embedded migrations once per handle, checks the
//!   version and runs the coverage pass of the handle's request
//!   (`PostgresBroker::ensure_schema_initialized`). A handle of the
//!   `OBSERVE_ONLY` or the `ASSUME_INITIALIZED` mode runs nothing.
//! - Status is the web schema gate's read: the version that the last
//!   successful migration reached, against the version this core expects. A
//!   database that cannot be read answers `UNKNOWN`, as the gate does.

use horsies_core::manifest::SchemaMode;
use horsies_core::web::{SchemaProbe, SchemaState, SchemaStatus};
use horsies_core::{expected_schema_version, DatabaseOperationError};
use napi::bindgen_prelude::BigInt;
use napi_derive::napi;

use crate::handle::core_handle::BrokerConnection;
use crate::values::boundary::bigint_of_i64;
use crate::values::catalog_values::RawCatalogError;

/// The subject of the cause of a failed install.
const INSTALL_SUBJECT: &str = "install_schema";

/// How one install ended.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InstallOutcome {
    /// The migrations ran, and the database is at `version`.
    Installed { version: BigInt },
    /// The schema mode of the manifest runs no install.
    NotRun,
    /// The catalog value with `SCHEMA_INIT_FAILED`.
    Failed { error: RawCatalogError },
}

/// The schema state of a database.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaStateName {
    Match,
    Mismatch,
    Absent,
    /// A horsies 0.x schema, which this build neither reads nor upgrades.
    ZeroX,
    /// The database could not be reached or read.
    Unknown,
}

/// The stored version of a database.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoredVersion {
    Stored {
        version: BigInt,
    },
    /// The database holds no version: no schema, or it could not be read.
    NoVersion,
}

/// The schema state of the database, against the version this core
/// expects.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaStatusRecord {
    pub state: SchemaStateName,
    pub stored: StoredVersion,
    pub expected_version: BigInt,
}

impl From<SchemaStatus> for SchemaStatusRecord {
    fn from(status: SchemaStatus) -> Self {
        Self {
            state: match status.state {
                SchemaState::Match => SchemaStateName::Match,
                SchemaState::Mismatch => SchemaStateName::Mismatch,
                SchemaState::Absent => SchemaStateName::Absent,
                SchemaState::ZeroX => SchemaStateName::ZeroX,
                SchemaState::Unknown => SchemaStateName::Unknown,
            },
            stored: match status.version {
                Some(version) => StoredVersion::Stored {
                    version: bigint_of_i64(version),
                },
                None => StoredVersion::NoVersion,
            },
            expected_version: bigint_of_i64(status.expected_version),
        }
    }
}

/// Install the schema through the handle's broker: the migrations, then the
/// coverage pass. Writes. The broker keeps a success, so a second install on
/// the same handle runs nothing.
pub async fn install_operation(connection: BrokerConnection) -> InstallOutcome {
    match connection.schema_mode() {
        SchemaMode::ObserveOnly | SchemaMode::AssumeInitialized => {
            return InstallOutcome::NotRun;
        }
        SchemaMode::MigrateAndValidate => {}
    }
    let installed = match connection.broker().await {
        Ok(broker) => broker.ensure_schema_initialized().await,
        Err(error) => Err(error),
    };
    match installed {
        Ok(()) => InstallOutcome::Installed {
            version: bigint_of_i64(expected_schema_version()),
        },
        Err(error) => InstallOutcome::Failed {
            error: RawCatalogError::of(
                INSTALL_SUBJECT,
                &DatabaseOperationError::schema_init_failed(error),
            ),
        },
    }
}

/// Read the schema state. Reads only; never fails: a database that cannot
/// be reached or read is `UNKNOWN`, and the cause is logged.
pub async fn schema_status_operation(connection: BrokerConnection) -> SchemaStatusRecord {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            tracing::warn!(error = %error, "schema status: the broker could not connect");
            return SchemaStatusRecord::from(SchemaStatus {
                state: SchemaState::Unknown,
                version: None,
                expected_version: expected_schema_version(),
            });
        }
    };
    SchemaStatusRecord::from(SchemaProbe::new(broker).status().await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_status_with_a_version_keeps_it_as_stored() {
        assert_eq!(
            SchemaStatusRecord::from(SchemaStatus {
                state: SchemaState::Mismatch,
                version: Some(1003),
                expected_version: 1006,
            }),
            SchemaStatusRecord {
                state: SchemaStateName::Mismatch,
                stored: StoredVersion::Stored {
                    version: bigint_of_i64(1003)
                },
                expected_version: bigint_of_i64(1006),
            }
        );
    }

    #[test]
    fn a_status_with_no_version_has_no_version() {
        assert_eq!(
            SchemaStatusRecord::from(SchemaStatus {
                state: SchemaState::Unknown,
                version: None,
                expected_version: 1006,
            })
            .stored,
            StoredVersion::NoVersion
        );
    }

    #[test]
    fn each_schema_state_has_its_name() {
        let cases = [
            (SchemaState::Match, SchemaStateName::Match),
            (SchemaState::Mismatch, SchemaStateName::Mismatch),
            (SchemaState::Absent, SchemaStateName::Absent),
            (SchemaState::ZeroX, SchemaStateName::ZeroX),
            (SchemaState::Unknown, SchemaStateName::Unknown),
        ];
        for (state, name) in cases {
            let record = SchemaStatusRecord::from(SchemaStatus {
                state,
                version: None,
                expected_version: 1006,
            });
            assert_eq!(record.state, name, "{state:?}");
        }
    }
}
