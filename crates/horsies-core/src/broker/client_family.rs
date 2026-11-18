//! The client family record: which client library installed a database.
//!
//! The install writes the one row of `horsies_client_family`; every handle
//! checks it once before its first write. A handle writes only into a
//! database of its own family; a database of the other family, or one with
//! no family row, is refused with [`BrokerError::ClientFamilyMismatch`], not
//! retryable. A family row whose value is not a client family is corrupt
//! stored data: it is refused with [`BrokerError::UnknownStoredFamily`], not
//! retryable, and nothing is written. Reads are not checked. Python
//! reference: `python/horsies/core/baseline/install.py` (`ClientFamily`,
//! `RecordedFamily`, `_INSERT_FAMILY_SQL`).

use crate::broker::core_rows::RowDecodeError;
use crate::broker::error::BrokerError;
use crate::broker::pools::PoolSettingsError;
use crate::db::{ParameterType, StatementFailure, Transience, TypedStatement};

/// The client library that installed a database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientFamily {
    Python,
    Rust,
}

impl ClientFamily {
    /// The stored value: `PYTHON` or `RUST`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Python => "PYTHON",
            Self::Rust => "RUST",
        }
    }

    /// Parse a stored value. The CHECK `horsies_client_family_family_known`
    /// holds the column to these two values; `None` for any other text.
    pub fn parse(stored: &str) -> Option<Self> {
        match stored {
            "PYTHON" => Some(Self::Python),
            "RUST" => Some(Self::Rust),
            _ => None,
        }
    }
}

/// What a database records about its family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordedFamily {
    /// The family row exists.
    Recorded(ClientFamily),
    /// The family table has no row: the install stopped before it wrote one.
    NotRecorded,
}

impl std::fmt::Display for RecordedFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Recorded(family) => write!(f, "the {} client", family.as_str()),
            Self::NotRecorded => write!(f, "no client family"),
        }
    }
}

/// The family row holds a value that is not a client family of this build.
/// The CHECK `horsies_client_family_family_known` refuses such a value, so
/// the row or its CHECK was changed outside horsies: the stored data is
/// corrupt. A handle does not install into or write into such a database.
/// Not retryable: the same row meets the same refusal until it is repaired.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "the client family row of this database is corrupt: horsies_client_family.family holds \
     {stored:?}, which is neither PYTHON nor RUST"
)]
pub struct UnknownStoredFamily {
    /// The stored text of `horsies_client_family.family`.
    pub stored: String,
}

impl From<UnknownStoredFamily> for BrokerError {
    fn from(unknown: UnknownStoredFamily) -> Self {
        BrokerError::UnknownStoredFamily(unknown)
    }
}

/// The database records another client family than the handle's, or none.
/// Not retryable: the database stays of its family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FamilyMismatch {
    /// What the database records.
    pub stored: RecordedFamily,
    /// The family of the handle.
    pub own: ClientFamily,
}

impl From<FamilyMismatch> for BrokerError {
    fn from(mismatch: FamilyMismatch) -> Self {
        BrokerError::ClientFamilyMismatch {
            stored: mismatch.stored,
            own: mismatch.own,
        }
    }
}

/// Why the family check before a handle's first write could not read the
/// family row. Nothing was written. The check keeps no answer after a
/// failure, so the next write reads the row again.
#[derive(Debug, thiserror::Error)]
pub enum FamilyCheckFailure {
    /// The read of the family row failed on the main core pool. Retryable
    /// when its cause may clear ([`StatementFailure::transience`]).
    #[error("the client-family read failed: {0}")]
    Statement(StatementFailure),
    /// The handle was closed, and its core pools could not be built again:
    /// the connection settings were refused. Not retryable: the same
    /// settings are refused again.
    #[error("the core pools could not be opened again: {0}")]
    PoolsNotReopened(PoolSettingsError),
    /// The `family` column of the row does not decode as text. Not
    /// retryable: the same row meets the same refusal.
    #[error("the client-family row does not decode: {0}")]
    FamilyColumnUndecodable(RowDecodeError),
}

impl FamilyCheckFailure {
    /// Whether the cause may clear, so a repeat of the check can pass.
    pub fn transience(&self) -> Transience {
        match self {
            Self::Statement(failure) => failure.transience(),
            Self::PoolsNotReopened(_) | Self::FamilyColumnUndecodable(_) => Transience::Permanent,
        }
    }
}

impl From<FamilyCheckFailure> for BrokerError {
    fn from(failure: FamilyCheckFailure) -> Self {
        match failure {
            FamilyCheckFailure::Statement(failure) => BrokerError::Statement(failure),
            FamilyCheckFailure::PoolsNotReopened(error) => BrokerError::PoolSettings(error),
            FamilyCheckFailure::FamilyColumnUndecodable(error) => BrokerError::RowDecode(error),
        }
    }
}

/// Why a handle may not write: the database is of another family, its family
/// row holds a value that is not a client family, or the family row could
/// not be read.
#[derive(Debug)]
pub(crate) enum FamilyCheckError {
    Mismatch(FamilyMismatch),
    UnknownStoredFamily(UnknownStoredFamily),
    ReadFailed(FamilyCheckFailure),
}

pub(crate) const READ_FAMILY_SQL: &str = "SELECT family FROM horsies_client_family";

/// Writes `$1` as the family row when the table has none (Python
/// `_INSERT_FAMILY_SQL`). The schema install runs it.
pub(crate) static RECORD_CLIENT_FAMILY: TypedStatement = TypedStatement::new(
    "record_client_family",
    "INSERT INTO horsies_client_family (only_row, family, recorded_at)
     VALUES (TRUE, $1, now())
     ON CONFLICT (only_row) DO NOTHING",
    &[ParameterType::Text],
);

/// The recorded family of the stored `family` value (`None`: no row). Pure.
///
/// Errors: [`UnknownStoredFamily`] for a stored value that is neither
/// `PYTHON` nor `RUST`.
pub(crate) fn recorded_family_from(
    stored: Option<String>,
) -> Result<RecordedFamily, UnknownStoredFamily> {
    match stored {
        None => Ok(RecordedFamily::NotRecorded),
        Some(text) => match ClientFamily::parse(&text) {
            Some(family) => Ok(RecordedFamily::Recorded(family)),
            None => Err(UnknownStoredFamily { stored: text }),
        },
    }
}

/// The answer to a recorded family for a handle of family `own`.
pub(crate) fn admit_family(
    recorded: RecordedFamily,
    own: ClientFamily,
) -> Result<(), FamilyMismatch> {
    match recorded {
        RecordedFamily::Recorded(stored) if stored == own => Ok(()),
        RecordedFamily::Recorded(_) | RecordedFamily::NotRecorded => Err(FamilyMismatch {
            stored: recorded,
            own,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stored_names_round_trip() {
        for family in [ClientFamily::Python, ClientFamily::Rust] {
            assert_eq!(ClientFamily::parse(family.as_str()), Some(family));
        }
        assert_eq!(ClientFamily::parse("rust"), None);
    }

    #[test]
    fn a_stored_value_is_a_family_no_family_or_an_unknown_family() {
        assert_eq!(recorded_family_from(None), Ok(RecordedFamily::NotRecorded));
        for family in [ClientFamily::Python, ClientFamily::Rust] {
            assert_eq!(
                recorded_family_from(Some(family.as_str().to_owned())),
                Ok(RecordedFamily::Recorded(family))
            );
        }
        for stored in ["GO", "rust", ""] {
            assert_eq!(
                recorded_family_from(Some(stored.to_owned())),
                Err(UnknownStoredFamily {
                    stored: stored.to_owned()
                })
            );
        }
    }

    /// The broker error of an unknown stored family names the table, the
    /// column and the stored value; the stored row is a broken read
    /// contract, not a precondition, and it does not clear.
    #[test]
    fn an_unknown_stored_family_is_a_named_broker_error_that_does_not_clear() {
        let unknown = UnknownStoredFamily {
            stored: "GO".to_owned(),
        };
        let error = BrokerError::from(unknown.clone());
        assert!(
            matches!(&error, BrokerError::UnknownStoredFamily(inner) if *inner == unknown),
            "{error:?}"
        );
        assert!(!error.is_retryable());
        assert_eq!(
            error.failure_cause(),
            crate::failure_cause::FailureCause::ContractViolation
        );
        let message = error.to_string();
        assert!(
            message.contains("horsies_client_family.family holds \"GO\""),
            "{message}"
        );
    }

    #[test]
    fn only_the_own_family_is_admitted() {
        assert!(admit_family(
            RecordedFamily::Recorded(ClientFamily::Rust),
            ClientFamily::Rust
        )
        .is_ok());
        for recorded in [
            RecordedFamily::Recorded(ClientFamily::Python),
            RecordedFamily::NotRecorded,
        ] {
            let refused = admit_family(recorded, ClientFamily::Rust).unwrap_err();
            assert_eq!(
                refused,
                FamilyMismatch {
                    stored: recorded,
                    own: ClientFamily::Rust,
                }
            );
            let broker_error = BrokerError::from(refused);
            assert!(matches!(
                broker_error,
                BrokerError::ClientFamilyMismatch { stored, own: ClientFamily::Rust } if stored == recorded
            ));
            assert!(!broker_error.is_retryable());
        }
    }

    fn statement_failure(cause: crate::db::NotSentCause) -> StatementFailure {
        StatementFailure::NotSent {
            statement: "read_client_family",
            cause,
        }
    }

    fn pools_not_reopened() -> FamilyCheckFailure {
        FamilyCheckFailure::PoolsNotReopened(PoolSettingsError::Environment(
            crate::db::EnvironmentError::NotUnicode { name: "PGPASSWORD" },
        ))
    }

    fn family_column_undecodable() -> FamilyCheckFailure {
        FamilyCheckFailure::FamilyColumnUndecodable(RowDecodeError::NoRow {
            statement: "read_client_family",
        })
    }

    /// A lost connection may clear; a closed pool does not.
    #[test]
    fn a_failed_family_read_keeps_the_transience_of_its_statement() {
        let lost = FamilyCheckFailure::Statement(statement_failure(
            crate::db::NotSentCause::ConnectionClosed,
        ));
        assert_eq!(lost.transience(), Transience::Transient);
        let closed = FamilyCheckFailure::Statement(statement_failure(
            crate::db::NotSentCause::Checkout(crate::db::CheckoutFailure::PoolClosed {
                role: crate::db::PoolRole::Main,
            }),
        ));
        assert_eq!(closed.transience(), Transience::Permanent);
    }

    #[test]
    fn refused_pool_settings_and_an_undecodable_family_column_do_not_clear() {
        assert_eq!(pools_not_reopened().transience(), Transience::Permanent);
        assert_eq!(
            family_column_undecodable().transience(),
            Transience::Permanent
        );
    }

    /// The write paths keep the broker error each failure had before.
    #[test]
    fn each_failure_keeps_its_broker_error() {
        let lost = FamilyCheckFailure::Statement(statement_failure(
            crate::db::NotSentCause::ConnectionClosed,
        ));
        let lost = BrokerError::from(lost);
        assert!(matches!(lost, BrokerError::Statement(_)));
        assert!(lost.is_retryable());
        assert!(matches!(
            BrokerError::from(pools_not_reopened()),
            BrokerError::PoolSettings(PoolSettingsError::Environment(_))
        ));
        assert!(matches!(
            BrokerError::from(family_column_undecodable()),
            BrokerError::RowDecode(RowDecodeError::NoRow { .. })
        ));
    }
}
