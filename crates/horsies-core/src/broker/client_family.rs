//! The client family record: which client library installed a database.
//!
//! The install writes the one row of `horsies_client_family`; every handle
//! checks it once before its first write. A handle writes only into a
//! database of its own family; a database of the other family, or one with
//! no family row, is refused with [`BrokerError::ClientFamilyMismatch`], not
//! retryable. Reads are not checked. Python reference:
//! `horsies/core/baseline/install.py` (`ClientFamily`, `RecordedFamily`,
//! `_INSERT_FAMILY_SQL`).

use sqlx::PgPool;

use crate::broker::error::BrokerError;

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
    /// holds the column to these two values.
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

const READ_FAMILY_SQL: &str = "SELECT family FROM horsies_client_family";

const INSERT_FAMILY_SQL: &str = "INSERT INTO horsies_client_family (only_row, family, recorded_at)
     VALUES (TRUE, $1, NOW())
     ON CONFLICT (only_row) DO NOTHING";

/// Read the recorded family.
///
/// # Panics
///
/// A stored value outside the two names: the CHECK
/// `horsies_client_family_family_known` makes it impossible, so it is a
/// broken schema, not a family (Python raises here too).
pub(crate) async fn read_recorded_family(pool: &PgPool) -> Result<RecordedFamily, BrokerError> {
    let stored: Option<String> = sqlx::query_scalar(READ_FAMILY_SQL)
        .fetch_optional(pool)
        .await?;
    match stored {
        None => Ok(RecordedFamily::NotRecorded),
        Some(text) => match ClientFamily::parse(&text) {
            Some(family) => Ok(RecordedFamily::Recorded(family)),
            None => panic!("horsies_client_family.family holds {text:?}, which its CHECK refuses"),
        },
    }
}

/// The answer to a recorded family for a handle of family `own`.
pub(crate) fn admit_family(recorded: RecordedFamily, own: ClientFamily) -> Result<(), BrokerError> {
    match recorded {
        RecordedFamily::Recorded(stored) if stored == own => Ok(()),
        RecordedFamily::Recorded(_) | RecordedFamily::NotRecorded => {
            Err(BrokerError::ClientFamilyMismatch {
                stored: recorded,
                own,
            })
        }
    }
}

/// Record `own` as the family of an installed database, then check it: the
/// insert does nothing when a row exists, and the row read back must name
/// `own`. Called by the install after the migrations.
pub(crate) async fn record_client_family(
    pool: &PgPool,
    own: ClientFamily,
) -> Result<(), BrokerError> {
    if let RecordedFamily::Recorded(stored) = read_recorded_family(pool).await? {
        return admit_family(RecordedFamily::Recorded(stored), own);
    }
    sqlx::query(INSERT_FAMILY_SQL)
        .bind(own.as_str())
        .execute(pool)
        .await?;
    admit_family(read_recorded_family(pool).await?, own)
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
            assert!(matches!(
                refused,
                BrokerError::ClientFamilyMismatch { stored, own: ClientFamily::Rust } if stored == recorded
            ));
            assert!(!refused.is_retryable());
        }
    }
}
