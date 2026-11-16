//! The client family record: which client library installed a database.
//!
//! The install writes the one row of `horsies_client_family`; every handle
//! checks it once before its first write. A handle writes only into a
//! database of its own family; a database of the other family, or one with
//! no family row, is refused with [`BrokerError::ClientFamilyMismatch`], not
//! retryable. Reads are not checked. Python reference:
//! `horsies/core/baseline/install.py` (`ClientFamily`, `RecordedFamily`,
//! `_INSERT_FAMILY_SQL`).

use crate::broker::error::BrokerError;
use crate::db::{ParameterType, TypedStatement};

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

/// The recorded family of the stored `family` value (none: no row).
///
/// # Panics
///
/// A stored value outside the two names: the CHECK
/// `horsies_client_family_family_known` makes it impossible, so it is a
/// broken schema, not a family (Python raises here too).
pub(crate) fn recorded_family_from(stored: Option<String>) -> RecordedFamily {
    match stored {
        None => RecordedFamily::NotRecorded,
        Some(text) => match ClientFamily::parse(&text) {
            Some(family) => RecordedFamily::Recorded(family),
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
