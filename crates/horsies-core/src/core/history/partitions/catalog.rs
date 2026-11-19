//! Fail-closed reads of the durable leaf catalog and PostgreSQL catalogs.

use chrono::{DateTime, Utc};

use crate::core::history::errors::HistoryError;

pub const ORDERING_INDEX_COLUMN: &str = "enqueued_at";

/// `lower_bound_kind` of a leaf whose partition starts at `MINVALUE`.
pub const OPEN_LOWER_BOUND_KIND: &str = "OPEN";
/// `lower_bound_kind` of a leaf whose partition starts at `lower_anchor`.
pub const AT_LOWER_BOUND_KIND: &str = "AT";

/// The lower bound of a cataloged leaf: the pair `(lower_bound_kind,
/// lower_anchor)` of the leaf catalog and the lookup manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LeafLowerBound {
    /// `OPEN` with no anchor: the partition starts at `MINVALUE`. Only the
    /// static `forever` leaf of the first migration has it. Sorts before every
    /// time, as `ORDER BY lower_anchor NULLS FIRST`.
    Open,
    /// `AT` with an anchor: every leaf that maintenance creates.
    At(DateTime<Utc>),
}

impl LeafLowerBound {
    /// The bound of a stored pair. The pair CHECKs of the catalog and the
    /// manifest allow only `OPEN` with no anchor and `AT` with one.
    pub fn decode(kind: &str, anchor: Option<DateTime<Utc>>) -> Result<Self, HistoryError> {
        match (kind, anchor) {
            (OPEN_LOWER_BOUND_KIND, None) => Ok(Self::Open),
            (AT_LOWER_BOUND_KIND, Some(anchor)) => Ok(Self::At(anchor)),
            (kind, anchor) => Err(HistoryError::contract(format!(
                "leaf lower bound kind {kind:?} with anchor {anchor:?} is not a stored pair"
            ))),
        }
    }

    /// The stored `lower_bound_kind`.
    pub fn kind(self) -> &'static str {
        match self {
            Self::Open => OPEN_LOWER_BOUND_KIND,
            Self::At(_) => AT_LOWER_BOUND_KIND,
        }
    }

    /// The stored `lower_anchor`: `None` for the open bound.
    pub fn at(self) -> Option<DateTime<Utc>> {
        match self {
            Self::Open => None,
            Self::At(value) => Some(value),
        }
    }
}

#[cfg(test)]
mod lower_bound_tests {
    use super::*;

    #[test]
    fn lower_bound_pairs_decode_closed() {
        let anchor = DateTime::from_timestamp(86_400, 0).unwrap();
        assert_eq!(
            LeafLowerBound::decode("OPEN", None).unwrap(),
            LeafLowerBound::Open
        );
        assert_eq!(
            LeafLowerBound::decode("AT", Some(anchor)).unwrap(),
            LeafLowerBound::At(anchor)
        );
        for (kind, stored) in [("OPEN", Some(anchor)), ("AT", None), ("MIN", None)] {
            assert!(
                LeafLowerBound::decode(kind, stored).is_err(),
                "{kind} {stored:?}"
            );
        }
        assert!(LeafLowerBound::Open < LeafLowerBound::At(anchor));
        assert_eq!(LeafLowerBound::Open.kind(), "OPEN");
        assert_eq!(LeafLowerBound::At(anchor).at(), Some(anchor));
    }
}
