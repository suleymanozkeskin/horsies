//! Validated partition-maintenance commands.

use chrono::{DateTime, Duration, TimeZone, Timelike, Utc};

use crate::core::history::ddl::classes::FOREVER_CLASS_KEY;

pub const DETACH_STATEMENT_TIMEOUT_MS: u64 = 5_000;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HistoryCommandError {
    #[error("{0}")]
    Invalid(&'static str),
}

pub fn is_safe_identifier(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafBounds {
    lower: DateTime<Utc>,
    upper: DateTime<Utc>,
}

impl LeafBounds {
    pub fn new(lower: DateTime<Utc>, upper: DateTime<Utc>) -> Result<Self, HistoryCommandError> {
        if lower >= upper {
            return Err(HistoryCommandError::Invalid(
                "leaf bounds must be increasing",
            ));
        }
        Ok(Self { lower, upper })
    }

    pub fn spans_one_day(&self) -> bool {
        self.upper - self.lower == Duration::days(1)
    }

    pub fn lower(&self) -> DateTime<Utc> {
        self.lower
    }

    pub fn upper(&self) -> DateTime<Utc> {
        self.upper
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafRef {
    leaf_name: String,
    class_key: String,
    bounds: LeafBounds,
}

impl LeafRef {
    pub fn new(
        leaf_name: impl Into<String>,
        class_key: impl Into<String>,
        bounds: LeafBounds,
    ) -> Result<Self, HistoryCommandError> {
        let leaf_name = leaf_name.into();
        let class_key = class_key.into();
        if !is_safe_identifier(&leaf_name) {
            return Err(HistoryCommandError::Invalid(
                "leaf name must be a safe PostgreSQL identifier",
            ));
        }
        if class_key.is_empty() {
            return Err(HistoryCommandError::Invalid("class key must be non-empty"));
        }
        Ok(Self {
            leaf_name,
            class_key,
            bounds,
        })
    }

    pub fn leaf_name(&self) -> &str {
        &self.leaf_name
    }

    pub fn class_key(&self) -> &str {
        &self.class_key
    }

    pub fn bounds(&self) -> &LeafBounds {
        &self.bounds
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDailyHistoryLeaf {
    leaf: LeafRef,
}

impl CreateDailyHistoryLeaf {
    pub fn new(leaf: LeafRef) -> Result<Self, HistoryCommandError> {
        if !leaf.bounds.spans_one_day() {
            return Err(HistoryCommandError::Invalid(
                "daily leaf bounds must span exactly one day",
            ));
        }
        Ok(Self { leaf })
    }

    pub fn leaf(&self) -> &LeafRef {
        &self.leaf
    }
}

/// Upper bound of the open-ended `forever` leaf. A finite timestamp, so
/// chrono, the partition bound, and the staged-reader literals carry it like
/// any other bound.
pub fn open_end_anchor() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(9999, 1, 1, 0, 0, 0)
        .single()
        .expect("9999-01-01T00:00:00Z is a valid UTC timestamp")
}

/// `open_end_anchor()` as a PostgreSQL literal.
pub const OPEN_END_ANCHOR_SQL: &str = "TIMESTAMPTZ '9999-01-01 00:00:00+00'";

/// Creates the one open-ended `forever` leaf: from a UTC midnight to
/// `open_end_anchor()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateOpenEndedHistoryLeaf {
    leaf: LeafRef,
}

impl CreateOpenEndedHistoryLeaf {
    pub fn new(leaf: LeafRef) -> Result<Self, HistoryCommandError> {
        if leaf.class_key != FOREVER_CLASS_KEY {
            return Err(HistoryCommandError::Invalid(
                "an open-ended leaf belongs to the forever class",
            ));
        }
        if leaf.bounds.upper != open_end_anchor() {
            return Err(HistoryCommandError::Invalid(
                "an open-ended leaf ends at the open-end anchor",
            ));
        }
        let lower = leaf.bounds.lower;
        if (
            lower.hour(),
            lower.minute(),
            lower.second(),
            lower.nanosecond(),
        ) != (0, 0, 0, 0)
        {
            return Err(HistoryCommandError::Invalid(
                "an open-ended leaf starts at a UTC midnight",
            ));
        }
        Ok(Self { leaf })
    }

    pub fn leaf(&self) -> &LeafRef {
        &self.leaf
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnsureLeafCoverage {
    class_key: String,
    horizon_days: u32,
}

impl EnsureLeafCoverage {
    pub fn new(
        class_key: impl Into<String>,
        horizon_days: u32,
    ) -> Result<Self, HistoryCommandError> {
        let class_key = class_key.into();
        if class_key.is_empty() {
            return Err(HistoryCommandError::Invalid("class key must be non-empty"));
        }
        if horizon_days < 2 {
            return Err(HistoryCommandError::Invalid(
                "coverage horizon must include at least two future leaves",
            ));
        }
        Ok(Self {
            class_key,
            horizon_days,
        })
    }

    pub fn class_key(&self) -> &str {
        &self.class_key
    }

    pub fn horizon_days(&self) -> u32 {
        self.horizon_days
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetachExpiredHistoryLeaf {
    leaf: LeafRef,
    quarantine_horizon: Option<Duration>,
    statement_timeout_ms: Option<u64>,
}

impl DetachExpiredHistoryLeaf {
    pub fn new(
        leaf: LeafRef,
        quarantine_horizon: Option<Duration>,
        statement_timeout_ms: Option<u64>,
    ) -> Result<Self, HistoryCommandError> {
        if quarantine_horizon.is_some_and(|duration| duration <= Duration::zero()) {
            return Err(HistoryCommandError::Invalid(
                "quarantine horizon must be positive",
            ));
        }
        if statement_timeout_ms == Some(0) {
            return Err(HistoryCommandError::Invalid(
                "statement timeout must be positive",
            ));
        }
        Ok(Self {
            leaf,
            quarantine_horizon,
            statement_timeout_ms,
        })
    }

    pub fn leaf(&self) -> &LeafRef {
        &self.leaf
    }

    pub fn quarantine_horizon(&self) -> Option<Duration> {
        self.quarantine_horizon
    }

    pub fn statement_timeout_ms(&self) -> Option<u64> {
        self.statement_timeout_ms
    }
}

/// What makes a leaf eligible to leave its parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafLifecycle {
    /// A finite retention class: eligible at the upper bound plus the class
    /// duration. A `forever` leaf is refused.
    Retention,
    /// A closed daily `forever` leaf: eligible at its upper bound, and only
    /// while it holds no rows.
    ClosedEmptyForever,
}

/// A daily `forever` leaf, the only leaf the `ClosedEmptyForever` lifecycle accepts.
fn closed_forever_leaf(leaf: &LeafRef) -> Result<(), HistoryCommandError> {
    if leaf.class_key != FOREVER_CLASS_KEY || !leaf.bounds.spans_one_day() {
        return Err(HistoryCommandError::Invalid(
            "the closed-empty-forever lifecycle accepts only daily forever leaves",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeInterruptedLeafDetach {
    leaf: LeafRef,
    statement_timeout_ms: Option<u64>,
    lifecycle: LeafLifecycle,
}

impl FinalizeInterruptedLeafDetach {
    pub fn new(
        leaf: LeafRef,
        statement_timeout_ms: Option<u64>,
    ) -> Result<Self, HistoryCommandError> {
        if statement_timeout_ms == Some(0) {
            return Err(HistoryCommandError::Invalid(
                "statement timeout must be positive",
            ));
        }
        Ok(Self {
            leaf,
            statement_timeout_ms,
            lifecycle: LeafLifecycle::Retention,
        })
    }

    /// Finalizes an interrupted detach of a closed daily `forever` leaf.
    pub fn closed_empty_forever(
        leaf: LeafRef,
        statement_timeout_ms: Option<u64>,
    ) -> Result<Self, HistoryCommandError> {
        closed_forever_leaf(&leaf)?;
        let command = Self::new(leaf, statement_timeout_ms)?;
        Ok(Self {
            lifecycle: LeafLifecycle::ClosedEmptyForever,
            ..command
        })
    }

    pub fn leaf(&self) -> &LeafRef {
        &self.leaf
    }

    pub fn statement_timeout_ms(&self) -> Option<u64> {
        self.statement_timeout_ms
    }

    pub fn lifecycle(&self) -> LeafLifecycle {
        self.lifecycle
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectHistoryLeaf {
    leaf: LeafRef,
    lifecycle: LeafLifecycle,
}

impl InspectHistoryLeaf {
    pub fn new(leaf: LeafRef) -> Self {
        Self {
            leaf,
            lifecycle: LeafLifecycle::Retention,
        }
    }

    /// Inspects a closed daily `forever` leaf.
    pub fn closed_empty_forever(leaf: LeafRef) -> Result<Self, HistoryCommandError> {
        closed_forever_leaf(&leaf)?;
        Ok(Self {
            leaf,
            lifecycle: LeafLifecycle::ClosedEmptyForever,
        })
    }

    pub fn with_lifecycle(
        leaf: LeafRef,
        lifecycle: LeafLifecycle,
    ) -> Result<Self, HistoryCommandError> {
        match lifecycle {
            LeafLifecycle::Retention => Ok(Self::new(leaf)),
            LeafLifecycle::ClosedEmptyForever => Self::closed_empty_forever(leaf),
        }
    }

    pub fn leaf(&self) -> &LeafRef {
        &self.leaf
    }

    pub fn lifecycle(&self) -> LeafLifecycle {
        self.lifecycle
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropDetachedHistoryLeaf {
    leaf: LeafRef,
    lifecycle: LeafLifecycle,
}

impl DropDetachedHistoryLeaf {
    pub fn new(leaf: LeafRef) -> Self {
        Self {
            leaf,
            lifecycle: LeafLifecycle::Retention,
        }
    }

    /// Drops a detached closed daily `forever` leaf, only while it holds no rows.
    pub fn closed_empty_forever(leaf: LeafRef) -> Result<Self, HistoryCommandError> {
        closed_forever_leaf(&leaf)?;
        Ok(Self {
            leaf,
            lifecycle: LeafLifecycle::ClosedEmptyForever,
        })
    }

    pub fn leaf(&self) -> &LeafRef {
        &self.leaf
    }

    pub fn lifecycle(&self) -> LeafLifecycle {
        self.lifecycle
    }
}

/// Detaches a closed daily `forever` leaf that holds no rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetachEmptyForeverLeaf {
    leaf: LeafRef,
    statement_timeout_ms: Option<u64>,
}

impl DetachEmptyForeverLeaf {
    pub fn new(
        leaf: LeafRef,
        statement_timeout_ms: Option<u64>,
    ) -> Result<Self, HistoryCommandError> {
        closed_forever_leaf(&leaf)?;
        if statement_timeout_ms == Some(0) {
            return Err(HistoryCommandError::Invalid(
                "statement timeout must be positive",
            ));
        }
        Ok(Self {
            leaf,
            statement_timeout_ms,
        })
    }

    pub fn leaf(&self) -> &LeafRef {
        &self.leaf
    }

    pub fn statement_timeout_ms(&self) -> Option<u64> {
        self.statement_timeout_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectPartitionHealth {
    class_key: String,
    application_managed: bool,
}

impl CollectPartitionHealth {
    pub fn new(
        class_key: impl Into<String>,
        application_managed: bool,
    ) -> Result<Self, HistoryCommandError> {
        let class_key = class_key.into();
        if class_key.is_empty() {
            return Err(HistoryCommandError::Invalid("class key must be non-empty"));
        }
        Ok(Self {
            class_key,
            application_managed,
        })
    }

    pub fn class_key(&self) -> &str {
        &self.class_key
    }

    pub fn application_managed(&self) -> bool {
        self.application_managed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionMaintenanceCommand {
    InspectHistoryLeaf(InspectHistoryLeaf),
    CreateDailyHistoryLeaf(CreateDailyHistoryLeaf),
    EnsureLeafCoverage(EnsureLeafCoverage),
    DetachExpiredHistoryLeaf(DetachExpiredHistoryLeaf),
    FinalizeInterruptedLeafDetach(FinalizeInterruptedLeafDetach),
    DropDetachedHistoryLeaf(DropDetachedHistoryLeaf),
    CollectPartitionHealth(CollectPartitionHealth),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bounds(days: i64) -> LeafBounds {
        LeafBounds::new(
            DateTime::from_timestamp(0, 0).unwrap(),
            DateTime::from_timestamp(days * 86_400, 0).unwrap(),
        )
        .unwrap()
    }

    fn leaf(days: i64) -> LeafRef {
        LeafRef::new("history_leaf", "finite", bounds(days)).unwrap()
    }

    #[test]
    fn identifier_contract_accepts_only_postgres_safe_names() {
        assert!(is_safe_identifier("a"));
        assert!(is_safe_identifier(&"a".repeat(63)));
        for value in ["", "Upper", "1leading", "has-hyphen", "has space"] {
            assert!(!is_safe_identifier(value), "accepted {value:?}");
        }
        assert!(!is_safe_identifier(&"a".repeat(64)));
    }

    #[test]
    fn bounds_and_leaf_reject_invalid_identity() {
        let instant = DateTime::from_timestamp(0, 0).unwrap();
        assert!(LeafBounds::new(instant, instant).is_err());
        assert!(LeafBounds::new(instant + Duration::days(1), instant).is_err());
        assert!(LeafRef::new("unsafe-name", "finite", bounds(1)).is_err());
        assert!(LeafRef::new("history_leaf", "", bounds(1)).is_err());
    }

    #[test]
    fn daily_and_coverage_commands_enforce_shape_and_floor() {
        assert!(CreateDailyHistoryLeaf::new(leaf(1)).is_ok());
        assert!(CreateDailyHistoryLeaf::new(leaf(2)).is_err());
        assert!(EnsureLeafCoverage::new("finite", 2).is_ok());
        assert!(EnsureLeafCoverage::new("finite", 1).is_err());
        assert!(EnsureLeafCoverage::new("", 3).is_err());
    }

    #[test]
    fn detach_and_finalize_require_explicit_positive_values() {
        assert!(DetachExpiredHistoryLeaf::new(leaf(1), None, None).is_ok());
        assert!(DetachExpiredHistoryLeaf::new(leaf(1), Some(Duration::zero()), None).is_err());
        assert!(DetachExpiredHistoryLeaf::new(leaf(1), None, Some(0)).is_err());
        assert!(FinalizeInterruptedLeafDetach::new(leaf(1), None).is_ok());
        assert!(FinalizeInterruptedLeafDetach::new(leaf(1), Some(0)).is_err());
    }

    #[test]
    fn command_union_carries_exactly_the_seven_variants() {
        let variants = [
            PartitionMaintenanceCommand::InspectHistoryLeaf(InspectHistoryLeaf::new(leaf(1))),
            PartitionMaintenanceCommand::CreateDailyHistoryLeaf(
                CreateDailyHistoryLeaf::new(leaf(1)).unwrap(),
            ),
            PartitionMaintenanceCommand::EnsureLeafCoverage(
                EnsureLeafCoverage::new("finite", 2).unwrap(),
            ),
            PartitionMaintenanceCommand::DetachExpiredHistoryLeaf(
                DetachExpiredHistoryLeaf::new(leaf(1), None, Some(5_000)).unwrap(),
            ),
            PartitionMaintenanceCommand::FinalizeInterruptedLeafDetach(
                FinalizeInterruptedLeafDetach::new(leaf(1), Some(5_000)).unwrap(),
            ),
            PartitionMaintenanceCommand::DropDetachedHistoryLeaf(DropDetachedHistoryLeaf::new(
                leaf(1),
            )),
            PartitionMaintenanceCommand::CollectPartitionHealth(
                CollectPartitionHealth::new("finite", true).unwrap(),
            ),
        ];
        assert_eq!(variants.len(), 7);
        assert!(CollectPartitionHealth::new("", false).is_err());
    }

    #[test]
    fn open_ended_leaf_command_accepts_only_forever_midnight_to_the_open_end() {
        let midnight = Utc.with_ymd_and_hms(2026, 9, 26, 0, 0, 0).unwrap();
        let open = |class_key: &str, lower: DateTime<Utc>, upper: DateTime<Utc>| {
            CreateOpenEndedHistoryLeaf::new(
                LeafRef::new(
                    "horsies_task_history_forever_open_2026_09_26",
                    class_key,
                    LeafBounds::new(lower, upper).unwrap(),
                )
                .unwrap(),
            )
        };
        assert!(open("forever", midnight, open_end_anchor()).is_ok());
        assert_eq!(
            open("standard_30d", midnight, open_end_anchor()),
            Err(HistoryCommandError::Invalid(
                "an open-ended leaf belongs to the forever class"
            ))
        );
        assert_eq!(
            open("forever", midnight, midnight + Duration::days(1)),
            Err(HistoryCommandError::Invalid(
                "an open-ended leaf ends at the open-end anchor"
            ))
        );
        assert_eq!(
            open("forever", midnight + Duration::hours(1), open_end_anchor()),
            Err(HistoryCommandError::Invalid(
                "an open-ended leaf starts at a UTC midnight"
            ))
        );
        assert_eq!(open_end_anchor().to_rfc3339(), "9999-01-01T00:00:00+00:00");
        assert_eq!(OPEN_END_ANCHOR_SQL, "TIMESTAMPTZ '9999-01-01 00:00:00+00'");
    }
}
