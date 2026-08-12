//! Validated partition-maintenance commands.

use chrono::{DateTime, Duration, Utc};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeInterruptedLeafDetach {
    leaf: LeafRef,
    statement_timeout_ms: Option<u64>,
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
pub struct InspectHistoryLeaf {
    leaf: LeafRef,
}

impl InspectHistoryLeaf {
    pub fn new(leaf: LeafRef) -> Self {
        Self { leaf }
    }

    pub fn leaf(&self) -> &LeafRef {
        &self.leaf
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropDetachedHistoryLeaf {
    leaf: LeafRef,
}

impl DropDetachedHistoryLeaf {
    pub fn new(leaf: LeafRef) -> Self {
        Self { leaf }
    }

    pub fn leaf(&self) -> &LeafRef {
        &self.leaf
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
}
