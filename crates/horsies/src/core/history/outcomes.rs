//! Exhaustive partition-maintenance outcomes and refusals.

use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogConflictKind {
    RelationWithoutCatalog,
    MetadataMismatch,
    PhysicalNonconformant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafAttachment {
    Attached,
    DetachInterrupted,
    Detached,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeafInspection {
    Detachable {
        leaf_name: String,
        expires_at: DateTime<Utc>,
    },
    NotExpired {
        leaf_name: String,
        expires_at: DateTime<Utc>,
    },
    PendingBlocked {
        leaf_name: String,
        blocker_count: i64,
        expires_at: DateTime<Utc>,
        attachment: LeafAttachment,
    },
    DetachInterrupted {
        leaf_name: String,
        expires_at: DateTime<Utc>,
    },
    Detached {
        leaf_name: String,
        expires_at: DateTime<Utc>,
    },
    Dropped {
        leaf_name: String,
    },
    Missing {
        leaf_name: String,
        cataloged: bool,
        expires_at: Option<DateTime<Utc>>,
    },
    RetentionClassAbsent {
        class_key: String,
    },
    ForeverClassLeaf {
        class_key: String,
    },
    CatalogConflict {
        leaf_name: String,
        kind: CatalogConflictKind,
        detail: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeafCreation {
    Busy {
        leaf_name: String,
    },
    Created {
        leaf_name: String,
        id_index_name: String,
    },
    AlreadyConformant {
        leaf_name: String,
    },
    IndexRepaired {
        leaf_name: String,
        id_index_name: String,
    },
    RetentionClassAbsent {
        class_key: String,
    },
    ForeverClassLeaf {
        class_key: String,
    },
    ClassIntervalMismatch {
        class_key: String,
        partition_interval_days: Option<i64>,
    },
    CatalogConflict {
        leaf_name: String,
        kind: CatalogConflictKind,
        detail: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeafDrop {
    Busy { leaf_name: String },
    Dropped { leaf_name: String },
    RefusedLoaderReferences { leaf_name: String },
    Inspection(LeafInspection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthFault {
    CoverageBelowFloor {
        class_key: String,
        complete_future_intervals: i64,
        coverage_until: Option<DateTime<Utc>>,
    },
    MissingDdlPrivilege {
        schema_create: bool,
        owns_parent: bool,
    },
    LeafNonconformant {
        leaf_name: String,
        kind: CatalogConflictKind,
        detail: String,
    },
    DetachAwaitingFinalize {
        leaf_name: String,
    },
    RetentionClassAbsent {
        class_key: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassCoverage {
    pub class_key: String,
    pub attached_leaf_count: i64,
    pub coverage_until: Option<DateTime<Utc>>,
    pub complete_future_intervals: i64,
    pub detachable_leaf_count: i64,
    pub pending_blocked_leaf_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionHealthReport {
    pub class_key: String,
    pub checked_at: DateTime<Utc>,
    pub coverage: Option<ClassCoverage>,
    pub faults: Vec<HealthFault>,
}

impl PartitionHealthReport {
    pub fn is_healthy(&self) -> bool {
        self.faults.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_754_482_400, 0).unwrap()
    }

    #[test]
    fn discriminant_and_union_vocabularies_are_exhaustive() {
        let conflict_kinds = [
            CatalogConflictKind::RelationWithoutCatalog,
            CatalogConflictKind::MetadataMismatch,
            CatalogConflictKind::PhysicalNonconformant,
        ];
        let attachments = [
            LeafAttachment::Attached,
            LeafAttachment::DetachInterrupted,
            LeafAttachment::Detached,
        ];
        assert_eq!(conflict_kinds.len(), 3);
        assert_eq!(attachments.len(), 3);

        let inspections = [
            LeafInspection::Detachable {
                leaf_name: "leaf".into(),
                expires_at: now(),
            },
            LeafInspection::NotExpired {
                leaf_name: "leaf".into(),
                expires_at: now(),
            },
            LeafInspection::PendingBlocked {
                leaf_name: "leaf".into(),
                blocker_count: 1,
                expires_at: now(),
                attachment: LeafAttachment::Attached,
            },
            LeafInspection::DetachInterrupted {
                leaf_name: "leaf".into(),
                expires_at: now(),
            },
            LeafInspection::Detached {
                leaf_name: "leaf".into(),
                expires_at: now(),
            },
            LeafInspection::Dropped {
                leaf_name: "leaf".into(),
            },
            LeafInspection::Missing {
                leaf_name: "leaf".into(),
                cataloged: true,
                expires_at: Some(now()),
            },
            LeafInspection::RetentionClassAbsent {
                class_key: "finite".into(),
            },
            LeafInspection::ForeverClassLeaf {
                class_key: "forever".into(),
            },
            LeafInspection::CatalogConflict {
                leaf_name: "leaf".into(),
                kind: CatalogConflictKind::MetadataMismatch,
                detail: "mismatch".into(),
            },
        ];
        assert_eq!(inspections.len(), 10);

        let creations = [
            LeafCreation::Busy {
                leaf_name: "leaf".into(),
            },
            LeafCreation::Created {
                leaf_name: "leaf".into(),
                id_index_name: "leaf_task_idx".into(),
            },
            LeafCreation::AlreadyConformant {
                leaf_name: "leaf".into(),
            },
            LeafCreation::IndexRepaired {
                leaf_name: "leaf".into(),
                id_index_name: "leaf_task_idx".into(),
            },
            LeafCreation::RetentionClassAbsent {
                class_key: "finite".into(),
            },
            LeafCreation::ForeverClassLeaf {
                class_key: "forever".into(),
            },
            LeafCreation::ClassIntervalMismatch {
                class_key: "finite".into(),
                partition_interval_days: Some(2),
            },
            LeafCreation::CatalogConflict {
                leaf_name: "leaf".into(),
                kind: CatalogConflictKind::PhysicalNonconformant,
                detail: "bound mismatch".into(),
            },
        ];
        assert_eq!(creations.len(), 8);

        let drops = [
            LeafDrop::Busy {
                leaf_name: "leaf".into(),
            },
            LeafDrop::Dropped {
                leaf_name: "leaf".into(),
            },
            LeafDrop::RefusedLoaderReferences {
                leaf_name: "leaf".into(),
            },
            LeafDrop::Inspection(LeafInspection::Missing {
                leaf_name: "leaf".into(),
                cataloged: false,
                expires_at: None,
            }),
        ];
        assert_eq!(drops.len(), 4);
    }

    #[test]
    fn every_health_fault_is_unhealthy_and_empty_faults_are_healthy() {
        let faults = vec![
            HealthFault::CoverageBelowFloor {
                class_key: "finite".into(),
                complete_future_intervals: 1,
                coverage_until: Some(now()),
            },
            HealthFault::MissingDdlPrivilege {
                schema_create: false,
                owns_parent: true,
            },
            HealthFault::LeafNonconformant {
                leaf_name: "leaf".into(),
                kind: CatalogConflictKind::PhysicalNonconformant,
                detail: "bound mismatch".into(),
            },
            HealthFault::DetachAwaitingFinalize {
                leaf_name: "leaf".into(),
            },
            HealthFault::RetentionClassAbsent {
                class_key: "finite".into(),
            },
        ];
        assert_eq!(faults.len(), 5);
        for fault in faults {
            assert!(!PartitionHealthReport {
                class_key: "finite".into(),
                checked_at: now(),
                coverage: None,
                faults: vec![fault],
            }
            .is_healthy());
        }
        assert!(PartitionHealthReport {
            class_key: "finite".into(),
            checked_at: now(),
            coverage: Some(ClassCoverage {
                class_key: "finite".into(),
                attached_leaf_count: 3,
                coverage_until: Some(now()),
                complete_future_intervals: 2,
                detachable_leaf_count: 0,
                pending_blocked_leaf_count: 0,
            }),
            faults: Vec::new(),
        }
        .is_healthy());
    }
}
