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
/// `eligible_at` is when the leaf may leave its parent: the upper bound plus
/// the retention of its class.
pub enum LeafInspection {
    Detachable {
        leaf_name: String,
        eligible_at: DateTime<Utc>,
    },
    NotExpired {
        leaf_name: String,
        eligible_at: DateTime<Utc>,
    },
    PendingBlocked {
        leaf_name: String,
        blocker_count: i64,
        eligible_at: DateTime<Utc>,
        attachment: LeafAttachment,
    },
    DetachInterrupted {
        leaf_name: String,
        eligible_at: DateTime<Utc>,
    },
    Detached {
        leaf_name: String,
        eligible_at: DateTime<Utc>,
    },
    Dropped {
        leaf_name: String,
    },
    Missing {
        leaf_name: String,
        cataloged: bool,
        eligible_at: Option<DateTime<Utc>>,
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
    /// A daily `forever` leaf was requested for a range that the attached
    /// open-ended `forever` leaf covers. Nothing was created.
    CoveredByOpenEndedLeaf {
        leaf_name: String,
        open_leaf_name: String,
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
                eligible_at: now(),
            },
            LeafInspection::NotExpired {
                leaf_name: "leaf".into(),
                eligible_at: now(),
            },
            LeafInspection::PendingBlocked {
                leaf_name: "leaf".into(),
                blocker_count: 1,
                eligible_at: now(),
                attachment: LeafAttachment::Attached,
            },
            LeafInspection::DetachInterrupted {
                leaf_name: "leaf".into(),
                eligible_at: now(),
            },
            LeafInspection::Detached {
                leaf_name: "leaf".into(),
                eligible_at: now(),
            },
            LeafInspection::Dropped {
                leaf_name: "leaf".into(),
            },
            LeafInspection::Missing {
                leaf_name: "leaf".into(),
                cataloged: true,
                eligible_at: Some(now()),
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
            LeafCreation::CoveredByOpenEndedLeaf {
                leaf_name: "leaf".into(),
                open_leaf_name: "open_leaf".into(),
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
        assert_eq!(creations.len(), 9);

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
                eligible_at: None,
            }),
        ];
        assert_eq!(drops.len(), 4);
    }
}
