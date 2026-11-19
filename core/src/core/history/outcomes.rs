//! Exhaustive partition-maintenance outcomes and refusals.

use chrono::{DateTime, Utc};

use crate::core::history::errors::HistoryError;

/// Why a leaf's catalog row and relation disagree with the expected leaf.
/// The leaf-creation functions of `0008_history_maintenance_functions.sql`
/// (`horsies_history_leaf_state`, `horsies_create_history_leaf`) and the leaf
/// inspection of `0006_history_functions.sql` (`horsies_inspect_history_leaf`)
/// name the same kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogConflictKind {
    RelationWithoutCatalog,
    MetadataMismatch,
    PhysicalNonconformant,
    /// A finite leaf's catalog row has an open lower bound.
    OpenLowerBound,
    /// The catalog recorded a drop and the relation exists.
    DroppedRelationExists,
    /// The leaf is attached to a parent other than its class parent.
    ForeignParent,
    /// The catalog recorded a detach and the leaf is attached.
    DetachRecordedWhileAttached,
    /// The parent relation does not exist.
    ParentMissing,
    /// Another relation holds a canonical index name of the leaf.
    ForeignIndex,
    /// The catalog row matches and the relation is gone.
    CatalogWithoutRelation,
    /// The static forever leaf has no catalog row and no relation.
    StaticLeafMissing,
}

impl CatalogConflictKind {
    /// The name the database functions use.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RelationWithoutCatalog => "RELATION_WITHOUT_CATALOG",
            Self::MetadataMismatch => "METADATA_MISMATCH",
            Self::PhysicalNonconformant => "PHYSICAL_NONCONFORMANT",
            Self::OpenLowerBound => "OPEN_LOWER_BOUND",
            Self::DroppedRelationExists => "DROPPED_RELATION_EXISTS",
            Self::ForeignParent => "FOREIGN_PARENT",
            Self::DetachRecordedWhileAttached => "DETACH_RECORDED_WHILE_ATTACHED",
            Self::ParentMissing => "PARENT_MISSING",
            Self::ForeignIndex => "FOREIGN_INDEX",
            Self::CatalogWithoutRelation => "CATALOG_WITHOUT_RELATION",
            Self::StaticLeafMissing => "STATIC_LEAF_MISSING",
        }
    }

    /// The kind of a database name; another name is `HistoryError::Contract`.
    pub fn parse(value: &str) -> Result<Self, HistoryError> {
        match value {
            "RELATION_WITHOUT_CATALOG" => Ok(Self::RelationWithoutCatalog),
            "METADATA_MISMATCH" => Ok(Self::MetadataMismatch),
            "PHYSICAL_NONCONFORMANT" => Ok(Self::PhysicalNonconformant),
            "OPEN_LOWER_BOUND" => Ok(Self::OpenLowerBound),
            "DROPPED_RELATION_EXISTS" => Ok(Self::DroppedRelationExists),
            "FOREIGN_PARENT" => Ok(Self::ForeignParent),
            "DETACH_RECORDED_WHILE_ATTACHED" => Ok(Self::DetachRecordedWhileAttached),
            "PARENT_MISSING" => Ok(Self::ParentMissing),
            "FOREIGN_INDEX" => Ok(Self::ForeignIndex),
            "CATALOG_WITHOUT_RELATION" => Ok(Self::CatalogWithoutRelation),
            "STATIC_LEAF_MISSING" => Ok(Self::StaticLeafMissing),
            unknown => Err(HistoryError::contract(format!(
                "catalog conflict kind {unknown:?} is not a known kind"
            ))),
        }
    }
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

impl LeafInspection {
    /// The name of the state in a pruning pass report: the class name of the
    /// outcome in `core/history/outcomes.py` of the `v0.7.0` package, so a report
    /// keeps the text of the Python 0.7.0 report.
    pub fn outcome_name(&self) -> &'static str {
        match self {
            Self::Detachable { .. } => "LeafDetachable",
            Self::NotExpired { .. } => "LeafNotExpired",
            Self::PendingBlocked { .. } => "LeafPendingBlocked",
            Self::DetachInterrupted { .. } => "LeafDetachInterrupted",
            Self::Detached { .. } => "LeafDetached",
            Self::Dropped { .. } => "LeafDropped",
            Self::Missing { .. } => "LeafMissing",
            Self::RetentionClassAbsent { .. } => "RetentionClassAbsent",
            Self::ForeverClassLeaf { .. } => "ForeverClassLeaf",
            Self::CatalogConflict { .. } => "LeafCatalogConflict",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeafDrop {
    Busy { leaf_name: String },
    Dropped { leaf_name: String },
    RefusedLoaderReferences { leaf_name: String },
    Inspection(LeafInspection),
}

impl LeafDrop {
    /// The name of the drop outcome in a pruning pass report, as
    /// [`LeafInspection::outcome_name`].
    pub fn outcome_name(&self) -> &'static str {
        match self {
            Self::Busy { .. } => "LeafMaintenanceBusy",
            Self::Dropped { .. } => "LeafDropped",
            Self::RefusedLoaderReferences { .. } => "DropRefusedLoaderReferences",
            Self::Inspection(inspection) => inspection.outcome_name(),
        }
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
            CatalogConflictKind::OpenLowerBound,
            CatalogConflictKind::DroppedRelationExists,
            CatalogConflictKind::ForeignParent,
            CatalogConflictKind::DetachRecordedWhileAttached,
            CatalogConflictKind::ParentMissing,
            CatalogConflictKind::ForeignIndex,
            CatalogConflictKind::CatalogWithoutRelation,
            CatalogConflictKind::StaticLeafMissing,
        ];
        let attachments = [
            LeafAttachment::Attached,
            LeafAttachment::DetachInterrupted,
            LeafAttachment::Detached,
        ];
        assert_eq!(conflict_kinds.len(), 11);
        for kind in conflict_kinds {
            assert_eq!(CatalogConflictKind::parse(kind.as_str()).unwrap(), kind);
        }
        assert!(CatalogConflictKind::parse("ABSENT").is_err());
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
