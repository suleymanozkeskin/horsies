//! The terminal-transition vocabulary (parity with Python's
//! `horsies/core/lifecycle/`).
//!
//! Every way a task reaches a terminal status is one of fifteen commands
//! (`commands`), each with the ownership fence its site requires (`fences`),
//! a frozen provenance kind and database function name (`operations`), and a
//! typed outcome decoded from the one row shape every operation returns
//! (`outcomes`). The SQL bodies installed by migration 0032 are the
//! projection of this vocabulary; the catalog-conformance test pins the two
//! against each other.

pub mod commands;
pub mod fences;
pub mod operations;
pub mod outcomes;

pub use commands::{BatchSize, TerminalizationCommand};
pub use fences::{
    CallerHoldsRowLock, OwnedClaim, OwnedClaimBatch, PriorLockedRead, TerminalFence, WorkerOwned,
};
pub use operations::{
    equivalence_class_of, function_name_of, is_already_applied, kind_of, TerminalizationKind,
    EQUIVALENCE_CLASSES,
};
pub use outcomes::{
    decode_outcome_row, GuardEvidence, GuardKind, ObservedTaskState, OutcomeDecodeError,
    TerminalizationOutcome,
};

/// A command or fence could not be constructed from the given data.
///
/// Raised at the call site that made the mistake, before a connection is
/// ever involved; the database functions enforce the same preconditions.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LifecycleValidationError {
    #[error(
        "duplicate task id in claim batch: {task_id:?}. Each task carries one \
         generation; a repeat means two generations claim the same row and \
         the fence is ambiguous."
    )]
    DuplicateTaskIdInBatch { task_id: String },

    #[error(
        "batch_size must be a positive integer, got {got}; the bound exists \
         to keep one pass from committing an unbounded notification burst"
    )]
    NonPositiveBatchSize { got: i32 },
}

#[cfg(test)]
mod tests {
    use super::commands::{BatchSize, TerminalizationCommand};
    use super::fences::{OwnedClaim, OwnedClaimBatch, PriorLockedRead, WorkerOwned};
    use super::operations::{
        equivalence_class_of, function_name_of, is_already_applied, kind_of, TerminalizationKind,
        EQUIVALENCE_CLASSES,
    };
    use super::*;
    use crate::core::types::status::TaskStatus;

    fn snake(variant: &str) -> String {
        let mut out = String::new();
        for (i, ch) in variant.chars().enumerate() {
            if ch.is_uppercase() && i > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        }
        out
    }

    fn sample_commands() -> Vec<(&'static str, TerminalizationCommand)> {
        let owned = OwnedClaim { worker_id: "w1".to_owned(), claimed_at: None };
        let prior = PriorLockedRead { worker_id: "w1".to_owned() };
        let batch = OwnedClaimBatch::new("w1".to_owned(), vec![("t1".to_owned(), None)]).unwrap();
        let bound = BatchSize::new(500).unwrap();
        vec![
            (
                "CompleteLockedTask",
                TerminalizationCommand::CompleteLockedTask {
                    task_id: "t".into(),
                    fence: prior.clone(),
                    result_json: "{}".into(),
                },
            ),
            (
                "CompleteTaskFused",
                TerminalizationCommand::CompleteTaskFused {
                    task_id: "t".into(),
                    fence: owned.clone(),
                    result_json: "{}".into(),
                    notify_channel: "c".into(),
                    notify_payload: "p".into(),
                },
            ),
            (
                "FailLockedTask",
                TerminalizationCommand::FailLockedTask {
                    task_id: "t".into(),
                    fence: prior,
                    result_json: "{}".into(),
                    error_code: None,
                    failed_reason: None,
                },
            ),
            (
                "FailStaleTask",
                TerminalizationCommand::FailStaleTask {
                    task_id: "t".into(),
                    stale_after_ms: 1000,
                    finalizing_stale_after_ms: 1000,
                    result_json: "{}".into(),
                    error_code: "E".into(),
                    failed_reason: "r".into(),
                },
            ),
            (
                "ExpireOwnedClaim",
                TerminalizationCommand::ExpireOwnedClaim {
                    task_id: "t".into(),
                    fence: WorkerOwned { worker_id: "w1".into() },
                    result_json: "{}".into(),
                    error_code: "E".into(),
                },
            ),
            (
                "ExpirePendingTasks",
                TerminalizationCommand::ExpirePendingTasks {
                    batch_size: bound,
                    result_json: "{}".into(),
                    error_code: "E".into(),
                },
            ),
            (
                "CancelLockedTask",
                TerminalizationCommand::CancelLockedTask {
                    task_id: "t".into(),
                    fence: super::fences::CallerHoldsRowLock,
                    permitted_source_statuses: vec![TaskStatus::Pending],
                },
            ),
            (
                "CancelOwnedOrphan",
                TerminalizationCommand::CancelOwnedOrphan {
                    task_id: "t".into(),
                    fence: owned.clone(),
                },
            ),
            (
                "CancelOrphanedTasks",
                TerminalizationCommand::CancelOrphanedTasks { batch_size: bound },
            ),
            (
                "AbandonOwnedNode",
                TerminalizationCommand::AbandonOwnedNode {
                    task_id: "t".into(),
                    fence: owned.clone(),
                },
            ),
            (
                "AbandonOwnedNodes",
                TerminalizationCommand::AbandonOwnedNodes { fence: batch.clone() },
            ),
            (
                "AbandonNodesOfPausedWorkflows",
                TerminalizationCommand::AbandonNodesOfPausedWorkflows {
                    workflow_ids: vec!["w".into()],
                },
            ),
            (
                "CancelOwnedNode",
                TerminalizationCommand::CancelOwnedNode {
                    task_id: "t".into(),
                    fence: owned,
                    accepts_requeued_pending: false,
                },
            ),
            (
                "CancelOwnedNodes",
                TerminalizationCommand::CancelOwnedNodes { fence: batch },
            ),
            (
                "CancelNodesOfCancelledWorkflow",
                TerminalizationCommand::CancelNodesOfCancelledWorkflow {
                    workflow_ids: vec!["w".into()],
                },
            ),
        ]
    }

    #[test]
    fn duplicate_task_id_in_batch_rejected() {
        let result = OwnedClaimBatch::new(
            "w1".to_owned(),
            vec![("t1".to_owned(), None), ("t1".to_owned(), None)],
        );
        assert_eq!(
            result.unwrap_err(),
            LifecycleValidationError::DuplicateTaskIdInBatch { task_id: "t1".to_owned() }
        );
    }

    #[test]
    fn non_positive_batch_size_rejected() {
        for bad in [0, -1, i32::MIN] {
            assert_eq!(
                BatchSize::new(bad).unwrap_err(),
                LifecycleValidationError::NonPositiveBatchSize { got: bad }
            );
        }
        assert_eq!(BatchSize::new(1).unwrap().get(), 1);
    }

    #[test]
    fn function_names_are_snake_of_variant_names() {
        for (variant, command) in sample_commands() {
            assert_eq!(
                function_name_of(&command),
                format!("horsies_{}", snake(variant)),
                "function name for {variant} must be horsies_ + snake(variant)"
            );
        }
    }

    #[test]
    fn fifteen_commands_fifteen_kinds() {
        let commands = sample_commands();
        assert_eq!(commands.len(), 15);
        let mut kinds: Vec<TerminalizationKind> =
            commands.iter().map(|(_, c)| kind_of(c)).collect();
        kinds.sort_by_key(|k| k.as_str());
        kinds.dedup();
        assert_eq!(kinds.len(), 15, "each command commits a distinct kind");
    }

    #[test]
    fn equivalence_classes_partition_all_kinds() {
        let mut seen = Vec::new();
        for class in EQUIVALENCE_CLASSES {
            for kind in class {
                assert!(!seen.contains(kind), "{kind:?} appears in two classes");
                seen.push(*kind);
            }
        }
        assert_eq!(seen.len(), TerminalizationKind::ALL.len());
        for kind in TerminalizationKind::ALL {
            assert!(
                equivalence_class_of(kind).contains(&kind),
                "{kind:?} must belong to its own class"
            );
        }
    }

    #[test]
    fn already_applied_requires_same_class_kind() {
        assert!(is_already_applied(
            TerminalizationKind::CompleteFused,
            Some(TerminalizationKind::CompleteLocked),
        ));
        assert!(is_already_applied(
            TerminalizationKind::PauseAbandonClaim,
            Some(TerminalizationKind::PauseAbandonWorkflow),
        ));
        assert!(!is_already_applied(
            TerminalizationKind::CancelOrphan,
            Some(TerminalizationKind::WorkflowCancelClaim),
        ));
    }

    #[test]
    fn null_committed_kind_is_never_already_applied() {
        for kind in TerminalizationKind::ALL {
            assert!(
                !is_already_applied(kind, None),
                "{kind:?}: unknown provenance must not satisfy a replay"
            );
        }
    }

    #[test]
    fn target_statuses_are_terminal() {
        for (_, command) in sample_commands() {
            assert!(command.target_status().is_terminal());
        }
    }

    #[test]
    fn kind_round_trips_through_storage_value() {
        for kind in TerminalizationKind::ALL {
            assert_eq!(TerminalizationKind::parse(kind.as_str()), Some(kind));
        }
        assert_eq!(TerminalizationKind::parse("NOT_A_KIND"), None);
    }
}
