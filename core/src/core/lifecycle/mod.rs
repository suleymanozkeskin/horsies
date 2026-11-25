//! The terminal-transition vocabulary (parity with Python's
//! `clients/python/horsies/core/lifecycle/`).
//!
//! Every way a task reaches a terminal status is one of eleven commands
//! (`commands`) or one of four writers with no command: the three attempt
//! writers, whose rows `attempt_outcomes` decodes, and the administrative
//! cancel (`horsies_cancel_task`). A command has the ownership fence its site
//! requires (`fences`), a frozen provenance kind and database function name
//! (`operations`), and a typed outcome decoded from the one row shape every
//! operation returns (`outcomes`). The SQL bodies installed by migration 0032 are the
//! projection of this vocabulary; the catalog-conformance test pins the two
//! against each other.

pub mod attempt_outcomes;
pub mod commands;
pub mod fences;
pub mod operations;
pub mod outcomes;
pub mod placement;

pub use attempt_outcomes::{
    decode_failed_attempt_row, decode_stale_attempt_row, decode_workflow_success_row, AfterFailure,
    AfterStaleFailure, AttemptWriteRefusal, FailedAttemptOutcome, PhaseTwoOwed, PhaseTwoStep,
    ReleasedClaim, RetryRefusal, StaleAttemptOutcome, UnreportedAttemptOutcome,
    WorkflowSuccessOutcome,
};
pub use commands::{BatchSize, TerminalizationCommand};
pub use fences::{OwnedClaim, OwnedClaimBatch, TerminalFence, WorkerOwned};
pub use operations::{
    equivalence_class_of, function_name_of, is_already_applied, kind_of, TerminalizationKind,
    EQUIVALENCE_CLASSES,
};
pub use outcomes::{
    decode_moved_row, decode_outcome_row, GuardEvidence, GuardKind, MovedRow, ObservedTaskState,
    ObservedWorkflowTask, OutcomeDecodeError, TerminalizationOutcome,
};
pub use placement::{AttemptPlacement, SlotWait};

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
    DuplicateTaskIdInBatch { task_id: uuid::Uuid },

    #[error(
        "batch_size must be a positive integer, got {got}; the bound exists \
         to keep one pass from committing an unbounded notification burst"
    )]
    NonPositiveBatchSize { got: i32 },
}

#[cfg(test)]
pub(crate) mod tests {
    use super::commands::{BatchSize, TerminalizationCommand};
    use super::fences::{OwnedClaim, OwnedClaimBatch, WorkerOwned};
    use super::operations::{
        equivalence_class_of, function_name_of, is_already_applied, kind_of, TerminalizationKind,
        EQUIVALENCE_CLASSES,
    };
    use super::*;
    use uuid::Uuid;

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

    /// One command of each of the eleven variants, with its variant name.
    pub(crate) fn sample_commands() -> Vec<(&'static str, TerminalizationCommand)> {
        let generation = chrono::DateTime::<chrono::Utc>::UNIX_EPOCH;
        let owned = OwnedClaim {
            worker_id: "w1".to_owned(),
            claimed_at: generation,
        };
        let batch = OwnedClaimBatch::new("w1".to_owned(), vec![(Uuid::nil(), generation)]).unwrap();
        let bound = BatchSize::new(500).unwrap();
        vec![
            (
                "CompleteTaskFused",
                TerminalizationCommand::CompleteTaskFused {
                    task_id: Uuid::nil(),
                    fence: owned.clone(),
                    result_json: "{}".into(),
                    placement: AttemptPlacement::NotObserved,
                },
            ),
            (
                "ExpireOwnedClaim",
                TerminalizationCommand::ExpireOwnedClaim {
                    task_id: Uuid::nil(),
                    fence: WorkerOwned {
                        worker_id: "w1".into(),
                    },
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
                "CancelOwnedOrphan",
                TerminalizationCommand::CancelOwnedOrphan {
                    task_id: Uuid::nil(),
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
                    task_id: Uuid::nil(),
                    fence: owned.clone(),
                },
            ),
            (
                "AbandonOwnedNodes",
                TerminalizationCommand::AbandonOwnedNodes {
                    fence: batch.clone(),
                },
            ),
            (
                "AbandonNodesOfPausedWorkflows",
                TerminalizationCommand::AbandonNodesOfPausedWorkflows {
                    workflow_ids: vec![Uuid::nil()],
                },
            ),
            (
                "CancelOwnedNode",
                TerminalizationCommand::CancelOwnedNode {
                    task_id: Uuid::nil(),
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
                    workflow_ids: vec![Uuid::nil()],
                },
            ),
        ]
    }

    #[test]
    fn duplicate_task_id_in_batch_rejected() {
        let result = OwnedClaimBatch::new(
            "w1".to_owned(),
            vec![
                (Uuid::nil(), chrono::DateTime::<chrono::Utc>::UNIX_EPOCH),
                (Uuid::nil(), chrono::DateTime::<chrono::Utc>::UNIX_EPOCH),
            ],
        );
        assert_eq!(
            result.unwrap_err(),
            LifecycleValidationError::DuplicateTaskIdInBatch {
                task_id: Uuid::nil()
            }
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
    fn eleven_commands_eleven_kinds() {
        let commands = sample_commands();
        assert_eq!(commands.len(), 11);
        let mut kinds: Vec<TerminalizationKind> =
            commands.iter().map(|(_, c)| kind_of(c)).collect();
        kinds.sort_by_key(|k| k.as_str());
        kinds.dedup();
        assert_eq!(kinds.len(), 11, "each command commits a distinct kind");
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
            TerminalizationKind::CompleteLocked,
        ));
        assert!(is_already_applied(
            TerminalizationKind::PauseAbandonClaim,
            TerminalizationKind::PauseAbandonWorkflow,
        ));
        assert!(!is_already_applied(
            TerminalizationKind::CancelOrphan,
            TerminalizationKind::WorkflowCancelClaim,
        ));
    }

    /// The clean-slate vocabulary has the fifteen wire kinds and no kind
    /// that only a relocated 0.x row carried.
    #[test]
    fn the_vocabulary_has_no_relocation_only_kind() {
        assert_eq!(TerminalizationKind::ALL.len(), 15);
        assert_eq!(TerminalizationKind::parse("LEGACY_TERMINAL"), None);
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

    /// What a command acts on, by its variant.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Subject {
        /// One task, named by its id.
        OneTask,
        /// The tasks of a claim batch, each at its own generation.
        ClaimBatch,
        /// The tasks of whole workflows.
        Workflows,
        /// The rows that a bounded sweep finds.
        Sweep,
    }

    /// The subject of `command`. Exhaustive, so a new variant must be given
    /// its subject here before the tests below compile.
    fn subject_of(command: &TerminalizationCommand) -> Subject {
        match command {
            TerminalizationCommand::CompleteTaskFused { .. }
            | TerminalizationCommand::ExpireOwnedClaim { .. }
            | TerminalizationCommand::CancelOwnedOrphan { .. }
            | TerminalizationCommand::AbandonOwnedNode { .. }
            | TerminalizationCommand::CancelOwnedNode { .. } => Subject::OneTask,
            TerminalizationCommand::AbandonOwnedNodes { .. }
            | TerminalizationCommand::CancelOwnedNodes { .. } => Subject::ClaimBatch,
            TerminalizationCommand::AbandonNodesOfPausedWorkflows { .. }
            | TerminalizationCommand::CancelNodesOfCancelledWorkflow { .. } => Subject::Workflows,
            TerminalizationCommand::ExpirePendingTasks { .. }
            | TerminalizationCommand::CancelOrphanedTasks { .. } => Subject::Sweep,
        }
    }

    fn has_batch_fence(command: &TerminalizationCommand) -> bool {
        matches!(command.fence(), Some(TerminalFence::OwnedClaimBatch(_)))
    }

    /// The variant names of the sample commands that `keep` selects.
    fn variants_where(keep: impl Fn(&TerminalizationCommand) -> bool) -> Vec<&'static str> {
        sample_commands()
            .into_iter()
            .filter(|(_, command)| keep(command))
            .map(|(variant, _)| variant)
            .collect()
    }

    /// The field names of `command`. Each pattern names every field of its
    /// variant and has no `..`, and the `match` has no wildcard arm: a field
    /// or a variant added to the enum fails to compile here until it is
    /// listed, and the tests below then judge it.
    fn field_names(command: &TerminalizationCommand) -> Vec<&'static str> {
        macro_rules! fields_of {
            ($command:expr; $($variant:ident { $($field:ident),* }),* $(,)?) => {
                match $command {
                    $(TerminalizationCommand::$variant { $($field: _),* } => {
                        vec![$(stringify!($field)),*]
                    })*
                }
            };
        }
        fields_of!(
            command;
            CompleteTaskFused { task_id, fence, result_json, placement },
            ExpireOwnedClaim { task_id, fence, result_json, error_code },
            ExpirePendingTasks { batch_size, result_json, error_code },
            CancelOwnedOrphan { task_id, fence },
            CancelOrphanedTasks { batch_size },
            AbandonOwnedNode { task_id, fence },
            AbandonOwnedNodes { fence },
            AbandonNodesOfPausedWorkflows { workflow_ids },
            CancelOwnedNode { task_id, fence, accepts_requeued_pending },
            CancelOwnedNodes { fence },
            CancelNodesOfCancelledWorkflow { workflow_ids },
        )
    }

    fn names_a_task(command: &TerminalizationCommand) -> bool {
        field_names(command).contains(&"task_id")
    }

    /// Every field that a command may carry, by role (Python
    /// `TestIllegalStatesAreUnrepresentable::test_no_variant_carries_an_unaccounted_field`).
    const ACCOUNTED_FIELDS: [&str; 11] = [
        // What the command acts on.
        "task_id",
        "workflow_ids",
        "batch_size",
        // What proves that it may.
        "fence",
        "accepts_requeued_pending",
        // What it records.
        "result_json",
        "error_code",
        "failed_reason",
        "placement",
        // What it wakes.
        "notify_channel",
        "notify_payload",
    ];

    /// A claim batch fence guards only the commands of a claim batch, which
    /// name no single task.
    #[test]
    fn a_batch_fence_guards_only_a_command_of_a_claim_batch() {
        assert_eq!(
            variants_where(has_batch_fence),
            variants_where(|command| subject_of(command) == Subject::ClaimBatch)
        );
        assert_eq!(
            variants_where(has_batch_fence),
            ["AbandonOwnedNodes", "CancelOwnedNodes"]
        );
        for (variant, command) in sample_commands() {
            match has_batch_fence(&command) {
                true => assert!(!names_a_task(&command), "{variant}"),
                false => {}
            }
        }
    }

    /// `TestIllegalStatesAreUnrepresentable::test_single_task_commands_do_not_carry_a_batch`:
    /// no command of one task carries a claim batch fence. A command names a
    /// task exactly when it is a command of one task.
    #[test]
    fn a_command_of_one_task_carries_no_batch_fence() {
        let single = variants_where(|command| subject_of(command) == Subject::OneTask);
        assert_eq!(single.len(), 5, "{single:?}");
        assert_eq!(variants_where(names_a_task), single);
        for (variant, command) in sample_commands() {
            match subject_of(&command) {
                Subject::OneTask => assert!(!has_batch_fence(&command), "{variant}"),
                Subject::ClaimBatch | Subject::Workflows | Subject::Sweep => {}
            }
        }
    }

    /// `TestIllegalStatesAreUnrepresentable::test_workflow_scoped_commands_carry_no_task_ids`:
    /// a command of whole workflows names no task and carries no ownership
    /// fence; the workflow's status is its guard.
    #[test]
    fn a_command_of_whole_workflows_has_no_fence() {
        let scoped = variants_where(|command| subject_of(command) == Subject::Workflows);
        assert_eq!(scoped.len(), 2, "{scoped:?}");
        for (variant, command) in sample_commands() {
            match subject_of(&command) {
                Subject::Workflows => {
                    assert!(command.fence().is_none(), "{variant}");
                    assert!(!names_a_task(&command), "{variant}");
                }
                Subject::OneTask | Subject::ClaimBatch | Subject::Sweep => {}
            }
        }
    }

    /// `TestIllegalStatesAreUnrepresentable::test_required_workflow_status_is_implied_by_the_variant`:
    /// no command names the workflow status or the node statuses that its
    /// guard requires. The variant means them, so a pause command cannot
    /// verify a cancellation.
    #[test]
    fn no_command_names_the_status_its_guard_requires() {
        for (variant, command) in sample_commands() {
            for field in field_names(&command) {
                assert!(
                    ![
                        "required_workflow_status",
                        "required_node_statuses",
                        "workflow_status"
                    ]
                    .contains(&field),
                    "{variant} carries {field}"
                );
            }
        }
    }

    /// `TestIllegalStatesAreUnrepresentable::test_node_disposition_is_implied_by_the_variant`:
    /// no command carries a disposition or a node status that could
    /// contradict its variant. A pause readies a node and a cancellation
    /// skips it, by which command it is.
    #[test]
    fn no_command_carries_a_node_disposition() {
        for (variant, command) in sample_commands() {
            for field in field_names(&command) {
                assert!(
                    !["disposition", "node_status"].contains(&field),
                    "{variant} carries {field}"
                );
            }
        }
    }

    /// `TestIllegalStatesAreUnrepresentable::test_no_variant_carries_an_unaccounted_field`:
    /// every field of every variant is what the command acts on, what
    /// proves that it may, what it records, or what it wakes.
    #[test]
    fn every_field_of_a_command_has_a_role() {
        for (variant, command) in sample_commands() {
            for field in field_names(&command) {
                assert!(
                    ACCOUNTED_FIELDS.contains(&field),
                    "{variant} carries the unaccounted field {field}"
                );
            }
        }
    }

    /// The ownership that a fence compares.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum FenceKind {
        /// The worker, not the claim generation.
        Worker,
        /// The worker and the claim generation of one task.
        WorkerAndGeneration,
        /// The worker and the claim generation of each task of a batch.
        WorkerAndGenerationPerTask,
        /// No ownership predicate.
        NoFence,
    }

    fn fence_kind(command: &TerminalizationCommand) -> FenceKind {
        match command.fence() {
            Some(TerminalFence::WorkerOwned(_)) => FenceKind::Worker,
            Some(TerminalFence::OwnedClaim(_)) => FenceKind::WorkerAndGeneration,
            Some(TerminalFence::OwnedClaimBatch(_)) => FenceKind::WorkerAndGenerationPerTask,
            None => FenceKind::NoFence,
        }
    }

    /// Per command, not per set, the fence is the ownership that its writer
    /// compares (the `fence` of each writer of `clients/python/tests/lifecycle_matrix.py`).
    /// The failed attempt and the success of a workflow task have writers of
    /// their own, whose commands hold an `OwnedClaim` by type
    /// (`FinishFailedAttempt`, `FinishWorkflowSuccess`).
    #[test]
    fn each_command_carries_the_fence_of_its_writer() {
        let expected = [
            ("CompleteTaskFused", FenceKind::WorkerAndGeneration),
            ("ExpireOwnedClaim", FenceKind::Worker),
            ("ExpirePendingTasks", FenceKind::NoFence),
            ("CancelOwnedOrphan", FenceKind::WorkerAndGeneration),
            ("CancelOrphanedTasks", FenceKind::NoFence),
            ("AbandonOwnedNode", FenceKind::WorkerAndGeneration),
            ("AbandonOwnedNodes", FenceKind::WorkerAndGenerationPerTask),
            ("AbandonNodesOfPausedWorkflows", FenceKind::NoFence),
            ("CancelOwnedNode", FenceKind::WorkerAndGeneration),
            ("CancelOwnedNodes", FenceKind::WorkerAndGenerationPerTask),
            ("CancelNodesOfCancelledWorkflow", FenceKind::NoFence),
        ];
        let actual: Vec<(&'static str, FenceKind)> = sample_commands()
            .iter()
            .map(|(variant, command)| (*variant, fence_kind(command)))
            .collect();
        assert_eq!(actual, expected);
    }
}
