//! What each command is called in the database, and what it commits as.
//!
//! Two identities per command, and they are deliberately not the same thing.
//!
//! The function name is re-created by DROP+CREATE on every migration apply
//! and owns no history, so it maps one-to-one from the variant: a new command
//! cannot ship without its function, and a stray function cannot survive
//! without its command (the catalog-conformance test enforces the set).
//!
//! The kind is persisted on the row and carried into history, so it is an
//! explicitly frozen vocabulary instead. A value here is a promise to every
//! row that already carries it: add members; do not rename or repurpose them.
//!
//! Kinds are written by the database, never supplied by a caller: each
//! function hardcodes its own, so a row cannot claim provenance it does not
//! have.

use super::commands::TerminalizationCommand;

/// The committed semantic operation, as stored on the task row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TerminalizationKind {
    CompleteLocked,
    CompleteFused,
    FailRunning,
    FailStale,
    ExpireClaimed,
    ExpirePending,
    CancelAdmin,
    CancelOrphan,
    CancelOrphanSweep,
    PauseAbandonClaim,
    PauseAbandonClaimBatch,
    PauseAbandonWorkflow,
    WorkflowCancelClaim,
    WorkflowCancelClaimBatch,
    WorkflowCancelWorkflow,
}

impl TerminalizationKind {
    /// The stored string value (frozen vocabulary).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CompleteLocked => "COMPLETE_LOCKED",
            Self::CompleteFused => "COMPLETE_FUSED",
            Self::FailRunning => "FAIL_RUNNING",
            Self::FailStale => "FAIL_STALE",
            Self::ExpireClaimed => "EXPIRE_CLAIMED",
            Self::ExpirePending => "EXPIRE_PENDING",
            Self::CancelAdmin => "CANCEL_ADMIN",
            Self::CancelOrphan => "CANCEL_ORPHAN",
            Self::CancelOrphanSweep => "CANCEL_ORPHAN_SWEEP",
            Self::PauseAbandonClaim => "PAUSE_ABANDON_CLAIM",
            Self::PauseAbandonClaimBatch => "PAUSE_ABANDON_CLAIM_BATCH",
            Self::PauseAbandonWorkflow => "PAUSE_ABANDON_WORKFLOW",
            Self::WorkflowCancelClaim => "WORKFLOW_CANCEL_CLAIM",
            Self::WorkflowCancelClaimBatch => "WORKFLOW_CANCEL_CLAIM_BATCH",
            Self::WorkflowCancelWorkflow => "WORKFLOW_CANCEL_WORKFLOW",
        }
    }

    /// Parse a stored value. `None` for a kind this build does not know —
    /// the caller decides whether that fails closed (it must, in decoding).
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "COMPLETE_LOCKED" => Some(Self::CompleteLocked),
            "COMPLETE_FUSED" => Some(Self::CompleteFused),
            "FAIL_RUNNING" => Some(Self::FailRunning),
            "FAIL_STALE" => Some(Self::FailStale),
            "EXPIRE_CLAIMED" => Some(Self::ExpireClaimed),
            "EXPIRE_PENDING" => Some(Self::ExpirePending),
            "CANCEL_ADMIN" => Some(Self::CancelAdmin),
            "CANCEL_ORPHAN" => Some(Self::CancelOrphan),
            "CANCEL_ORPHAN_SWEEP" => Some(Self::CancelOrphanSweep),
            "PAUSE_ABANDON_CLAIM" => Some(Self::PauseAbandonClaim),
            "PAUSE_ABANDON_CLAIM_BATCH" => Some(Self::PauseAbandonClaimBatch),
            "PAUSE_ABANDON_WORKFLOW" => Some(Self::PauseAbandonWorkflow),
            "WORKFLOW_CANCEL_CLAIM" => Some(Self::WorkflowCancelClaim),
            "WORKFLOW_CANCEL_CLAIM_BATCH" => Some(Self::WorkflowCancelClaimBatch),
            "WORKFLOW_CANCEL_WORKFLOW" => Some(Self::WorkflowCancelWorkflow),
            _ => None,
        }
    }

    /// Every kind, for domain tests and rendering.
    pub const ALL: [Self; 15] = [
        Self::CompleteLocked,
        Self::CompleteFused,
        Self::FailRunning,
        Self::FailStale,
        Self::ExpireClaimed,
        Self::ExpirePending,
        Self::CancelAdmin,
        Self::CancelOrphan,
        Self::CancelOrphanSweep,
        Self::PauseAbandonClaim,
        Self::PauseAbandonClaimBatch,
        Self::PauseAbandonWorkflow,
        Self::WorkflowCancelClaim,
        Self::WorkflowCancelClaimBatch,
        Self::WorkflowCancelWorkflow,
    ];
}

/// Kinds whose committed effect is interchangeable. A replay that finds one
/// of its own class already committed learns nothing new happened; a replay
/// that finds a kind from another class has been overtaken by a different
/// event and must be told so, because the coupled workflow-node write
/// differs.
///
/// Cardinality does not separate a class: a batch pause and a single pause
/// commit the same effect on the row they touch. Families do: an orphan
/// sweep and a workflow cancellation reaching the same row are different
/// events.
pub const EQUIVALENCE_CLASSES: [&[TerminalizationKind]; 8] = [
    &[
        TerminalizationKind::CompleteLocked,
        TerminalizationKind::CompleteFused,
    ],
    &[TerminalizationKind::FailRunning],
    &[TerminalizationKind::FailStale],
    &[
        TerminalizationKind::ExpireClaimed,
        TerminalizationKind::ExpirePending,
    ],
    &[TerminalizationKind::CancelAdmin],
    &[
        TerminalizationKind::CancelOrphan,
        TerminalizationKind::CancelOrphanSweep,
    ],
    &[
        TerminalizationKind::PauseAbandonClaim,
        TerminalizationKind::PauseAbandonClaimBatch,
        TerminalizationKind::PauseAbandonWorkflow,
    ],
    &[
        TerminalizationKind::WorkflowCancelClaim,
        TerminalizationKind::WorkflowCancelClaimBatch,
        TerminalizationKind::WorkflowCancelWorkflow,
    ],
];

/// The kinds interchangeable with this one, including itself.
///
/// Total by exhaustive match: an unclassified kind is unrepresentable, which
/// is the property Python enforces with a raise.
pub fn equivalence_class_of(kind: TerminalizationKind) -> &'static [TerminalizationKind] {
    match kind {
        TerminalizationKind::CompleteLocked | TerminalizationKind::CompleteFused => {
            EQUIVALENCE_CLASSES[0]
        }
        TerminalizationKind::FailRunning => EQUIVALENCE_CLASSES[1],
        TerminalizationKind::FailStale => EQUIVALENCE_CLASSES[2],
        TerminalizationKind::ExpireClaimed | TerminalizationKind::ExpirePending => {
            EQUIVALENCE_CLASSES[3]
        }
        TerminalizationKind::CancelAdmin => EQUIVALENCE_CLASSES[4],
        TerminalizationKind::CancelOrphan | TerminalizationKind::CancelOrphanSweep => {
            EQUIVALENCE_CLASSES[5]
        }
        TerminalizationKind::PauseAbandonClaim
        | TerminalizationKind::PauseAbandonClaimBatch
        | TerminalizationKind::PauseAbandonWorkflow => EQUIVALENCE_CLASSES[6],
        TerminalizationKind::WorkflowCancelClaim
        | TerminalizationKind::WorkflowCancelClaimBatch
        | TerminalizationKind::WorkflowCancelWorkflow => EQUIVALENCE_CLASSES[7],
    }
}

/// Whether a terminal row's committed kind satisfies a repeated request.
///
/// A `None` committed kind is a row written before the kind column existed.
/// Its provenance is unknown and is never inferred, so it answers `false`:
/// the caller is told the state conflicts rather than told its own coupled
/// write committed when nothing proves it did.
pub fn is_already_applied(
    requested: TerminalizationKind,
    committed: Option<TerminalizationKind>,
) -> bool {
    match committed {
        None => false,
        Some(kind) => equivalence_class_of(requested).contains(&kind),
    }
}

/// The kind the database function for this command hardcodes.
pub fn kind_of(command: &TerminalizationCommand) -> TerminalizationKind {
    match command {
        TerminalizationCommand::CompleteLockedTask { .. } => TerminalizationKind::CompleteLocked,
        TerminalizationCommand::CompleteTaskFused { .. } => TerminalizationKind::CompleteFused,
        TerminalizationCommand::FailLockedTask { .. } => TerminalizationKind::FailRunning,
        TerminalizationCommand::FailStaleTask { .. } => TerminalizationKind::FailStale,
        TerminalizationCommand::ExpireOwnedClaim { .. } => TerminalizationKind::ExpireClaimed,
        TerminalizationCommand::ExpirePendingTasks { .. } => TerminalizationKind::ExpirePending,
        TerminalizationCommand::CancelLockedTask { .. } => TerminalizationKind::CancelAdmin,
        TerminalizationCommand::CancelOwnedOrphan { .. } => TerminalizationKind::CancelOrphan,
        TerminalizationCommand::CancelOrphanedTasks { .. } => TerminalizationKind::CancelOrphanSweep,
        TerminalizationCommand::AbandonOwnedNode { .. } => TerminalizationKind::PauseAbandonClaim,
        TerminalizationCommand::AbandonOwnedNodes { .. } => {
            TerminalizationKind::PauseAbandonClaimBatch
        }
        TerminalizationCommand::AbandonNodesOfPausedWorkflows { .. } => {
            TerminalizationKind::PauseAbandonWorkflow
        }
        TerminalizationCommand::CancelOwnedNode { .. } => TerminalizationKind::WorkflowCancelClaim,
        TerminalizationCommand::CancelOwnedNodes { .. } => {
            TerminalizationKind::WorkflowCancelClaimBatch
        }
        TerminalizationCommand::CancelNodesOfCancelledWorkflow { .. } => {
            TerminalizationKind::WorkflowCancelWorkflow
        }
    }
}

/// The database function this command is executed by.
///
/// `horsies_` + the variant name in snake case. The mapping is tabulated
/// (Rust has no runtime variant-name reflection); the unit test derives each
/// name from the variant identifier and asserts the table matches, so the
/// two cannot drift.
pub fn function_name_of(command: &TerminalizationCommand) -> &'static str {
    match command {
        TerminalizationCommand::CompleteLockedTask { .. } => "horsies_complete_locked_task",
        TerminalizationCommand::CompleteTaskFused { .. } => "horsies_complete_task_fused",
        TerminalizationCommand::FailLockedTask { .. } => "horsies_fail_locked_task",
        TerminalizationCommand::FailStaleTask { .. } => "horsies_fail_stale_task",
        TerminalizationCommand::ExpireOwnedClaim { .. } => "horsies_expire_owned_claim",
        TerminalizationCommand::ExpirePendingTasks { .. } => "horsies_expire_pending_tasks",
        TerminalizationCommand::CancelLockedTask { .. } => "horsies_cancel_locked_task",
        TerminalizationCommand::CancelOwnedOrphan { .. } => "horsies_cancel_owned_orphan",
        TerminalizationCommand::CancelOrphanedTasks { .. } => "horsies_cancel_orphaned_tasks",
        TerminalizationCommand::AbandonOwnedNode { .. } => "horsies_abandon_owned_node",
        TerminalizationCommand::AbandonOwnedNodes { .. } => "horsies_abandon_owned_nodes",
        TerminalizationCommand::AbandonNodesOfPausedWorkflows { .. } => {
            "horsies_abandon_nodes_of_paused_workflows"
        }
        TerminalizationCommand::CancelOwnedNode { .. } => "horsies_cancel_owned_node",
        TerminalizationCommand::CancelOwnedNodes { .. } => "horsies_cancel_owned_nodes",
        TerminalizationCommand::CancelNodesOfCancelledWorkflow { .. } => {
            "horsies_cancel_nodes_of_cancelled_workflow"
        }
    }
}
