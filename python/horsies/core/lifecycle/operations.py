"""What each command is called in the database, and what it commits as.

Two identities per command, and they are deliberately not the same thing.

The function name is re-created by DROP+CREATE on every migration apply and
owns no history, so it is derived from the variant: a new command cannot ship
without its function, and a stray function cannot survive without its command.

The kind is persisted on the row and carried into history, so it is an
explicitly frozen vocabulary instead. Deriving it from a class name would let
a rename silently reinterpret rows already written — the value in a row from
last month has to keep meaning what it meant when it was written.

Kinds are written by the database, never supplied by a caller: each function
hardcodes its own, so a row cannot claim provenance it does not have. The
mapping here is what the function bodies are generated against, and what the
equivalence check below reads.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import assert_never

from .commands import (
    AbandonNodesOfPausedWorkflows,
    AbandonOwnedNode,
    AbandonOwnedNodes,
    CancelLockedTask,
    CancelNodesOfCancelledWorkflow,
    CancelOrphanedTasks,
    CancelOwnedNode,
    CancelOwnedNodes,
    CancelOwnedOrphan,
    CompleteLockedTask,
    CompleteTaskFused,
    ExpireOwnedClaim,
    ExpirePendingTasks,
    FailLockedTask,
    FailStaleTask,
    TerminalizationCommand,
)


class TerminalizationKind(Enum):
    """The committed semantic operation, as stored on the task row.

    Frozen vocabulary: a value here is a promise to every row that already
    carries it. Add members; do not rename or repurpose them.
    """

    COMPLETE_LOCKED = 'COMPLETE_LOCKED'
    COMPLETE_FUSED = 'COMPLETE_FUSED'
    FAIL_RUNNING = 'FAIL_RUNNING'
    FAIL_STALE = 'FAIL_STALE'
    EXPIRE_CLAIMED = 'EXPIRE_CLAIMED'
    EXPIRE_PENDING = 'EXPIRE_PENDING'
    CANCEL_ADMIN = 'CANCEL_ADMIN'
    CANCEL_ORPHAN = 'CANCEL_ORPHAN'
    CANCEL_ORPHAN_SWEEP = 'CANCEL_ORPHAN_SWEEP'
    PAUSE_ABANDON_CLAIM = 'PAUSE_ABANDON_CLAIM'
    PAUSE_ABANDON_CLAIM_BATCH = 'PAUSE_ABANDON_CLAIM_BATCH'
    PAUSE_ABANDON_WORKFLOW = 'PAUSE_ABANDON_WORKFLOW'
    WORKFLOW_CANCEL_CLAIM = 'WORKFLOW_CANCEL_CLAIM'
    WORKFLOW_CANCEL_CLAIM_BATCH = 'WORKFLOW_CANCEL_CLAIM_BATCH'
    WORKFLOW_CANCEL_WORKFLOW = 'WORKFLOW_CANCEL_WORKFLOW'


# Kinds whose committed effect is interchangeable. A replay that finds one of
# its own class already committed learns nothing new happened; a replay that
# finds a kind from another class has been overtaken by a different event and
# must be told so, because the coupled workflow-node write differs.
#
# Cardinality does not separate a class: a batch pause and a single pause
# commit the same effect on the row they touch. Families do: an orphan sweep
# and a workflow cancellation reaching the same row are different events.
EQUIVALENCE_CLASSES: tuple[frozenset[TerminalizationKind], ...] = (
    frozenset({
        TerminalizationKind.COMPLETE_LOCKED,
        TerminalizationKind.COMPLETE_FUSED,
    }),
    frozenset({TerminalizationKind.FAIL_RUNNING}),
    frozenset({TerminalizationKind.FAIL_STALE}),
    frozenset({
        TerminalizationKind.EXPIRE_CLAIMED,
        TerminalizationKind.EXPIRE_PENDING,
    }),
    frozenset({TerminalizationKind.CANCEL_ADMIN}),
    frozenset({
        TerminalizationKind.CANCEL_ORPHAN,
        TerminalizationKind.CANCEL_ORPHAN_SWEEP,
    }),
    frozenset({
        TerminalizationKind.PAUSE_ABANDON_CLAIM,
        TerminalizationKind.PAUSE_ABANDON_CLAIM_BATCH,
        TerminalizationKind.PAUSE_ABANDON_WORKFLOW,
    }),
    frozenset({
        TerminalizationKind.WORKFLOW_CANCEL_CLAIM,
        TerminalizationKind.WORKFLOW_CANCEL_CLAIM_BATCH,
        TerminalizationKind.WORKFLOW_CANCEL_WORKFLOW,
    }),
)


def equivalence_class_of(
    kind: TerminalizationKind,
) -> frozenset[TerminalizationKind]:
    """The kinds interchangeable with this one, including itself."""
    for members in EQUIVALENCE_CLASSES:
        if kind in members:
            return members
    raise ValueError(
        f'{kind.value} belongs to no equivalence class. Every kind must be '
        f'classified: an unclassified kind would make already-applied '
        f'undecidable for the operation that writes it.'
    )


def is_already_applied(
    *,
    requested: TerminalizationKind,
    committed: TerminalizationKind | None,
) -> bool:
    """Whether a terminal row's committed kind satisfies a repeated request.

    A NULL committed kind is a row written before the kind column existed.
    Its provenance is unknown and is never inferred, so it answers False: the
    caller is told the state conflicts rather than told its own coupled write
    committed when nothing proves it did.
    """
    if committed is None:
        return False
    return committed in equivalence_class_of(requested)


def kind_of(command: TerminalizationCommand) -> TerminalizationKind:
    """The kind the database function for this command hardcodes."""
    match command:
        case CompleteLockedTask():
            return TerminalizationKind.COMPLETE_LOCKED
        case CompleteTaskFused():
            return TerminalizationKind.COMPLETE_FUSED
        case FailLockedTask():
            return TerminalizationKind.FAIL_RUNNING
        case FailStaleTask():
            return TerminalizationKind.FAIL_STALE
        case ExpireOwnedClaim():
            return TerminalizationKind.EXPIRE_CLAIMED
        case ExpirePendingTasks():
            return TerminalizationKind.EXPIRE_PENDING
        case CancelLockedTask():
            return TerminalizationKind.CANCEL_ADMIN
        case CancelOwnedOrphan():
            return TerminalizationKind.CANCEL_ORPHAN
        case CancelOrphanedTasks():
            return TerminalizationKind.CANCEL_ORPHAN_SWEEP
        case AbandonOwnedNode():
            return TerminalizationKind.PAUSE_ABANDON_CLAIM
        case AbandonOwnedNodes():
            return TerminalizationKind.PAUSE_ABANDON_CLAIM_BATCH
        case AbandonNodesOfPausedWorkflows():
            return TerminalizationKind.PAUSE_ABANDON_WORKFLOW
        case CancelOwnedNode():
            return TerminalizationKind.WORKFLOW_CANCEL_CLAIM
        case CancelOwnedNodes():
            return TerminalizationKind.WORKFLOW_CANCEL_CLAIM_BATCH
        case CancelNodesOfCancelledWorkflow():
            return TerminalizationKind.WORKFLOW_CANCEL_WORKFLOW
        case _ as unreachable:
            assert_never(unreachable)


_CAMEL_BOUNDARY = re.compile(r'(?<!^)(?=[A-Z])')


def function_name_of(command: TerminalizationCommand) -> str:
    """The database function this command is executed by.

    Derived rather than tabulated so the two cannot drift: a variant without a
    function fails the conformance test, and a function without a variant has
    no name that maps to it.
    """
    return 'horsies_' + _CAMEL_BOUNDARY.sub('_', type(command).__name__).lower()
