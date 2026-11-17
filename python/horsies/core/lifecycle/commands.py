"""The ways the client ends a task, as thirteen commands.

Sixteen writer contracts terminalize tasks. They are fifteen operations, not
sixteen: two failure writers agree on fence, cardinality, coupled write, target
status and guards, and differ only in whether they carry a failure reason.
Everything else differs in at least one of those five, and so needs its own
contract. Two of the fifteen have no command here: the administrative cancel
(``horsies_cancel_task``, which the core runs) and the stale RUNNING recovery
(``horsies_finish_stale_attempt``, which ``core/brokers/stale_running.py``
sends). ``CompleteLockedTask`` and
``FailLockedTask`` are finalize decisions that the attempt writers execute
(``horsies_finish_workflow_success``, ``horsies_finish_failed_attempt``).

The count is not the point and compression was never the goal. What this
vocabulary buys is that states which cannot occur cannot be written down: a
batch fence cannot be attached to a single-task command, a pause disposition
cannot be attached to a cancellation, and a workflow-scoped command cannot name
the workflow status it requires — the variant means it. Each variant holds
exactly the data its guard needs and nothing that could contradict it.

Payload obeys the same rule and is per-variant. A variant carries a result, an
error code, or a failure reason only where its statement takes that value from
the caller; eight of the thirteen own those values as literals — a workflow pause
always writes the same reason — so those variants carry none, and cannot be
handed a payload the database would silently ignore.

One union, not one per cardinality. Cardinality is variant identity here — a
separate batch union would encode shape twice and let the two encodings drift.

`tests/unit/test_lifecycle_commands.py` asserts this union against the writer
matrix: a new writer differing in any signature field forces a new variant, and
a variant matching an existing signature has to justify itself or merge.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..types.status import TaskStatus
from .fences import (
    OwnedClaim,
    OwnedClaimBatch,
    PriorLockedRead,
    TerminalFence,
    WorkerOwned,
)


# ---------------------------------------------------------------------------
# COMPLETED
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CompleteLockedTask:
    """Success for a task whose row the caller already locked and read.

    The attempt row is written separately by the caller in the same
    transaction, from the context that locked read produced.
    """

    task_id: str
    fence: PriorLockedRead
    result_json: str


@dataclass(frozen=True, slots=True)
class CompleteTaskFused:
    """Success for a plain task, as one statement.

    Locks the row, writes the attempt from the locked row's own context,
    transitions, and wakes queue capacity — one round trip. The fence lives in
    the locking read ahead of the update, not in the update's predicate, and a
    reimplementation must keep that structure: restating the guard as an update
    predicate preserves the semantics and loses the single-statement property
    the fusion exists for. The function builds the capacity wake from the
    locked row: channel ``task_queue_<queue>``, payload ``capacity``.
    """

    task_id: str
    fence: OwnedClaim
    result_json: str


# ---------------------------------------------------------------------------
# FAILED
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FailLockedTask:
    """Failure for a task whose row the caller already locked and read.

    Covers both application failure and worker-level failure; they differ only
    in whether a failure reason is carried.

    `failed_reason` is assigned unconditionally: the terminal writer owns the
    complete final-attempt summary. None means this final attempt has no
    failure reason, and clears any value a requeued earlier attempt left on
    the row. Per-attempt history lives in `horsies_task_attempts`.
    """

    task_id: str
    fence: PriorLockedRead
    result_json: str
    error_code: str | None
    failed_reason: str | None


def _require_positive_batch_size(batch_size: int) -> None:
    """A discovery batch's bound is load-bearing, so it must actually bound.

    The database function raises on the same precondition; validating at
    construction reports the mistake at the call site that made it, before a
    connection is ever involved.
    """
    if batch_size <= 0:
        raise ValueError(
            f'batch_size must be a positive integer, got {batch_size}; the '
            f'bound exists to keep one pass from committing an unbounded '
            f'notification burst'
        )


# ---------------------------------------------------------------------------
# EXPIRED
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ExpireOwnedClaim:
    """A claimed task whose deadline passed before user code started.

    Fences on worker ownership but deliberately not on claim generation: once
    the deadline has passed, expiry is the correct outcome for whichever
    generation holds the row.
    """

    task_id: str
    fence: WorkerOwned
    result_json: str
    error_code: str


@dataclass(frozen=True, slots=True)
class ExpirePendingTasks:
    """Unclaimed tasks whose deadline passed, in bounded batches.

    Batched rather than done in one statement because a mass expiry commits
    two notifications per row at once and overflows listener queues. Rows a
    concurrent claim holds are skipped; the claim re-checks the deadline
    itself, so the race resolves the same way either way.
    """

    batch_size: int
    result_json: str
    error_code: str

    def __post_init__(self) -> None:
        _require_positive_batch_size(self.batch_size)


# ---------------------------------------------------------------------------
# CANCELLED — orphan cleanup
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CancelOwnedOrphan:
    """One workflow task this worker holds that can never progress.

    An orphan has no workflow-task row in a runnable state, so its transition
    to running can never succeed. Left alone it holds a claim forever.
    """

    task_id: str
    fence: OwnedClaim


@dataclass(frozen=True, slots=True)
class CancelOrphanedTasks:
    """The same condition, swept in bounded batches across all workers."""

    batch_size: int

    def __post_init__(self) -> None:
        _require_positive_batch_size(self.batch_size)


# ---------------------------------------------------------------------------
# CANCELLED — workflow pause, resumable
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AbandonOwnedNode:
    """One claimed node abandoned because its workflow paused.

    The backing row is abandoned so resume can enqueue a fresh one; the node is
    returned to ready and detached from this task. Pause is resumable, which is
    why the node is readied rather than skipped — the disposition follows from
    which command this is, and cannot be set to disagree with it.
    """

    task_id: str
    fence: OwnedClaim


@dataclass(frozen=True, slots=True)
class AbandonOwnedNodes:
    """The same, for a batch this worker just claimed."""

    fence: OwnedClaimBatch


@dataclass(frozen=True, slots=True)
class AbandonNodesOfPausedWorkflows:
    """Every claimed node under workflows that are paused right now.

    Reaches claims other workers hold, which is the point: a claim taken after
    the pause is exactly what must be abandoned.
    """

    workflow_ids: tuple[str, ...]


# ---------------------------------------------------------------------------
# CANCELLED — workflow cancellation, final
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CancelOwnedNode:
    """One node cancelled because its workflow was cancelled.

    Also accepts a row already requeued to pending. That row carries no claim,
    so there is no generation to fence — and refusing it would leave a task of
    a cancelled workflow live. The workflow's cancellation is the guard there,
    and it is final, so it cannot go stale the way a pause can.
    """

    task_id: str
    fence: OwnedClaim
    accepts_requeued_pending: bool


@dataclass(frozen=True, slots=True)
class CancelOwnedNodes:
    """The same, for a batch this worker just claimed."""

    fence: OwnedClaimBatch


@dataclass(frozen=True, slots=True)
class CancelNodesOfCancelledWorkflow:
    """Nodes enqueued but not yet started under a cancelled workflow.

    Reaches a backing row that is briefly running: user code starts only after
    the node's own transition to running, so a node still enqueued has not
    begun executing whatever its task row says.
    """

    workflow_ids: tuple[str, ...]


type TerminalizationCommand = (
    CompleteLockedTask
    | CompleteTaskFused
    | FailLockedTask
    | ExpireOwnedClaim
    | ExpirePendingTasks
    | CancelOwnedOrphan
    | CancelOrphanedTasks
    | AbandonOwnedNode
    | AbandonOwnedNodes
    | AbandonNodesOfPausedWorkflows
    | CancelOwnedNode
    | CancelOwnedNodes
    | CancelNodesOfCancelledWorkflow
)


def target_status(command: TerminalizationCommand) -> TaskStatus:
    """The status the command writes. Exhaustive over the union."""
    match command:
        case CompleteLockedTask() | CompleteTaskFused():
            return TaskStatus.COMPLETED
        case FailLockedTask():
            return TaskStatus.FAILED
        case ExpireOwnedClaim() | ExpirePendingTasks():
            return TaskStatus.EXPIRED
        case (
            CancelOwnedOrphan()
            | CancelOrphanedTasks()
            | AbandonOwnedNode()
            | AbandonOwnedNodes()
            | AbandonNodesOfPausedWorkflows()
            | CancelOwnedNode()
            | CancelOwnedNodes()
            | CancelNodesOfCancelledWorkflow()
        ):
            return TaskStatus.CANCELLED


def fence_of(command: TerminalizationCommand) -> TerminalFence | None:
    """The command's claim-ownership guard, for outcome reporting.

    None means the statement carries no ownership predicate. Three kinds of
    command have none, for three different reasons: batch expiry acts across
    workers because the owner is irrelevant; orphan sweeping acts on rows whose
    linkage, not their claim, is the defect; and the workflow-scoped commands
    must reach claims other workers hold, so an ownership predicate would skip
    their whole purpose.
    """
    match command:
        case CompleteLockedTask(fence=fence) | FailLockedTask(fence=fence):
            return fence
        case CompleteTaskFused(fence=fence):
            return fence
        case ExpireOwnedClaim(fence=fence):
            return fence
        case CancelOwnedOrphan(fence=fence):
            return fence
        case AbandonOwnedNode(fence=fence) | CancelOwnedNode(fence=fence):
            return fence
        case AbandonOwnedNodes(fence=fence) | CancelOwnedNodes(fence=fence):
            return fence
        case (
            ExpirePendingTasks()
            | CancelOrphanedTasks()
            | AbandonNodesOfPausedWorkflows()
            | CancelNodesOfCancelledWorkflow()
        ):
            return None
