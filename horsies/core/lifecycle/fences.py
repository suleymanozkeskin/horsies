"""Concurrency guards a terminal transition applies to the rows it touches.

A fence answers "may this caller end this task's life right now". It is not the
source-status check, which every transition also carries; it is the ownership
or coordination predicate layered on top.

Four exist, and which one a transition needs is a property of where it runs,
not a preference:

- a caller holding the row lock needs none in the statement;
- a caller acting on one claim it was handed needs the owner pair;
- a caller acting on a batch it was handed needs that pair per task, because a
  batch can span claim transactions;
- a caller acting on behalf of a workflow needs the workflow's own state under
  lock, and must *not* fence on claim ownership — it exists to reach claims
  other workers hold.

The claim generation is `claimed_at`: set by the claim, cleared by every
requeue. Worker id alone cannot separate generations, because a worker whose
lease lapsed can re-claim its own task and match again.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class CallerHoldsRowLock:
    """No predicate in the statement; the caller locked the row first.

    Used where the decision to terminalize was made against a locked read, so
    re-checking ownership in the write would guard nothing that has not already
    been established.
    """


@dataclass(frozen=True, slots=True)
class OwnedClaim:
    """One task, held by this worker at this claim generation.

    `claimed_at` of None disables the generation half, leaving worker
    ownership. That is not a loophole but a compatibility seam: a caller
    without a dispatch context still fences on ownership rather than silently
    fencing on nothing.
    """

    worker_id: str
    claimed_at: datetime | None


@dataclass(frozen=True, slots=True)
class OwnedClaimBatch:
    """Many tasks, each at its own claim generation.

    One batch can span several claim transactions, so a single generation
    cannot describe it: that would either spare every task or terminalize
    every task. Generations travel with their task id.
    """

    worker_id: str
    claim_generations: tuple[tuple[str, datetime | None], ...]

    def task_ids(self) -> tuple[str, ...]:
        return tuple(task_id for task_id, _ in self.claim_generations)

    def generations(self) -> tuple[datetime | None, ...]:
        return tuple(generation for _, generation in self.claim_generations)


@dataclass(frozen=True, slots=True)
class WorkflowStateUnderLock:
    """The containing workflow's state is the guard; claim ownership is not.

    Deliberately cross-worker. A task re-claimed while its workflow is still
    paused is exactly what this reaches — fencing on the caller's view of the
    claim would skip the claims it exists to catch.

    Verified in-statement rather than trusted from the caller, so the guard
    holds for any caller rather than only the one that happens to lock first.
    """

    workflow_ids: tuple[str, ...]
    required_workflow_status: str
    required_node_statuses: tuple[str, ...]


type TerminalFence = (
    CallerHoldsRowLock | OwnedClaim | OwnedClaimBatch | WorkflowStateUnderLock
)
