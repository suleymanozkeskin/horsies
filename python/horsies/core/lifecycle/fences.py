"""Ownership guards a terminal transition applies to the rows it touches.

A fence answers "may this caller end this task's life right now". It is not the
source-status check, which every transition also carries, and it is not target
selection. It is the claim-ownership predicate layered on top.

Which fence a transition needs follows from where it runs, not from preference:

- a caller acting on a deadline needs the worker but not the generation, since
  the outcome is correct for whichever generation holds an expired row;
- a caller acting on one claim it was handed needs the full owner pair;
- a caller acting on a batch it was handed needs that pair per task, because a
  batch can span claim transactions.

The claim generation is `claimed_at`: set by the claim, cleared by every
requeue. Worker id alone cannot separate generations, because a worker whose
lease lapsed can re-claim its own task and match again.

Transitions that act on behalf of a workflow carry no fence at all. They exist
to reach claims other workers hold, so an ownership predicate would skip
exactly the rows they are for. Their guard is the workflow's own state, which
is implied by the command and verified in-statement rather than carried as
data — see `commands.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class WorkerOwned:
    """Worker ownership, deliberately without a claim generation.

    Used where a non-ownership guard already makes the outcome correct for any
    generation — an expired deadline does not become unexpired because the row
    was re-claimed.
    """

    worker_id: str


@dataclass(frozen=True, slots=True)
class OwnedClaim:
    """One task, held by this worker at this claim generation.

    Both halves are always checked. Every dispatch carries the claim time
    that its claim returned, so there is no fence on the worker alone.
    """

    worker_id: str
    claimed_at: datetime


@dataclass(frozen=True, slots=True)
class OwnedClaimBatch:
    """Many tasks, each at its own claim generation.

    One batch can span several claim transactions, so a single generation
    cannot describe it: that would either spare every task or terminalize every
    task. Generations travel with their task id, which also makes a length
    mismatch between ids and generations unrepresentable.
    """

    worker_id: str
    claim_generations: tuple[tuple[str, datetime], ...]

    def __post_init__(self) -> None:
        seen: set[str] = set()
        for task_id, _ in self.claim_generations:
            if task_id in seen:
                raise ValueError(
                    f'duplicate task id in claim batch: {task_id!r}. Each task '
                    f'carries one generation; a repeat means two generations '
                    f'claim the same row and the fence is ambiguous.'
                )
            seen.add(task_id)

    def task_ids(self) -> tuple[str, ...]:
        return tuple(task_id for task_id, _ in self.claim_generations)

    def generations(self) -> tuple[datetime, ...]:
        return tuple(generation for _, generation in self.claim_generations)


type TerminalFence = (
    WorkerOwned
    | OwnedClaim
    | OwnedClaimBatch
)
