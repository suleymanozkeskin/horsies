"""Soft-cap mode: a capped queue's running count is checked before each start.

In soft-cap mode (``prefetch_buffer > 0``) the claim counts only the RUNNING
rows of a queue against its ``max_concurrency``, so a worker can claim more
rows of a capped queue than its cap while none of them runs. The worker does
not start those rows at once: it holds them in the parent (``QueueCapHold``),
and each claim pass reads one count per capped queue that has held rows
(``COUNT_QUEUE_STARTS_SQL`` in ``worker/sql.py``):

- the RUNNING rows of the queue, on every worker;
- the rows of the queue that this worker started and that are still CLAIMED,
  because their child has not set them RUNNING yet.

The pass then starts held rows in claim order while that count stays under
the cap and a child of the row's route has a free slot or seat at that moment
(the free capacity of each budget, read after the count). An admitted row
therefore goes to a child at once and does not wait in the slot pool. Another
worker counts the row once its child sets it RUNNING, so the time in which
another worker does not count it is one start. A held row stays CLAIMED, and
the claimer heartbeat keeps its lease, until a later pass starts it or the
worker stops and returns it to the queue.

Rows of an uncapped queue start at once. In hard-cap mode every row starts at
once: there the claim counts the CLAIMED rows against the cap too.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from horsies.core.models.tasks import Execution


def start_check_caps(
    prefetch_buffer: int,
    queue_max_concurrency: Mapping[str, int],
) -> Mapping[str, int]:
    """The cap of each queue whose rows wait for the start check: every
    capped queue in soft-cap mode (``prefetch_buffer > 0``), none in hard-cap
    mode (``prefetch_buffer == 0``). Does no I/O."""
    match prefetch_buffer:
        case 0:
            return {}
        case int() if prefetch_buffer > 0:
            return dict(queue_max_concurrency)
        case _:
            raise AssertionError(
                f'prefetch_buffer {prefetch_buffer} is negative; the config refuses it'
            )


@dataclass(frozen=True, slots=True)
class HeldRow:
    """A claimed row of a capped queue that waits for its queue's count and
    for a free child of its route.

    ``budget`` is the budget class of the claim plan that the row's class
    routes to: the children of that budget run the row. ``claimed_at`` is
    the claim generation of the row's claim. ``row`` is the row as
    ``horsies_claim`` returned it.
    """

    task_id: str
    queue_name: str
    budget: Execution
    claimed_at: datetime
    row: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class StartedClaim:
    """A row that this worker started and whose finalizer has not ended, with
    the claim generation of the claim it started under."""

    task_id: str
    claimed_at: datetime


def read_queue_starts(
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
) -> dict[str, int]:
    """The count of each queue that ``COUNT_QUEUE_STARTS_SQL`` returned: its
    RUNNING rows plus the rows this worker started that are not RUNNING yet.
    A repeated queue or a negative count is a defect of the statement
    (``AssertionError``). Does no I/O."""
    starts: dict[str, int] = {}
    for values in rows:
        row = dict(zip(columns, values))
        queue_name = str(row['queue_name'])
        running = int(row['running'])
        started_not_running = int(row['started_not_running'])
        assert queue_name not in starts, f'queue {queue_name!r} counted twice'
        assert running >= 0 and started_not_running >= 0, (
            f'queue {queue_name!r} counted {running} running and '
            f'{started_not_running} started rows'
        )
        starts[queue_name] = running + started_not_running
    return starts


class QueueCapHold:
    """The held rows of the capped queues of one worker, and the rows that it
    started and that have not ended.

    Holds no lock: the claim loop and the done callbacks of the finalizers
    run on the event loop of the worker process. Does no I/O. The held rows
    are CLAIMED rows of this worker, so the claim budget of the worker bounds
    their number.
    """

    def __init__(self, caps: Mapping[str, int]) -> None:
        self._caps: Mapping[str, int] = dict(caps)
        self._held: deque[HeldRow] = deque()
        self._started: dict[str, datetime] = {}

    def holds(self, queue_name: str) -> bool:
        """Whether a row of ``queue_name`` waits for the start check."""
        return queue_name in self._caps

    def hold(self, row: HeldRow) -> None:
        """Hold ``row`` until a pass starts it. A row of a queue that the
        check does not hold is a defect of the caller (``AssertionError``)."""
        assert self.holds(row.queue_name), (
            f'task {row.task_id} of queue {row.queue_name!r} is not held by the start check'
        )
        self._held.append(row)

    def held_queues(self) -> tuple[str, ...]:
        """The queues that have held rows, in the order of their first row."""
        return tuple(dict.fromkeys(row.queue_name for row in self._held))

    def started_ids(self) -> tuple[str, ...]:
        """The task ids that this worker started and that have not ended."""
        return tuple(self._started)

    def started_claims(self) -> tuple[StartedClaim, ...]:
        """The rows that this worker started and that have not ended, with
        their claim generations."""
        return tuple(
            StartedClaim(task_id=task_id, claimed_at=claimed_at)
            for task_id, claimed_at in self._started.items()
        )

    def held_count(self) -> int:
        """How many rows wait for the start check."""
        return len(self._held)

    def admit(
        self,
        starts: Mapping[str, int],
        free: Mapping[Execution, int],
    ) -> list[HeldRow]:
        """Take the held rows that may start, in claim order, and record them
        as started.

        ``starts`` is the count of each queue (``read_queue_starts``); a queue
        that it does not name has 0. ``free`` is the free capacity of each
        budget now: its children or seats that take a task at once, less the
        tasks that already wait for them; a budget that it does not name has
        0. A row starts while its queue's count plus the rows that this call
        started of it stays under the cap, and its budget's free capacity
        less the rows that this call started of it stays above 0. The other
        rows stay held, in their order.
        """
        admitted: list[HeldRow] = []
        kept: deque[HeldRow] = deque()
        taken: dict[str, int] = {}
        seated: dict[Execution, int] = {}
        for row in self._held:
            count = starts.get(row.queue_name, 0) + taken.get(row.queue_name, 0)
            room = free.get(row.budget, 0) - seated.get(row.budget, 0)
            if count < self._caps[row.queue_name] and room > 0:
                taken[row.queue_name] = taken.get(row.queue_name, 0) + 1
                seated[row.budget] = seated.get(row.budget, 0) + 1
                self._started[row.task_id] = row.claimed_at
                admitted.append(row)
            else:
                kept.append(row)
        self._held = kept
        return admitted

    def end(self, task_id: str) -> None:
        """Forget a started row whose finalizer ended. A task id that was not
        started here is a defect of the caller (``AssertionError``)."""
        assert task_id in self._started, f'task {task_id} was not started by the start check'
        del self._started[task_id]

    def drain(self) -> list[HeldRow]:
        """Take every held row, in claim order, at the stop of the worker."""
        drained = list(self._held)
        self._held.clear()
        return drained
