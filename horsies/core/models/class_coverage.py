"""Execution classes across the fleet: pending streams, coverage, budget load.

A pending stream is the ``PENDING`` rows of one queue and one execution
class. A live worker serves a stream when the queue is one of its queues and
one of its claim budgets serves the class (``ClassBudget.serves``). A worker
is live while its latest state snapshot is at most
``WORKER_STATE_LIVE_WINDOW_S`` old.

The types are frozen reports. The functions are pure; the database reads
that feed them live in ``horsies.core.brokers.class_coverage_reads``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TypeAlias, assert_never

from horsies.core.models.health import (
    ClassBudget,
    ClassBudgets,
    ClassCapacityNotRecorded,
    parse_class_capacity,
)
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Err, Ok, Result


@dataclass(slots=True, frozen=True)
class StreamServed:
    """Live workers serve the stream, and they have capacity in service for it.

    ``workers`` counts the live workers that serve the stream, 1 or more.
    ``capacity`` sums, over those workers, the capacity in service of the
    budget that serves the class, 1 or more. A budget that serves more than
    one class counts in full for each class.
    """

    workers: int
    capacity: int


@dataclass(slots=True, frozen=True)
class StreamWithoutCapacity:
    """Live workers serve the stream, and none of them has capacity in service
    for it: every child or seat of the serving budgets was down at the
    worker's snapshot. ``workers`` counts them, 1 or more."""

    workers: int


@dataclass(slots=True, frozen=True)
class StreamUnserved:
    """No live worker serves the stream. Its rows wait until a worker that
    serves the class runs on the queue."""


StreamCoverageState: TypeAlias = StreamServed | StreamWithoutCapacity | StreamUnserved


@dataclass(slots=True, frozen=True)
class StreamCoverage:
    """The coverage of one pending stream."""

    queue_name: str
    execution: Execution
    state: StreamCoverageState


@dataclass(slots=True, frozen=True)
class ClassCoverage:
    """The coverage of every pending stream, read at ``checked_at``.

    ``checked_at`` is database time. ``live_workers`` counts the workers
    whose latest snapshot was live. ``streams`` holds one entry per (queue,
    class) that has ``PENDING`` rows, ordered by queue name, then by
    ``Execution`` order. Empty when no row is pending.
    """

    checked_at: datetime
    live_workers: int
    streams: tuple[StreamCoverage, ...]


@dataclass(slots=True, frozen=True)
class OldestWaiting:
    """At least one row of the stream is due.

    ``enqueued_at`` is the due time of the oldest due row: its send time, or
    the time its retry became due. ``wait`` is how long that row has waited
    at ``checked_at``, 0 or more.
    """

    enqueued_at: datetime
    wait: timedelta


@dataclass(slots=True, frozen=True)
class NoneDue:
    """Every row of the stream is due later: a delayed send, or a retry that
    waits for its interval. ``next_due_at`` is the earliest due time."""

    next_due_at: datetime


StreamWait: TypeAlias = OldestWaiting | NoneDue


@dataclass(slots=True, frozen=True)
class PendingStream:
    """One pending stream: how long its rows wait, and whether a live worker serves it."""

    queue_name: str
    execution: Execution
    wait: StreamWait
    state: StreamCoverageState


@dataclass(slots=True, frozen=True)
class BudgetLoad:
    """One claim budget of a live worker.

    ``capacity`` and ``serves`` come from the worker's latest snapshot.
    ``in_flight`` counts, at the read, the worker's ``RUNNING`` rows and its
    ``CLAIMED`` rows with a live lease whose class the budget serves. The
    claim counts the same rows against the budget.
    """

    budget: Execution
    capacity: int
    serves: tuple[Execution, ...]
    in_flight: int

    @property
    def free(self) -> int:
        """Capacity that holds no row, at least 0.

        A worker with a prefetch buffer can hold more rows than its
        capacity, and a slot that goes down lowers the capacity below the
        rows already held.
        """
        return max(0, self.capacity - self.in_flight)


@dataclass(slots=True, frozen=True)
class WorkerClassLoad:
    """The claim budgets of one live worker, in ``Execution`` order."""

    worker_id: str
    snapshot_at: datetime
    budgets: tuple[BudgetLoad, ...]


@dataclass(slots=True, frozen=True)
class ClassReport:
    """Pending streams and the budget load of each live worker.

    ``checked_at`` is database time. ``streams`` is ordered like
    ``ClassCoverage.streams``; ``workers`` by worker id.
    """

    checked_at: datetime
    streams: tuple[PendingStream, ...]
    workers: tuple[WorkerClassLoad, ...]


@dataclass(slots=True, frozen=True)
class ClassReadRefused:
    """Stored data that the class reads cannot decide on, or a read bound that was hit.

    ``reason`` names the subject (a worker, a class, a bound) and the cause.
    Nothing changed.
    """

    reason: str


@dataclass(slots=True, frozen=True)
class LiveWorker:
    """A worker whose latest snapshot is live, with the budgets it recorded."""

    worker_id: str
    snapshot_at: datetime
    queues: frozenset[str]
    budgets: tuple[ClassBudget, ...]


@dataclass(slots=True, frozen=True)
class StreamHead:
    """The earliest ``enqueued_at`` of the pending rows of one (queue, class, priority)."""

    queue_name: str
    execution: Execution
    enqueued_at: datetime


def live_worker(
    worker_id: str,
    snapshot_at: datetime,
    queues: Sequence[str],
    class_capacity: object,
) -> Result[LiveWorker, ClassReadRefused]:
    """Build a live worker from its latest snapshot row.

    ``Err``: the stored ``class_capacity`` is unreadable, or it is NULL. A
    NULL value comes from a worker version that did not record its budgets,
    so its routes are unknown and no stream can be decided. Reads only.
    """
    match parse_class_capacity(class_capacity):
        case Ok(capacity):
            pass
        case Err(unreadable):
            return Err(
                ClassReadRefused(
                    f'worker {worker_id} snapshot at {snapshot_at}: '
                    f'class_capacity is unreadable: {unreadable.reason}',
                ),
            )
    match capacity:
        case ClassBudgets(budgets=budgets):
            return Ok(LiveWorker(worker_id, snapshot_at, frozenset(queues), budgets))
        case ClassCapacityNotRecorded():
            return Err(
                ClassReadRefused(
                    f'worker {worker_id} is live and its snapshot at {snapshot_at} '
                    'has no class_capacity, so the classes it serves are unknown',
                ),
            )
        case _ as unreachable:
            assert_never(unreachable)


def stream_coverage(
    queue_name: str,
    execution: Execution,
    workers: Sequence[LiveWorker],
) -> StreamCoverageState:
    """Whether the live ``workers`` serve the stream, and with what capacity."""
    budgets = [
        budget
        for worker in workers
        if queue_name in worker.queues
        for budget in worker.budgets
        if execution in budget.serves
    ]
    if not budgets:
        return StreamUnserved()
    capacity = sum(budget.capacity for budget in budgets)
    if capacity == 0:
        return StreamWithoutCapacity(workers=len(budgets))
    return StreamServed(workers=len(budgets), capacity=capacity)


def stream_wait(heads: Sequence[datetime], checked_at: datetime) -> StreamWait:
    """The wait of a stream from the head of each of its priorities.

    A row is due when its ``enqueued_at`` is at or before ``checked_at``.
    ``heads`` holds at least one instant; an empty one is a defect of the
    caller (``AssertionError``).
    """
    assert heads, 'a pending stream has at least one head'
    due = [head for head in heads if head <= checked_at]
    if due:
        oldest = min(due)
        return OldestWaiting(enqueued_at=oldest, wait=checked_at - oldest)
    return NoneDue(next_due_at=min(heads))


def class_coverage(
    checked_at: datetime,
    workers: Sequence[LiveWorker],
    streams: Sequence[tuple[str, Execution]],
) -> ClassCoverage:
    """The coverage of each pending (queue, class) in ``streams``."""
    return ClassCoverage(
        checked_at=checked_at,
        live_workers=len(workers),
        streams=tuple(
            StreamCoverage(
                queue_name, execution, stream_coverage(queue_name, execution, workers)
            )
            for queue_name, execution in sorted(streams, key=_stream_order)
        ),
    )


def class_report(
    checked_at: datetime,
    workers: Sequence[LiveWorker],
    heads: Sequence[StreamHead],
    in_flight: Mapping[tuple[str, Execution], int],
) -> Result[ClassReport, ClassReadRefused]:
    """Pending streams from their heads, and the budget load of each live worker.

    ``in_flight`` maps (worker id, class) to the rows that the worker holds.
    ``Err``: a worker holds rows of a class that none of its budgets serves.
    """
    grouped: dict[tuple[str, Execution], list[datetime]] = {}
    for head in heads:
        grouped.setdefault((head.queue_name, head.execution), []).append(
            head.enqueued_at
        )
    streams = tuple(
        PendingStream(
            queue_name=queue_name,
            execution=execution,
            wait=stream_wait(grouped[(queue_name, execution)], checked_at),
            state=stream_coverage(queue_name, execution, workers),
        )
        for queue_name, execution in sorted(grouped, key=_stream_order)
    )
    loads: list[WorkerClassLoad] = []
    for worker in sorted(workers, key=lambda entry: entry.worker_id):
        held = {
            execution: count
            for (worker_id, execution), count in in_flight.items()
            if worker_id == worker.worker_id
        }
        match budget_loads(worker, held):
            case Ok(budgets):
                loads.append(
                    WorkerClassLoad(worker.worker_id, worker.snapshot_at, budgets)
                )
            case Err(refused):
                return Err(refused)
    return Ok(ClassReport(checked_at=checked_at, streams=streams, workers=tuple(loads)))


def budget_loads(
    worker: LiveWorker,
    in_flight: Mapping[Execution, int],
) -> Result[tuple[BudgetLoad, ...], ClassReadRefused]:
    """The load of each budget of ``worker`` from the rows it holds per class.

    ``Err``: the worker holds rows of a class that none of its budgets
    serves. The claim takes only rows of a served class, so such a row
    means that the stored data and the recorded budgets disagree.
    """
    served = {member for budget in worker.budgets for member in budget.serves}
    for execution, count in in_flight.items():
        if count > 0 and execution not in served:
            return Err(
                ClassReadRefused(
                    f'worker {worker.worker_id} holds {count} {execution.value} '
                    'row(s), and no budget of its snapshot serves that class',
                ),
            )
    return Ok(
        tuple(
            BudgetLoad(
                budget=budget.budget,
                capacity=budget.capacity,
                serves=budget.serves,
                in_flight=sum(in_flight.get(member, 0) for member in budget.serves),
            )
            for budget in worker.budgets
        ),
    )


def _stream_order(stream: tuple[str, Execution]) -> tuple[str, int]:
    queue_name, execution = stream
    return queue_name, list(Execution).index(execution)
