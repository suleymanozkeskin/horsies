"""Execution classes across the fleet: pending streams, coverage, budget load.

A pending stream is the ``PENDING`` rows of one queue and one execution
class. A live worker serves a stream when the queue is one of its queues and
one of its claim budgets serves the class (``ClassBudget.serves``). A worker
is live while its latest state snapshot is at most
``WORKER_STATE_LIVE_WINDOW_S`` old.

The types are frozen reports. The class reads of
``PostgresBroker`` run in the core and give these types
(``horsies.core.brokers.worker_state_records``).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TypeAlias

from horsies.core.models.tasks import Execution


@dataclass(slots=True, frozen=True)
class StreamCoverageStateServed:
    """Live workers serve the stream, and they have capacity in service for it.

    ``workers`` counts the live workers that serve the stream, 1 or more.
    ``capacity`` sums, over those workers, the capacity in service of the
    budget that serves the class, 1 or more. A budget that serves more than
    one class counts in full for each class.
    """

    workers: int
    capacity: int


@dataclass(slots=True, frozen=True)
class StreamCoverageStateNoCapacity:
    """Live workers serve the stream, and none of them has capacity in service
    for it: every child or seat of the serving budgets was down at the
    worker's snapshot. ``workers`` counts them, 1 or more."""

    workers: int


@dataclass(slots=True, frozen=True)
class StreamCoverageStateUnserved:
    """No live worker serves the stream. Its rows wait until a worker that
    serves the class runs on the queue."""


StreamCoverageState: TypeAlias = StreamCoverageStateServed | StreamCoverageStateNoCapacity | StreamCoverageStateUnserved


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
    whose latest snapshot was live; the coverage of each stream comes from
    these. ``streams`` holds one entry per (queue, class) that has
    ``PENDING`` rows, ordered by queue name, then by ``Execution`` order.
    Empty when no row is pending.
    """

    checked_at: datetime
    live_workers: int
    streams: tuple[StreamCoverage, ...]


@dataclass(slots=True, frozen=True)
class StreamWaitDue:
    """At least one row of the stream is due.

    ``enqueued_at`` is the due time of the oldest due row: its send time, or
    the time its retry became due. ``wait`` is how long that row has waited
    at ``checked_at``, 0 or more.
    """

    enqueued_at: datetime
    wait: timedelta


@dataclass(slots=True, frozen=True)
class StreamWaitNoneDue:
    """Every row of the stream is due later: a delayed send, or a retry that
    waits for its interval. ``next_due_at`` is the earliest due time."""

    next_due_at: datetime


StreamWait: TypeAlias = StreamWaitDue | StreamWaitNoneDue


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
    ``ClassCoverage.streams``; ``workers`` (the live workers) by worker id.
    """

    checked_at: datetime
    streams: tuple[PendingStream, ...]
    workers: tuple[WorkerClassLoad, ...]


