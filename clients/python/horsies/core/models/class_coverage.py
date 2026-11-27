"""Execution classes across the fleet: pending streams, coverage, budget load.

A pending stream is the ``PENDING`` rows of one queue and one execution
class. A live worker serves a stream when the queue is one of its queues and
one of its claim budgets serves the class (``ExecutionClassBudget.serves``).
A worker is live while its latest state snapshot is at most
``WORKER_STATE_LIVE_WINDOW_SECONDS`` old.

The types are frozen reports. The execution class reads of
``PostgresBroker`` run in the core and give these types
(``horsies.core.brokers.worker_state_records``). The execution class
report and its parts are also the JSON of the monitoring route
``GET /api/workers/classes``: a union member carries its ``kind`` tag, and
a duration is a number of seconds under a ``_seconds`` key.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Annotated, Literal, TypeAlias

from pydantic import Field, PlainSerializer

from horsies.core.models.tasks import Execution


def _seconds(duration: timedelta) -> float:
    """The seconds of ``duration`` as a JSON number."""
    return duration.total_seconds()


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
    kind: Literal['served'] = 'served'


@dataclass(slots=True, frozen=True)
class StreamCoverageStateNoCapacity:
    """Live workers serve the stream, and none of them has capacity in service
    for it: every child or seat of the serving budgets was down at the
    worker's snapshot. ``workers`` counts them, 1 or more."""

    workers: int
    kind: Literal['no_capacity'] = 'no_capacity'


@dataclass(slots=True, frozen=True)
class StreamCoverageStateUnserved:
    """No live worker serves the stream. Its rows wait until a worker that
    serves the class runs on the queue."""

    kind: Literal['unserved'] = 'unserved'


StreamCoverageState: TypeAlias = Annotated[
    StreamCoverageStateServed | StreamCoverageStateNoCapacity | StreamCoverageStateUnserved,
    Field(discriminator='kind'),
]
"""Whether live workers serve a pending stream."""


@dataclass(slots=True, frozen=True)
class StreamCoverage:
    """The coverage of one pending stream."""

    queue: str
    execution: Execution
    coverage: StreamCoverageState


@dataclass(slots=True, frozen=True)
class ExecutionClassCoverage:
    """The coverage of every pending stream, read at ``checked_at``.

    ``checked_at`` is database time. ``live_workers`` counts the workers
    whose latest snapshot was live; the coverage of each stream comes from
    these. ``streams`` holds one entry per (queue, class) that has
    ``PENDING`` rows, ordered by queue, then by ``Execution`` order.
    Empty when no row is pending.
    """

    checked_at: datetime
    live_workers: int
    streams: tuple[StreamCoverage, ...]


@dataclass(slots=True, frozen=True)
class StreamWaitDue:
    """At least one row of the stream is due.

    ``oldest_due_at`` is the due time of the oldest due row: its send time,
    or the time its retry became due. ``wait`` is how long that row has
    waited at ``checked_at``, 0 or more. In JSON, ``wait`` is
    ``wait_seconds``, a number of seconds.
    """

    oldest_due_at: datetime
    wait: Annotated[
        timedelta,
        PlainSerializer(_seconds, return_type=float, when_used='json'),
        Field(serialization_alias='wait_seconds'),
    ]
    kind: Literal['due'] = 'due'


@dataclass(slots=True, frozen=True)
class StreamWaitNoneDue:
    """Every row of the stream is due later: a delayed send, or a retry that
    waits for its interval. ``next_due_at`` is the earliest due time."""

    next_due_at: datetime
    kind: Literal['none_due'] = 'none_due'


StreamWait: TypeAlias = Annotated[
    StreamWaitDue | StreamWaitNoneDue,
    Field(discriminator='kind'),
]
"""How long the rows of a pending stream wait."""


@dataclass(slots=True, frozen=True)
class PendingStream:
    """One pending stream: whether a live worker serves it, and how long its rows wait."""

    queue: str
    execution: Execution
    coverage: StreamCoverageState
    wait: StreamWait


@dataclass(slots=True, frozen=True)
class BudgetLoad:
    """One claim budget of a live worker.

    ``capacity`` and ``serves`` come from the worker's latest snapshot.
    ``in_flight`` counts, at the read, the worker's ``RUNNING`` rows and its
    ``CLAIMED`` rows with a live lease whose class the budget serves. The
    claim counts the same rows against the budget. The JSON holds these four
    fields; a reader computes ``free``.
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
class WorkerExecutionClassLoad:
    """The claim budgets of one live worker, in ``Execution`` order."""

    worker_id: str
    snapshot_at: datetime
    budgets: tuple[BudgetLoad, ...]


@dataclass(slots=True, frozen=True)
class ExecutionClassReport:
    """Pending streams and the budget load of each live worker.

    ``checked_at`` is database time. A worker is live while its latest
    snapshot is at most ``live_window`` old
    (``WORKER_STATE_LIVE_WINDOW_SECONDS``). ``streams`` is ordered like
    ``ExecutionClassCoverage.streams``; ``workers`` (the live workers) by
    worker id. In JSON, ``live_window`` is ``live_window_seconds``, a number
    of seconds.
    """

    checked_at: datetime
    live_window: Annotated[
        timedelta,
        PlainSerializer(_seconds, return_type=float, when_used='json'),
        Field(serialization_alias='live_window_seconds'),
    ]
    streams: tuple[PendingStream, ...]
    workers: tuple[WorkerExecutionClassLoad, ...]
