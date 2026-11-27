"""The arguments of the health reads and the rows of the task lists.

A ping takes a ``PingTarget`` and a ``MinResponses``; a worker state
history read takes a ``WorkerHistoryLimit``. The stale and expired task
lists return a ``BoundedTaskList`` of ``StaleTaskRow`` or
``ExpiredTaskRow``. Every type is a frozen dataclass with no behaviour.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TypeAlias
from uuid import UUID


@dataclass(slots=True, frozen=True)
class PingTargetEveryWorker:
    """The ping asks every worker that listens."""


@dataclass(slots=True, frozen=True)
class PingTargetWorker:
    """The ping asks the worker ``worker_id``; it returns as soon as that
    worker replies."""

    worker_id: str


PingTarget: TypeAlias = PingTargetEveryWorker | PingTargetWorker


@dataclass(slots=True, frozen=True)
class MinResponsesFullWindow:
    """The ping waits its full window and lists every worker that replied."""


@dataclass(slots=True, frozen=True)
class MinResponsesAtLeast:
    """The ping returns as soon as ``count`` distinct workers replied.

    ``count`` is 1 or more; the ping refuses a lower count with
    ``INVALID_INPUT``.
    """

    count: int


MinResponses: TypeAlias = MinResponsesFullWindow | MinResponsesAtLeast


@dataclass(slots=True, frozen=True)
class WorkerHistoryLimitAllRetained:
    """The history read returns every retained snapshot of the worker."""


@dataclass(slots=True, frozen=True)
class WorkerHistoryLimitNewest:
    """The history read returns the newest ``count`` snapshots of the worker.

    ``count`` is 1 or more; the read refuses a lower count with
    ``INVALID_INPUT``.
    """

    count: int


WorkerHistoryLimit: TypeAlias = (
    WorkerHistoryLimitAllRetained | WorkerHistoryLimitNewest
)


@dataclass(slots=True, frozen=True)
class StaleTaskRow:
    """A RUNNING task whose latest runner heartbeat, or its start when it has
    no heartbeat, is older than the threshold of the read.

    ``last_heartbeat_at`` is the latest runner heartbeat sent at or after
    ``started_at``, and ``None`` when the task has none: the start writes no
    heartbeat, and a heartbeat of an earlier attempt does not count.
    """

    task_id: UUID
    task_name: str
    worker_hostname: str | None
    worker_pid: int | None
    worker_process_name: str | None
    started_at: datetime
    last_heartbeat_at: datetime | None


@dataclass(slots=True, frozen=True)
class ExpiredTaskRow:
    """A PENDING task whose ``good_until`` passed before a worker took it.

    ``expired_for`` is how long ago ``good_until`` passed, on the database
    clock, exact to the microsecond.
    """

    task_id: UUID
    task_name: str
    queue_name: str
    priority: int
    sent_at: datetime
    enqueued_at: datetime
    good_until: datetime
    expired_for: timedelta


@dataclass(slots=True, frozen=True)
class BoundedTaskListComplete[RowT]:
    """Every matching row of a task list, in list order."""

    rows: tuple[RowT, ...]


@dataclass(slots=True, frozen=True)
class BoundedTaskListTruncated[RowT]:
    """The first ``bound`` matching rows of a task list, in list order; more
    rows match. ``bound`` is ``TASK_LIST_ROW_BOUND``."""

    rows: tuple[RowT, ...]
    bound: int


type BoundedTaskList[RowT] = BoundedTaskListComplete[RowT] | BoundedTaskListTruncated[RowT]
"""A task list and whether it stopped at its bound."""
