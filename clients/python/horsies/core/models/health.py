"""Typed models for the worker/database health API.

The result models (`DatabasePing`, `WorkerPong`, `WorkerStateSnapshot` with
its slot health, class capacity and profile) are frozen dataclasses returned
to callers inside ``BrokerResult``. They carry no behaviour, only observed
data.
The core encodes and decodes the ping-pong payloads.

The worker-state snapshot mirrors ``WorkerStateModel`` (the timeseries row
written every few seconds); reads expose it without coercion.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypeAlias


from horsies.core.defaults import (
    WORKER_STATE_LIVE_WINDOW_S as WORKER_STATE_LIVE_WINDOW_S,
)
from horsies.core.models.boot_failure import (
    ClearableBootCause,
)
from horsies.core.models.cluster_cap import ClusterCap, NoClusterCap
from horsies.core.models.tasks import Execution


@dataclass(slots=True, frozen=True)
class DatabasePing:
    """Result of a ``SELECT 1`` round-trip through the live broker pool."""

    latency_ms: float


@dataclass(slots=True, frozen=True)
class WorkerPong:
    """A single worker's reply to a ping, with measured round-trip latency."""

    worker_id: str
    hostname: str
    pid: int
    round_trip_ms: float


@dataclass(slots=True, frozen=True)
class DownSlot:
    """A worker slot whose last child boot failed for a cause that can clear.

    The slot takes no task, and the worker's claim budget leaves it out. The
    worker boots it again after a delay that grows up to a bound, and it
    returns to service when a boot succeeds. ``detail`` is the error text of
    the last failed boot, at most ``MAX_BOOT_FAILURE_DETAIL_CHARS``
    characters. ``failed_boots`` counts the failed boots since the slot went
    down, at least 1.
    """

    slot: int
    cause: ClearableBootCause
    detail: str
    down_since: datetime
    failed_boots: int


@dataclass(slots=True, frozen=True)
class AllSlotsServing:
    """No slot of the worker was down when it wrote the snapshot."""


@dataclass(slots=True, frozen=True)
class SlotsDown:
    """The worker is degraded: these slots were down, in slot order. Never empty."""

    slots: tuple[DownSlot, ...]


SlotHealth: TypeAlias = AllSlotsServing | SlotsDown


@dataclass(slots=True, frozen=True)
class ClassBudget:
    """One claim budget of a worker, as its snapshot recorded it.

    ``capacity`` counts the children (``ISOLATED``) or seats (a shared class)
    of ``budget`` that were in service, 0 or more. ``serves`` lists the
    execution classes whose rows the budget claims, in ``Execution`` order.
    It always holds ``budget`` itself.
    """

    budget: Execution
    capacity: int
    serves: tuple[Execution, ...]


@dataclass(slots=True, frozen=True)
class ClassBudgets:
    """The claim budgets of a worker, in ``Execution`` order.

    No class is in the ``serves`` of two budgets. A class that no budget
    serves is not claimed by the worker. Empty when the worker serves no
    class.
    """

    budgets: tuple[ClassBudget, ...]


ClassCapacity: TypeAlias = ClassBudgets


@dataclass(slots=True, frozen=True)
class ProfileNamed:
    """The worker ran the profile ``name`` of ``AppConfig.worker_profiles``.

    ``name`` has 1 to 64 characters: letters, digits, ``_``, ``.`` and
    ``-``, and the first character is not ``.`` or ``-``.
    """

    name: str


@dataclass(slots=True, frozen=True)
class ProfileNotRecorded:
    """The snapshot holds no profile name: the worker that wrote it did not
    record one. A worker of a build before schema migration 1004 wrote it;
    every worker of this build records its profile name."""


SnapshotProfile: TypeAlias = ProfileNamed | ProfileNotRecorded


@dataclass(slots=True, frozen=True)
class WorkerStateSnapshot:
    """One ``horsies_worker_states`` row — a worker's state at a point in time.

    Mirrors ``WorkerStateModel``. Returned as the latest snapshot per worker
    (cluster-wide listing) or as a timeseries (single-worker history).
    ``queue_priorities``, ``queue_max_concurrency`` and ``recovery_config``
    are empty when the worker had none set.
    ``slot_health`` says whether any slot was down, and why.
    ``class_capacity`` holds the claim budget of each execution class that
    the worker runs children for: its capacity in service and the classes
    whose rows it claims.
    ``cluster_wide_cap`` is the cap that the worker ran with:
    ``ClusterCap(tasks)`` or ``NoClusterCap()``.
    ``profile`` names the worker profile that the worker ran:
    ``ProfileNamed(name)`` or ``ProfileNotRecorded()``.
    """

    worker_id: str
    snapshot_at: datetime
    hostname: str
    pid: int
    processes: int
    max_claim_batch: int
    max_claim_per_worker: int
    cluster_wide_cap: ClusterCap | NoClusterCap
    queues: list[str]
    queue_priorities: dict[str, int]
    queue_max_concurrency: dict[str, int]
    recovery_config: dict[str, Any]
    tasks_running: int
    tasks_claimed: int
    memory_usage_mb: float | None
    memory_percent: float | None
    cpu_percent: float | None
    children_memory_mb: float | None
    worker_started_at: datetime
    slot_health: SlotHealth
    class_capacity: ClassCapacity
    profile: SnapshotProfile
