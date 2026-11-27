"""Typed models for the worker/database health API.

The result models (`DatabasePing`, `WorkerPong`, `WorkerStateSnapshot` with
its slot health, execution class budgets and profile) are frozen dataclasses
returned to callers inside ``DatabaseResult``. They carry no behaviour, only
observed data.
The core encodes and decodes the ping-pong payloads.

The worker-state snapshot mirrors ``WorkerStateModel`` (the timeseries row
written every few seconds); reads expose it without coercion.

A ``WorkerPong``, a ``DownSlot``, a ``SlotHealth`` and a ``SnapshotProfile``
are also the JSON of the monitoring routes: a union member carries its
``kind`` tag, and the cause of a down slot is its text.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import Field, PlainSerializer

from horsies.core.defaults import (
    WORKER_STATE_LIVE_WINDOW_SECONDS as WORKER_STATE_LIVE_WINDOW_SECONDS,
)
from horsies.core.models.boot_failure import (
    ClearableBootCause,
    describe_boot_cause,
)
from horsies.core.models.cluster_cap import ClusterCap, NoClusterCap
from horsies.core.models.tasks import Execution
from horsies.core.models.worker_profile import ClaimLimits


@dataclass(slots=True, frozen=True)
class DatabasePing:
    """Result of a ``SELECT 1`` round-trip through the live broker pool."""

    latency_ms: float
    kind: Literal['ping'] = 'ping'


@dataclass(slots=True, frozen=True)
class DatabaseUnreachable:
    """The ``SELECT 1`` round-trip failed: the database was not reachable."""

    kind: Literal['unreachable'] = 'unreachable'


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
    down, at least 1. In JSON, ``cause`` is its text.
    """

    slot: int
    cause: Annotated[
        ClearableBootCause,
        PlainSerializer(describe_boot_cause, return_type=str, when_used='json'),
    ]
    detail: str
    down_since: datetime
    failed_boots: int


@dataclass(slots=True, frozen=True)
class SlotHealthAllServing:
    """No slot of the worker was down when it wrote the snapshot."""

    kind: Literal['all_serving'] = 'all_serving'


@dataclass(slots=True, frozen=True)
class SlotHealthSlotsDown:
    """The worker is degraded: these slots were down, in slot order. Never empty."""

    slots: tuple[DownSlot, ...]
    kind: Literal['slots_down'] = 'slots_down'


SlotHealth: TypeAlias = Annotated[
    SlotHealthAllServing | SlotHealthSlotsDown,
    Field(discriminator='kind'),
]
"""Whether any slot of the worker was down when it wrote the snapshot."""


@dataclass(slots=True, frozen=True)
class ExecutionClassBudget:
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
class ExecutionClassBudgets:
    """The claim budgets of a worker, in ``Execution`` order.

    No class is in the ``serves`` of two budgets. A class that no budget
    serves is not claimed by the worker. Empty when the worker serves no
    class.
    """

    budgets: tuple[ExecutionClassBudget, ...]


@dataclass(slots=True, frozen=True)
class SnapshotProfileNamed:
    """The worker ran the profile ``name`` of ``AppConfig.worker_profiles``.

    ``name`` has 1 to 64 characters: letters, digits, ``_``, ``.`` and
    ``-``, and the first character is not ``.`` or ``-``.
    """

    name: str
    kind: Literal['named'] = 'named'


@dataclass(slots=True, frozen=True)
class SnapshotProfileNotRecorded:
    """The snapshot holds no profile name: the worker that wrote it did not
    record one. A worker of a build before schema migration 1004 wrote it;
    every worker of this build records its profile name."""

    kind: Literal['not_recorded'] = 'not_recorded'


SnapshotProfile: TypeAlias = Annotated[
    SnapshotProfileNamed | SnapshotProfileNotRecorded,
    Field(discriminator='kind'),
]
"""The worker profile that a snapshot names."""


@dataclass(slots=True, frozen=True)
class WorkerStateSnapshot:
    """One ``horsies_worker_states`` row — a worker's state at a point in time.

    Mirrors ``WorkerStateModel``. Returned as the latest snapshot per worker
    (cluster-wide listing) or as a timeseries (single-worker history).
    ``queue_priorities``, ``queue_max_concurrency`` and ``recovery_config``
    are empty when the worker had none set.
    ``child_count`` counts the children of the worker.
    ``claim_limits`` holds the claim limits of the worker:
    ``claimed_per_worker`` is the limit that the worker resolved, the seat
    count of the worker when its profile sets none.
    ``slot_health`` says whether any slot was down, and why.
    ``execution_class_budgets`` holds the claim budget of each execution
    class that the worker runs children for: its capacity in service and
    the classes whose rows it claims.
    ``cluster_wide_cap`` is the cap that the worker ran with:
    ``ClusterCap(tasks)`` or ``NoClusterCap()``.
    ``profile`` names the worker profile that the worker ran:
    ``SnapshotProfileNamed(name)`` or ``SnapshotProfileNotRecorded()``.
    """

    worker_id: str
    snapshot_at: datetime
    hostname: str
    pid: int
    child_count: int
    claim_limits: ClaimLimits
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
    execution_class_budgets: ExecutionClassBudgets
    profile: SnapshotProfile
