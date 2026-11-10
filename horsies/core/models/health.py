"""Typed models for the worker/database health API.

Two families live here:

* **Wire payloads** (`WorkerPingRequest`, `WorkerPongPayload`) — pydantic
  models that travel as JSON over the LISTEN/NOTIFY ping-pong channels.
  They forbid extras and are validated on both encode and decode, matching
  the library's "signature is the wire contract" stance.

* **Result models** (`DatabasePing`, `WorkerPong`, `WorkerStateSnapshot`) —
  frozen dataclasses returned to callers inside ``BrokerResult``. They carry
  no behaviour, only observed data.

The worker-state snapshot mirrors ``WorkerStateModel`` (the timeseries row
written every few seconds); reads expose it without coercion.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, TypeAlias, assert_never, cast

from pydantic import BaseModel, ConfigDict

from horsies.core.defaults import (
    WORKER_STATE_LIVE_WINDOW_S as WORKER_STATE_LIVE_WINDOW_S,
)
from horsies.core.models.boot_failure import (
    MAX_BOOT_FAILURE_DETAIL_CHARS,
    AppLoadFailed,
    ChildPoolSetupFailed,
    ClearableBootCause,
    ResourceShortage,
    SpawnFailed,
    TaskLoopCreateFailed,
    ThreadStartRefused,
    boot_cause_json,
    parse_boot_cause,
)
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Err, Ok, Result


# Shared inbound channel every worker subscribes to. A ping NOTIFY carries a
# ``WorkerPingRequest`` payload; each worker replies on the request's
# per-request ``reply_channel``.
WORKER_PING_CHANNEL = 'horsies_worker_ping'


class WorkerPingRequest(BaseModel):
    """Request payload broadcast on ``WORKER_PING_CHANNEL``.

    ``target_worker_id`` of ``None`` is a broadcast (every worker replies);
    otherwise only the worker whose instance id matches replies.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    correlation_id: str
    reply_channel: str
    target_worker_id: str | None = None


class WorkerPongPayload(BaseModel):
    """Reply payload a worker sends on the request's ``reply_channel``.

    Carries only identity. A pong proves the worker's event loop is
    responsive and that it can reach Postgres (the reply travels through the
    worker's own connection). Load/config data lives in the worker-state
    snapshot, not here, so the reply path stays a pure liveness signal.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    correlation_id: str
    worker_id: str
    hostname: str
    pid: int


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


@dataclass(slots=True, frozen=True)
class SlotHealthNotRecorded:
    """The snapshot comes from a worker version that did not record slot health."""


SlotHealth: TypeAlias = AllSlotsServing | SlotsDown | SlotHealthNotRecorded

# A stored count stays in the range of a Postgres bigint.
_COUNT_LIMIT: Final = 2**63


@dataclass(slots=True, frozen=True)
class SlotHealthUnreadable:
    """A stored ``slots_down`` value does not have the shape that the worker writes."""

    reason: str


def slots_down_json(slots: Sequence[DownSlot]) -> list[dict[str, object]]:
    """Encode the down slots for the ``slots_down`` column. An empty list means
    that no slot was down."""
    return [
        {
            'slot': down.slot,
            'cause': boot_cause_json(down.cause),
            'detail': down.detail,
            'down_since': down.down_since.isoformat(),
            'failed_boots': down.failed_boots,
        }
        for down in slots
    ]


def parse_slot_health(
    value: object,
    processes: int,
) -> Result[SlotHealth, SlotHealthUnreadable]:
    """Build the slot health from a stored ``slots_down`` value.

    ``None`` (SQL NULL) is ``SlotHealthNotRecorded``. An empty list is
    ``AllSlotsServing``. Each entry must name a distinct slot below
    ``processes``, a cause that can clear, a bounded detail, an aware
    ``down_since`` and ``failed_boots`` of at least 1. Reads only.
    """
    match value:
        case None:
            return Ok(SlotHealthNotRecorded())
        case list():
            entries = cast('list[object]', value)
        case _:
            return Err(SlotHealthUnreadable('slots_down is not a JSON array'))
    if not entries:
        return Ok(AllSlotsServing())
    if len(entries) > processes:
        return Err(SlotHealthUnreadable('slots_down lists more slots than processes'))
    slots: list[DownSlot] = []
    for entry in entries:
        match _parse_down_slot(entry, processes):
            case Ok(down):
                slots.append(down)
            case Err(unreadable):
                return Err(unreadable)
    if len({down.slot for down in slots}) != len(slots):
        return Err(SlotHealthUnreadable('slots_down names one slot twice'))
    return Ok(SlotsDown(tuple(sorted(slots, key=lambda down: down.slot))))


def _parse_down_slot(
    value: object,
    processes: int,
) -> Result[DownSlot, SlotHealthUnreadable]:
    if not isinstance(value, dict):
        return Err(SlotHealthUnreadable('a slots_down entry is not a JSON object'))
    fields = cast('Mapping[str, Any]', value)
    detail = fields.get('detail')
    if not isinstance(detail, str) or len(detail) > MAX_BOOT_FAILURE_DETAIL_CHARS:
        return Err(SlotHealthUnreadable('detail is not text within its bound'))
    match (
        _whole_in(fields.get('slot'), 'slot', range(processes)),
        _whole_in(fields.get('failed_boots'), 'failed_boots', range(1, _COUNT_LIMIT)),
        _aware_instant(fields.get('down_since')),
        _clearable_cause(fields.get('cause')),
    ):
        case (Ok(slot), Ok(failed_boots), Ok(since), Ok(cause)):
            return Ok(DownSlot(slot, cause, detail, since, failed_boots))
        case (Err(unreadable), _, _, _) | (_, Err(unreadable), _, _):
            return Err(unreadable)
        case (_, _, Err(unreadable), _) | (_, _, _, Err(unreadable)):
            return Err(unreadable)


def _whole_in(
    value: object,
    name: str,
    valid: range,
) -> Result[int, SlotHealthUnreadable]:
    """An int, not a bool, inside ``valid``."""
    match value:
        case bool():
            return Err(SlotHealthUnreadable(f'{name} {value!r} is not a whole number'))
        case int() if value in valid:
            return Ok(value)
        case _:
            return Err(SlotHealthUnreadable(f'{name} {value!r} is out of range'))


def _aware_instant(value: object) -> Result[datetime, SlotHealthUnreadable]:
    if not isinstance(value, str):
        return Err(SlotHealthUnreadable('down_since is not text'))
    try:
        instant = datetime.fromisoformat(value)
    except ValueError:
        return Err(SlotHealthUnreadable(f'down_since {value!r} is not an ISO instant'))
    if instant.tzinfo is None:
        return Err(SlotHealthUnreadable(f'down_since {value!r} has no offset'))
    return Ok(instant)


def _clearable_cause(value: object) -> Result[ClearableBootCause, SlotHealthUnreadable]:
    match parse_boot_cause(value):
        case Ok(cause):
            pass
        case Err(unreadable):
            return Err(SlotHealthUnreadable(f'cause: {unreadable.reason}'))
    match cause:
        case (
            ResourceShortage()
            | TaskLoopCreateFailed()
            | ThreadStartRefused()
            | SpawnFailed()
        ):
            return Ok(cause)
        case AppLoadFailed() | ChildPoolSetupFailed():
            return Err(SlotHealthUnreadable(f'cause {cause!r} cannot keep a slot down'))
        case _ as unreachable:
            assert_never(unreachable)


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


@dataclass(slots=True, frozen=True)
class ClassCapacityNotRecorded:
    """The snapshot comes from a worker version that did not record its budgets."""


ClassCapacity: TypeAlias = ClassBudgets | ClassCapacityNotRecorded


@dataclass(slots=True, frozen=True)
class ClassCapacityUnreadable:
    """A stored ``class_capacity`` value does not have the shape that the worker writes."""

    reason: str


def parse_class_capacity(
    value: object,
) -> Result[ClassCapacity, ClassCapacityUnreadable]:
    """Build the class capacity from a stored ``class_capacity`` value.

    ``None`` (SQL NULL) is ``ClassCapacityNotRecorded``. Each key must be an
    ``Execution`` value, and each entry an object with a ``capacity`` (a
    whole number, 0 or more) and a ``serves`` list of distinct ``Execution``
    values that holds the key. No class may be in the ``serves`` of two
    budgets. Other keys of an entry are not read. Reads only.
    """
    match value:
        case None:
            return Ok(ClassCapacityNotRecorded())
        case dict():
            entries = cast('dict[object, object]', value)
        case _:
            return Err(ClassCapacityUnreadable('class_capacity is not a JSON object'))
    budgets: list[ClassBudget] = []
    for name, entry in entries.items():
        match _parse_class_budget(name, entry):
            case Ok(budget):
                budgets.append(budget)
            case Err(unreadable):
                return Err(unreadable)
    served = [member for budget in budgets for member in budget.serves]
    if len(set(served)) != len(served):
        return Err(ClassCapacityUnreadable('a class is in the serves of two budgets'))
    order = list(Execution)
    return Ok(
        ClassBudgets(
            tuple(sorted(budgets, key=lambda entry: order.index(entry.budget))),
        ),
    )


def _parse_class_budget(
    name: object,
    value: object,
) -> Result[ClassBudget, ClassCapacityUnreadable]:
    match _execution_named(name, 'budget'):
        case Ok(budget):
            pass
        case Err(unreadable):
            return Err(unreadable)
    if not isinstance(value, dict):
        return Err(
            ClassCapacityUnreadable(f'budget {budget.value} is not a JSON object'),
        )
    fields = cast('Mapping[str, object]', value)
    match fields.get('capacity'):
        case bool() as capacity:
            return Err(
                ClassCapacityUnreadable(f'capacity {capacity!r} is not a whole number'),
            )
        case int() as capacity if capacity in range(_COUNT_LIMIT):
            pass
        case capacity:
            return Err(
                ClassCapacityUnreadable(f'capacity {capacity!r} is out of range'),
            )
    match _served_classes(fields.get('serves')):
        case Ok(serves):
            pass
        case Err(unreadable):
            return Err(unreadable)
    if budget not in serves:
        return Err(
            ClassCapacityUnreadable(
                f'budget {budget.value} does not serve its own class',
            ),
        )
    return Ok(ClassBudget(budget=budget, capacity=capacity, serves=serves))


def _served_classes(
    value: object,
) -> Result[tuple[Execution, ...], ClassCapacityUnreadable]:
    """The distinct ``Execution`` values of a ``serves`` list, in ``Execution`` order."""
    if not isinstance(value, list):
        return Err(ClassCapacityUnreadable('serves is not a JSON array'))
    members: list[Execution] = []
    for entry in cast('list[object]', value):
        match _execution_named(entry, 'serves entry'):
            case Ok(member):
                members.append(member)
            case Err(unreadable):
                return Err(unreadable)
    if len(set(members)) != len(members):
        return Err(ClassCapacityUnreadable('serves names one class twice'))
    return Ok(tuple(member for member in Execution if member in members))


def _execution_named(
    value: object,
    name: str,
) -> Result[Execution, ClassCapacityUnreadable]:
    for member in Execution:
        if value == member.value:
            return Ok(member)
    return Err(ClassCapacityUnreadable(f'{name} {value!r} is not an execution class'))


@dataclass(slots=True, frozen=True)
class WorkerStateSnapshot:
    """One ``horsies_worker_states`` row — a worker's state at a point in time.

    Mirrors ``WorkerStateModel``. Returned as the latest snapshot per worker
    (cluster-wide listing) or as a timeseries (single-worker history).
    ``slot_health`` says whether any slot was down, and why.
    ``class_capacity`` holds the claim budget of each execution class that
    the worker runs children for: its capacity in service and the classes
    whose rows it claims.
    """

    worker_id: str
    snapshot_at: datetime
    hostname: str
    pid: int
    processes: int
    max_claim_batch: int
    max_claim_per_worker: int
    cluster_wide_cap: int | None
    queues: list[str]
    queue_priorities: dict[str, int] | None
    queue_max_concurrency: dict[str, int] | None
    recovery_config: dict[str, Any] | None
    tasks_running: int
    tasks_claimed: int
    memory_usage_mb: float | None
    memory_percent: float | None
    cpu_percent: float | None
    children_memory_mb: float | None
    worker_started_at: datetime
    slot_health: SlotHealth
    class_capacity: ClassCapacity
