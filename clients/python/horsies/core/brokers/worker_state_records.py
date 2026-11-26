"""The worker state and class records of the core, as the public dataclasses.

The core answers the worker state reads and the class reads of
``PostgresBroker`` with one record class per type and per union member
(``horsies._core``). The functions here build the frozen dataclasses of
``horsies.core.models.health`` and ``horsies.core.models.class_coverage``
from them, field by field.

This module is pure. It does no I/O. A record that breaks the core's
contract (recovery settings that are not a JSON object) is a defect and
raises ``AssertionError``.
"""

from __future__ import annotations

from typing import Any, Literal, assert_never

from horsies._core import (
    AllSlotsServingRecord,
    BudgetLoadRecord,
    ClassBudgetRecord,
    ClassCoverageRecord,
    ClassReportRecord,
    DownSlotRecord,
    NoneDueRecord,
    OldestWaitingRecord,
    PendingStreamRecord,
    ProfileNamedRecord,
    ProfileNotRecordedRecord,
    ResourceShortageRecord,
    SlotsDownRecord,
    StreamCoverageRecord,
    StreamServedRecord,
    StreamUnservedRecord,
    StreamWithoutCapacityRecord,
    WorkerClassLoadRecord,
    WorkerStateRecord,
)
from horsies.core.codec.json_io import loads_json
from horsies.core.models.boot_failure import (
    ClearableBootCause,
    ResourceShortage,
    ShortageErrno,
    ThreadStartRefused,
)
from horsies.core.models.class_coverage import (
    BudgetLoad,
    ClassCoverage,
    ClassReport,
    NoneDue,
    OldestWaiting,
    PendingStream,
    StreamCoverage,
    StreamCoverageState,
    StreamServed,
    StreamUnserved,
    StreamWait,
    StreamWithoutCapacity,
    WorkerClassLoad,
)
from horsies.core.models.cluster_cap import ClusterCap, NoClusterCap
from horsies.core.models.health import (
    AllSlotsServing,
    ClassBudget,
    ClassBudgets,
    DownSlot,
    ProfileNamed,
    ProfileNotRecorded,
    SlotHealth,
    SlotsDown,
    SnapshotProfile,
    WorkerStateSnapshot,
)
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Err, Ok


def _boot_cause(
    cause: ResourceShortageRecord | Literal['THREAD_START_REFUSED'],
) -> ClearableBootCause:
    match cause:
        case ResourceShortageRecord(errno=errno):
            return ResourceShortage(errno_name=ShortageErrno(errno))
        case 'THREAD_START_REFUSED':
            return ThreadStartRefused()
        case _ as unreachable:
            assert_never(unreachable)


def _down_slot(record: DownSlotRecord) -> DownSlot:
    return DownSlot(
        slot=record.slot,
        cause=_boot_cause(record.cause),
        detail=record.detail,
        down_since=record.down_since,
        failed_boots=record.failed_boots,
    )


def _slot_health(record: AllSlotsServingRecord | SlotsDownRecord) -> SlotHealth:
    match record:
        case AllSlotsServingRecord():
            return AllSlotsServing()
        case SlotsDownRecord(slots=slots):
            return SlotsDown(tuple(_down_slot(slot) for slot in slots))
        case _ as unreachable:
            assert_never(unreachable)


def _snapshot_profile(
    record: ProfileNamedRecord | ProfileNotRecordedRecord,
) -> SnapshotProfile:
    match record:
        case ProfileNamedRecord(name=name):
            return ProfileNamed(name=name)
        case ProfileNotRecordedRecord():
            return ProfileNotRecorded()
        case _ as unreachable:
            assert_never(unreachable)


def _class_budget(record: ClassBudgetRecord) -> ClassBudget:
    return ClassBudget(
        budget=Execution(record.budget),
        capacity=record.capacity,
        serves=tuple(Execution(served) for served in record.serves),
    )


def _recovery_config(text: str) -> dict[str, Any]:
    match loads_json(text):
        case Ok(dict() as value):
            return value
        case Ok(other):
            raise AssertionError(f'the core gave recovery settings that are not an object: {other!r}')
        case Err(refused):
            raise AssertionError(f'the core gave recovery settings that do not decode: {refused}')


def _cluster_cap(tasks: int | None) -> ClusterCap | NoClusterCap:
    """The cap of a record: the core gives the stored cap as an int, or
    None for no cap."""
    match tasks:
        case None:
            return NoClusterCap()
        case int():
            return ClusterCap(tasks=tasks)


def worker_state_snapshot(record: WorkerStateRecord) -> WorkerStateSnapshot:
    """The snapshot of one worker state record."""
    return WorkerStateSnapshot(
        worker_id=record.worker_id,
        snapshot_at=record.snapshot_at,
        hostname=record.hostname,
        pid=record.pid,
        processes=record.processes,
        max_claim_batch=record.max_claim_batch,
        max_claim_per_worker=record.max_claim_per_worker,
        cluster_wide_cap=_cluster_cap(record.cluster_wide_cap),
        queues=list(record.queues),
        queue_priorities=dict(record.queue_priorities),
        queue_max_concurrency=dict(record.queue_max_concurrency),
        recovery_config=_recovery_config(record.recovery_config_json),
        tasks_running=record.tasks_running,
        tasks_claimed=record.tasks_claimed,
        memory_usage_mb=record.memory_usage_mb,
        memory_percent=record.memory_percent,
        cpu_percent=record.cpu_percent,
        children_memory_mb=record.children_memory_mb,
        worker_started_at=record.worker_started_at,
        slot_health=_slot_health(record.slot_health),
        class_capacity=ClassBudgets(
            tuple(_class_budget(budget) for budget in record.class_capacity),
        ),
        profile=_snapshot_profile(record.profile),
    )


def _coverage_state(
    record: StreamServedRecord | StreamWithoutCapacityRecord | StreamUnservedRecord,
) -> StreamCoverageState:
    match record:
        case StreamServedRecord(workers=workers, capacity=capacity):
            return StreamServed(workers=workers, capacity=capacity)
        case StreamWithoutCapacityRecord(workers=workers):
            return StreamWithoutCapacity(workers=workers)
        case StreamUnservedRecord():
            return StreamUnserved()
        case _ as unreachable:
            assert_never(unreachable)


def _stream_coverage(record: StreamCoverageRecord) -> StreamCoverage:
    return StreamCoverage(
        queue_name=record.queue_name,
        execution=Execution(record.execution),
        state=_coverage_state(record.state),
    )


def class_coverage_of(record: ClassCoverageRecord) -> ClassCoverage:
    """The fleet check of its record."""
    return ClassCoverage(
        checked_at=record.checked_at,
        live_workers=record.live_workers,
        streams=tuple(_stream_coverage(stream) for stream in record.streams),
    )


def _stream_wait(record: OldestWaitingRecord | NoneDueRecord) -> StreamWait:
    match record:
        case OldestWaitingRecord(enqueued_at=enqueued_at, wait=wait):
            return OldestWaiting(enqueued_at=enqueued_at, wait=wait)
        case NoneDueRecord(next_due_at=next_due_at):
            return NoneDue(next_due_at=next_due_at)
        case _ as unreachable:
            assert_never(unreachable)


def _pending_stream(record: PendingStreamRecord) -> PendingStream:
    return PendingStream(
        queue_name=record.queue_name,
        execution=Execution(record.execution),
        wait=_stream_wait(record.wait),
        state=_coverage_state(record.state),
    )


def _budget_load(record: BudgetLoadRecord) -> BudgetLoad:
    return BudgetLoad(
        budget=Execution(record.budget),
        capacity=record.capacity,
        serves=tuple(Execution(served) for served in record.serves),
        in_flight=record.in_flight,
    )


def _worker_class_load(record: WorkerClassLoadRecord) -> WorkerClassLoad:
    return WorkerClassLoad(
        worker_id=record.worker_id,
        snapshot_at=record.snapshot_at,
        budgets=tuple(_budget_load(budget) for budget in record.budgets),
    )


def class_report_of(record: ClassReportRecord) -> ClassReport:
    """The class report of its record."""
    return ClassReport(
        checked_at=record.checked_at,
        streams=tuple(_pending_stream(stream) for stream in record.streams),
        workers=tuple(_worker_class_load(worker) for worker in record.workers),
    )
