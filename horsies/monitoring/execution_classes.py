"""Response models of the slot health and the execution class report.

The worker routes read the app's typed worker-health methods; these
functions turn their results into the wire models of the monitoring UI.
Pure, no I/O.
"""

from __future__ import annotations

from typing import Literal, assert_never

from horsies.core.models.boot_failure import describe_boot_cause
from horsies.core.models.class_coverage import (
    ClassReport,
    NoneDue,
    OldestWaiting,
    PendingStream,
    StreamServed,
    StreamUnserved,
    StreamWithoutCapacity,
    WorkerClassLoad,
)
from horsies.core.models.health import (
    WORKER_STATE_LIVE_WINDOW_S,
    AllSlotsServing,
    SlotHealth,
    SlotHealthNotRecorded,
    SlotsDown,
)
from horsies.monitoring.models import (
    BudgetLoadInfo,
    ClassReportInfo,
    DownSlotInfo,
    PendingStreamInfo,
    StreamNoCapacityInfo,
    StreamNoneDueInfo,
    StreamServedInfo,
    StreamUnservedInfo,
    StreamWaitingInfo,
    WorkerClassLoadInfo,
)

SlotHealthKind = Literal['all_serving', 'slots_down', 'not_recorded']


def slot_health_info(
    slot_health: SlotHealth,
) -> tuple[SlotHealthKind, list[DownSlotInfo]]:
    """The ``slot_health`` and ``down_slots`` fields of ``WorkerStateInfo``."""
    match slot_health:
        case AllSlotsServing():
            return 'all_serving', []
        case SlotsDown(slots=slots):
            return 'slots_down', [
                DownSlotInfo(
                    slot=down.slot,
                    cause=describe_boot_cause(down.cause),
                    detail=down.detail,
                    down_since=down.down_since,
                    failed_boots=down.failed_boots,
                )
                for down in slots
            ]
        case SlotHealthNotRecorded():
            return 'not_recorded', []
        case _ as unreachable:
            assert_never(unreachable)


def class_report_info(report: ClassReport) -> ClassReportInfo:
    """The wire model of a class report."""
    return ClassReportInfo(
        checked_at=report.checked_at,
        live_window_s=WORKER_STATE_LIVE_WINDOW_S,
        streams=[_stream_info(stream) for stream in report.streams],
        workers=[_worker_info(worker) for worker in report.workers],
    )


def _stream_info(stream: PendingStream) -> PendingStreamInfo:
    coverage: StreamServedInfo | StreamNoCapacityInfo | StreamUnservedInfo
    match stream.state:
        case StreamServed(workers=workers, capacity=capacity):
            coverage = StreamServedInfo(
                kind='served', workers=workers, capacity=capacity
            )
        case StreamWithoutCapacity(workers=workers):
            coverage = StreamNoCapacityInfo(kind='no_capacity', workers=workers)
        case StreamUnserved():
            coverage = StreamUnservedInfo(kind='unserved')
        case _ as unreachable:
            assert_never(unreachable)
    wait: StreamWaitingInfo | StreamNoneDueInfo
    match stream.wait:
        case OldestWaiting(enqueued_at=enqueued_at, wait=waited):
            wait = StreamWaitingInfo(
                kind='waiting',
                oldest_due_at=enqueued_at,
                wait_s=int(waited.total_seconds()),
            )
        case NoneDue(next_due_at=next_due_at):
            wait = StreamNoneDueInfo(kind='none_due', next_due_at=next_due_at)
        case _ as unreachable:
            assert_never(unreachable)
    return PendingStreamInfo(
        queue_name=stream.queue_name,
        execution=stream.execution,
        coverage=coverage,
        wait=wait,
    )


def _worker_info(worker: WorkerClassLoad) -> WorkerClassLoadInfo:
    return WorkerClassLoadInfo(
        worker_id=worker.worker_id,
        snapshot_at=worker.snapshot_at,
        budgets=[
            BudgetLoadInfo(
                budget=budget.budget,
                capacity=budget.capacity,
                serves=list(budget.serves),
                in_flight=budget.in_flight,
                free=budget.free,
            )
            for budget in worker.budgets
        ],
    )
