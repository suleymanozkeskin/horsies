"""Unit tests for the worker/database health wire payloads and result models."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import TypeAdapter

from horsies.core.models.boot_failure import ResourceShortage, ShortageErrno
from horsies.core.models.class_coverage import (
    ExecutionClassReport,
    NoneDue,
    OldestWaiting,
    PendingStream,
    StreamServed,
    StreamWithoutCapacity,
)
from horsies.core.models.cluster_cap import NoClusterCap
from horsies.core.models.health import (
    AllSlotsServing,
    DatabasePing,
    DownSlot,
    ExecutionClassBudgets,
    ProfileNamed,
    SlotHealth,
    SlotsDown,
    WorkerPong,
    WorkerStateSnapshot,
)
from horsies.core.models.tasks import Execution
from horsies.core.models.worker_profile import ClaimLimits, NoTaskLimit, TaskLimit

_AT = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)


@pytest.mark.unit
class TestResultModels:
    """Frozen result dataclasses returned to callers."""


    def test_database_ping_is_frozen(self) -> None:
        ping = DatabasePing(latency_ms=1.5)
        with pytest.raises(AttributeError):
            ping.latency_ms = 2.0  # type: ignore[misc]

    def test_worker_pong_is_frozen(self) -> None:
        pong = WorkerPong(worker_id='w', hostname='h', pid=1, round_trip_ms=3.0)
        with pytest.raises(AttributeError):
            pong.worker_id = 'other'  # type: ignore[misc]

    def test_worker_state_snapshot_holds_optional_fields(self) -> None:
        """Snapshot tolerates NULL psutil columns as None; unset maps are empty."""
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        snap = WorkerStateSnapshot(
            worker_id='w',
            snapshot_at=now,
            hostname='h',
            pid=1,
            child_count=4,
            claim_limits=ClaimLimits(
                per_queue_per_pass=TaskLimit(tasks=10),
                claimed_per_worker=NoTaskLimit(),
            ),
            cluster_wide_cap=NoClusterCap(),
            queues=['default'],
            queue_priorities={},
            queue_max_concurrency={},
            recovery_config={},
            tasks_running=0,
            tasks_claimed=0,
            memory_usage_mb=None,
            memory_percent=None,
            cpu_percent=None,
            children_memory_mb=None,
            worker_started_at=now,
            slot_health=AllSlotsServing(),
            execution_class_budgets=ExecutionClassBudgets(()),
            profile=ProfileNamed(name='default'),
        )
        assert snap.cluster_wide_cap == NoClusterCap()
        assert snap.queues == ['default']
        assert snap.tasks_running == 0


@pytest.mark.unit
class TestJsonForm:
    """The health records are the JSON of the monitoring routes."""

    def test_down_slots_are_tagged_and_the_cause_is_text(self) -> None:
        health = SlotsDown(
            (
                DownSlot(
                    slot=1,
                    cause=ResourceShortage(errno_name=ShortageErrno.EMFILE),
                    detail='OSError: [Errno 24] Too many open files',
                    down_since=_AT,
                    failed_boots=2,
                ),
            ),
        )

        dumped = TypeAdapter(SlotHealth).dump_python(health, mode='json')

        assert dumped == {
            'kind': 'slots_down',
            'slots': [
                {
                    'slot': 1,
                    'cause': 'the OS ran short of a resource (EMFILE)',
                    'detail': 'OSError: [Errno 24] Too many open files',
                    'down_since': '2026-11-10T12:00:00Z',
                    'failed_boots': 2,
                },
            ],
        }
        assert TypeAdapter(SlotHealth).dump_python(AllSlotsServing(), mode='json') == {
            'kind': 'all_serving',
        }

    def test_a_report_writes_its_durations_as_seconds(self) -> None:
        report = ExecutionClassReport(
            checked_at=_AT,
            live_window=timedelta(seconds=120),
            streams=(
                PendingStream(
                    'a',
                    Execution.ISOLATED,
                    StreamServed(workers=2, capacity=6),
                    OldestWaiting(oldest_due_at=_AT, wait=timedelta(milliseconds=90_500)),
                ),
                PendingStream(
                    'b',
                    Execution.SHARED_ASYNC,
                    StreamWithoutCapacity(workers=1),
                    NoneDue(next_due_at=_AT),
                ),
            ),
            workers=(),
        )

        dumped = TypeAdapter(ExecutionClassReport).dump_python(
            report, mode='json', by_alias=True,
        )

        assert dumped['live_window_seconds'] == 120.0
        assert dumped['streams'][0] == {
            'queue': 'a',
            'execution': 'ISOLATED',
            'coverage': {'kind': 'served', 'workers': 2, 'capacity': 6},
            'wait': {
                'kind': 'due',
                'oldest_due_at': '2026-11-10T12:00:00Z',
                'wait_seconds': 90.5,
            },
        }
        assert dumped['streams'][1]['coverage'] == {'kind': 'no_capacity', 'workers': 1}
        assert dumped['streams'][1]['wait'] == {
            'kind': 'none_due',
            'next_due_at': '2026-11-10T12:00:00Z',
        }
