"""The wire models of the slot health and the execution class report."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from horsies.core.models.boot_failure import ResourceShortage, ShortageErrno
from horsies.core.models.class_coverage import (
    BudgetLoad,
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
    DownSlot,
    SlotHealthNotRecorded,
    SlotsDown,
)
from horsies.core.models.tasks import Execution
from horsies.monitoring.execution_classes import class_report_info, slot_health_info

AT = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)


@pytest.mark.unit
class TestSlotHealthInfo:
    def test_all_serving_has_no_down_slot(self) -> None:
        assert slot_health_info(AllSlotsServing()) == ('all_serving', [])

    def test_not_recorded_has_no_down_slot(self) -> None:
        assert slot_health_info(SlotHealthNotRecorded()) == ('not_recorded', [])

    def test_down_slots_carry_the_described_cause(self) -> None:
        down = DownSlot(
            slot=1,
            cause=ResourceShortage(ShortageErrno.EMFILE),
            detail='OSError: [Errno 24] Too many open files',
            down_since=AT,
            failed_boots=3,
        )
        kind, slots = slot_health_info(SlotsDown((down,)))

        assert kind == 'slots_down'
        assert [slot.model_dump() for slot in slots] == [
            {
                'slot': 1,
                'cause': 'the OS ran short of a resource (EMFILE)',
                'detail': 'OSError: [Errno 24] Too many open files',
                'down_since': AT,
                'failed_boots': 3,
            },
        ]


@pytest.mark.unit
class TestClassReportInfo:
    def test_each_coverage_and_wait_state_has_its_kind(self) -> None:
        report = ClassReport(
            checked_at=AT,
            streams=(
                PendingStream(
                    'a',
                    Execution.ISOLATED,
                    OldestWaiting(
                        AT - timedelta(seconds=90.6), timedelta(seconds=90.6)
                    ),
                    StreamServed(workers=2, capacity=8),
                ),
                PendingStream(
                    'a',
                    Execution.SHARED_ASYNC,
                    NoneDue(AT + timedelta(minutes=1)),
                    StreamWithoutCapacity(workers=1),
                ),
                PendingStream(
                    'b',
                    Execution.ISOLATED,
                    OldestWaiting(AT, timedelta(0)),
                    StreamUnserved(),
                ),
            ),
            workers=(),
        )
        body = class_report_info(report).model_dump(mode='json')

        assert body['live_window_s'] == WORKER_STATE_LIVE_WINDOW_S
        assert [s['coverage'] for s in body['streams']] == [
            {'kind': 'served', 'workers': 2, 'capacity': 8},
            {'kind': 'no_capacity', 'workers': 1},
            {'kind': 'unserved'},
        ]
        assert body['streams'][0]['wait'] == {
            'kind': 'waiting',
            'oldest_due_at': '2026-11-10T11:58:29.400000Z',
            'wait_s': 90,
        }
        assert body['streams'][1]['wait'] == {
            'kind': 'none_due',
            'next_due_at': '2026-11-10T12:01:00Z',
        }
        assert body['streams'][1]['execution'] == 'SHARED_ASYNC'

    def test_budgets_carry_their_free_capacity(self) -> None:
        report = ClassReport(
            checked_at=AT,
            streams=(),
            workers=(
                WorkerClassLoad(
                    'w1',
                    AT,
                    (BudgetLoad(Execution.ISOLATED, 4, tuple(Execution), in_flight=6),),
                ),
            ),
        )
        body = class_report_info(report).model_dump(mode='json')

        assert body['workers'] == [
            {
                'worker_id': 'w1',
                'snapshot_at': '2026-11-10T12:00:00Z',
                'budgets': [
                    {
                        'budget': 'ISOLATED',
                        'capacity': 4,
                        'serves': ['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD'],
                        'in_flight': 6,
                        'free': 0,
                    },
                ],
            },
        ]
