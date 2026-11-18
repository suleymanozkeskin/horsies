"""The record of a pair's Python case, the judgement of its core case, and
the notification section of the snapshot. No database."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Final

import pytest

from horsies.core.types.result import Err, Ok
from tests.dualrun.catalog import (
    NOTIFICATIONS,
    TASK_ID,
    NotificationPayload,
    notification_payload,
    notification_row,
)
from tests.dualrun.cells import RawRow, RawSnapshot, RawTable
from tests.dualrun.compare import ExtraRow, compare_snapshots
from tests.dualrun.normalize import NormalizedSnapshot, normalize_snapshot
from tests.dualrun.pair_outcomes import (
    ClockBoundaryStraddle,
    CompareSnapshots,
    DualRunMismatch,
    NoCompare,
    NoReference,
    PairEqual,
    ReferenceCaptured,
    ReferenceFailed,
    ReferenceNotInformative,
    ReferenceRunMissing,
    ReferenceSkipped,
    core_step,
    crosses_hour,
    judge_pair,
)
from tests.dualrun.rules import ColumnSpec, Exact, SnapshotCatalog, TableRules
from tests.dualrun.values import RunWindow

pytestmark = [pytest.mark.unit]

TASKS: Final = TableRules(
    name='tasks',
    columns=(
        ColumnSpec(name='id', rule=TASK_ID),
        ColumnSpec(name='name', rule=Exact()),
    ),
    order_by=('name',),
    identity=('id',),
)

CATALOG: Final = SnapshotCatalog(tables=(TASKS, NOTIFICATIONS))

WITHIN_HOUR: Final = RunWindow(
    start=datetime(2026, 11, 17, 10, 5, tzinfo=UTC),
    end=datetime(2026, 11, 17, 10, 9, tzinfo=UTC),
)
ACROSS_HOUR: Final = RunWindow(
    start=datetime(2026, 11, 17, 10, 59, tzinfo=UTC),
    end=datetime(2026, 11, 17, 11, 0, 1, tzinfo=UTC),
)


def _run(
    tasks: list[tuple[str, str]],
    notifications: list[tuple[str, str]],
    window: RunWindow = WITHIN_HOUR,
) -> NormalizedSnapshot:
    task_rows: tuple[RawRow, ...] = tuple((raw_id, name) for raw_id, name in tasks)
    raw = RawSnapshot(
        tables=(
            RawTable(name='tasks', rows=task_rows),
            RawTable(
                name=NOTIFICATIONS.name,
                rows=tuple(
                    notification_row(channel, payload)
                    for channel, payload in notifications
                ),
            ),
        ),
    )
    match normalize_snapshot(raw, CATALOG, window):
        case Ok(snapshot):
            return snapshot
        case Err(error):
            raise AssertionError(f'normalize failed: {error}')


# ---- Notification rows ----


@pytest.mark.parametrize(
    ('channel', 'payload'),
    [
        pytest.param('task_new', NotificationPayload.TASK_ID, id='task-new'),
        pytest.param('task_done', NotificationPayload.TASK_ID, id='task-done'),
        pytest.param(
            'horsies_task_status', NotificationPayload.TASK_ID, id='task-status'
        ),
        pytest.param(
            'workflow_done', NotificationPayload.WORKFLOW_ID, id='workflow-done'
        ),
        pytest.param(
            'horsies_workflow_status',
            NotificationPayload.WORKFLOW_ID,
            id='workflow-status',
        ),
        pytest.param('task_queue_default', NotificationPayload.TEXT, id='queue'),
    ],
)
def test_each_channel_has_its_payload_kind(
    channel: str, payload: NotificationPayload
) -> None:
    assert notification_payload(channel) is payload


def test_a_payload_goes_into_the_column_of_its_kind() -> None:
    assert notification_row('task_done', 't1') == ('task_done', 't1', None, None)
    assert notification_row('workflow_done', 'w1') == (
        'workflow_done',
        None,
        'w1',
        None,
    )
    assert notification_row('task_queue_q', 'new') == (
        'task_queue_q',
        None,
        None,
        'new',
    )


def test_a_channel_without_a_payload_rule_is_a_defect() -> None:
    with pytest.raises(AssertionError, match='horsies_worker_state'):
        notification_payload('horsies_worker_state')


def test_notifications_of_two_runs_with_other_ids_compare_equal() -> None:
    reference = _run(
        [('a1', 'first'), ('a2', 'second')],
        [
            ('task_new', 'a1'),
            ('task_queue_default', 'new'),
            ('task_done', 'a2'),
            ('task_new', 'a2'),
            ('task_queue_default', 'new'),
        ],
    )
    candidate = _run(
        [('b2', 'second'), ('b1', 'first')],
        [
            ('task_new', 'b2'),
            ('task_new', 'b1'),
            ('task_queue_default', 'new'),
            ('task_queue_default', 'new'),
            ('task_done', 'b2'),
        ],
    )

    assert compare_snapshots(reference, candidate).entries == ()


def test_a_notification_more_or_for_another_task_is_a_difference() -> None:
    reference = _run(
        [('a1', 'first'), ('a2', 'second')],
        [('task_done', 'a1'), ('task_queue_default', 'capacity')],
    )
    candidate = _run(
        [('b1', 'first'), ('b2', 'second')],
        [
            ('task_done', 'b2'),
            ('task_queue_default', 'capacity'),
            ('task_queue_default', 'capacity'),
        ],
    )

    entries = compare_snapshots(reference, candidate).entries
    assert [type(entry).__name__ for entry in entries] == [
        'MissingRow',
        'ExtraRow',
        'ExtraRow',
    ]
    assert all(
        entry.table == NOTIFICATIONS.name
        for entry in entries
        if isinstance(entry, ExtraRow)
    )


# ---- Pair outcomes ----


def test_the_core_case_compares_only_with_a_captured_reference() -> None:
    captured = ReferenceCaptured(snapshot=_run([], []), window=WITHIN_HOUR)

    assert core_step(captured) == CompareSnapshots(reference=captured)
    assert core_step(ReferenceNotInformative()) == NoCompare()
    assert core_step(ReferenceSkipped()) == NoCompare()
    assert core_step(ReferenceFailed()) == ReferenceRunMissing(found=ReferenceFailed())
    assert core_step(NoReference()) == ReferenceRunMissing(found=NoReference())


def test_a_window_crosses_an_hour_when_its_end_is_in_a_later_utc_hour() -> None:
    india = timezone(timedelta(hours=5, minutes=30))
    local_hour_only = RunWindow(
        start=datetime(2026, 11, 17, 12, 50, tzinfo=india),
        end=datetime(2026, 11, 17, 13, 5, tzinfo=india),
    )

    assert not crosses_hour(WITHIN_HOUR)
    assert crosses_hour(ACROSS_HOUR)
    assert not crosses_hour(local_hour_only), '07:20 to 07:35 UTC'


def test_judge_pair_names_equal_mismatch_and_straddle() -> None:
    reference = ReferenceCaptured(
        snapshot=_run([('a1', 'first')], [('task_done', 'a1')]),
        window=WITHIN_HOUR,
    )
    same = _run([('b1', 'first')], [('task_done', 'b1')])
    other = _run([('b1', 'first')], [])

    assert judge_pair(reference, same, WITHIN_HOUR) == PairEqual()
    mismatch = judge_pair(reference, other, WITHIN_HOUR)
    assert isinstance(mismatch, DualRunMismatch)
    assert len(mismatch.diff.entries) == 1
    straddle = judge_pair(reference, other, ACROSS_HOUR)
    assert isinstance(straddle, ClockBoundaryStraddle)
    assert straddle.candidate_window == ACROSS_HOUR
    assert judge_pair(reference, same, ACROSS_HOUR) == PairEqual()
