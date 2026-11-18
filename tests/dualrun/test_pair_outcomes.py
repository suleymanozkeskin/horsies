"""The record of a pair's Python case, the judgement of its core case, and
the notification section of the snapshot. No database."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Final

import psycopg
import pytest

from horsies.core.types.result import Err, Ok
from tests.dualrun.catalog import (
    NOTIFICATIONS,
    RETRY_WAKE_PREFIX,
    TASK_ID,
    NotificationPayload,
    notification_payload,
    notification_row,
)
from tests.dualrun.capture import (
    CaptureBlocked,
    CaptureFailure,
    CaptureStatementFailed,
    CatalogCoverageGap,
    ClockReadFailed,
    NotificationLimitExceeded,
    NotificationMarkerMissing,
    NotificationReadFailed,
    SchemaColumnLimitExceeded,
    TableRowLimitExceeded,
    render_capture_failure,
)
from tests.dualrun.cells import (
    RawRow,
    RawSnapshot,
    RawTable,
    UnanchoredReason,
    UnanchoredTime,
    UnreadableBytes,
    UnreadableJson,
    UnreadableTime,
    UnsupportedStoredFormat,
)
from tests.dualrun.compare import ExtraRow, compare_snapshots
from tests.dualrun.normalize import NormalizedSnapshot, normalize_snapshot
from tests.dualrun.operations.table import WholeSnapshot
from tests.dualrun.pair_outcomes import (
    ClockBoundaryStraddle,
    CompareSnapshots,
    DualRunMismatch,
    NoCompare,
    NoReference,
    PairEqual,
    ReferenceCaptured,
    ReferenceCaptureFailed,
    ReferenceFailed,
    ReferenceNotCaptured,
    ReferenceNotInformative,
    ReferenceRunMissing,
    ReferenceSkipped,
    core_step,
    crosses_hour,
    judge_pair,
)
from tests.dualrun.rules import ColumnSpec, Exact, SnapshotCatalog, TableRules
from tests.dualrun.values import RunWindow, RunWindowFault, RunWindowInvalid

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
NEXT_HOUR: Final = RunWindow(
    start=datetime(2026, 11, 17, 11, 0, 4, tzinfo=UTC),
    end=datetime(2026, 11, 17, 11, 1, 23, tzinfo=UTC),
)
"""A window in the hour after ``WITHIN_HOUR``; neither crosses an hour."""


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
        pytest.param('task_queue_default', NotificationPayload.QUEUE_WAKE, id='queue'),
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


RETRIED_A: Final = '01a1eced-c0c0-7000-8804-a4eff891f806'
RETRIED_B: Final = '01a1eced-cb0e-7000-a133-d366bdd10d31'
OTHER_B: Final = '01a1eced-cb0e-7000-a133-d366bdd10d32'
"""Task ids in their canonical text form, as a uuid column gives them."""


def test_a_retry_wake_puts_its_task_id_into_the_task_column() -> None:
    assert notification_row('task_queue_q', f'retry:{RETRIED_A}') == (
        'task_queue_q',
        RETRIED_A,
        None,
        RETRY_WAKE_PREFIX,
    )


@pytest.mark.parametrize(
    'payload',
    [
        pytest.param('retry:', id='no-id'),
        pytest.param('retry:not-a-task', id='not-an-id'),
        pytest.param(f'retry:{RETRIED_A.upper()}', id='upper-case-id'),
        pytest.param(f'retry: {RETRIED_A}', id='space-before-the-id'),
        pytest.param(f'retry:{RETRIED_A}x', id='text-after-the-id'),
        pytest.param(f'retried:{RETRIED_A}', id='other-prefix'),
    ],
)
def test_a_queue_wake_that_is_not_a_retry_of_a_task_id_stays_its_text(
    payload: str,
) -> None:
    assert notification_row('task_queue_q', payload) == (
        'task_queue_q',
        None,
        None,
        payload,
    )


def test_retry_wakes_of_two_runs_with_other_task_ids_compare_equal() -> None:
    reference = _run(
        [(RETRIED_A, 'retried'), ('a2', 'other')],
        [
            ('task_queue_default', f'retry:{RETRIED_A}'),
            ('task_queue_default', f'retry:{RETRIED_A}'),
            ('task_new', 'a2'),
        ],
    )
    candidate = _run(
        [('b2', 'other'), (RETRIED_B, 'retried')],
        [
            ('task_new', 'b2'),
            ('task_queue_default', f'retry:{RETRIED_B}'),
            ('task_queue_default', f'retry:{RETRIED_B}'),
        ],
    )

    assert compare_snapshots(reference, candidate).entries == ()


def test_a_retry_wake_for_another_task_or_one_more_is_a_difference() -> None:
    reference = _run(
        [(RETRIED_A, 'retried'), ('a2', 'other')],
        [('task_queue_default', f'retry:{RETRIED_A}')],
    )
    other_task = _run(
        [(RETRIED_B, 'retried'), (OTHER_B, 'other')],
        [('task_queue_default', f'retry:{OTHER_B}')],
    )
    one_more = _run(
        [(RETRIED_B, 'retried'), (OTHER_B, 'other')],
        [
            ('task_queue_default', f'retry:{RETRIED_B}'),
            ('task_queue_default', f'retry:{RETRIED_B}'),
        ],
    )

    assert sorted(
        type(entry).__name__
        for entry in compare_snapshots(reference, other_task).entries
    ) == ['ExtraRow', 'MissingRow']
    assert [
        type(entry).__name__ for entry in compare_snapshots(reference, one_more).entries
    ] == ['ExtraRow']


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


def test_a_failed_capture_of_the_python_case_is_no_reference_and_carries_its_error() -> None:
    error = TableRowLimitExceeded(table='horsies_tasks', limit=10)

    assert core_step(ReferenceCaptureFailed(error=error)) == ReferenceNotCaptured(error=error)


_CLOCK: Final = datetime(2026, 11, 17, 10, 5, tzinfo=UTC)


@pytest.mark.parametrize(
    ('error', 'text'),
    [
        pytest.param(
            ClockReadFailed(cause=psycopg.OperationalError('server closed')),
            'the run end clock read raised OperationalError: server closed',
            id='clock',
        ),
        pytest.param(
            RunWindowInvalid(
                start=_CLOCK, end=_CLOCK - timedelta(seconds=1), fault=RunWindowFault.END_BEFORE_START
            ),
            'the run window from 2026-11-17T10:05:00+00:00 to 2026-11-17T10:04:59+00:00: '
            'the end reading is before the start reading',
            id='window',
        ),
        pytest.param(
            NotificationReadFailed(cause=psycopg.OperationalError('gone')),
            'the notification read raised OperationalError: gone',
            id='notification-read',
        ),
        pytest.param(
            NotificationMarkerMissing(wait=timedelta(seconds=10)),
            'the notification marker did not arrive within 0:00:10',
            id='marker',
        ),
        pytest.param(
            NotificationLimitExceeded(limit=5),
            'the run sent more than 5 notifications',
            id='notification-limit',
        ),
        pytest.param(
            CaptureStatementFailed(cause=psycopg.errors.UndefinedTable('no table')),
            'a capture statement raised UndefinedTable: no table',
            id='statement',
        ),
        pytest.param(
            CaptureBlocked(lock_timeout=timedelta(seconds=5)),
            'a table lock was held longer than 0:00:05',
            id='blocked',
        ),
        pytest.param(
            SchemaColumnLimitExceeded(limit=2),
            'the schema has more than 2 horsies_ columns',
            id='schema-columns',
        ),
        pytest.param(
            CatalogCoverageGap(gaps=()),
            'the live schema and the snapshot catalog differ',
            id='coverage',
        ),
        pytest.param(
            TableRowLimitExceeded(table='horsies_tasks', limit=10),
            'horsies_tasks: more than 10 rows',
            id='rows',
        ),
        pytest.param(
            UnreadableTime(table='horsies_tasks', column='sent_at', text='soon', cause='bad'),
            "horsies_tasks.sent_at: the clock text 'soon' does not parse: bad",
            id='time',
        ),
        pytest.param(
            UnanchoredTime(
                table='horsies_tasks',
                column='next_retry_at',
                anchor='updated_at',
                reason=UnanchoredReason.ANCHOR_NULL,
            ),
            'horsies_tasks.next_retry_at: no offset from updated_at: '
            'the anchor column is NULL while the value is set',
            id='anchor',
        ),
        pytest.param(
            UnreadableJson(table='horsies_workflows', column='error', cause='text path'),
            'horsies_workflows.error: text path',
            id='json',
        ),
        pytest.param(
            UnsupportedStoredFormat(
                table='horsies_task_history',
                column='attempt_snapshot',
                version_column='attempt_archive_version',
                found='2',
            ),
            'horsies_task_history.attempt_snapshot: attempt_archive_version is 2',
            id='format',
        ),
        pytest.param(
            UnreadableBytes(table='horsies_task_history', column='result_digest'),
            'horsies_task_history.result_digest: the cell is not the hex text',
            id='bytes',
        ),
    ],
)
def test_a_capture_failure_names_its_subject_and_cause(
    error: CaptureFailure, text: str
) -> None:
    assert text in render_capture_failure(error)


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

    assert judge_pair(reference, same, WITHIN_HOUR, WholeSnapshot()) == PairEqual()
    mismatch = judge_pair(reference, other, WITHIN_HOUR, WholeSnapshot())
    assert isinstance(mismatch, DualRunMismatch)
    assert len(mismatch.diff.entries) == 1
    straddle = judge_pair(reference, other, ACROSS_HOUR, WholeSnapshot())
    assert isinstance(straddle, ClockBoundaryStraddle)
    assert straddle.candidate_window == ACROSS_HOUR
    assert judge_pair(reference, same, ACROSS_HOUR, WholeSnapshot()) == PairEqual()


def test_cases_of_a_pair_in_two_hours_are_a_straddle() -> None:
    """#1954: the reference case in one hour and the core case in the next,
    neither window crossing an hour. The core case's coverage adds the next
    hourly leaf to the seed leaves, and leaf names are read relative to each
    case's hour, so the snapshots differ by the clock alone."""
    reference = ReferenceCaptured(
        snapshot=_run([('a1', 'first')], [('task_done', 'a1')]),
        window=WITHIN_HOUR,
    )
    same = _run([('b1', 'first')], [('task_done', 'b1')])
    other = _run([('b1', 'first')], [])

    assert not crosses_hour(WITHIN_HOUR)
    assert not crosses_hour(NEXT_HOUR)
    straddle = judge_pair(reference, other, NEXT_HOUR, WholeSnapshot())
    assert isinstance(straddle, ClockBoundaryStraddle)
    assert straddle.reference_window == WITHIN_HOUR
    assert straddle.candidate_window == NEXT_HOUR
    assert judge_pair(reference, same, NEXT_HOUR, WholeSnapshot()) == PairEqual()
