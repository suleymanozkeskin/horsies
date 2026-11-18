"""Unit tests of the run-measure text rule (``RunMeasuresInText``) and the
run-written clock rule (``TimeOrderBeforeRun``) that the whole-run pairs use."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Final

import pytest

from horsies.core.types.result import Err, Ok
from tests.dualrun.catalog import KILL_REPORT_MEASURES
from tests.dualrun.cells import RawRow, RawSnapshot, RawTable, measured_text
from tests.dualrun.compare import ColumnDifference, compare_snapshots
from tests.dualrun.normalize import NormalizedSnapshot, normalize_snapshot
from tests.dualrun.rules import (
    ColumnSpec,
    EachItem,
    IdFamily,
    Index,
    InvalidMeasurePattern,
    JsonPath,
    JsonStorage,
    Key,
    NormalisedId,
    RunMeasure,
    RunMeasuresInText,
    SnapshotCatalog,
    StoredJson,
    TableRules,
    TimeOrderBeforeRun,
    TimeOrderOnly,
    Unversioned,
    catalog_defects,
)
from tests.dualrun.values import (
    DuringRun,
    MeasuredText,
    MeasureToken,
    NormalizedValue,
    RunWindow,
    TimeRank,
)

pytestmark = [pytest.mark.unit]

RUN: Final = datetime(2026, 11, 17, 21, 0, tzinfo=UTC)
WINDOW: Final = RunWindow(start=RUN, end=RUN + timedelta(minutes=2))

_CULPRIT: Final = (
    'Task held the event loop of its shared async child for {ms} ms, past the block '
    'limit, at /app/whole_run_app.py:295 in blocks_loop_once; the worker killed the '
    'child (pid={pid}, signal 9 (SIGKILL))'
)
"""The ``TASK_BLOCKED_LOOP`` report, as both workers write it."""

_NEIGHBOUR: Final = (
    'Task 01a1eb1f-3373-7001-a708-8ecceb383ee1 held the event loop of the shared async '
    'child of this task for {ms} ms, past the block limit; the worker killed the child '
    '(pid={pid}, signal 9 (SIGKILL))'
)
"""The ``NEIGHBOUR_BLOCKED_LOOP`` report, as both workers write it."""

_BLOCKED: Final = MeasureToken(name='blocked_ms')
_PID: Final = MeasureToken(name='killed_child_pid')


# ---- The text rule ----


def test_a_blocked_loop_report_keeps_its_text_and_tokens_its_two_measures() -> None:
    assert measured_text(
        _CULPRIT.format(ms=5078, pid=47945), KILL_REPORT_MEASURES.measures
    ) == MeasuredText(
        parts=(
            'Task held the event loop of its shared async child for ',
            _BLOCKED,
            ' ms, past the block limit, at /app/whole_run_app.py:295 in '
            'blocks_loop_once; the worker killed the child (pid=',
            _PID,
            ', signal 9 (SIGKILL))',
        ),
    )
    assert measured_text(
        _NEIGHBOUR.format(ms=5101, pid=42766), KILL_REPORT_MEASURES.measures
    ) == MeasuredText(
        parts=(
            'Task 01a1eb1f-3373-7001-a708-8ecceb383ee1 held the event loop of the '
            'shared async child of this task for ',
            _BLOCKED,
            ' ms, past the block limit; the worker killed the child (pid=',
            _PID,
            ', signal 9 (SIGKILL))',
        ),
    )


def test_a_text_without_a_report_stays_one_literal_part() -> None:
    for text in ('boom', 'slept for 5000 ms', 'the child (pid=12) ended'):
        assert measured_text(text, KILL_REPORT_MEASURES.measures) == MeasuredText(
            parts=(text,)
        )


@pytest.mark.parametrize(
    'pattern',
    ['no group', r'(\d+) and (\d+)', r'(unclosed'],
)
def test_a_measure_pattern_without_exactly_one_group_is_a_catalog_defect(
    pattern: str,
) -> None:
    rule = StoredJson(
        storage=JsonStorage.TEXT,
        format=Unversioned(),
        paths=(
            JsonPath(
                steps=(Key(name='message'),),
                rule=RunMeasuresInText(
                    measures=(RunMeasure(name='measure', pattern=pattern),)
                ),
            ),
        ),
    )
    catalog = SnapshotCatalog(
        tables=(
            TableRules(
                name='reports',
                columns=(ColumnSpec(name='doc', rule=rule),),
                order_by=('doc',),
                identity=('doc',),
            ),
        ),
    )
    assert catalog_defects(catalog) == (
        InvalidMeasurePattern(table='reports', column='doc', measure='measure'),
    )


def test_the_kill_report_measures_are_valid_catalog_rules() -> None:
    rule = StoredJson(
        storage=JsonStorage.TEXT,
        format=Unversioned(),
        paths=(JsonPath(steps=(Key(name='message'),), rule=KILL_REPORT_MEASURES),),
    )
    catalog = SnapshotCatalog(
        tables=(
            TableRules(
                name='reports',
                columns=(ColumnSpec(name='doc', rule=rule),),
                order_by=('doc',),
                identity=('doc',),
            ),
        ),
    )
    assert catalog_defects(catalog) == ()


_REPORTS: Final = SnapshotCatalog(
    tables=(
        TableRules(
            name='reports',
            columns=(
                ColumnSpec(name='task_id', rule=NormalisedId(family=IdFamily.TASK)),
                ColumnSpec(
                    name='result',
                    rule=StoredJson(
                        storage=JsonStorage.TEXT,
                        format=Unversioned(),
                        paths=(
                            JsonPath(
                                steps=(Key(name='err'), Key(name='message')),
                                rule=KILL_REPORT_MEASURES,
                            ),
                        ),
                    ),
                ),
                ColumnSpec(
                    name='archive',
                    rule=StoredJson(
                        storage=JsonStorage.TEXT,
                        format=Unversioned(),
                        paths=(
                            JsonPath(
                                steps=(EachItem(), Index(position=0)),
                                rule=KILL_REPORT_MEASURES,
                            ),
                            JsonPath(
                                steps=(EachItem(), Index(position=1)),
                                rule=KILL_REPORT_MEASURES,
                            ),
                        ),
                    ),
                ),
            ),
            order_by=('task_id',),
            identity=('task_id',),
        ),
    ),
)
"""A result whose ``err.message``, and an attempt list whose error message
and failed reason (fields 0 and 1), can hold a kill report."""


def _report_row(task_id: str, message: str, failed_reason: str | None) -> RawRow:
    result = {'err': {'error_code': 'NEIGHBOUR_BLOCKED_LOOP', 'message': message}}
    return (task_id, json.dumps(result), json.dumps([[message, failed_reason]]))


def _snapshot(
    catalog: SnapshotCatalog, table: str, rows: Sequence[RawRow]
) -> NormalizedSnapshot:
    raw = RawSnapshot(tables=(RawTable(name=table, rows=tuple(rows)),))
    match normalize_snapshot(raw, catalog, WINDOW):
        case Ok(snapshot):
            return snapshot
        case Err(error):
            raise AssertionError(f'normalize failed: {error}')


def _report_columns(reference_row: RawRow, candidate_row: RawRow) -> set[str]:
    """The columns in which the two one-row snapshots differ."""
    diff = compare_snapshots(
        _snapshot(_REPORTS, 'reports', [reference_row]),
        _snapshot(_REPORTS, 'reports', [candidate_row]),
    )
    columns: set[str] = set()
    for entry in diff.entries:
        assert isinstance(entry, ColumnDifference), entry
        columns.add(entry.column)
    return columns


def test_two_runs_whose_reports_differ_in_block_time_and_child_pid_compare_equal() -> (
    None
):
    reference = _NEIGHBOUR.format(ms=5078, pid=47945)
    candidate = _NEIGHBOUR.format(ms=5089, pid=48203)
    assert (
        _report_columns(
            _report_row('a', reference, reference),
            _report_row('b', candidate, candidate),
        )
        == set()
    )


def test_a_report_that_differs_outside_its_measures_still_differs() -> None:
    reference = _CULPRIT.format(ms=5078, pid=47945)
    other_place = reference.replace(':295 in', ':296 in')
    assert _report_columns(
        _report_row('a', reference, reference),
        _report_row('b', other_place, other_place),
    ) == {'result', 'archive'}


def test_a_failed_reason_that_one_side_leaves_empty_still_differs() -> None:
    reference = _NEIGHBOUR.format(ms=5078, pid=47945)
    candidate = _NEIGHBOUR.format(ms=5089, pid=48203)
    assert _report_columns(
        _report_row('a', reference, reference),
        _report_row('b', candidate, None),
    ) == {'archive'}


# ---- The run-written clock rule ----

_SENDS: Final = SnapshotCatalog(
    tables=(
        TableRules(
            name='sends',
            columns=(
                ColumnSpec(name='task_id', rule=NormalisedId(family=IdFamily.TASK)),
                ColumnSpec(name='sent_at', rule=TimeOrderOnly()),
                ColumnSpec(name='enqueued_at', rule=TimeOrderBeforeRun()),
            ),
            order_by=('task_id', 'sent_at'),
            identity=('task_id',),
        ),
    ),
)


def _at(offset: timedelta) -> str:
    return (RUN + offset).isoformat(sep=' ')


def _send(task_id: str, position: int, enqueued: timedelta) -> RawRow:
    """A send written ``position`` ms after the first, enqueued at ``RUN + enqueued``."""
    return (
        task_id,
        _at(timedelta(minutes=-1, milliseconds=position)),
        _at(enqueued),
    )


_BEFORE_RUN: Final = timedelta(seconds=-30)
"""The insert time of the first send, before the run start."""


def _sends(retry_times: dict[int, timedelta]) -> list[RawRow]:
    """Four sends inserted before the run in send order; the sends in
    ``retry_times`` were retried at that offset from the run start."""
    return [
        _send(
            f'task-{position}',
            position,
            retry_times.get(position, _BEFORE_RUN + timedelta(milliseconds=position)),
        )
        for position in range(4)
    ]


def _enqueued(snapshot: NormalizedSnapshot) -> list[NormalizedValue]:
    table = snapshot.table('sends')
    column = table.rules.position('enqueued_at')
    return [row[column] for row in table.rows]


def test_retry_times_in_either_order_compare_equal_and_sends_keep_their_rank() -> None:
    reference = _snapshot(
        _SENDS,
        'sends',
        _sends({1: timedelta(seconds=3), 3: timedelta(seconds=4)}),
    )
    candidate = _snapshot(
        _SENDS,
        'sends',
        _sends({1: timedelta(seconds=4), 3: timedelta(seconds=3)}),
    )
    assert compare_snapshots(reference, candidate).entries == ()
    assert _enqueued(reference) == [TimeRank(rank=1), DuringRun(), TimeRank(rank=2), DuringRun()]


def test_a_retry_that_one_side_does_not_make_still_differs() -> None:
    reference = _snapshot(
        _SENDS,
        'sends',
        _sends({1: timedelta(seconds=3), 3: timedelta(seconds=4)}),
    )
    candidate = _snapshot(_SENDS, 'sends', _sends({1: timedelta(seconds=3)}))
    columns = {
        entry.column
        for entry in compare_snapshots(reference, candidate).entries
        if isinstance(entry, ColumnDifference)
    }
    assert columns == {'enqueued_at'}
