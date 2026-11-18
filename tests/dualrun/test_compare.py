"""Unit tests of normalize and compare over hand-built raw snapshots. No database."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Final

import pytest

from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.catalog import BASELINE_CATALOG
from tests.dualrun.cells import (
    NormalizeError,
    RawRow,
    RawSnapshot,
    RawTable,
    UnanchoredReason,
    UnanchoredTime,
    UnreadableTime,
)
from tests.dualrun.compare import (
    DIFF_REPORT_ROW_LIMIT,
    AmbiguousRows,
    ColumnDifference,
    ExtraRow,
    MissingRow,
    Side,
    SnapshotDiff,
    compare_snapshots,
    render_diff,
)
from tests.dualrun.normalize import NormalizedSnapshot, normalize_snapshot
from tests.dualrun.rules import (
    AnchorReference,
    ColumnSpec,
    Exact,
    IdFamily,
    Ignored,
    NormalisedId,
    RunWindowReference,
    SnapshotCatalog,
    TableRules,
    TimeOrderOnly,
    TimeWithin,
)
from tests.dualrun.values import (
    Cell,
    DuringRun,
    ExactValue,
    IdLabel,
    LiteralTime,
    NullValue,
    OffsetFromAnchor,
    OrderKeyCell,
    RunWindow,
    TimeRank,
    UnlabelledId,
    parse_time_text,
)

pytestmark = [pytest.mark.unit]

BOUND: Final = timedelta(seconds=1)

CATALOG: Final = SnapshotCatalog(
    tables=(
        TableRules(
            name='parents',
            columns=(
                ColumnSpec(name='id', rule=NormalisedId(family=IdFamily.TASK)),
                ColumnSpec(name='name', rule=Exact()),
                ColumnSpec(name='seen_at', rule=TimeOrderOnly()),
                ColumnSpec(
                    name='made_at',
                    rule=TimeWithin(reference=RunWindowReference(), bound=BOUND),
                ),
                ColumnSpec(
                    name='lease_end',
                    rule=TimeWithin(
                        reference=AnchorReference(column='made_at'), bound=BOUND
                    ),
                ),
                ColumnSpec(name='sample', rule=Ignored(reason='process sample')),
            ),
            order_by=('id', 'name'),
            identity=('id',),
        ),
        TableRules(
            name='children',
            columns=(
                ColumnSpec(name='parent_id', rule=NormalisedId(family=IdFamily.TASK)),
                ColumnSpec(name='seq', rule=Exact()),
                ColumnSpec(name='worker', rule=NormalisedId(family=IdFamily.WORKER)),
            ),
            order_by=('parent_id', 'seq'),
            identity=('parent_id', 'seq'),
        ),
    ),
)

RUN_A: Final = datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
RUN_B: Final = datetime(2026, 3, 5, 17, 30, tzinfo=UTC)
FIXED: Final = '2025-06-01 00:00:00+00'


def _window(start: datetime) -> RunWindow:
    return RunWindow(start=start, end=start + timedelta(minutes=1))


def _text(at: datetime) -> str:
    return at.isoformat(sep=' ')


def _parent(
    raw_id: str,
    name: str,
    run: datetime,
    *,
    seen_s: int = 0,
    lease_s: float = 30,
    made_at: str | None = None,
    sample: str = '1.5',
) -> RawRow:
    made = made_at if made_at is not None else _text(run + timedelta(seconds=1))
    lease_end = _text(datetime.fromisoformat(made) + timedelta(seconds=lease_s))
    return (
        raw_id,
        name,
        _text(run + timedelta(seconds=seen_s)),
        made,
        lease_end,
        sample,
    )


def _raw(parents: Sequence[RawRow], children: Sequence[RawRow]) -> RawSnapshot:
    return RawSnapshot(
        tables=(
            RawTable(name='parents', rows=tuple(parents)),
            RawTable(name='children', rows=tuple(children)),
        ),
    )


def _unwrap(result: Result[NormalizedSnapshot, NormalizeError]) -> NormalizedSnapshot:
    match result:
        case Ok(snapshot):
            return snapshot
        case Err(error):
            raise AssertionError(f'normalize failed: {error}')


def _normalize(raw: RawSnapshot, run: datetime) -> NormalizedSnapshot:
    return _unwrap(normalize_snapshot(raw, CATALOG, _window(run)))


def _diff(reference: RawSnapshot, candidate: RawSnapshot) -> SnapshotDiff:
    return compare_snapshots(_normalize(reference, RUN_A), _normalize(candidate, RUN_B))


def _state(run: datetime, prefix: str, *, swap_rows: bool = False) -> RawSnapshot:
    """Two parents, two children; raw ids carry ``prefix`` so each run has its own ids."""
    parents = [
        _parent(f'{prefix}-p2', 'beta', run, seen_s=2),
        _parent(f'{prefix}-p1', 'alpha', run, seen_s=1),
    ]
    children = [
        (f'{prefix}-p1', '1', f'{prefix}-w'),
        (f'{prefix}-p2', '1', f'{prefix}-w'),
    ]
    if swap_rows:
        parents.reverse()
        children.reverse()
    return _raw(parents, children)


# ---- Equality and labels ----


def test_same_state_with_other_ids_times_and_row_order_has_no_difference() -> None:
    diff = _diff(_state(RUN_A, 'a'), _state(RUN_B, 'b', swap_rows=True))

    assert diff == SnapshotDiff(entries=())
    assert render_diff(diff) == 'the snapshots are equal'


def test_labels_follow_order_by_and_references_reuse_them() -> None:
    snapshot = _normalize(_state(RUN_A, 'a'), RUN_A)

    parents = snapshot.table('parents')
    children = snapshot.table('children')
    assert [(row[0], row[1]) for row in parents.rows] == [
        (IdLabel(family=IdFamily.TASK, ordinal=1), ExactValue(text='alpha')),
        (IdLabel(family=IdFamily.TASK, ordinal=2), ExactValue(text='beta')),
    ]
    assert list(children.rows) == [
        (
            IdLabel(family=IdFamily.TASK, ordinal=1),
            ExactValue(text='1'),
            IdLabel(family=IdFamily.WORKER, ordinal=1),
        ),
        (
            IdLabel(family=IdFamily.TASK, ordinal=2),
            ExactValue(text='1'),
            IdLabel(family=IdFamily.WORKER, ordinal=1),
        ),
    ]


def test_a_value_keeps_the_family_of_its_first_appearance() -> None:
    raw = _raw([_parent('shared', 'alpha', RUN_A)], [('shared', '1', 'shared')])

    children = _normalize(raw, RUN_A).table('children')

    assert children.rows == (
        (
            IdLabel(family=IdFamily.TASK, ordinal=1),
            ExactValue(text='1'),
            IdLabel(family=IdFamily.TASK, ordinal=1),
        ),
    )


def test_a_changed_reference_in_the_identity_is_a_missing_and_an_extra_row() -> None:
    reference = _raw(
        [_parent('a1', 'alpha', RUN_A), _parent('a2', 'beta', RUN_A)],
        [('a1', '1', 'w')],
    )
    candidate = _raw(
        [_parent('b1', 'alpha', RUN_B), _parent('b2', 'beta', RUN_B)],
        [('b2', '1', 'w')],
    )

    diff = _diff(reference, candidate)

    one = IdLabel(family=IdFamily.TASK, ordinal=1)
    two = IdLabel(family=IdFamily.TASK, ordinal=2)
    assert diff.entries == (
        MissingRow(
            table='children',
            identity=(
                Cell(column='parent_id', value=one),
                Cell(column='seq', value=ExactValue(text='1')),
            ),
            cells=(
                Cell(column='parent_id', value=one),
                Cell(column='seq', value=ExactValue(text='1')),
                Cell(column='worker', value=IdLabel(family=IdFamily.WORKER, ordinal=1)),
            ),
        ),
        ExtraRow(
            table='children',
            identity=(
                Cell(column='parent_id', value=two),
                Cell(column='seq', value=ExactValue(text='1')),
            ),
            cells=(
                Cell(column='parent_id', value=two),
                Cell(column='seq', value=ExactValue(text='1')),
                Cell(column='worker', value=IdLabel(family=IdFamily.WORKER, ordinal=1)),
            ),
        ),
    )


# ---- Column differences, missing and extra rows ----


def test_one_field_difference_is_one_column_difference_with_both_values() -> None:
    reference = _raw([_parent('a1', 'alpha', RUN_A, lease_s=30)], [('a1', '1', 'w')])
    candidate = _raw([_parent('b1', 'alpha', RUN_B, lease_s=45)], [('b1', '1', 'w')])

    diff = _diff(reference, candidate)

    assert diff.entries == (
        ColumnDifference(
            table='parents',
            identity=(
                Cell(column='id', value=IdLabel(family=IdFamily.TASK, ordinal=1)),
            ),
            column='lease_end',
            reference=OffsetFromAnchor(offset=timedelta(seconds=30)),
            candidate=OffsetFromAnchor(offset=timedelta(seconds=45)),
        ),
    )


def test_missing_and_extra_rows_are_reported_by_side() -> None:
    reference = _raw(
        [_parent('a1', 'alpha', RUN_A)], [('a1', '1', 'w'), ('a1', '2', 'w')]
    )
    candidate = _raw(
        [_parent('b1', 'alpha', RUN_B)], [('b1', '1', 'w'), ('b1', '3', 'w')]
    )

    diff = _diff(reference, candidate)

    assert [type(entry) for entry in diff.entries] == [MissingRow, ExtraRow]
    assert [
        entry.identity[1].value
        for entry in diff.entries
        if isinstance(entry, MissingRow | ExtraRow)
    ] == [
        ExactValue(text='2'),
        ExactValue(text='3'),
    ]


def test_null_against_a_value_is_a_difference() -> None:
    reference = _raw([_parent('a1', 'alpha', RUN_A)], [('a1', '1', 'w')])
    candidate = _raw([_parent('b1', 'alpha', RUN_B)], [('b1', '1', None)])

    diff = _diff(reference, candidate)

    assert diff.entries == (
        ColumnDifference(
            table='children',
            identity=(
                Cell(
                    column='parent_id', value=IdLabel(family=IdFamily.TASK, ordinal=1)
                ),
                Cell(column='seq', value=ExactValue(text='1')),
            ),
            column='worker',
            reference=IdLabel(family=IdFamily.WORKER, ordinal=1),
            candidate=NullValue(),
        ),
    )


def test_ignored_column_never_differs() -> None:
    reference = _raw([_parent('a1', 'alpha', RUN_A, sample='12.5')], [])
    candidate = _raw([_parent('b1', 'alpha', RUN_B, sample='97.0')], [])

    assert _diff(reference, candidate).entries == ()


# ---- Clock rules ----


def test_time_order_only_compares_the_rank_not_the_time() -> None:
    same_order = _raw(
        [
            _parent('b1', 'alpha', RUN_B, seen_s=5),
            _parent('b2', 'beta', RUN_B, seen_s=50),
        ],
        [],
    )
    swapped = _raw(
        [
            _parent('b1', 'alpha', RUN_B, seen_s=50),
            _parent('b2', 'beta', RUN_B, seen_s=5),
        ],
        [],
    )
    reference = _raw(
        [
            _parent('a1', 'alpha', RUN_A, seen_s=1),
            _parent('a2', 'beta', RUN_A, seen_s=2),
        ],
        [],
    )

    assert _diff(reference, same_order).entries == ()
    differences = _diff(reference, swapped).entries
    assert [
        (entry.column, entry.reference, entry.candidate)
        for entry in differences
        if isinstance(entry, ColumnDifference)
    ] == [
        ('seen_at', TimeRank(rank=1), TimeRank(rank=2)),
        ('seen_at', TimeRank(rank=2), TimeRank(rank=1)),
    ]


def test_run_window_value_is_a_token_and_a_value_outside_stays_literal() -> None:
    snapshot = _normalize(
        _raw(
            [
                _parent('a1', 'alpha', RUN_A),
                _parent('a2', 'beta', RUN_A, made_at=FIXED),
            ],
            [],
        ),
        RUN_A,
    )

    made_at = [row[3] for row in snapshot.table('parents').rows]
    match parse_time_text(FIXED):
        case Ok(fixed):
            assert made_at == [DuringRun(), LiteralTime(time=fixed)]
        case Err(rejected):
            raise AssertionError(rejected)


def test_run_window_bound_widens_the_window() -> None:
    inside = _text(RUN_A - BOUND)
    outside = _text(RUN_A - BOUND - timedelta(microseconds=1))

    snapshot = _normalize(
        _raw(
            [
                _parent('a1', 'alpha', RUN_A, made_at=inside),
                _parent('a2', 'beta', RUN_A, made_at=outside),
            ],
            [],
        ),
        RUN_A,
    )

    made_at = [row[3] for row in snapshot.table('parents').rows]
    assert made_at[0] == DuringRun()
    assert isinstance(made_at[1], LiteralTime)


def test_fixed_time_outside_the_window_differs_when_it_differs() -> None:
    reference = _raw([_parent('a1', 'alpha', RUN_A, made_at=FIXED)], [])
    candidate = _raw(
        [_parent('b1', 'alpha', RUN_B, made_at='2025-06-01 00:00:01+00')], []
    )

    diff = _diff(reference, candidate)

    assert [
        entry.column for entry in diff.entries if isinstance(entry, ColumnDifference)
    ] == ['made_at']


def test_anchor_offsets_within_the_bound_are_equal_and_beyond_it_differ() -> None:
    reference = _raw([_parent('a1', 'alpha', RUN_A, lease_s=30)], [])
    within = _raw([_parent('b1', 'alpha', RUN_B, lease_s=30.999)], [])
    beyond = _raw([_parent('b1', 'alpha', RUN_B, lease_s=31.001)], [])

    assert _diff(reference, within).entries == ()
    assert [
        entry.column
        for entry in _diff(reference, beyond).entries
        if isinstance(entry, ColumnDifference)
    ] == [
        'lease_end',
    ]


def test_anchored_value_with_a_null_anchor_is_an_error() -> None:
    row = ('a1', 'alpha', _text(RUN_A), None, _text(RUN_A), '1')

    outcome = normalize_snapshot(_raw([row], []), CATALOG, _window(RUN_A))

    assert outcome == Err(
        UnanchoredTime(
            table='parents',
            column='lease_end',
            anchor='made_at',
            reason=UnanchoredReason.ANCHOR_NULL,
        ),
    )


def test_anchored_infinite_value_is_an_error() -> None:
    row = ('a1', 'alpha', _text(RUN_A), _text(RUN_A), 'infinity', '1')

    outcome = normalize_snapshot(_raw([row], []), CATALOG, _window(RUN_A))

    assert outcome == Err(
        UnanchoredTime(
            table='parents',
            column='lease_end',
            anchor='made_at',
            reason=UnanchoredReason.VALUE_INFINITE,
        ),
    )


def test_unreadable_clock_text_is_an_error() -> None:
    row = ('a1', 'alpha', 'yesterday', _text(RUN_A), _text(RUN_A), '1')

    outcome = normalize_snapshot(_raw([row], []), CATALOG, _window(RUN_A))

    match outcome:
        case Err(UnreadableTime(table='parents', column='seen_at', text='yesterday')):
            pass
        case _:
            raise AssertionError(f'expected UnreadableTime for seen_at, got {outcome}')


# ---- Ties ----


def test_tied_rows_are_ordered_by_the_rows_that_reference_them() -> None:
    reference = _raw(
        [_parent('a1', 'alpha', RUN_A), _parent('a2', 'alpha', RUN_A)],
        [('a1', '1', 'w'), ('a1', '2', 'w'), ('a2', '1', 'w')],
    )
    candidate = _raw(
        [_parent('b2', 'alpha', RUN_B), _parent('b1', 'alpha', RUN_B)],
        [('b2', '1', 'w'), ('b1', '1', 'w'), ('b1', '2', 'w')],
    )

    reference_snapshot = _normalize(reference, RUN_A)
    candidate_snapshot = _normalize(candidate, RUN_B)

    assert reference_snapshot.ambiguities == ()
    assert candidate_snapshot.ambiguities == ()
    assert compare_snapshots(reference_snapshot, candidate_snapshot).entries == ()


def test_reference_order_still_finds_a_changed_reference() -> None:
    reference = _raw(
        [_parent('a1', 'alpha', RUN_A), _parent('a2', 'alpha', RUN_A)],
        [('a1', '1', 'w'), ('a1', '2', 'w'), ('a2', '1', 'w')],
    )
    candidate = _raw(
        [_parent('b1', 'alpha', RUN_B), _parent('b2', 'alpha', RUN_B)],
        [('b1', '1', 'w'), ('b1', '3', 'w'), ('b2', '1', 'w')],
    )

    diff = _diff(reference, candidate)

    assert [type(entry) for entry in diff.entries] == [MissingRow, ExtraRow]


def test_tied_rows_whose_references_hold_an_id_without_a_label_are_ambiguous() -> None:
    """The children hold a worker that the children table labels later, in
    the children's order, which follows the labels of the tied parents."""
    reference = _raw(
        [_parent('a1', 'alpha', RUN_A), _parent('a2', 'alpha', RUN_A)],
        [('a1', '1', 'w'), ('a2', '1', 'w')],
    )
    candidate = _raw(
        [_parent('b1', 'alpha', RUN_B), _parent('b2', 'alpha', RUN_B)],
        [('b1', '1', 'w'), ('b2', '1', 'w')],
    )

    diff = _diff(reference, candidate)

    key = (
        OrderKeyCell(column='id', value=UnlabelledId()),
        OrderKeyCell(column='name', value=ExactValue(text='alpha')),
    )
    assert diff.entries[:2] == (
        AmbiguousRows(side=Side.REFERENCE, table='parents', order_key=key, row_count=2),
        AmbiguousRows(side=Side.CANDIDATE, table='parents', order_key=key, row_count=2),
    )


def test_tied_rows_that_differ_apart_from_their_new_labels_are_ambiguous() -> None:
    raw = _raw(
        [
            _parent('a1', 'alpha', RUN_A, seen_s=1),
            _parent('a2', 'alpha', RUN_A, seen_s=2),
        ],
        [],
    )

    snapshot = _normalize(raw, RUN_A)

    assert [group.row_count for group in snapshot.ambiguities] == [2]


def test_tied_rows_equal_apart_from_unreferenced_new_labels_are_interchangeable() -> (
    None
):
    reference = _raw([_parent('a1', 'alpha', RUN_A), _parent('a2', 'alpha', RUN_A)], [])
    candidate = _raw([_parent('b2', 'alpha', RUN_B), _parent('b1', 'alpha', RUN_B)], [])

    reference_snapshot = _normalize(reference, RUN_A)

    assert reference_snapshot.ambiguities == ()
    assert (
        compare_snapshots(reference_snapshot, _normalize(candidate, RUN_B)).entries
        == ()
    )


def test_tied_rows_whose_references_differ_through_a_later_id_stay_ambiguous() -> None:
    """Parent a1's child shares its worker with a third parent's child, and
    a2's child does not. Each order of a1 and a2 gives other worker labels to
    the children, so the order is not free."""
    raw = _raw(
        [
            _parent('a1', 'alpha', RUN_A),
            _parent('a2', 'alpha', RUN_A),
            _parent('a3', 'beta', RUN_A),
        ],
        [('a1', '1', 'w1'), ('a2', '1', 'w2'), ('a3', '1', 'w1')],
    )

    snapshot = _normalize(raw, RUN_A)

    assert [group.row_count for group in snapshot.ambiguities] == [2]


TIES: Final = SnapshotCatalog(
    tables=(
        TableRules(
            name='tasks',
            columns=(
                ColumnSpec(name='id', rule=NormalisedId(family=IdFamily.TASK)),
                ColumnSpec(name='name', rule=Exact()),
                ColumnSpec(name='owner', rule=NormalisedId(family=IdFamily.WORKER)),
                ColumnSpec(
                    name='digest',
                    rule=NormalisedId(family=IdFamily.COMMAND_FINGERPRINT),
                ),
            ),
            order_by=('id', 'name'),
            identity=('id',),
        ),
        TableRules(
            name='events',
            columns=(
                ColumnSpec(name='channel', rule=Exact()),
                ColumnSpec(name='task_id', rule=NormalisedId(family=IdFamily.TASK)),
                ColumnSpec(name='worker', rule=NormalisedId(family=IdFamily.WORKER)),
            ),
            order_by=('channel', 'task_id', 'worker'),
            identity=('channel', 'task_id', 'worker'),
        ),
    ),
)
"""Task rows, then the events that name them: each tied task is held by the
event rows of its own id."""


def _ties(
    tasks: Sequence[tuple[str, str | None, str]],
    events: Sequence[tuple[str, str | None, str | None]],
    run: datetime,
) -> NormalizedSnapshot:
    raw = RawSnapshot(
        tables=(
            RawTable(
                name='tasks',
                rows=tuple((task, 'bench', owner, digest) for task, owner, digest in tasks),
            ),
            RawTable(name='events', rows=tuple(events)),
        ),
    )
    return _unwrap(normalize_snapshot(raw, TIES, _window(run)))


def test_tied_rows_with_equal_labelled_references_are_interchangeable() -> None:
    """Each task is named by one event of its own; one of them is also
    claimed by the worker that every task holds."""
    reference = _ties(
        [('a1', 'w', 'd1'), ('a2', 'w', 'd2'), ('a3', 'w', 'd3')],
        [('new', 'a1', None), ('new', 'a2', None), ('new', 'a3', None), ('claimed', 'a2', 'w')],
        RUN_A,
    )
    candidate = _ties(
        [('b3', 'v', 'e3'), ('b2', 'v', 'e2'), ('b1', 'v', 'e1')],
        [('claimed', 'b3', 'v'), ('new', 'b1', None), ('new', 'b3', None), ('new', 'b2', None)],
        RUN_B,
    )

    assert reference.ambiguities == ()
    assert candidate.ambiguities == ()
    assert compare_snapshots(reference, candidate).entries == ()


def test_a_shared_new_value_with_references_elsewhere_keeps_a_tie_interchangeable() -> None:
    """The owner is new in the tie and held by every tied row; its label does
    not depend on their order, so its rows elsewhere do not matter."""
    snapshot = _ties(
        [('a1', 'w', 'd1'), ('a2', 'w', 'd2')],
        [('new', 'a1', None), ('new', 'a2', None), ('beat', None, 'w')],
        RUN_A,
    )

    assert snapshot.ambiguities == ()


def test_a_reference_that_holds_the_new_values_of_two_tied_rows_keeps_them_ambiguous() -> None:
    snapshot = _ties(
        [('a1', None, 'd1'), ('a2', None, 'd2')],
        [('link', 'a1', 'd2'), ('link', 'a2', 'd1')],
        RUN_A,
    )

    assert [group.row_count for group in snapshot.ambiguities] == [2]


def test_a_tie_that_a_change_breaks_still_reports_the_change() -> None:
    """The claimed task is another one in the candidate: the tie of the
    other two orders freely, and the claim still compares as a difference."""
    reference = _ties(
        [('a1', 'w', 'd1'), ('a2', 'w', 'd2')],
        [('new', 'a1', None), ('new', 'a2', None), ('claimed', 'a1', 'w')],
        RUN_A,
    )
    candidate = _ties(
        [('b1', 'v', 'e1'), ('b2', 'v', 'e2')],
        [('new', 'b1', None), ('new', 'b2', None), ('claimed', 'b1', 'v'), ('claimed', 'b2', 'v')],
        RUN_B,
    )

    diff = compare_snapshots(reference, candidate)

    assert reference.ambiguities == ()
    assert [type(entry) for entry in diff.entries] == [ExtraRow]


def _baseline_raw(table: str, rows: Sequence[dict[str, str | None]]) -> RawSnapshot:
    """A raw snapshot of ``BASELINE_CATALOG`` with ``rows`` in ``table`` and no
    other row; a column that a row does not name is NULL."""
    return RawSnapshot(
        tables=tuple(
            RawTable(
                name=rules.name,
                rows=tuple(
                    tuple(row.get(spec.name) for spec in rules.columns)
                    for row in (rows if rules.name == table else ())
                ),
            )
            for rules in BASELINE_CATALOG.tables
        ),
    )


def _seeded_task(raw_id: str, run: datetime, execution: str, status: str) -> dict[str, str | None]:
    """A task row as a claim test seeds it: one name, queue, priority, input
    and send time for every row."""
    at = _text(run)
    return {
        'id': raw_id,
        'task_name': 'class_task',
        'queue_name': 'class_q',
        'priority': '100',
        'input': '{}',
        'execution': execution,
        'retention_class_key': 'standard_30d',
        'sent_at': at,
        'created_at': at,
        'command_fingerprint_version': '1',
        'command_fingerprint': f'{raw_id}-fingerprint',
        'status': status,
        'enqueued_at': at,
        'retry_count': '0',
        'max_retries': '0',
        'is_workflow_task': 'false',
        'updated_at': at,
        'retain_rerun_input': 'false',
    }


def _seeded_workflow(raw_id: str, run: datetime, status: str) -> dict[str, str | None]:
    """A workflow row as a claim test seeds it: one name and send time for every row."""
    at = _text(run)
    return {
        'id': raw_id,
        'name': 'bounds_wf',
        'status': status,
        'on_error': 'fail',
        'root_workflow_id': raw_id,
        'depth': '0',
        'sent_at': at,
        'created_at': at,
        'started_at': at,
        'updated_at': at,
    }


def _baseline(raw: RawSnapshot, run: datetime) -> NormalizedSnapshot:
    return _unwrap(normalize_snapshot(raw, BASELINE_CATALOG, _window(run)))


def test_seeded_tasks_that_differ_only_in_class_or_status_are_ordered_by_them() -> None:
    classes = [
        ('ISOLATED', 'PENDING'),
        ('SHARED_THREAD', 'PENDING'),
        ('ISOLATED', 'CLAIMED'),
        ('SHARED_ASYNC', 'RUNNING'),
    ]
    reference = _baseline(
        _baseline_raw(
            'horsies_tasks',
            [_seeded_task(f'a{n}', RUN_A, *each) for n, each in enumerate(classes)],
        ),
        RUN_A,
    )
    candidate = _baseline(
        _baseline_raw(
            'horsies_tasks',
            [
                _seeded_task(f'b{n}', RUN_B, *each)
                for n, each in reversed(list(enumerate(classes)))
            ],
        ),
        RUN_B,
    )

    assert reference.ambiguities == ()
    assert compare_snapshots(reference, candidate).entries == ()


def test_seeded_workflows_that_differ_only_in_status_are_ordered_by_it() -> None:
    statuses = ['PAUSED', 'CANCELLED', 'RUNNING']
    reference = _baseline(
        _baseline_raw(
            'horsies_workflows',
            [_seeded_workflow(f'a{n}', RUN_A, status) for n, status in enumerate(statuses)],
        ),
        RUN_A,
    )
    candidate = _baseline(
        _baseline_raw(
            'horsies_workflows',
            [
                _seeded_workflow(f'b{n}', RUN_B, status)
                for n, status in reversed(list(enumerate(statuses)))
            ],
        ),
        RUN_B,
    )

    assert reference.ambiguities == ()
    assert compare_snapshots(reference, candidate).entries == ()


def test_tied_rows_that_assign_no_new_label_are_not_ambiguous() -> None:
    raw = _raw([_parent('a1', 'alpha', RUN_A)], [('a1', '1', 'w1'), ('a1', '1', 'w1')])

    snapshot = _normalize(raw, RUN_A)

    assert snapshot.ambiguities == ()


def test_duplicate_rows_compare_as_a_multiset() -> None:
    reference = _raw(
        [_parent('a1', 'alpha', RUN_A)], [('a1', '1', 'w'), ('a1', '1', 'w')]
    )
    candidate = _raw([_parent('b1', 'alpha', RUN_B)], [('b1', '1', 'w')])

    diff = _diff(reference, candidate)

    assert [type(entry) for entry in diff.entries] == [MissingRow]


# ---- Report and preconditions ----


def test_render_diff_lists_at_most_the_limit_and_counts_the_rest() -> None:
    reference = _raw(
        [_parent('a1', 'alpha', RUN_A)],
        [('a1', str(seq), 'w') for seq in range(DIFF_REPORT_ROW_LIMIT + 3)],
    )
    candidate = _raw([_parent('b1', 'alpha', RUN_B)], [])

    lines = render_diff(_diff(reference, candidate)).splitlines()

    assert len(lines) == DIFF_REPORT_ROW_LIMIT + 1
    assert lines[-1] == '... and 3 more differences'
    assert lines[0].startswith(
        "children [parent_id=task#1, seq='0']: row only in the reference: "
    )


def test_column_difference_line_names_the_row_column_and_both_values() -> None:
    reference = _raw([_parent('a1', 'alpha', RUN_A)], [('a1', '1', 'w')])
    candidate = _raw([_parent('b1', 'alpha', RUN_B)], [('b1', '1', None)])

    assert render_diff(_diff(reference, candidate)) == (
        "children [parent_id=task#1, seq='1']: worker: reference worker#1, candidate NULL"
    )


def test_snapshots_of_different_catalogs_are_not_compared() -> None:
    children = CATALOG.tables[1]
    other = SnapshotCatalog(
        tables=(
            CATALOG.tables[0],
            TableRules(
                name='children',
                columns=children.columns,
                order_by=('seq',),
                identity=children.identity,
            ),
        ),
    )
    raw = _state(RUN_A, 'a')
    other_snapshot = _unwrap(normalize_snapshot(raw, other, _window(RUN_A)))

    with pytest.raises(AssertionError, match='different catalogs'):
        compare_snapshots(_normalize(raw, RUN_A), other_snapshot)


def test_raw_rows_that_do_not_follow_the_catalog_are_a_defect() -> None:
    raw = RawSnapshot(
        tables=(RawTable(name='children', rows=()), RawTable(name='parents', rows=()))
    )

    with pytest.raises(AssertionError, match='do not follow the catalog'):
        normalize_snapshot(raw, CATALOG, _window(RUN_A))
