"""The compare of two parity sides (`compare_sides`), with no database: the
planned difference, the judgement of the other snapshot differences by the
hour rule of the whole-run compare, and the report differences."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Final

import pytest

from tests.parity.snapshot.compare import AmbiguousRows, Side, SnapshotDiff
from tests.parity.snapshot.normalize import AmbiguousGroup, NormalizedSnapshot, NormalizedTable
from tests.parity.snapshot.values import ExactValue, RunWindow
from tests.parity.snapshot.worker_run import (
    WHOLE_RUN_CATALOG,
    ClockBoundaryStraddle,
    DualRunMismatch,
    PairEqual,
)
from tests.parity.runner import (
    AttemptReport,
    Client,
    ReportEntry,
    ReportKey,
    SideRun,
    TaskReport,
    compare_sides,
)

pytestmark = [pytest.mark.unit]

ONE_HOUR: Final = RunWindow(
    start=datetime(2026, 11, 18, 12, 5, tzinfo=UTC),
    end=datetime(2026, 11, 18, 12, 9, tzinfo=UTC),
)
ACROSS_HOUR: Final = RunWindow(
    start=datetime(2026, 11, 18, 12, 59, 48, tzinfo=UTC),
    end=datetime(2026, 11, 18, 13, 1, 2, tzinfo=UTC),
)
"""The run span of a Python side across 13:00Z."""
NEXT_HOUR: Final = RunWindow(
    start=datetime(2026, 11, 18, 13, 1, 4, tzinfo=UTC),
    end=datetime(2026, 11, 18, 13, 2, 3, tzinfo=UTC),
)
"""A span in the hour after `ONE_HOUR`; it does not cross an hour."""

TIE: Final = AmbiguousGroup(table='horsies_tasks', order_key=(), row_count=2)
"""An unplanned difference: two task rows of one side tie on their order."""

COMPLETED: Final = TaskReport(
    status='COMPLETED',
    error_code=None,
    retry_count=0,
    attempts=(AttemptReport(attempt=1, outcome='COMPLETED', error_code=None),),
)


def _snapshot(family: str, ambiguities: tuple[AmbiguousGroup, ...]) -> NormalizedSnapshot:
    """A snapshot of the whole-run catalog whose only row is the client
    family row of `family`."""
    return NormalizedSnapshot(
        tables=tuple(
            NormalizedTable(
                rules=table,
                rows=(
                    (ExactValue(text='true'), ExactValue(text=family), ExactValue(text='t')),
                )
                if table.name == 'horsies_client_family'
                else (),
            )
            for table in WHOLE_RUN_CATALOG.tables
        ),
        ambiguities=ambiguities,
    )


def _side(
    client: Client,
    span: RunWindow,
    *,
    ambiguities: tuple[AmbiguousGroup, ...] = (),
    report: dict[ReportKey, ReportEntry] | None = None,
) -> SideRun:
    return SideRun(
        client=client,
        snapshot=_snapshot(client.value, ambiguities),
        report={} if report is None else report,
        span=span,
    )


def test_the_planned_difference_alone_is_equal() -> None:
    result = compare_sides(_side(Client.PYTHON, ONE_HOUR), _side(Client.RUST, NEXT_HOUR))

    assert result.snapshots == PairEqual()
    assert result.planned_seen
    assert result.report_differences == ()


def test_equal_client_family_rows_are_no_planned_difference() -> None:
    python = _side(Client.PYTHON, ONE_HOUR)
    also_python = SideRun(
        client=Client.RUST,
        snapshot=_snapshot(Client.PYTHON.value, ()),
        report={},
        span=ONE_HOUR,
    )

    result = compare_sides(python, also_python)

    assert result.snapshots == PairEqual()
    assert not result.planned_seen


def test_an_unplanned_difference_in_one_hour_is_a_mismatch() -> None:
    result = compare_sides(
        _side(Client.PYTHON, ONE_HOUR, ambiguities=(TIE,)),
        _side(Client.RUST, ONE_HOUR),
    )

    assert result.snapshots == DualRunMismatch(
        diff=SnapshotDiff(
            entries=(
                AmbiguousRows(
                    side=Side.REFERENCE, table='horsies_tasks', order_key=(), row_count=2,
                ),
            ),
        ),
    )
    assert result.planned_seen


def test_an_unplanned_difference_across_an_hour_is_a_straddle_with_every_entry() -> None:
    result = compare_sides(
        _side(Client.PYTHON, ACROSS_HOUR, ambiguities=(TIE,)),
        _side(Client.RUST, NEXT_HOUR),
    )

    assert result.snapshots == ClockBoundaryStraddle(
        diff=SnapshotDiff(
            entries=(
                AmbiguousRows(
                    side=Side.REFERENCE, table='horsies_tasks', order_key=(), row_count=2,
                ),
            ),
        ),
        reference_span=ACROSS_HOUR,
        candidate_span=NEXT_HOUR,
    )


def test_a_report_difference_names_the_key_and_both_sides() -> None:
    """The whole-run pairs had the same rule (`compare_reports`)."""
    retried = TaskReport(status=None, error_code=None, retry_count=None, attempts=())
    python_report: dict[ReportKey, ReportEntry] = {('task', 0): COMPLETED, ('task', 1): COMPLETED}
    rust_report: dict[ReportKey, ReportEntry] = {('task', 0): COMPLETED, ('task', 1): retried}

    differing = compare_sides(
        _side(Client.PYTHON, ONE_HOUR, report=python_report),
        _side(Client.RUST, ONE_HOUR, report=rust_report),
    )
    equal = compare_sides(
        _side(Client.PYTHON, ONE_HOUR, report=python_report),
        _side(Client.RUST, ONE_HOUR, report=dict(python_report)),
    )

    assert differing.report_differences == ((('task', 1), COMPLETED, retried),)
    assert equal.report_differences == ()
