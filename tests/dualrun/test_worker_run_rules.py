"""The whole-run compare rules that parity uses: the catalog, the compared
part of a snapshot and the judgement of two snapshots. No database."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Final

import pytest

from tests.dualrun.catalog import (
    ATTEMPT_ARCHIVE,
    BASELINE_CATALOG,
    KILL_REPORT_MEASURES,
    RESULT_ENVELOPE,
)
from tests.dualrun.compare import MissingRow, SnapshotDiff
from tests.dualrun.normalize import AmbiguousGroup, NormalizedSnapshot, NormalizedTable
from tests.dualrun.rules import (
    EachItem,
    Ignored,
    Index,
    JsonPath,
    Key,
    TimeOrderBeforeRun,
    TimeOrderOnly,
    catalog_defects,
)
from tests.dualrun.values import RunWindow
from tests.dualrun.worker_run import (
    ARCHIVE_PROCESS_ID_POSITIONS,
    ARCHIVE_REPORT_POSITIONS,
    PROCESS_ID_COLUMNS,
    RUN_WRITTEN_ORDER_COLUMNS,
    UNCOMPARED_TABLES,
    WHOLE_RUN_ATTEMPT_ARCHIVE,
    WHOLE_RUN_CATALOG,
    WHOLE_RUN_RESULT_ENVELOPE,
    ClockBoundaryStraddle,
    DualRunMismatch,
    PairEqual,
    compared_part,
    crosses_hour,
    in_one_hour,
    judge_snapshots,
)

pytestmark = [pytest.mark.unit]


def test_only_the_whole_run_columns_change_rule() -> None:
    changed: set[tuple[str, str]] = set()
    for base, whole in zip(
        BASELINE_CATALOG.tables, WHOLE_RUN_CATALOG.tables, strict=True
    ):
        assert (base.name, base.order_by, base.identity) == (
            whole.name,
            whole.order_by,
            whole.identity,
        )
        for base_column, whole_column in zip(base.columns, whole.columns, strict=True):
            assert base_column.name == whole_column.name
            if base_column == whole_column:
                continue
            match base_column.rule:
                case rule if rule == ATTEMPT_ARCHIVE:
                    assert whole_column.rule == WHOLE_RUN_ATTEMPT_ARCHIVE
                case rule if rule == RESULT_ENVELOPE:
                    assert whole_column.rule == WHOLE_RUN_RESULT_ENVELOPE
                case TimeOrderOnly():
                    assert base_column.name in RUN_WRITTEN_ORDER_COLUMNS
                    assert whole_column.rule == TimeOrderBeforeRun()
                case _:
                    assert base_column.name in PROCESS_ID_COLUMNS
                    assert isinstance(whole_column.rule, Ignored)
            changed.add((base.name, base_column.name))
    assert {column for _, column in changed} == (
        PROCESS_ID_COLUMNS
        | RUN_WRITTEN_ORDER_COLUMNS
        | {'attempt_snapshot', 'result_payload'}
    )
    assert {('horsies_tasks', 'enqueued_at'), ('horsies_task_history', 'enqueued_at')} <= changed
    assert not {table for table, _ in changed} & UNCOMPARED_TABLES
    assert catalog_defects(WHOLE_RUN_CATALOG) == ()


def test_the_attempt_archive_adds_the_process_id_and_kill_report_fields() -> None:
    added = WHOLE_RUN_ATTEMPT_ARCHIVE.paths[len(ATTEMPT_ARCHIVE.paths):]
    assert WHOLE_RUN_ATTEMPT_ARCHIVE.paths[: len(ATTEMPT_ARCHIVE.paths)] == ATTEMPT_ARCHIVE.paths
    assert (WHOLE_RUN_ATTEMPT_ARCHIVE.storage, WHOLE_RUN_ATTEMPT_ARCHIVE.format) == (
        ATTEMPT_ARCHIVE.storage,
        ATTEMPT_ARCHIVE.format,
    )
    ignored = added[: len(ARCHIVE_PROCESS_ID_POSITIONS)]
    reports = added[len(ARCHIVE_PROCESS_ID_POSITIONS) :]
    assert [path.steps for path in ignored] == [
        (EachItem(), Index(position=position)) for position in ARCHIVE_PROCESS_ID_POSITIONS
    ]
    assert all(isinstance(path.rule, Ignored) for path in ignored)
    assert list(reports) == [
        JsonPath(steps=(EachItem(), Index(position=position)), rule=KILL_REPORT_MEASURES)
        for position in ARCHIVE_REPORT_POSITIONS
    ]


def test_the_result_envelope_adds_only_the_kill_report_of_its_error_message() -> None:
    assert (WHOLE_RUN_RESULT_ENVELOPE.storage, WHOLE_RUN_RESULT_ENVELOPE.format) == (
        RESULT_ENVELOPE.storage,
        RESULT_ENVELOPE.format,
    )
    assert WHOLE_RUN_RESULT_ENVELOPE.paths == (
        *RESULT_ENVELOPE.paths,
        JsonPath(steps=(Key(name='err'), Key(name='message')), rule=KILL_REPORT_MEASURES),
    )


def test_the_compared_part_leaves_out_the_time_driven_tables() -> None:
    snapshot = NormalizedSnapshot(
        tables=tuple(
            NormalizedTable(rules=table, rows=()) for table in BASELINE_CATALOG.tables
        ),
        ambiguities=(
            AmbiguousGroup(table='horsies_heartbeats', order_key=(), row_count=2),
            AmbiguousGroup(table='horsies_tasks', order_key=(), row_count=2),
        ),
    )
    compared = compared_part(snapshot)
    names = {table.rules.name for table in compared.tables}
    assert (
        names == {table.name for table in BASELINE_CATALOG.tables} - UNCOMPARED_TABLES
    )
    assert [group.table for group in compared.ambiguities] == ['horsies_tasks']


ONE_HOUR: Final = RunWindow(
    start=datetime(2026, 11, 18, 12, 5, tzinfo=UTC),
    end=datetime(2026, 11, 18, 12, 9, tzinfo=UTC),
)
ACROSS_HOUR: Final = RunWindow(
    start=datetime(2026, 11, 18, 12, 59, 48, tzinfo=UTC),
    end=datetime(2026, 11, 18, 13, 1, 2, tzinfo=UTC),
)
"""The run span of the reference side of a pair across 13:00Z."""
NEXT_HOUR: Final = RunWindow(
    start=datetime(2026, 11, 18, 13, 1, 4, tzinfo=UTC),
    end=datetime(2026, 11, 18, 13, 2, 3, tzinfo=UTC),
)
"""A span in the hour after ``ONE_HOUR``; neither crosses an hour."""
LEAF_ONLY_IN_THE_REFERENCE: Final = SnapshotDiff(
    entries=(MissingRow(table='horsies_task_history_leaf_catalog', identity=(), cells=()),),
)


def test_a_span_crosses_an_hour_when_its_end_is_in_a_later_utc_hour() -> None:
    india = timezone(timedelta(hours=5, minutes=30))
    local_hour_only = RunWindow(
        start=datetime(2026, 11, 18, 12, 50, tzinfo=india),
        end=datetime(2026, 11, 18, 13, 5, tzinfo=india),
    )
    assert not crosses_hour(ONE_HOUR)
    assert crosses_hour(ACROSS_HOUR)
    assert not crosses_hour(local_hour_only), '07:20 to 07:35 UTC'


def test_equal_snapshots_are_equal_in_any_hour() -> None:
    equal = SnapshotDiff(entries=())
    assert judge_snapshots(equal, ONE_HOUR, ONE_HOUR) == PairEqual()
    assert judge_snapshots(equal, ACROSS_HOUR, NEXT_HOUR) == PairEqual()


def test_a_difference_in_one_hour_is_a_mismatch() -> None:
    assert in_one_hour(ONE_HOUR, ONE_HOUR)
    judgement = judge_snapshots(LEAF_ONLY_IN_THE_REFERENCE, ONE_HOUR, ONE_HOUR)
    assert judgement == DualRunMismatch(diff=LEAF_ONLY_IN_THE_REFERENCE)


def test_a_difference_across_an_hour_is_a_straddle_with_every_entry() -> None:
    """#2017: the reference side ran across 13:00Z, so its worker added the
    next hourly heartbeat leaf, and the candidate side ran after 13:00Z."""
    assert not in_one_hour(ACROSS_HOUR, NEXT_HOUR)
    judgement = judge_snapshots(LEAF_ONLY_IN_THE_REFERENCE, ACROSS_HOUR, NEXT_HOUR)
    assert judgement == ClockBoundaryStraddle(
        diff=LEAF_ONLY_IN_THE_REFERENCE,
        reference_span=ACROSS_HOUR,
        candidate_span=NEXT_HOUR,
    )


def test_a_difference_of_two_sides_in_two_hours_is_a_straddle() -> None:
    assert not crosses_hour(ONE_HOUR)
    assert not crosses_hour(NEXT_HOUR)
    assert not in_one_hour(ONE_HOUR, NEXT_HOUR)
    judgement = judge_snapshots(LEAF_ONLY_IN_THE_REFERENCE, ONE_HOUR, NEXT_HOUR)
    assert judgement == ClockBoundaryStraddle(
        diff=LEAF_ONLY_IN_THE_REFERENCE,
        reference_span=ONE_HOUR,
        candidate_span=NEXT_HOUR,
    )
