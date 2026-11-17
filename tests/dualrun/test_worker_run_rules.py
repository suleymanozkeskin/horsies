"""The pure parts of the whole-run pairs: the catalog, the compared part of a
snapshot and the report compare. No database."""

from __future__ import annotations

from uuid import uuid4

import pytest

from tests.dualrun.catalog import BASELINE_CATALOG
from tests.dualrun.normalize import AmbiguousGroup, NormalizedSnapshot, NormalizedTable
from tests.dualrun.rules import Ignored
from tests.dualrun.worker_run import (
    PROCESS_ID_COLUMNS,
    UNCOMPARED_TABLES,
    WHOLE_RUN_CATALOG,
    Absent,
    AttemptReport,
    Live,
    TaskReport,
    Terminal,
    compare_reports,
    compared_part,
)

pytestmark = [pytest.mark.unit]


def test_only_the_process_id_columns_change_rule() -> None:
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
            if base_column != whole_column:
                assert isinstance(whole_column.rule, Ignored)
                changed.add((base.name, base_column.name))
    assert changed, 'the catalog has process id columns to ignore'
    assert {column for _, column in changed} == PROCESS_ID_COLUMNS
    assert not {table for table, _ in changed} & UNCOMPARED_TABLES


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


def test_a_report_difference_names_the_task_and_both_sides() -> None:
    same, other = uuid4(), uuid4()
    completed = TaskReport(
        state=Terminal(status='COMPLETED', error_code=None, retry_count=0),
        attempts=(AttemptReport(attempt=1, outcome='COMPLETED', error_code=None),),
    )
    retried = TaskReport(state=Live(status='PENDING', retry_count=1), attempts=())
    reference = {same: completed, other: completed}
    candidate = {same: completed, other: retried}
    differences = compare_reports(reference, candidate)
    assert [difference.task_id for difference in differences] == [other]
    assert differences[0].reference == completed
    assert differences[0].candidate == retried
    assert compare_reports(reference, dict(reference)) == ()
    assert TaskReport(state=Absent(), attempts=()) != completed
