"""The snapshot against real databases: two fresh baseline installs, one logical state."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import LiteralString, TypeVar

import psycopg
import pytest

from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.capture import (
    CaptureBlocked,
    CatalogCoverageGap,
    TableRowLimitExceeded,
    capture_snapshot,
    read_database_clock,
    read_raw_snapshot,
)
from tests.dualrun.catalog import BASELINE_CATALOG, RUN_CLOCK
from tests.dualrun.compare import ColumnDifference, compare_snapshots, render_diff
from tests.dualrun.coverage import (
    DatabaseType,
    RuleTypeMismatch,
    RuleWithoutColumn,
    UnclassifiedColumn,
)
from tests.dualrun.databases import DualRunDatabase, install_baseline_schema
from tests.dualrun.normalize import NormalizedSnapshot
from tests.dualrun.rules import IdFamily
from tests.dualrun.scenario import (
    RowOrder,
    ScenarioFacts,
    ScenarioVariant,
    write_scenario,
)
from tests.dualrun.values import Cell, ExactValue, IdLabel, make_run_window

pytestmark = [pytest.mark.integration]

T = TypeVar('T')
E = TypeVar('E')

MakeDatabase = Callable[[], DualRunDatabase]


def _ok(result: Result[T, E], subject: str) -> T:
    match result:
        case Ok(value):
            return value
        case Err(error):
            pytest.fail(f'{subject}: {error}')


@dataclass(frozen=True, slots=True)
class _Run:
    snapshot: NormalizedSnapshot
    facts: ScenarioFacts


def _installed(make: MakeDatabase) -> DualRunDatabase:
    database = make()
    _ok(install_baseline_schema(database), f'install the baseline into {database.name}')
    return database


def _run_scenario(make: MakeDatabase, variant: ScenarioVariant) -> _Run:
    """Install, write the scenario and capture, with the run window around both."""
    database = make()
    with psycopg.connect(database.url, autocommit=True) as connection:
        start = _ok(read_database_clock(connection), 'read the run start')
        _ok(
            install_baseline_schema(database),
            f'install the baseline into {database.name}',
        )
        facts = _ok(write_scenario(connection, variant), 'write the scenario')
        end = _ok(read_database_clock(connection), 'read the run end')
        window = _ok(make_run_window(start, end), 'make the run window')
        snapshot = _ok(
            capture_snapshot(connection, BASELINE_CATALOG, window),
            'capture the snapshot',
        )
    return _Run(snapshot=snapshot, facts=facts)


def _coverage_gaps_after(make: MakeDatabase, ddl: LiteralString) -> object:
    database = _installed(make)
    with psycopg.connect(database.url, autocommit=True) as connection:
        _ = connection.execute(ddl)
        match read_raw_snapshot(connection, BASELINE_CATALOG):
            case Err(CatalogCoverageGap(gaps=gaps)):
                return gaps
            case other:
                pytest.fail(
                    f'the capture after {ddl!r} returned {other}, not a coverage gap'
                )


def test_catalog_covers_every_column_of_a_fresh_install(
    empty_database: MakeDatabase,
) -> None:
    database = _installed(empty_database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        raw = _ok(
            read_raw_snapshot(connection, BASELINE_CATALOG), 'read the fresh install'
        )
    rows = {table.name: len(table.rows) for table in raw.tables}
    assert tuple(rows) == tuple(table.name for table in BASELINE_CATALOG.tables)
    assert {name: count for name, count in rows.items() if count} == {
        'horsies_migrations': 1,
        'horsies_schema_version': 1,
        'horsies_client_family': 1,
        'horsies_retention_classes': 1,
        'horsies_task_history_leaf_catalog': 1,
        'horsies_recovery_scan_cursors': 2,
    }


def test_same_state_with_other_ids_times_and_row_order_compares_equal(
    empty_database: MakeDatabase,
) -> None:
    reference = _run_scenario(
        empty_database,
        ScenarioVariant(row_order=RowOrder.AS_LISTED, invoice_max_retries=3),
    )
    candidate = _run_scenario(
        empty_database,
        ScenarioVariant(row_order=RowOrder.REVERSED, invoice_max_retries=3),
    )

    assert set(reference.facts.task_ids).isdisjoint(candidate.facts.task_ids)
    assert reference.facts.workflow_id != candidate.facts.workflow_id
    assert reference.facts.worker_id != candidate.facts.worker_id
    assert reference.facts.base_time != candidate.facts.base_time
    diff = compare_snapshots(reference.snapshot, candidate.snapshot)
    assert diff.entries == (), render_diff(diff)
    written = {table.rules.name: len(table.rows) for table in reference.snapshot.tables}
    assert written['horsies_tasks'] == 3
    assert written['horsies_workflow_tasks'] == 2
    assert written['horsies_task_history'] == 1
    assert written['horsies_task_attempts'] == 1
    assert reference.snapshot.ambiguities == ()


def test_history_row_is_read_through_its_parent_with_its_leaf(
    empty_database: MakeDatabase,
) -> None:
    run = _run_scenario(
        empty_database,
        ScenarioVariant(row_order=RowOrder.AS_LISTED, invoice_max_retries=3),
    )

    history = run.snapshot.table('horsies_task_history')
    leaves = run.snapshot.table('horsies_task_history_leaf_catalog')
    nodes = run.snapshot.table('horsies_workflow_tasks')
    (history_row,) = history.rows
    (leaf_row,) = leaves.rows
    extract_node = nodes.rows[0]
    assert history_row[history.rules.position('tableoid')] == IdLabel(
        family=IdFamily.PARTITION_LEAF, ordinal=1
    )
    assert leaf_row[leaves.rules.position('leaf_name')] == IdLabel(
        family=IdFamily.PARTITION_LEAF, ordinal=1
    )
    assert (
        history_row[history.rules.position('task_id')]
        == extract_node[nodes.rules.position('task_id')]
    )


def test_one_field_difference_is_reported_as_one_column_difference(
    empty_database: MakeDatabase,
) -> None:
    reference = _run_scenario(
        empty_database,
        ScenarioVariant(row_order=RowOrder.AS_LISTED, invoice_max_retries=3),
    )
    candidate = _run_scenario(
        empty_database,
        ScenarioVariant(row_order=RowOrder.REVERSED, invoice_max_retries=4),
    )

    diff = compare_snapshots(reference.snapshot, candidate.snapshot)

    # Labels: task#1 and task#2 come from horsies_workflow_tasks; the
    # standalone tasks sort by name, so the rerun 'extract' is task#3 and
    # 'send_invoice' is task#4.
    assert diff.entries == (
        ColumnDifference(
            table='horsies_tasks',
            identity=(
                Cell(column='id', value=IdLabel(family=IdFamily.TASK, ordinal=4)),
            ),
            column='max_retries',
            reference=ExactValue(text='3'),
            candidate=ExactValue(text='4'),
        ),
    ), render_diff(diff)


def test_added_column_fails_the_capture_as_unclassified(
    empty_database: MakeDatabase,
) -> None:
    gaps = _coverage_gaps_after(
        empty_database, 'ALTER TABLE horsies_tasks ADD COLUMN dualrun_probe integer'
    )

    assert gaps == (UnclassifiedColumn(table='horsies_tasks', column='dualrun_probe'),)


def test_added_table_fails_the_capture_as_unclassified(
    empty_database: MakeDatabase,
) -> None:
    gaps = _coverage_gaps_after(
        empty_database, 'CREATE TABLE horsies_dualrun_probe (note text NOT NULL)'
    )

    assert gaps == (UnclassifiedColumn(table='horsies_dualrun_probe', column='note'),)


def test_dropped_column_fails_the_capture_as_rule_without_column(
    empty_database: MakeDatabase,
) -> None:
    gaps = _coverage_gaps_after(
        empty_database, 'ALTER TABLE horsies_schedule_state DROP COLUMN config_hash'
    )

    assert gaps == (
        RuleWithoutColumn(table='horsies_schedule_state', column='config_hash'),
    )


def test_changed_column_type_fails_the_capture_as_rule_type_mismatch(
    empty_database: MakeDatabase,
) -> None:
    gaps = _coverage_gaps_after(
        empty_database,
        'ALTER TABLE horsies_schedule_state ALTER COLUMN updated_at TYPE text',
    )

    assert gaps == (
        RuleTypeMismatch(
            table='horsies_schedule_state',
            column='updated_at',
            rule=RUN_CLOCK,
            column_type=DatabaseType.TEXT,
        ),
    )


def test_capture_is_blocked_by_an_open_table_lock(empty_database: MakeDatabase) -> None:
    database = _installed(empty_database)
    lock_timeout = timedelta(milliseconds=200)
    with (
        psycopg.connect(database.url, autocommit=True) as holder,
        psycopg.connect(database.url, autocommit=True) as reader,
    ):
        with holder.transaction(force_rollback=True):
            _ = holder.execute('LOCK TABLE horsies_tasks IN ACCESS EXCLUSIVE MODE')
            outcome = read_raw_snapshot(
                reader, BASELINE_CATALOG, lock_timeout=lock_timeout
            )
        released = read_raw_snapshot(reader, BASELINE_CATALOG)

    assert outcome == Err(CaptureBlocked(lock_timeout=lock_timeout))
    assert isinstance(released, Ok)


def test_table_over_the_row_limit_fails_the_capture(
    empty_database: MakeDatabase,
) -> None:
    database = _installed(empty_database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        outcome = read_raw_snapshot(connection, BASELINE_CATALOG, row_limit=1)

    assert outcome == Err(
        TableRowLimitExceeded(table='horsies_recovery_scan_cursors', limit=1)
    )
