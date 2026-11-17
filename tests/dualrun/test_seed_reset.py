"""The seed state of the dual-run session database and the reset to it.

The pure helpers run without a database. The database tests install the
baseline on a fresh ``dualrun_`` database, seed it with the baseline's own
coverage functions, change it as a test run does, and reset it.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from typing import Final, LiteralString, TypeVar

import psycopg
import pytest
from psycopg import sql

from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.capture import read_raw_snapshot
from tests.dualrun.catalog import BASELINE_CATALOG
from tests.dualrun.cells import RawRow
from tests.dualrun.databases import DualRunDatabase, install_baseline_schema
from tests.dualrun.seed_reset import (
    RESET_LOCK_TIMEOUT,
    PartitionAbsent,
    PartitionChange,
    PublishedReader,
    ResetBlocked,
    SeedForeignKeyCycle,
    SeedPartition,
    SeedPartitionsChanged,
    SeedState,
    SeedTable,
    insert_order,
    partition_changes,
    relations_to_drop,
    reset_to_seed,
    save_seed,
)

T = TypeVar('T')
E = TypeVar('E')

MakeDatabase = Callable[[], DualRunDatabase]

_LEAF: Final = SeedPartition(
    parent='horsies_task_history_standard_30d',
    name='horsies_task_history_standard_30d_2026_11_17',
    bound="FOR VALUES FROM ('2026-11-17 00:00:00+00') TO ('2026-11-18 00:00:00+00')",
    detach_pending=False,
)


def _ok(result: Result[T, E], subject: str) -> T:
    match result:
        case Ok(value):
            return value
        case Err(error):
            pytest.fail(f'{subject}: {error}')


# ---- Pure helpers ----


@pytest.mark.unit
def test_insert_order_puts_a_referenced_table_first_and_ties_by_name() -> None:
    tables = ('c_leaf', 'a_class', 'b_free')
    edges = (('c_leaf', 'a_class'),)

    assert insert_order(tables, edges) == Ok(('a_class', 'b_free', 'c_leaf'))
    assert insert_order((), ()) == Ok(())


@pytest.mark.unit
def test_insert_order_of_a_cycle_names_the_tables_of_the_cycle() -> None:
    tables = ('a', 'b', 'free')
    edges = (('a', 'b'), ('b', 'a'))

    assert insert_order(tables, edges) == Err(SeedForeignKeyCycle(tables=('a', 'b')))


@pytest.mark.unit
def test_partition_changes_names_an_absent_and_a_rebound_seed_partition() -> None:
    rebound = SeedPartition(
        parent=_LEAF.parent, name=_LEAF.name, bound='DEFAULT', detach_pending=False
    )
    pending = SeedPartition(
        parent=_LEAF.parent, name=_LEAF.name, bound=_LEAF.bound, detach_pending=True
    )

    assert partition_changes((_LEAF,), (_LEAF,)) == ()
    assert partition_changes((_LEAF,), ()) == (
        PartitionChange(seed=_LEAF, found=PartitionAbsent()),
    )
    assert partition_changes((_LEAF,), (rebound,)) == (
        PartitionChange(seed=_LEAF, found=rebound),
    )
    assert partition_changes((_LEAF,), (pending,)) == (
        PartitionChange(seed=_LEAF, found=pending),
    )


@pytest.mark.unit
def test_relations_to_drop_are_new_partitions_and_new_cataloged_leaves() -> None:
    seed = SeedState(
        tables=(SeedTable(name='horsies_tasks', rows_json='[]', row_count=0),),
        partitions=(_LEAF,),
        readers=(),
    )
    new_leaf = SeedPartition(
        parent=_LEAF.parent,
        name='horsies_task_history_standard_30d_2026_11_20',
        bound='FOR VALUES FROM (x) TO (y)',
        detach_pending=False,
    )

    assert relations_to_drop(
        seed,
        (_LEAF, new_leaf),
        (_LEAF.name, 'horsies_task_history_standard_7d_2026_11_17', 'horsies_tasks'),
    ) == (
        'horsies_task_history_standard_30d_2026_11_20',
        'horsies_task_history_standard_7d_2026_11_17',
    )


# ---- Against a database ----

_SEED_SQL: Final[tuple[LiteralString, ...]] = (
    'SELECT * FROM horsies_register_retention_classes(ARRAY[]::smallint[], 3::smallint)',
    "SELECT * FROM horsies_create_history_leaf('standard_30d', date_trunc('day', now(), 'UTC'))",
    "SELECT * FROM horsies_create_history_leaf('standard_30d', date_trunc('day', now(), 'UTC') + interval '1 day')",
    "SELECT * FROM horsies_create_heartbeat_leaf(date_trunc('hour', now(), 'UTC'))",
    'SELECT * FROM horsies_publish_history_readers()',
)
"""The coverage of the default class and of the heartbeats, then the readers."""

_RUN_SQL: Final[tuple[LiteralString, ...]] = (
    'SELECT * FROM horsies_register_retention_classes(ARRAY[7]::smallint[], 3::smallint)',
    "SELECT * FROM horsies_create_history_leaf('standard_7d', date_trunc('day', now(), 'UTC'))",
    "SELECT * FROM horsies_create_history_leaf('standard_30d', date_trunc('day', now(), 'UTC') + interval '5 days')",
    "SELECT * FROM horsies_create_heartbeat_leaf(date_trunc('hour', now(), 'UTC') + interval '5 hours')",
    'SELECT * FROM horsies_publish_history_readers()',
    """
    INSERT INTO horsies_workflows (
        id, name, status, on_error, depth, root_workflow_id, sent_at, created_at, updated_at
    )
    VALUES (gen_random_uuid(), 'run', 'RUNNING', 'fail', 0, gen_random_uuid(), now(), now(), now())
    """,
    'DELETE FROM horsies_recovery_scan_cursors',
)
"""What a run leaves: a new class with a leaf, new leaves of the seed
classes, republished readers, a row, a removed baseline row."""

_STATE_SQL: Final[LiteralString] = """
    SELECT
        (SELECT string_agg(c.relname || ' ' || coalesce(pg_get_expr(c.relpartbound, c.oid), '-'), ', '
                           ORDER BY c.relname)
         FROM pg_catalog.pg_class AS c
         JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
         WHERE n.nspname = current_schema()
           AND c.relkind IN ('r', 'p')
           AND starts_with(c.relname::text, 'horsies_')),
        (SELECT string_agg(md5(pg_get_functiondef(to_regprocedure(s.signature)))
                           || ' ' || coalesce(obj_description(to_regprocedure(s.signature), 'pg_proc'), '-'),
                           ', ' ORDER BY s.signature)
         FROM horsies_history_reader_signatures() AS s)
"""
"""The relations with their bounds, and each reader's definition digest and comment."""


@pytest.fixture
def seeded(empty_database: MakeDatabase) -> DualRunDatabase:
    """A fresh database with the baseline and the seed coverage."""
    database = empty_database()
    _ok(install_baseline_schema(database), f'install the baseline into {database.name}')
    with psycopg.connect(database.url) as connection:
        for statement in _SEED_SQL:
            _ = connection.execute(statement).fetchall()
    return database


def _run(database: DualRunDatabase) -> None:
    with psycopg.connect(database.url) as connection:
        for statement in _RUN_SQL:
            _ = connection.execute(statement)


def _relations(
    connection: psycopg.Connection[tuple[object, ...]],
) -> tuple[object, ...]:
    """The relations with their bounds, and the readers."""
    with connection.transaction():
        _ = connection.execute("SET LOCAL TimeZone = 'UTC'")
        found = connection.execute(_STATE_SQL).fetchone()
    assert found is not None, 'the state read returns one row'
    return found


def _state(
    connection: psycopg.Connection[tuple[object, ...]],
) -> tuple[tuple[object, ...], dict[str, Counter[RawRow]]]:
    """The relations and readers, and the rows of every catalog table as a multiset."""
    raw = _ok(read_raw_snapshot(connection, BASELINE_CATALOG), 'read the rows')
    return _relations(connection), {
        table.name: Counter(table.rows) for table in raw.tables
    }


def _workflow_count(connection: psycopg.Connection[tuple[object, ...]]) -> object:
    with connection.transaction():
        found = connection.execute('SELECT count(*) FROM horsies_workflows').fetchone()
    assert found is not None, 'a count returns one row'
    return found[0]


@pytest.mark.integration
def test_a_reset_restores_the_rows_relations_and_readers_of_the_seed(
    seeded: DualRunDatabase,
) -> None:
    with psycopg.connect(seeded.url) as connection:
        seed = _ok(save_seed(connection), 'save the seed')
        before = _state(connection)
        _run(seeded)
        changed = _state(connection)
        reset = reset_to_seed(connection, seed)
        after = _state(connection)
        again = reset_to_seed(connection, seed)
        unchanged = _state(connection)

    assert all(isinstance(reader, PublishedReader) for reader in seed.readers)
    assert changed[0][0] != before[0][0], 'the run added relations'
    assert changed[0][1] != before[0][1], 'the run republished the readers'
    assert changed[1] != before[1], 'the run changed rows'
    assert reset == Ok(None)
    assert after == before
    assert again == Ok(None)
    assert unchanged == before


@pytest.mark.integration
def test_a_detached_seed_leaf_fails_the_reset_and_changes_nothing(
    seeded: DualRunDatabase,
) -> None:
    with psycopg.connect(seeded.url) as connection:
        seed = _ok(save_seed(connection), 'save the seed')
        _run(seeded)
        leaf = next(
            partition
            for partition in seed.partitions
            if partition.parent == 'horsies_heartbeats'
        )
        with connection.transaction():
            _ = connection.execute(
                sql.SQL('ALTER TABLE horsies_heartbeats DETACH PARTITION {}').format(
                    sql.Identifier(leaf.name)
                )
            )
        changed = _relations(connection)
        outcome = reset_to_seed(connection, seed)
        after = _relations(connection)
        workflows = _workflow_count(connection)

    assert outcome == Err(
        SeedPartitionsChanged(
            changes=(PartitionChange(seed=leaf, found=PartitionAbsent()),),
        )
    )
    assert after == changed
    assert workflows == 1, 'the row of the run is still there'


@pytest.mark.integration
def test_a_held_lock_blocks_the_reset_and_changes_nothing(
    seeded: DualRunDatabase,
) -> None:
    with (
        psycopg.connect(seeded.url) as connection,
        psycopg.connect(seeded.url, autocommit=True) as holder,
    ):
        seed = _ok(save_seed(connection), 'save the seed')
        _run(seeded)
        changed = _state(connection)
        with holder.transaction(force_rollback=True):
            _ = holder.execute('LOCK TABLE horsies_workflows IN ACCESS EXCLUSIVE MODE')
            outcome = reset_to_seed(connection, seed)
        after = _state(connection)
        released = reset_to_seed(connection, seed)

    assert outcome == Err(ResetBlocked(lock_timeout=RESET_LOCK_TIMEOUT))
    assert after == changed
    assert released == Ok(None)
