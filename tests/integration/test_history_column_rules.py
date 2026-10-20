"""The column-rule registry and the v49 drop of the history CHECKs.

The history DDL creates one single-column CHECK per rule; each rule's
predicate renders as that constraint. Schema v49 drops those 24
constraints and keeps the cross-column ones. The DDL runs here in a
scratch schema inside a transaction that is rolled back.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

import psycopg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.ddl.column_rules import (
    DROP_SINGLE_COLUMN_CHECKS_DDL,
    HISTORY_COLUMN_RULES,
)
from horsies.core.schemas.migrations import (
    ADD_ATTEMPT_SNAPSHOT_COLUMNS_SQL,
    ADD_RERUN_INPUT_COLUMNS_SQL,
    CREATE_HISTORY_FOUNDATION_SQL,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

SCRATCH_SCHEMA = 'history_column_rules_scratch'
CROSS_COLUMN_CHECK_COUNT = 8

CHECKS_SQL = text("""
    SELECT conname::text, pg_get_constraintdef(oid),
           array_length(conkey, 1) = 1 AS single_column
    FROM pg_constraint
    WHERE conrelid = to_regclass('horsies_task_history')
      AND contype = 'c'
""")


async def _checks(connection: AsyncConnection) -> tuple[dict[str, str], set[str]]:
    """The single-column CHECKs with their definitions, and the
    cross-column CHECK names, of the visible history parent."""
    single: dict[str, str] = {}
    cross: set[str] = set()
    for name, definition, single_column in (
        await connection.execute(CHECKS_SQL)
    ).tuples():
        if single_column:
            single[name] = definition
        else:
            cross.add(name)
    return single, cross


@pytest_asyncio.fixture()
async def scratch_history(
    engine: AsyncEngine,
) -> AsyncGenerator[AsyncConnection, None]:
    """A connection whose search path starts at a scratch schema that holds
    the history parent as the v29 DDL creates it. Everything rolls back."""
    async with engine.connect() as connection:
        transaction = await connection.begin()
        try:
            await connection.execute(text(f'CREATE SCHEMA {SCRATCH_SCHEMA}'))
            await connection.execute(
                text("SELECT set_config('search_path', :path, true)"),
                {'path': f'{SCRATCH_SCHEMA}, public'},
            )
            for statement in (
                *CREATE_HISTORY_FOUNDATION_SQL,
                *ADD_ATTEMPT_SNAPSHOT_COLUMNS_SQL,
                *ADD_RERUN_INPUT_COLUMNS_SQL,
            ):
                await connection.execute(statement)
            yield connection
        finally:
            await transaction.rollback()


async def test_each_rule_renders_as_the_constraint_the_ddl_creates(
    scratch_history: AsyncConnection,
) -> None:
    connection = scratch_history
    created, _ = await _checks(connection)
    await connection.execute(text(
        'CREATE TEMP TABLE history_rule_probe (LIKE horsies_task_history)'
    ))
    rendered: dict[str, str] = {}
    for index, rule in enumerate(HISTORY_COLUMN_RULES):
        probe_name = f'history_rule_probe_{index}'
        await connection.execute(text(
            f'ALTER TABLE history_rule_probe ADD CONSTRAINT {probe_name} '
            f'CHECK ({rule.predicate}) NOT VALID'
        ))
        rendered[rule.constraint_name] = str((await connection.execute(
            text(
                'SELECT pg_get_constraintdef(oid) FROM pg_constraint '
                'WHERE conname = :name'
            ),
            {'name': probe_name},
        )).scalar_one()).removesuffix(' NOT VALID')

    assert set(rendered) == set(created)
    assert rendered == created


async def test_the_drop_removes_the_rule_checks_and_keeps_the_rest(
    scratch_history: AsyncConnection,
) -> None:
    connection = scratch_history
    single_before, cross_before = await _checks(connection)
    assert len(single_before) == len(HISTORY_COLUMN_RULES)
    assert len(cross_before) == CROSS_COLUMN_CHECK_COUNT

    await connection.execute(text(DROP_SINGLE_COLUMN_CHECKS_DDL))
    single_after, cross_after = await _checks(connection)
    assert single_after == {}
    assert cross_after == cross_before

    await connection.execute(text(DROP_SINGLE_COLUMN_CHECKS_DDL))
    assert await _checks(connection) == ({}, cross_before)


async def test_the_drop_refuses_a_diverged_set_and_drops_nothing(
    scratch_history: AsyncConnection,
) -> None:
    connection = scratch_history
    removed = HISTORY_COLUMN_RULES[0].constraint_name
    await connection.execute(
        text(f'ALTER TABLE horsies_task_history DROP CONSTRAINT {removed}')
    )
    single_before, _ = await _checks(connection)

    savepoint = await connection.begin_nested()
    with pytest.raises(DBAPIError) as refused:
        await connection.execute(text(DROP_SINGLE_COLUMN_CHECKS_DDL))
    await savepoint.rollback()
    # The server's own message; the DBAPIError text also quotes the SQL.
    orig = refused.value.orig
    assert isinstance(orig, psycopg.Error), orig
    primary = orig.diag.message_primary
    assert primary is not None
    assert primary.startswith(
        'horsies_task_history single-column CHECKs differ from the expected set'
    ), primary

    single_after, _ = await _checks(connection)
    assert single_after == single_before
    assert len(single_after) == len(HISTORY_COLUMN_RULES) - 1


async def test_an_initialized_database_has_no_single_column_history_check(
    broker: PostgresBroker,
    engine: AsyncEngine,
) -> None:
    _ = broker
    async with engine.connect() as connection:
        single, cross = await _checks(connection)
    assert single == {}
    assert len(cross) == CROSS_COLUMN_CHECK_COUNT
