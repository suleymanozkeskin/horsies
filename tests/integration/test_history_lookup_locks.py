"""The relations that the published detail reader locks.

`horsies_task_detail_staged` (rendered by `horsies_publish_history_readers`)
is static PL/pgSQL: one statement per relation, the live table first, then
each history leaf by its own name, never the partitioned parent. PostgreSQL
plans and locks a statement only when it runs, so a lookup locks only the
relations it probed:

- an id in the live table: the live table, no history leaf;
- a UUIDv7 id: the forever leaf, then the finite leaves from the one that can
  hold the id's birth, up to the leaf that holds the row;
- an id that is not UUIDv7 and is absent: every attached history leaf.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Final
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.commands import (
    CreateDailyHistoryLeaf,
    EnsureLeafCoverage,
    LeafBounds,
    LeafRef,
)
from horsies.core.history.identity.uuid7 import MonotonicUuid7Generator
from horsies.core.history.names import FOREVER_LEAF, TASK_DETAIL_FUNCTION
from horsies.core.history.outcomes import LeafCreated
from horsies.core.history.partitions.catalog import daily_leaf_name
from horsies.core.history.partitions.manager import (
    create_daily_leaf,
    ensure_leaf_coverage,
)
from horsies.core.history.partitions.publication import UnpublishedLoader
from horsies.core.history.reads.publisher import StagedLoaderPublisher
from tests.integration.task_history_harness import (
    INSERT_HISTORY_ROW_SQL,
    HistorySchema,
    day_bounds,
    frozen_history_row,
    insert_live_task,
    register_class,
    task_history_schema_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

UTC = timezone.utc
CLASS_KEY: Final = 'standard_30d'
COVERAGE_DAYS: Final = 2
HISTORY_PARENTS: Final = frozenset(
    {
        'horsies_task_history',
        'horsies_task_history_forever',
        f'horsies_task_history_{CLASS_KEY}',
    }
)

history_schema = task_history_schema_fixture('task_history_it_lookup_locks')


def v7_with_birth(birth: datetime) -> str:
    milliseconds = int(birth.timestamp() * 1_000)
    return str(MonotonicUuid7Generator(clock_ms=lambda: milliseconds).mint())


async def create_past_leaf(
    connection: AsyncConnection, parent: str, days_ago: int
) -> LeafRef:
    lower, upper = day_bounds(datetime.now(UTC) - timedelta(days=days_ago))
    ref = LeafRef(
        leaf_name=daily_leaf_name(parent, lower),
        class_key=CLASS_KEY,
        bounds=LeafBounds(lower=lower, upper=upper),
    )
    created = await create_daily_leaf(
        connection, CreateDailyHistoryLeaf(leaf=ref), UnpublishedLoader()
    )
    assert isinstance(created, LeafCreated)
    return ref


async def attached_history_leaves(connection: AsyncConnection) -> frozenset[str]:
    rows = await connection.execute(
        text(
            'SELECT leaf_name FROM horsies_task_history_leaf_catalog '
            "WHERE class_key IN ('forever', :class_key) "
            'AND detached_at IS NULL'
        ),
        {'class_key': CLASS_KEY},
    )
    return frozenset(str(row.leaf_name) for row in rows)


async def locked_history_relations(
    schema: HistorySchema, task_id: str, history_relations: frozenset[str]
) -> tuple[bool, frozenset[str], bool]:
    """Run one lookup; return found, the history relations locked, and
    whether the live table is locked. The transaction ends with the read."""
    async with schema.engine.connect() as connection:
        async with connection.begin():
            found = (
                await connection.execute(
                    text(
                        f'SELECT EXISTS (SELECT 1 FROM {TASK_DETAIL_FUNCTION}('
                        'CAST(:task_id AS uuid)))'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            locked = frozenset(
                str(row.relname)
                for row in await connection.execute(
                    text(
                        """
                        SELECT relation.relname
                        FROM pg_locks AS held
                        JOIN pg_class AS relation
                          ON relation.oid = held.relation
                        JOIN pg_namespace AS namespace
                          ON namespace.oid = relation.relnamespace
                        WHERE held.pid = pg_backend_pid()
                          AND namespace.nspname = :schema
                          AND relation.relkind IN ('r', 'p')
                        """
                    ),
                    {'schema': schema.schema_name},
                )
            )
    return bool(found), locked & history_relations, 'horsies_tasks' in locked


async def test_a_lookup_locks_only_the_relations_it_probes(
    history_schema: HistorySchema,
) -> None:
    async with history_schema.engine.begin() as connection:
        parent = await register_class(connection, CLASS_KEY)
        await create_past_leaf(connection, parent, days_ago=2)
        yesterday = await create_past_leaf(connection, parent, days_ago=1)
        await ensure_leaf_coverage(
            connection,
            EnsureLeafCoverage(class_key=CLASS_KEY, horizon_days=COVERAGE_DAYS),
            UnpublishedLoader(),
        )
        terminal_at = yesterday.bounds.lower + timedelta(hours=6)
        retained_id = v7_with_birth(terminal_at - timedelta(seconds=30))
        await connection.execute(
            text(INSERT_HISTORY_ROW_SQL),
            frozen_history_row(
                task_id=retained_id, class_key=CLASS_KEY, terminal_at=terminal_at
            ),
        )
        live_id = await insert_live_task(
            connection,
            class_key=CLASS_KEY,
            status='PENDING',
            worker=None,
            task_id=v7_with_birth(datetime.now(UTC)),
        )
        await StagedLoaderPublisher().republish(connection)
        leaves = await attached_history_leaves(connection)
        definition = (
            await connection.execute(
                text(
                    'SELECT pg_get_functiondef('
                    f"'{TASK_DETAIL_FUNCTION}(uuid)'::regprocedure)"
                )
            )
        ).scalar_one()
    # Two past leaves, today's leaf and the coverage ahead, and the forever leaf.
    assert {FOREVER_LEAF, yesterday.leaf_name} < leaves
    assert len(leaves) >= 4 + COVERAGE_DAYS
    assert 'EXECUTE' not in definition
    history_relations = leaves | HISTORY_PARENTS

    assert await locked_history_relations(
        history_schema, live_id, history_relations
    ) == (True, frozenset(), True)
    assert await locked_history_relations(
        history_schema, retained_id, history_relations
    ) == (True, frozenset({FOREVER_LEAF, yesterday.leaf_name}), True)
    assert await locked_history_relations(
        history_schema, str(uuid4()), history_relations
    ) == (False, leaves, True)
