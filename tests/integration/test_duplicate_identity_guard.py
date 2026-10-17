"""The pruned duplicate-identity guard against real PostgreSQL.

The guard probes only the leaves that can hold an id born at its time.
A producer whose clock ran ahead mints an id born after the row's terminal
time; the move records that id as a clock-skew exception, and the guard
finds it there. Dropping a leaf deletes the exceptions of its rows.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.ddl.classes import finite_class_parent_name
from horsies.core.history.commands import (
    CreateDailyHistoryLeaf,
    DetachExpiredHistoryLeaf,
    DropDetachedHistoryLeaf,
    LeafBounds,
    LeafRef,
)
from horsies.core.history.maintenance.database import PartitionMaintenanceDatabase
from horsies.core.history.names import CLOCK_SKEW_EXCEPTIONS
from horsies.core.history.outcomes import LeafCreated, LeafDetached, LeafDropped
from horsies.core.history.partitions.catalog import daily_leaf_name
from horsies.core.history.partitions.manager import (
    create_daily_leaf,
    detach_expired_leaf,
    drop_detached_leaf,
)
from horsies.core.history.reads.publisher import (
    StagedLoaderPublisher,
    staged_duplicate_guard_rendered,
)
from tests.integration.task_history_harness import (
    HistorySchema,
    day_bounds,
    insert_live_task,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

CLASS_KEY = 'it_dup_guard'
WORKER = 'worker-dup-guard'

terminalization_schema = terminalization_schema_fixture('task_history_it_dup_guard')


def uuid7_born_at(birth: datetime) -> str:
    """A v7 id whose embedded birth is `birth`, with random tail bits."""
    milliseconds = int(birth.timestamp() * 1000)
    tail = int.from_bytes(os.urandom(10), 'big')
    value = (milliseconds << 80) | tail
    value = (value & ~(0xF << 76)) | (0x7 << 76)
    value = (value & ~(0x3 << 62)) | (0x2 << 62)
    return str(uuid.UUID(int=value))


async def complete_fused(connection: AsyncConnection, task_id: str) -> str:
    return str((await connection.execute(
        text(
            'SELECT outcome FROM horsies_complete_task_fused('
            'CAST(:task_id AS uuid), :worker, NULL, :result, '
            "'task_done', CAST(:task_id AS text))"
        ),
        {'task_id': task_id, 'worker': WORKER, 'result': '{"ok":true}'},
    )).scalar_one())


async def guard(connection: AsyncConnection, task_id: str) -> bool:
    return bool((await connection.execute(
        text('SELECT horsies_task_history_duplicate_staged(CAST(:id AS uuid))'),
        {'id': task_id},
    )).scalar_one())


async def provenance_found(connection: AsyncConnection, task_id: str) -> bool:
    return bool((await connection.execute(
        text(
            'SELECT found FROM horsies_task_provenance_staged('
            'CAST(:id AS uuid), FALSE)'
        ),
        {'id': task_id},
    )).scalar_one())


async def exception_ids(connection: AsyncConnection) -> set[str]:
    rows = (await connection.execute(
        text(f'SELECT task_id FROM {CLOCK_SKEW_EXCEPTIONS}'),
    )).scalars().all()
    return {str(row) for row in rows}


async def test_publication_renders_the_marked_guard_over_a_first_version(
    terminalization_schema: HistorySchema,
) -> None:
    async with terminalization_schema.engine.begin() as connection:
        assert not await staged_duplicate_guard_rendered(connection)
        await prepare_move_storage(connection, CLASS_KEY)
        assert await staged_duplicate_guard_rendered(connection)


async def test_a_clock_ahead_move_is_recorded_and_the_guard_finds_it(
    terminalization_schema: HistorySchema,
) -> None:
    async with terminalization_schema.engine.begin() as connection:
        await prepare_move_storage(connection, CLASS_KEY)
        ahead = await insert_live_task(
            connection,
            class_key=CLASS_KEY,
            worker=WORKER,
            task_id=uuid7_born_at(datetime.now(UTC) + timedelta(days=1)),
        )
        ordinary = await insert_live_task(
            connection, class_key=CLASS_KEY, worker=WORKER,
        )
        assert await complete_fused(connection, ahead) == 'APPLIED'
        assert await complete_fused(connection, ordinary) == 'APPLIED'

        assert await exception_ids(connection) == {ahead}
        assert await guard(connection, ahead)
        assert await guard(connection, ordinary)
        assert not await guard(connection, uuid7_born_at(datetime.now(UTC)))

        # The pruned probes alone skip the leaf that holds the ahead row:
        # the exception is what keeps the duplicate detectable.
        await connection.execute(text(f'DELETE FROM {CLOCK_SKEW_EXCEPTIONS}'))
        assert not await guard(connection, ahead)
        assert await provenance_found(connection, ahead)


async def test_dropping_a_leaf_deletes_the_exceptions_of_its_rows(
    terminalization_schema: HistorySchema,
) -> None:
    async with terminalization_schema.engine.begin() as connection:
        await prepare_move_storage(connection, CLASS_KEY)
        parent = finite_class_parent_name(CLASS_KEY)
        lower, _ = day_bounds(datetime.now(UTC) - timedelta(days=40))
        ref = LeafRef(
            leaf_name=daily_leaf_name(parent, lower),
            class_key=CLASS_KEY,
            bounds=LeafBounds(lower=lower, upper=lower + timedelta(days=1)),
        )
        created = await create_daily_leaf(
            connection, CreateDailyHistoryLeaf(leaf=ref), StagedLoaderPublisher(),
        )
        assert isinstance(created, LeafCreated)
        inside, outside = str(uuid.uuid4()), str(uuid.uuid4())
        for task_id, terminal_at in (
            (inside, lower + timedelta(hours=1)),
            (outside, datetime.now(UTC)),
        ):
            await connection.execute(
                text(
                    f'INSERT INTO {CLOCK_SKEW_EXCEPTIONS} (task_id, '
                    'retention_class_key, terminal_at, birth_at, recorded_at) '
                    'VALUES (CAST(:id AS uuid), :class_key, :terminal_at, '
                    ':terminal_at, NOW())'
                ),
                {'id': task_id, 'class_key': CLASS_KEY, 'terminal_at': terminal_at},
            )

    detached = await detach_expired_leaf(
        PartitionMaintenanceDatabase(terminalization_schema.engine, connection_capacity=2),
        DetachExpiredHistoryLeaf(
            leaf=ref, quarantine_horizon=None, statement_timeout_ms=None,
        ),
        StagedLoaderPublisher(),
    )
    assert isinstance(detached, LeafDetached)
    async with terminalization_schema.engine.begin() as connection:
        dropped = await drop_detached_leaf(
            connection, DropDetachedHistoryLeaf(leaf=ref), StagedLoaderPublisher(),
        )
        assert dropped == LeafDropped(leaf_name=ref.leaf_name)
        assert await exception_ids(connection) == {outside}
