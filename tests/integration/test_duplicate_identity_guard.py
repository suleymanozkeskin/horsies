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

from horsies.core.history.names import CLOCK_SKEW_EXCEPTIONS
from tests.integration.task_history_harness import (
    HistorySchema,
    insert_live_task,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

CLASS_KEY = 'standard_30d'
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
            'CAST(:task_id AS uuid), :worker, '
            'COALESCE((SELECT claimed_at FROM horsies_tasks WHERE id = CAST(:task_id AS uuid)), NOW()), '
            ':result, TRUE)'
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
            'SELECT EXISTS (SELECT 1 FROM horsies_task_provenance_staged('
            'CAST(:id AS uuid)))'
        ),
        {'id': task_id},
    )).scalar_one())


async def exception_ids(connection: AsyncConnection) -> set[str]:
    rows = (await connection.execute(
        text(f'SELECT task_id FROM {CLOCK_SKEW_EXCEPTIONS}'),
    )).scalars().all()
    return {str(row) for row in rows}


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


