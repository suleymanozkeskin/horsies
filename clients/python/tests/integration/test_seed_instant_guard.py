"""The guards of a seeded instant (``cover_heartbeat_instant``,
``cover_history_instant``) against real PostgreSQL.

A coverage creates the leaves of its own hour or day and later ones. Each
case seeds one minute before the first leaf: the insert without the guard
is refused with SQLSTATE 23514 (no partition for the row), the failure of
a test that starts just after the hour or the day. With the guard the row
lands in the leaf of its own hour or day.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Final
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.integration.history_seeding import (
    cover_heartbeat_instant,
    cover_history_instant,
)
from tests.integration.task_history_harness import (
    INSERT_HISTORY_ROW_SQL,
    HistorySchema,
    frozen_history_row,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

CLASS_KEY: Final = 'standard_30d'
NO_PARTITION: Final = '23514'

terminalization_schema = terminalization_schema_fixture('seed_instant_guard_it')

_INSERT_BEAT_SQL = text(
    'INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid) '
    "VALUES (CAST(:task_id AS uuid), 'guard-worker', 'runner', "
    "CAST(:sent_at AS timestamptz), 'guard-host', 1)"
)


async def _database_now(connection: AsyncConnection) -> datetime:
    now = (await connection.execute(text('SELECT statement_timestamp()'))).scalar_one()
    assert isinstance(now, datetime), now
    return now.astimezone(timezone.utc)


async def _refusal_sqlstate(
    connection: AsyncConnection, statement: Callable[[], Awaitable[None]],
) -> str | None:
    """Run ``statement`` in a savepoint, expect a refusal, and return its
    SQLSTATE."""
    with pytest.raises(DBAPIError) as refused:
        async with connection.begin_nested():
            await statement()
    cause = refused.value.orig
    assert isinstance(cause, psycopg.Error), cause
    return cause.sqlstate


class TestSeedInstantGuard:
    async def test_a_beat_before_the_first_hour_gets_the_leaf_of_its_hour(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            # The leaf of the current hour, as a coverage pass creates it.
            this_hour = (await _database_now(connection)).replace(
                minute=0, second=0, microsecond=0,
            )
            first = await cover_heartbeat_instant(connection, this_hour)
            before = first - timedelta(minutes=1)
            task_id = str(uuid4())

            async def raw_insert() -> None:
                await connection.execute(
                    _INSERT_BEAT_SQL, {'task_id': task_id, 'sent_at': before},
                )

            refused = await _refusal_sqlstate(connection, raw_insert)
            covered = await cover_heartbeat_instant(connection, before)
            await connection.execute(
                _INSERT_BEAT_SQL, {'task_id': task_id, 'sent_at': covered},
            )
            leaf = (
                await connection.execute(
                    text(
                        'SELECT tableoid::regclass::text FROM horsies_heartbeats '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()

        assert refused == NO_PARTITION
        assert covered == before
        assert leaf.endswith(f'horsies_heartbeats_{before:%Y_%m_%d_%H}'), leaf

    async def test_a_history_row_before_the_first_day_gets_the_leaf_of_its_day(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            today = (await _database_now(connection)).replace(
                hour=0, minute=0, second=0, microsecond=0,
            )
            before = today - timedelta(minutes=1)
            task_id = str(uuid4())

            async def raw_insert() -> None:
                await connection.execute(
                    text(INSERT_HISTORY_ROW_SQL),
                    frozen_history_row(
                        task_id=task_id, class_key=CLASS_KEY, terminal_at=before,
                    ),
                )

            refused = await _refusal_sqlstate(connection, raw_insert)
            covered = await cover_history_instant(connection, CLASS_KEY, before)
            await connection.execute(
                text(INSERT_HISTORY_ROW_SQL),
                frozen_history_row(
                    task_id=task_id, class_key=CLASS_KEY, terminal_at=covered,
                ),
            )
            leaf = (
                await connection.execute(
                    text(
                        'SELECT tableoid::regclass::text FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()

        assert refused == NO_PARTITION
        assert covered == before
        assert leaf.endswith(
            f'horsies_task_history_{CLASS_KEY}_{before:%Y_%m_%d}'
        ), leaf
