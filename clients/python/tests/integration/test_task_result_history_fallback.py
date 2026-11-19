"""The result surface resolves from history — the core promise kept.

Terminalization deletes the live row, so a result wait that only
polls `horsies_tasks` would never resolve for any finished task.
These cases run on databases installed by the production schema
install, with history rows seeded through the shared history
seeding: the absent-live path resolves the terminal record from the
staged detail read (digest verified over the exact stored bytes
before parsing); the ruled boundary — the row vanishing between the
live status poll and the record fetch — falls through to the same
resolution; a cancelled task resolves with no payload; and a
database with no published staged function keeps the old semantics
exactly (absent means absent).

With the staged result function published, one statement reads a live
or a history result. An absence from that function is confirmed through
the detail reader.
"""

from __future__ import annotations


import uuid
from typing import Final

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
)
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskStatus
from horsies.core.types.result import is_err
from tests.integration.monitoring_seeding import insert_rows
from tests.integration.test_monitoring_queries import ago, make_task
from tests.integration.isolated_database import (
    MakeDatabase,
    make_database,
)
from tests.sqlalchemy_url import sqlalchemy_url

__all__ = ['make_database']

"""A result reader that probes no history leaf: the reader of a publication
that ran before any history leaf existed."""

pytestmark = [pytest.mark.integration]

TASK_RESULT_FUNCTION: Final = 'horsies_task_result_staged'
"""The published history reader of a task result
(``horsies_publish_history_readers``)."""


async def seed_history_task(
    session: AsyncSession,
    *,
    status: TaskStatus,
    result_json: str | None,
    is_workflow_task: bool = False,
    workflow_id: str | None = None,
) -> str:
    """Write one terminal ``legacy.task`` row to history; return its id.

    The task was enqueued two hours and started one hour before now, by
    ``legacy-worker`` on ``legacy-host``. ``result_json`` is the stored
    result payload; ``workflow_id`` is the history row's workflow link.
    """
    task = make_task(
        task_name='legacy.task',
        status=status,
        is_workflow_task=is_workflow_task,
        worker_id='legacy-worker',
        worker_hostname='legacy-host',
        enqueued_at=ago(7200),
        started_at=ago(3600),
    )
    await insert_rows(session, task)
    payload = result_json.encode() if result_json is not None else None
    _ = await session.execute(
        text(
            'UPDATE horsies_task_history '
            'SET result_payload = CAST(:payload AS bytea), '
            'result_digest = sha256(CAST(:payload AS bytea)), '
            'workflow_id = CAST(:workflow_id AS uuid) '
            'WHERE task_id = CAST(:task_id AS uuid)'
        ),
        {'payload': payload, 'workflow_id': workflow_id, 'task_id': task.id},
    )
    await session.commit()
    return task.id


async def populated_history(url: str) -> tuple[str, str]:
    """History holds one COMPLETED task with a result and one CANCELLED
    task without one; the staged readers are published."""
    engine = create_async_engine(sqlalchemy_url(url))
    try:
        async with AsyncSession(engine) as session:
            completed = await seed_history_task(
                session,
                status=TaskStatus.COMPLETED,
                result_json='{"ok": {"value": 7}}',
            )
            cancelled = await seed_history_task(
                session, status=TaskStatus.CANCELLED, result_json=None
            )
        return completed, cancelled
    finally:
        await engine.dispose()


async def _execute(url: str, statement: str) -> None:
    engine = create_async_engine(sqlalchemy_url(url))
    try:
        async with engine.begin() as connection:
            await connection.execute(text(statement))
    finally:
        await engine.dispose()


async def _drop_result_function(url: str) -> None:
    await _execute(url, f'DROP FUNCTION {TASK_RESULT_FUNCTION}(uuid)')


class TestHistoryFallback:
    @pytest.mark.asyncio
    async def test_absent_live_row_resolves_from_history(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(url))
        )
        try:
            await broker.ensure_schema_initialized()
            completed, cancelled = await populated_history(url)

            outcome = await broker.get_raw_result_record_async(uuid.UUID(completed))
            assert not is_err(outcome), outcome
            record = outcome.ok_value
            assert record is not None
            assert record.status is TaskStatus.COMPLETED
            assert record.raw_result == {'ok': {'value': 7}}

            outcome = await broker.get_raw_result_record_async(uuid.UUID(cancelled))
            assert not is_err(outcome), outcome
            record = outcome.ok_value
            assert record is not None
            assert record.status is TaskStatus.CANCELLED
            assert record.raw_result is None
        finally:
            await broker.close_async()

    @pytest.mark.asyncio
    async def test_an_id_held_nowhere_is_absent(
        self, make_database: MakeDatabase
    ) -> None:
        """An id that neither the live table nor history holds: the readers
        that the install publishes answer absent, and the result is Ok(None)."""
        url = await make_database()
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(url))
        )
        try:
            await broker.ensure_schema_initialized()
            outcome = await broker.get_raw_result_record_async(
                uuid.UUID('11111111-1111-1111-1111-111111111111')
            )
            assert not is_err(outcome), outcome
            assert outcome.ok_value is None
        finally:
            await broker.close_async()

    @pytest.mark.asyncio
    async def test_digest_mismatch_is_a_typed_corruption_error(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(url))
        )
        try:
            await broker.ensure_schema_initialized()
            completed, _ = await populated_history(url)
            engine = create_async_engine(sqlalchemy_url(url))
            try:
                async with engine.begin() as connection:
                    await connection.execute(
                        text(
                            'UPDATE horsies_task_history '
                            "SET result_digest = decode(repeat('00', 32), "
                            "'hex') "
                            'WHERE task_id = CAST(:t AS uuid)'
                        ),
                        {'t': completed},
                    )
            finally:
                await engine.dispose()
            outcome = await broker.get_raw_result_record_async(uuid.UUID(completed))
            assert is_err(outcome)
            assert 'digest mismatch' in outcome.err_value.message

            info = await broker.get_task_info_async(
                uuid.UUID(completed), include_result=True
            )
            assert is_err(info)
            assert info.err_value.code.value == 'INVALID_JSON_PAYLOAD'
            assert 'failed validation' in info.err_value.message
        finally:
            await broker.close_async()


class TestStagedResultReader:


    @pytest.mark.asyncio
    async def test_a_reader_dropped_after_it_was_seen_fails_once(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(url))
        )
        try:
            await broker.ensure_schema_initialized()
            completed, _ = await populated_history(url)
            warm = await broker.get_raw_result_record_async(uuid.UUID(completed))
            assert not is_err(warm), warm
            await _drop_result_function(url)

            failed = await broker.get_raw_result_record_async(uuid.UUID(completed))
            assert is_err(failed)
            assert failed.err_value.code is BrokerErrorCode.TASK_INFO_QUERY_FAILED
            assert TASK_RESULT_FUNCTION in failed.err_value.message

            outcome = await broker.get_raw_result_record_async(uuid.UUID(completed))
            assert not is_err(outcome), outcome
            record = outcome.ok_value
            assert record is not None
            assert record.raw_result == {'ok': {'value': 7}}
        finally:
            await broker.close_async()
