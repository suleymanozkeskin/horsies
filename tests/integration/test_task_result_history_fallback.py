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

from datetime import datetime, timezone

import uuid
from typing import Any, cast

import pytest
from pydantic import SecretStr
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from horsies.core.brokers.enqueue_task import SendKind
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerResult,
    RawResultRecord,
)
from horsies.core.history.names import TASK_RESULT_FUNCTION
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskStatus
from horsies.core.types.result import Err, Ok, is_err
from tests.integration.monitoring_seeding import insert_rows
from tests.integration.test_monitoring_queries import ago, make_task
from tests.integration.test_task_history_schema_emission import (
    MakeDatabase,
    make_database,
)
from tests.broker_resources_support import current_resources
from tests.dualrun.reference import result_caller
from horsies.core.utils.url import sqlalchemy_url

__all__ = ['make_database']

_STALE_RESULT_READER_SQL = f"""
CREATE OR REPLACE FUNCTION {TASK_RESULT_FUNCTION}(p_task_id uuid)
RETURNS TABLE(location text, status text, failed_reason text, result_envelope_version smallint,
              result_payload bytea, result_digest bytea, task_name text)
LANGUAGE sql STABLE STRICT AS $$
    SELECT 'LIVE'::text, t.status::text, NULL::text, NULL::smallint, NULL::bytea, NULL::bytea,
        t.task_name::text
    FROM horsies_tasks AS t WHERE t.id = p_task_id
$$
"""
"""A result reader that probes no history leaf: the reader of a publication
that ran before any history leaf existed."""

pytestmark = [pytest.mark.integration]


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


async def _probe_statements(
    broker: PostgresBroker,
    task_id: uuid.UUID,
    *,
    non_terminal_snapshot: bool = False,
) -> tuple[BrokerResult[RawResultRecord | None], list[str]]:
    """One probe of the Python result read (kept as the dual-run reference)
    that ends the wait, and the statements it sent."""
    statements: list[str] = []

    def record(
        _connection: Any,
        _cursor: Any,
        statement: str,
        _parameters: Any,
        _context: Any,
        _executemany: bool,
    ) -> None:
        statements.append(statement)

    engine = current_resources(broker).session_factory.kw['bind'].sync_engine
    event.listen(engine, 'before_cursor_execute', record)
    try:
        async with current_resources(broker).session_factory() as session:
            outcome = await broker._probe_result_row(  # pyright: ignore[reportPrivateUsage]
                session,
                task_id,
                non_terminal_snapshot=non_terminal_snapshot,
            )
    finally:
        event.remove(engine, 'before_cursor_execute', record)
    assert isinstance(outcome, (Ok, Err)), outcome
    return cast('BrokerResult[RawResultRecord | None]', outcome), statements


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
    async def test_a_history_result_is_one_statement(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(url))
        )
        try:
            await broker.ensure_schema_initialized()
            completed, _ = await populated_history(url)
            warm = await result_caller.get_raw_result_record_async(
                broker, uuid.UUID(completed),
            )
            assert not is_err(warm), warm

            outcome, statements = await _probe_statements(broker, uuid.UUID(completed))
            assert len(statements) == 1, statements
            assert TASK_RESULT_FUNCTION in statements[0]
            assert not is_err(outcome), outcome
            record = outcome.ok_value
            assert record is not None
            assert record.task_name == 'legacy.task'
            assert record.status is TaskStatus.COMPLETED
            assert record.raw_result == {'ok': {'value': 7}}
        finally:
            await broker.close_async()

    @pytest.mark.asyncio
    async def test_a_live_task_is_one_statement(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(url))
        )
        try:
            await broker.ensure_schema_initialized()
            await populated_history(url)
            task_id = uuid.uuid4()
            enqueued = await broker.enqueue_async(
                'result.reader.task',
                task_id=task_id,
                send_kind=SendKind.FIRST,
                enqueue_sha='d' * 64,
                sent_at=datetime.now(timezone.utc),
                args_json='[]',
                kwargs_json='{}',
            )
            assert not is_err(enqueued), enqueued
            warm = await result_caller.get_raw_result_record_async(
                broker, task_id, timeout_ms=1
            )
            assert not is_err(warm), warm

            outcome, statements = await _probe_statements(
                broker, task_id, non_terminal_snapshot=True
            )
            assert len(statements) == 1, statements
            assert TASK_RESULT_FUNCTION in statements[0]
            assert not is_err(outcome), outcome
            record = outcome.ok_value
            assert record is not None
            assert record.task_name == 'result.reader.task'
            assert record.status is TaskStatus.PENDING
            assert record.raw_result is None
        finally:
            await broker.close_async()

    @pytest.mark.asyncio
    async def test_an_absence_from_a_stale_reader_is_confirmed_by_detail(
        self, make_database: MakeDatabase
    ) -> None:
        """A publisher of release 0.5.3 adds leaves without rendering the
        result function. The function then misses a row in such a leaf,
        and the detail reader finds it."""
        url = await make_database()
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(url))
        )
        try:
            await broker.ensure_schema_initialized()
            completed, _ = await populated_history(url)
            await _execute(url, _STALE_RESULT_READER_SQL)
            engine = create_async_engine(sqlalchemy_url(url))
            try:
                async with engine.connect() as connection:
                    stale_rows = (
                        await connection.execute(
                            text(
                                f'SELECT 1 FROM {TASK_RESULT_FUNCTION}'
                                '(CAST(:task_id AS uuid))'
                            ),
                            {'task_id': completed},
                        )
                    ).all()
                assert stale_rows == []
            finally:
                await engine.dispose()

            outcome = await broker.get_raw_result_record_async(uuid.UUID(completed))
            assert not is_err(outcome), outcome
            record = outcome.ok_value
            assert record is not None
            assert record.status is TaskStatus.COMPLETED
            assert record.raw_result == {'ok': {'value': 7}}
        finally:
            await broker.close_async()

    @pytest.mark.asyncio
    async def test_an_unpublished_result_reader_uses_the_live_and_detail_reads(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(url))
        )
        try:
            await broker.ensure_schema_initialized()
            completed, _ = await populated_history(url)
            await _drop_result_function(url)

            outcome, statements = await _probe_statements(broker, uuid.UUID(completed))
            assert not any(
                TASK_RESULT_FUNCTION in statement
                and 'to_regprocedure' not in statement
                for statement in statements
            ), statements
            assert not is_err(outcome), outcome
            record = outcome.ok_value
            assert record is not None
            assert record.raw_result == {'ok': {'value': 7}}
        finally:
            await broker.close_async()

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
