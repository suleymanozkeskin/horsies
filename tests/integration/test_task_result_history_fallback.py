"""The result surface resolves from history — the core promise kept.

Terminalization deletes the live row, so a result wait that only
polls `horsies_tasks` would never resolve for any task finishing
after the cutover. These cases run on databases installed by the
production migration chain, with history populated through the real
relocation: the absent-live path resolves the terminal record from
the staged detail read (digest verified over the exact stored bytes
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
from typing import Any, cast

import pytest
from pydantic import SecretStr
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import create_async_engine

import horsies.core.brokers.postgres as broker_module
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerResult,
    RawResultRecord,
)
from horsies.core.history.names import TASK_RESULT_FUNCTION
from horsies.core.history.reads.lookup_generation import (
    LookupManifest,
    render_staged_result_function,
)
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskStatus
from horsies.core.types.result import Err, Ok, is_err
from horsies.core.history.cutover.tighten import (
    TightenComplete,
    confirmation_phrase,
    tighten_to_frozen,
)
from tests.integration.test_task_history_relocation import (
    install_program_state,
    insert_legacy_task,
    relocate_all,
)
from tests.integration.test_task_history_schema_emission import (
    MakeDatabase,
    make_database,
)
from tests.broker_resources_support import current_resources

__all__ = ['make_database']

pytestmark = [pytest.mark.integration]


async def _populated_history(url: str) -> tuple[str, str]:
    """A migrated, RELOCATED, TIGHTENED database — the state a 0.5.0
    broker actually serves. History holds one COMPLETED and one
    CANCELLED task, moved by the real relocation; the staged detail
    function is valid only against the post-tighten live shape."""
    engine = create_async_engine(url)
    try:
        async with engine.begin() as connection:
            await install_program_state(connection)
            completed = await insert_legacy_task(
                connection,
                status='COMPLETED',
                kind='COMPLETE_LOCKED',
                result='{"ok": {"value": 7}}',
            )
            cancelled = await insert_legacy_task(
                connection, status='CANCELLED', kind=None
            )
            await relocate_all(connection)
            tightened = await tighten_to_frozen(
                connection,
                backup_label='fallback-test',
                operator_confirmation=confirmation_phrase(
                    'fallback-test'
                ),
            )
            assert isinstance(tightened, TightenComplete), tightened
        return completed, cancelled
    finally:
        await engine.dispose()


async def _execute(url: str, statement: str) -> None:
    engine = create_async_engine(url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(statement))
    finally:
        await engine.dispose()


async def _drop_result_function(url: str) -> None:
    await _execute(url, f'DROP FUNCTION {TASK_RESULT_FUNCTION}(uuid)')


async def _probe_statements(
    broker: PostgresBroker,
    task_id: str,
    *,
    non_terminal_snapshot: bool = False,
) -> tuple[BrokerResult[RawResultRecord | None], list[str]]:
    """One result probe that ends the wait, and the statements it sent."""
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
            completed, cancelled = await _populated_history(url)

            outcome = await broker.get_raw_result_record_async(completed)
            assert not is_err(outcome), outcome
            record = outcome.ok_value
            assert record is not None
            assert record.status is TaskStatus.COMPLETED
            assert record.raw_result == {'ok': {'value': 7}}

            outcome = await broker.get_raw_result_record_async(cancelled)
            assert not is_err(outcome), outcome
            record = outcome.ok_value
            assert record is not None
            assert record.status is TaskStatus.CANCELLED
            assert record.raw_result is None
        finally:
            await broker.close_async()

    @pytest.mark.asyncio
    async def test_row_vanishing_between_poll_and_fetch_falls_through(
        self,
        make_database: MakeDatabase,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The ruled boundary. The mid-move instant — live status seen
        terminal, record fetch finding nothing — cannot exist at rest
        post-tighten (the live domain forbids terminal statuses), so
        both probe results are forced to the mid-move readings while
        the real history row stands; the fallback must resolve from
        it."""
        url = await make_database()
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(url))
        )
        try:
            await broker.ensure_schema_initialized()
            completed, _ = await _populated_history(url)
            # The live probe runs only while the result function is not
            # published.
            await _drop_result_function(url)
            monkeypatch.setattr(
                broker_module,
                'GET_TASK_STATUS_NAME_SQL',
                text(
                    "SELECT 'COMPLETED' AS status, "
                    "'legacy.task' AS task_name"
                ),
            )
            monkeypatch.setattr(
                broker_module,
                'GET_TASK_RESULT_RECORD_SQL',
                text(
                    'SELECT task_name, status, result '
                    'FROM horsies_tasks WHERE FALSE'
                ),
            )
            outcome = await broker.get_raw_result_record_async(completed)
            assert not is_err(outcome), outcome
            record = outcome.ok_value
            assert record is not None
            assert record.status is TaskStatus.COMPLETED
            assert record.raw_result == {'ok': {'value': 7}}
        finally:
            await broker.close_async()

    @pytest.mark.asyncio
    async def test_unpublished_staged_function_keeps_old_semantics(
        self, make_database: MakeDatabase
    ) -> None:
        """A pre-coverage database has no history to consult: absent
        means absent, exactly as before."""
        url = await make_database()
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(url))
        )
        try:
            await broker.ensure_schema_initialized()
            outcome = await broker.get_raw_result_record_async(
                '11111111-1111-1111-1111-111111111111'
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
            completed, _ = await _populated_history(url)
            engine = create_async_engine(url)
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
            outcome = await broker.get_raw_result_record_async(completed)
            assert is_err(outcome)
            assert 'digest mismatch' in outcome.err_value.message

            info = await broker.get_task_info_async(
                completed, include_result=True
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
            completed, _ = await _populated_history(url)
            warm = await broker.get_raw_result_record_async(completed)
            assert not is_err(warm), warm

            outcome, statements = await _probe_statements(broker, completed)
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
            await _populated_history(url)
            task_id = str(uuid.uuid4())
            enqueued = await broker.enqueue_async(
                'result.reader.task',
                task_id=task_id,
                enqueue_sha='d' * 64,
                args_json='[]',
                kwargs_json='{}',
            )
            assert not is_err(enqueued), enqueued
            warm = await broker.get_raw_result_record_async(
                task_id, timeout_ms=1
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
            completed, _ = await _populated_history(url)
            await _execute(
                url,
                render_staged_result_function(
                    LookupManifest(leaves=(), birth_floor=None)
                ),
            )
            engine = create_async_engine(url)
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

            outcome = await broker.get_raw_result_record_async(completed)
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
            completed, _ = await _populated_history(url)
            await _drop_result_function(url)

            outcome, statements = await _probe_statements(broker, completed)
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
            completed, _ = await _populated_history(url)
            warm = await broker.get_raw_result_record_async(completed)
            assert not is_err(warm), warm
            await _drop_result_function(url)

            failed = await broker.get_raw_result_record_async(completed)
            assert is_err(failed)
            assert failed.err_value.code is BrokerErrorCode.TASK_INFO_QUERY_FAILED
            assert TASK_RESULT_FUNCTION in failed.err_value.message

            outcome = await broker.get_raw_result_record_async(completed)
            assert not is_err(outcome), outcome
            record = outcome.ok_value
            assert record is not None
            assert record.raw_result == {'ok': {'value': 7}}
        finally:
            await broker.close_async()
