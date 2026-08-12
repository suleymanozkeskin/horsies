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
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

import horsies.core.brokers.postgres as broker_module
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskStatus
from horsies.core.types.result import is_err
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
