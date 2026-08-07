"""Stage-4a envelope preparation on the real migrated schema.

One policy owner, both readers: the backfill consults the same
app-level retain-input default every fresh enqueue consults. Proven:
under a retaining default, valid stored inputs prepare inline
envelopes with the canonical digest, oversized inputs take
OVER_BOUND, and stored JSON that fails the strict decode takes
DECLINED_BY_POLICY with the decode-failed count reported separately
— the archive column collapses the two, the batch outcome must not.
The fingerprint always lands (it covers the carried strings as-is).
The disposition marker makes a resumed run idempotent, and a
prepared row relocates carrying its envelope into history.
"""

from __future__ import annotations

from hashlib import sha256

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.cutover.preparation import (
    PreparationBatch,
    PreparationComplete,
    prepare_legacy_batch,
)
from horsies.core.history.cutover.relocation import RelocationComplete
from horsies.core.history.rerun.input_envelope import (
    encode_input_envelope_v1,
)
from horsies.core.models.broker import PostgresConfig
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


async def _prepare_db(url: str) -> None:
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(url)))
    try:
        await broker.ensure_schema_initialized()
    finally:
        await broker.close_async()


async def run_preparation_to_complete(
    connection: AsyncConnection,
    *,
    retain_default: bool,
    batch_size: int = 2,
) -> tuple[list[PreparationBatch], PreparationComplete]:
    batches: list[PreparationBatch] = []
    while True:
        outcome = await prepare_legacy_batch(
            connection,
            retain_default=retain_default,
            batch_size=batch_size,
        )
        if isinstance(outcome, PreparationComplete):
            return batches, outcome
        batches.append(outcome)


class TestPreparation:
    @pytest.mark.asyncio
    async def test_retaining_default_prepares_real_inputs(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        await _prepare_db(url)
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                await install_program_state(connection)
                valid = await insert_legacy_task(
                    connection,
                    status='FAILED',
                    kind=None,
                    disposition=None,
                    retain=None,
                    args_json='[1, 2]',
                    kwargs_json='{"a": true}',
                )
                oversized = await insert_legacy_task(
                    connection,
                    status='FAILED',
                    kind=None,
                    disposition=None,
                    retain=None,
                    kwargs_json=(
                        '{"blob": "' + 'x' * 70_000 + '"}'
                    ),
                )
                malformed = await insert_legacy_task(
                    connection,
                    status='FAILED',
                    kind=None,
                    disposition=None,
                    retain=None,
                    args_json='[1, unquoted]',
                )

                batches, complete = await run_preparation_to_complete(
                    connection, retain_default=True
                )
                assert complete.rows_prepared == 3
                assert sum(b.inline_rows for b in batches) == 1
                assert sum(b.over_bound_rows for b in batches) == 1
                assert sum(b.decode_failed_rows for b in batches) == 1
                assert sum(b.policy_declined_rows for b in batches) == 0

                rows = {
                    str(row.id): row
                    for row in (
                        await connection.execute(
                            text(
                                'SELECT id, command_fingerprint, '
                                'input_digest, retain_rerun_input, '
                                'prepared_rerun_input_disposition, '
                                'prepared_rerun_input_inline, '
                                'prepared_rerun_input_digest '
                                'FROM horsies_tasks'
                            )
                        )
                    ).all()
                }
                expected_payload = encode_input_envelope_v1(
                    args=[1, 2], kwargs={'a': True}, options=None
                )
                assert (
                    bytes(rows[valid].prepared_rerun_input_inline)
                    == expected_payload
                )
                assert (
                    bytes(rows[valid].prepared_rerun_input_digest)
                    == sha256(expected_payload).digest()
                )
                assert (
                    rows[valid].prepared_rerun_input_disposition == 'INLINE'
                )
                assert rows[valid].retain_rerun_input is True
                assert (
                    rows[oversized].prepared_rerun_input_disposition
                    == 'OVER_BOUND'
                )
                assert rows[oversized].input_digest is not None
                assert (
                    rows[malformed].prepared_rerun_input_disposition
                    == 'DECLINED_BY_POLICY'
                )
                assert rows[malformed].input_digest is None
                for row in rows.values():
                    assert row.command_fingerprint is not None

                # The marker is the idempotence instrument: a resumed
                # run has nothing to prepare.
                resumed = await prepare_legacy_batch(
                    connection, retain_default=True, batch_size=10
                )
                assert isinstance(resumed, PreparationComplete)
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_declining_default_reports_the_policy_split(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        await _prepare_db(url)
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                await install_program_state(connection)
                await insert_legacy_task(
                    connection,
                    status='FAILED',
                    kind=None,
                    disposition=None,
                    retain=None,
                    args_json='[1]',
                )
                batches, complete = await run_preparation_to_complete(
                    connection, retain_default=False
                )
                assert complete.rows_prepared == 1
                assert sum(b.policy_declined_rows for b in batches) == 1
                assert sum(b.decode_failed_rows for b in batches) == 0
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_prepared_row_relocates_with_its_envelope(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        await _prepare_db(url)
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                await install_program_state(connection)
                task_id = await insert_legacy_task(
                    connection,
                    status='FAILED',
                    kind=None,
                    disposition=None,
                    retain=None,
                    args_json='[42]',
                )
                await run_preparation_to_complete(connection, retain_default=True)
                relocated = await relocate_all(connection)
                assert isinstance(relocated, RelocationComplete)
                row = (
                    await connection.execute(
                        text(
                            'SELECT rerun_input_disposition, '
                            'rerun_input_inline '
                            'FROM horsies_task_history '
                            'WHERE task_id = CAST(:t AS uuid)'
                        ),
                        {'t': task_id},
                    )
                ).one()
                assert row.rerun_input_disposition == 'INLINE'
                assert bytes(row.rerun_input_inline) == (
                    encode_input_envelope_v1(
                        args=[42], kwargs={}, options=None
                    )
                )
        finally:
            await engine.dispose()
