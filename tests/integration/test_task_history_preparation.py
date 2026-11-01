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

from typing import Any

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.cutover.preparation import (
    PreparationBatch,
    PreparationBlocked,
    PreparationComplete,
    PreparationCursor,
    RefusedRow,
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
    """Drive batches to completion, threading the cursor.

    Each batch's ``cursor`` feeds the next call — the loop contract
    that keeps preparation linear: a driver that never advances the
    cursor re-walks all previously prepared rows every batch and the
    stage goes quadratic in rows.
    """
    batches: list[PreparationBatch] = []
    cursor = PreparationCursor.start()
    while True:
        outcome = await prepare_legacy_batch(
            connection,
            retain_default=retain_default,
            batch_size=batch_size,
            cursor=cursor,
        )
        if isinstance(outcome, PreparationComplete):
            return batches, outcome
        if isinstance(outcome, PreparationBlocked):
            raise AssertionError(f'preparation is blocked: {outcome!r}')
        batches.append(outcome)
        cursor = outcome.cursor


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
                    connection,
                    retain_default=True,
                    batch_size=10,
                    cursor=PreparationCursor.start(),
                )
                assert isinstance(resumed, PreparationComplete)
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'refused_kwargs',
        ['{"x": 1e9999}', r'{"x": "\ud800"}'],
    )
    async def test_refused_value_is_a_decode_failure_of_its_row_only(
        self, make_database: MakeDatabase, refused_kwargs: str
    ) -> None:
        url = await make_database()
        await _prepare_db(url)
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                await install_program_state(connection)
                refused = await insert_legacy_task(
                    connection,
                    status='FAILED',
                    kind=None,
                    disposition=None,
                    retain=None,
                    kwargs_json=refused_kwargs,
                )
                valid = await insert_legacy_task(
                    connection,
                    status='FAILED',
                    kind=None,
                    disposition=None,
                    retain=None,
                    kwargs_json='{"x": 1.5}',
                )

                batches, complete = await run_preparation_to_complete(
                    connection, retain_default=True
                )
                assert complete.rows_prepared == 2
                assert sum(b.decode_failed_rows for b in batches) == 1
                assert sum(b.inline_rows for b in batches) == 1

                rows = {
                    str(row.id): row
                    for row in (
                        await connection.execute(
                            text(
                                'SELECT id, input_digest, '
                                'prepared_rerun_input_disposition, '
                                'prepared_rerun_input_inline '
                                'FROM horsies_tasks'
                            )
                        )
                    ).all()
                }
                assert (
                    rows[refused].prepared_rerun_input_disposition
                    == 'DECLINED_BY_POLICY'
                )
                assert rows[refused].input_digest is None
                assert rows[refused].prepared_rerun_input_inline is None
                assert rows[valid].prepared_rerun_input_disposition == 'INLINE'
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

async def run_preparation_to_the_end(
    connection: AsyncConnection,
    *,
    batch_size: int,
) -> tuple[list[PreparationBatch], PreparationComplete | PreparationBlocked]:
    """Drive every batch, and return the outcome that ends the walk."""
    batches: list[PreparationBatch] = []
    cursor = PreparationCursor.start()
    while True:
        outcome = await prepare_legacy_batch(
            connection,
            retain_default=True,
            batch_size=batch_size,
            cursor=cursor,
        )
        match outcome:
            case PreparationBatch():
                batches.append(outcome)
                cursor = outcome.cursor
            case PreparationComplete() | PreparationBlocked():
                return batches, outcome


async def break_row(
    connection: AsyncConnection, task_id: str, assignment: str,
) -> None:
    await connection.execute(
        text(
            f'UPDATE horsies_tasks SET {assignment} '
            'WHERE id::text = :task_id'
        ),
        {'task_id': task_id},
    )


async def disposition_of(connection: AsyncConnection, task_id: str) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT prepared_rerun_input_disposition FROM horsies_tasks '
                'WHERE id::text = :task_id'
            ),
            {'task_id': task_id},
        )
    ).scalar_one()


class TestRefusedRows:
    """A row with no possible fingerprint stops itself, and nothing else."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('assignment', 'reason'),
        [
            ("task_name = ''", 'task_name must be non-empty'),
            ("queue_name = ''", 'queue_name must be non-empty'),
            ('priority = 0', 'priority must be between 1 and 100'),
            ('priority = 101', 'priority must be between 1 and 100'),
        ],
    )
    async def test_the_row_is_named_and_the_batch_continues(
        self, make_database: MakeDatabase, assignment: str, reason: str,
    ) -> None:
        url = await make_database()
        await _prepare_db(url)
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                await install_program_state(connection)
                healthy = await insert_legacy_task(
                    connection, status='FAILED', kind=None,
                    disposition=None, retain=None,
                )
                refused = await insert_legacy_task(
                    connection, status='FAILED', kind=None,
                    disposition=None, retain=None,
                )
                await break_row(connection, refused, assignment)

                batches, final = await run_preparation_to_the_end(
                    connection, batch_size=10,
                )

                named = [row for batch in batches for row in batch.refused_rows]
                assert named == [RefusedRow(task_id=refused, reason=reason)]
                assert sum(batch.rows_prepared for batch in batches) == 1
                assert final == PreparationBlocked(
                    rows_prepared=1, rows_unprepared=1,
                )
                assert await disposition_of(connection, healthy) is not None
                assert await disposition_of(connection, refused) is None
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_a_batch_of_only_refused_rows_advances_the_walk(
        self, make_database: MakeDatabase,
    ) -> None:
        url = await make_database()
        await _prepare_db(url)
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                await install_program_state(connection)
                task_ids = [
                    await insert_legacy_task(
                        connection, status='FAILED', kind=None,
                        disposition=None, retain=None,
                    )
                    for _ in range(3)
                ]
                for task_id in task_ids:
                    await break_row(connection, task_id, 'priority = 0')

                batches, final = await run_preparation_to_the_end(
                    connection, batch_size=1,
                )

                assert [batch.rows_prepared for batch in batches] == [0, 0, 0]
                assert final == PreparationBlocked(
                    rows_prepared=0, rows_unprepared=3,
                )
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_a_refused_live_row_is_not_counted_as_prepared(
        self, make_database: MakeDatabase,
    ) -> None:
        url = await make_database()
        await _prepare_db(url)
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                await install_program_state(connection)
                healthy = await insert_legacy_task(
                    connection, status='FAILED', kind=None,
                    disposition=None, retain=None,
                )
                refused = await insert_legacy_task(
                    connection, status='PENDING', kind=None,
                    disposition=None, retain=None,
                )
                await break_row(connection, refused, 'priority = 0')

                batches, _ = await run_preparation_to_the_end(
                    connection, batch_size=10,
                )

                # One batch holds both rows, so the count is computed.
                assert len(batches) == 1
                assert batches[0].rows_prepared == 1
                assert batches[0].live_rows_prepared == 0
                assert await disposition_of(connection, healthy) is not None
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_a_repaired_row_completes_on_the_next_run(
        self, make_database: MakeDatabase,
    ) -> None:
        url = await make_database()
        await _prepare_db(url)
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                await install_program_state(connection)
                healthy = await insert_legacy_task(
                    connection, status='FAILED', kind=None,
                    disposition=None, retain=None,
                )
                refused = await insert_legacy_task(
                    connection, status='FAILED', kind=None,
                    disposition=None, retain=None,
                )
                await break_row(connection, refused, 'priority = 0')
                _, blocked = await run_preparation_to_the_end(
                    connection, batch_size=10,
                )
                assert isinstance(blocked, PreparationBlocked)

                await break_row(connection, refused, 'priority = 50')
                _, final = await run_preparation_to_the_end(
                    connection, batch_size=10,
                )

                assert final == PreparationComplete(rows_prepared=2)
                assert await disposition_of(connection, healthy) is not None
                assert await disposition_of(connection, refused) is not None
        finally:
            await engine.dispose()


class TestKeysetWatermark:
    @pytest.mark.asyncio
    async def test_the_watermark_bounds_the_selection(
        self, make_database: MakeDatabase
    ) -> None:
        """Rows at or below the watermark are never revisited.

        The functional half of the linearity guarantee: a watermark past
        every id selects nothing even while unprepared rows remain below
        it, and a threaded watermark walks the remainder exactly once in
        id order. If the keyset predicate is dropped, the past-everything
        call finds rows again and this fails.
        """
        url = await make_database()
        await _prepare_db(url)
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                await install_program_state(connection)
                for _ in range(4):
                    await insert_legacy_task(
                        connection,
                        status='FAILED',
                        kind=None,
                        disposition=None,
                        retain=None,
                        args_json='[]',
                        kwargs_json='{}',
                    )
                ordered = [
                    row.id
                    for row in (
                        await connection.execute(
                            text('SELECT id FROM horsies_tasks ORDER BY id')
                        )
                    ).all()
                ]

                beyond = await prepare_legacy_batch(
                    connection,
                    retain_default=True,
                    batch_size=2,
                    cursor=PreparationCursor(after_id=ordered[-1]),
                )
                # Nothing is selected past the watermark, and four rows
                # remain, so this is not a completion.
                assert beyond == PreparationBlocked(
                    rows_prepared=0, rows_unprepared=4,
                )

                first = await prepare_legacy_batch(
                    connection,
                    retain_default=True,
                    batch_size=2,
                    cursor=PreparationCursor.start(),
                )
                assert isinstance(first, PreparationBatch)
                assert first.rows_prepared == 2
                assert first.cursor.after_id == ordered[1]

                second = await prepare_legacy_batch(
                    connection,
                    retain_default=True,
                    batch_size=2,
                    cursor=first.cursor,
                )
                assert isinstance(second, PreparationBatch)
                assert second.rows_prepared == 2
                assert second.cursor.after_id == ordered[-1]

                done = await prepare_legacy_batch(
                    connection,
                    retain_default=True,
                    batch_size=2,
                    cursor=second.cursor,
                )
                assert isinstance(done, PreparationComplete)
        finally:
            await engine.dispose()
