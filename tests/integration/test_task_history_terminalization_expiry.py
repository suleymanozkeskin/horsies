"""The expiry family against real PostgreSQL.

Owned expiry proves the deadline guard from a single locked capture —
applied on an elapsed deadline, DEADLINE refusal with the capture's own
facts otherwise. The discovery batch proves the set-wise move: mixed
plain and workflow-backing rows in one sweep, only transitioned rows
returned, oldest-first bound honored, SKIP LOCKED stepping around a
concurrently held row, empty attempt snapshots for never-started tasks,
set-wise pending creation, the batch deferred-result fence, and the
non-positive batch-size raise.
"""

from __future__ import annotations

from datetime import timedelta, timezone
from hashlib import sha256
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.archive.attempts import decode_attempt_snapshot
from horsies.core.history.archive.versions import DecodedArchiveValue

from tests.integration.task_history_harness import (
    HistorySchema,
    insert_live_task,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration]

UTC = timezone.utc
CLASS_KEY = 'it_expire'
WORKER = 'worker-expire-1'

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_expire'
)


async def expire_owned(
    connection: AsyncConnection,
    task_id: str,
    *,
    worker: str = WORKER,
) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_expire_owned_claim('
                'CAST(:task_id AS uuid), :worker, '
                "'{\"error\":{\"code\":\"TASK_EXPIRED\"}}', 'TASK_EXPIRED')"
            ),
            {'task_id': task_id, 'worker': worker},
        )
    ).one()


async def expire_pending(
    connection: AsyncConnection,
    *,
    batch_size: int = 10,
    result: str | None = '{"error":{"code":"TASK_EXPIRED"}}',
) -> list[Any]:
    return list(
        (
            await connection.execute(
                text(
                    'SELECT * FROM horsies_expire_pending_tasks('
                    ':batch_size, :result, \'TASK_EXPIRED\')'
                ),
                {'batch_size': batch_size, 'result': result},
            )
        ).all()
    )


class TestOwnedExpiry:
    @pytest.mark.asyncio
    async def test_elapsed_deadline_expires_with_the_pre_image(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='CLAIMED',
                worker=WORKER,
                good_until_offset=timedelta(minutes=-5),
            )
            outcome = await expire_owned(connection, task_id)
            assert outcome.outcome == 'APPLIED'
            assert outcome.terminalization_kind == 'EXPIRE_CLAIMED'
            assert outcome.observed_status == 'CLAIMED'
            assert outcome.observed_worker_id == WORKER
            status = (
                await connection.execute(
                    text(
                        'SELECT status FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert status == 'EXPIRED'

    @pytest.mark.asyncio
    async def test_future_deadline_refuses_with_the_capture(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='CLAIMED',
                worker=WORKER,
                good_until_offset=timedelta(minutes=30),
            )
            refusal = await expire_owned(connection, task_id)
            assert refusal.outcome == 'SOURCE_STATE_CONFLICT'
            assert refusal.guard_kind == 'DEADLINE'
            assert set(refusal.observed_guard) == {
                'good_until',
                'evaluated_at',
            }
            live = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM horsies_tasks '
                        'WHERE id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert live == 1

    @pytest.mark.asyncio
    async def test_foreign_claim_is_a_lost_claim(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='CLAIMED',
                worker='worker-other',
                good_until_offset=timedelta(minutes=-5),
            )
            outcome = await expire_owned(connection, task_id)
            assert outcome.outcome == 'LOST_CLAIM'
            assert outcome.observed_worker_id == 'worker-other'


class TestDiscoveryBatch:
    @pytest.mark.asyncio
    async def test_mixed_batch_moves_only_eligible_rows(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            expired_plain = [
                await insert_live_task(
                    connection,
                    class_key=CLASS_KEY,
                    status='PENDING',
                    worker=None,
                    good_until_offset=timedelta(minutes=-(index + 1)),
                )
                for index in range(3)
            ]
            wf_task = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='PENDING',
                worker=None,
                is_workflow_task=True,
                retain=True,
                prepared_disposition='INLINE',
                good_until_offset=timedelta(minutes=-10),
            )
            workflow_id = str(uuid4())
            node_row_id = str(uuid4())
            await connection.execute(
                text(
                    'INSERT INTO horsies_workflow_tasks '
                    '(id, workflow_id, task_id, task_index) VALUES '
                    '(CAST(:node_id AS uuid), CAST(:workflow_id AS uuid), '
                    'CAST(:task_id AS uuid), 0)'
                ),
                {
                    'node_id': node_row_id,
                    'workflow_id': workflow_id,
                    'task_id': wf_task,
                },
            )
            not_expired = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='PENDING',
                worker=None,
                good_until_offset=timedelta(minutes=30),
            )
            claimed = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='CLAIMED',
                worker=WORKER,
                good_until_offset=timedelta(minutes=-5),
            )

            outcomes = await expire_pending(connection)
            moved = {str(row.task_id) for row in outcomes}
            assert moved == {*expired_plain, wf_task}
            assert all(row.outcome == 'APPLIED' for row in outcomes)
            assert all(
                row.terminalization_kind == 'EXPIRE_PENDING'
                for row in outcomes
            )
            assert all(row.observed_status == 'PENDING' for row in outcomes)

            survivors = {
                str(row.id)
                for row in (
                    await connection.execute(
                        text('SELECT id FROM horsies_tasks')
                    )
                ).all()
            }
            assert survivors == {not_expired, claimed}

            snapshot = (
                await connection.execute(
                    text(
                        'SELECT attempt_snapshot, attempt_snapshot_digest, '
                        'rerun_input_disposition '
                        'FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': expired_plain[0]},
                )
            ).one()
            decoded = decode_attempt_snapshot(
                version=1,
                codec='json-utf8',
                content_type='application/json',
                payload=bytes(snapshot.attempt_snapshot),
                digest=bytes(snapshot.attempt_snapshot_digest),
            )
            assert decoded == DecodedArchiveValue(())
            assert snapshot.rerun_input_disposition == 'INLINE'

            pending = (
                await connection.execute(
                    text(
                        'SELECT workflow_id, recovery_source, '
                        'terminal_status, result_digest '
                        'FROM horsies_workflow_phase2_pending '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': wf_task},
                )
            ).one()
            assert str(pending.workflow_id) == workflow_id
            assert pending.recovery_source == 'HISTORY'
            assert pending.terminal_status == 'EXPIRED'
            assert bytes(pending.result_digest) == sha256(
                b'{"error":{"code":"TASK_EXPIRED"}}'
            ).digest()

            wf_disposition = (
                await connection.execute(
                    text(
                        'SELECT rerun_input_disposition '
                        'FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': wf_task},
                )
            ).scalar_one()
            assert wf_disposition == 'NEVER_ELIGIBLE'

    @pytest.mark.asyncio
    async def test_oldest_first_bound_honors_the_batch_size(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            oldest = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='PENDING',
                worker=None,
                good_until_offset=timedelta(minutes=-30),
            )
            middle = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='PENDING',
                worker=None,
                good_until_offset=timedelta(minutes=-20),
            )
            newest = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='PENDING',
                worker=None,
                good_until_offset=timedelta(minutes=-10),
            )
            outcomes = await expire_pending(connection, batch_size=2)
            moved = {str(row.task_id) for row in outcomes}
            assert moved == {oldest, middle}
            survivor = (
                await connection.execute(
                    text('SELECT id FROM horsies_tasks')
                )
            ).scalar_one()
            assert str(survivor) == newest

    @pytest.mark.asyncio
    async def test_skip_locked_steps_around_a_held_row(
        self, terminalization_schema: HistorySchema
    ) -> None:
        engine = terminalization_schema.engine
        async with engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            held = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='PENDING',
                worker=None,
                good_until_offset=timedelta(minutes=-10),
            )
            free = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='PENDING',
                worker=None,
                good_until_offset=timedelta(minutes=-5),
            )
        async with engine.connect() as holder:
            await holder.execute(text('BEGIN'))
            await holder.execute(
                text(
                    'SELECT id FROM horsies_tasks '
                    'WHERE id = CAST(:task_id AS uuid) FOR UPDATE'
                ),
                {'task_id': held},
            )
            async with engine.begin() as connection:
                outcomes = await expire_pending(connection)
                moved = {str(row.task_id) for row in outcomes}
                assert moved == {free}
            await holder.execute(text('ROLLBACK'))
        async with engine.begin() as connection:
            still_live = (
                await connection.execute(
                    text('SELECT id FROM horsies_tasks')
                )
            ).scalar_one()
            assert str(still_live) == held

    @pytest.mark.asyncio
    async def test_empty_sweep_returns_no_rows(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            assert await expire_pending(connection) == []

    @pytest.mark.asyncio
    async def test_non_positive_batch_size_raises_typed(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.connect() as connection:
            with pytest.raises(DBAPIError, match='positive integer'):
                await expire_pending(connection, batch_size=0)

    @pytest.mark.asyncio
    async def test_batch_deferred_fence_raises_before_any_move(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            wf_task = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='PENDING',
                worker=None,
                is_workflow_task=True,
                good_until_offset=timedelta(minutes=-10),
            )
            await connection.execute(
                text(
                    'INSERT INTO horsies_workflow_tasks '
                    '(id, workflow_id, task_id, task_index) VALUES '
                    '(CAST(:node_id AS uuid), CAST(:workflow_id AS uuid), '
                    'CAST(:task_id AS uuid), 0)'
                ),
                {
                    'node_id': str(uuid4()),
                    'workflow_id': str(uuid4()),
                    'task_id': wf_task,
                },
            )
            with pytest.raises(
                DBAPIError, match='requires a result payload'
            ):
                await expire_pending(connection, result=None)
