"""The expiry family against real PostgreSQL.

Owned expiry proves the deadline guard from a single locked capture —
applied on an elapsed deadline, DEADLINE refusal with the capture's own
facts otherwise. The discovery batch proves the set-wise move: only
transitioned rows returned, oldest-first bound honored, SKIP LOCKED
stepping around a concurrently held row, the batch deferred-result fence,
and the non-positive batch-size raise. The mixed sweep of plain and
workflow rows (empty attempt snapshots, the phase-2 records) is a core
test (``core/src/broker/lifecycle_tests/
task_history_terminalization_expiry.rs``, ``mixed_batch_moves_only_eligible_rows``).
"""

from __future__ import annotations

from datetime import timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.integration.task_history_harness import (
    HistorySchema,
    create_workflow,
    insert_live_task,
    link_workflow_node,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration]

UTC = timezone.utc
CLASS_KEY = 'standard_30d'
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
    async def test_outcomes_follow_ascending_deadlines(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """Outcome order is deadline order, not id, heap, or insertion order.

        Each later insert mints a larger v7 id and lands later in the heap
        while carrying an earlier deadline, so id order and heap order are
        both the reverse of deadline order.
        """
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            inserted = [
                await insert_live_task(
                    connection,
                    class_key=CLASS_KEY,
                    status='PENDING',
                    worker=None,
                    good_until_offset=timedelta(minutes=-minutes),
                )
                for minutes in (10, 20, 30, 40)
            ]
            outcomes = await expire_pending(connection, batch_size=10)
            assert [str(row.task_id) for row in outcomes] == inserted[::-1]

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
            _ = await link_workflow_node(
                connection,
                wf_task,
                workflow_id=await create_workflow(
                    connection, status='RUNNING'
                ),
                node_status='ENQUEUED',
            )
            with pytest.raises(
                DBAPIError, match='requires a result payload'
            ):
                await expire_pending(connection, result=None)
