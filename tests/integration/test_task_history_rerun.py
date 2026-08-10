"""The rerun operation against real PostgreSQL: the ruled matrix.

A failed plain task reruns end to end — new identity, atomic lineage,
source record byte-untouched, the new PENDING row written strictly by
the provenance table with its envelope re-prepared canonically. Every
typed refusal is exercised: live source, absent source with floor
classification, both ineligibilities, input unavailable carrying the
caller's reference locator, corrupt envelope failing closed. Keyed
rerun rides the real reservation registry: replay returns the committed
rerun, and a key still reserved by the source conflicts by contract.
Policy resolution at enqueue is proven by flipping the effective class
between source and rerun.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.identity.keys import ScopedIdempotencyKey
from horsies.core.history.identity.reservations import (
    ReservationApplied,
    claim_key_reservation,
)
from horsies.core.history.rerun.input_envelope import (
    encode_input_envelope_v1,
)
from horsies.core.history.rerun.operations import (
    NotEligibleReason,
    RerunEnqueued,
    RerunEnqueuePolicy,
    RerunInputCorrupt,
    RerunInputUnavailable,
    RerunKeyConflict,
    RerunKeyReplay,
    RerunNotEligible,
    RerunSourceAbsent,
    RerunSourceLive,
    RerunTask,
    rerun_task,
)

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
CLASS_KEY = 'it_rerun'
SECOND_CLASS = 'it_rerun_b'
WORKER = 'worker-rerun-1'
INPUT_PAYLOAD = encode_input_envelope_v1(
    args=[1, 'x'], kwargs={'k': 'v'}, options={'timeout_ms': 5}
)

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_rerun'
)


def policy(
    *,
    class_key: str = CLASS_KEY,
    retain: bool = True,
) -> RerunEnqueuePolicy:
    return RerunEnqueuePolicy(
        retention_class_key=class_key,
        retain_rerun_input=retain,
        reservation_window=timedelta(hours=24),
    )


async def seed_failed_source(
    connection: AsyncConnection,
    *,
    prepared_inline: bytes = INPUT_PAYLOAD,
    prepared_disposition: str = 'INLINE',
) -> str:
    task_id = await insert_live_task(
        connection,
        class_key=CLASS_KEY,
        worker=WORKER,
        retain=True,
        prepared_disposition=prepared_disposition,
        prepared_inline=prepared_inline,
    )
    outcome = (
        await connection.execute(
            text(
                'SELECT outcome FROM horsies_fail_locked_task('
                'CAST(:task_id AS uuid), :worker, :result, '
                "'BOOM', 'exploded')"
            ),
            {
                'task_id': task_id,
                'worker': WORKER,
                'result': '{"error":{"code":"BOOM"}}',
            },
        )
    ).one()
    assert outcome.outcome == 'APPLIED'
    return task_id


async def live_row(connection: AsyncConnection, task_id: str) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_tasks '
                'WHERE id = CAST(:task_id AS uuid)'
            ),
            {'task_id': task_id},
        )
    ).one()


async def history_row_bytes(
    connection: AsyncConnection, task_id: str
) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_task_history '
                'WHERE task_id = CAST(:task_id AS uuid)'
            ),
            {'task_id': task_id},
        )
    ).one()


class TestRerunEndToEnd:
    @pytest.mark.asyncio
    async def test_failed_task_reruns_by_the_provenance_table(
        self, terminalization_schema: HistorySchema
    ) -> None:
        deadline = datetime.now(UTC) + timedelta(hours=2)
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            source = await seed_failed_source(connection)
            before = await history_row_bytes(connection, source)

            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=source, deadline=deadline),
                policy(),
            )
            assert isinstance(outcome, RerunEnqueued)
            assert outcome.source_task_id == source
            assert outcome.rerun_root_task_id == source
            assert outcome.new_task_id != source

            row = await live_row(connection, outcome.new_task_id)
            # Replayed from the source row or its carried envelope.
            assert row.task_name == 'it.move'
            assert row.queue_name == 'default'
            assert row.priority == 50
            assert row.args == '[1,"x"]'
            assert row.kwargs == '{"k":"v"}'
            assert row.task_options == '{"timeout_ms":5}'
            assert row.max_retries == 0
            assert row.is_workflow_task is False
            # Lineage, atomic pair.
            assert str(row.rerun_of_task_id) == source
            assert str(row.rerun_root_task_id) == source
            # Caller-explicit deadline.
            assert row.good_until == deadline
            # Fresh runtime state.
            assert row.status == 'PENDING'
            assert row.retry_count == 0
            assert row.claimed_by_worker_id is None
            # Policy resolved at enqueue, envelope re-prepared
            # canonically: byte-identical to the source's.
            assert row.retention_class_key == CLASS_KEY
            assert row.prepared_rerun_input_disposition == 'INLINE'
            assert bytes(row.prepared_rerun_input_inline) == INPUT_PAYLOAD
            assert bytes(row.input_digest) == sha256(INPUT_PAYLOAD).digest()

            after = await history_row_bytes(connection, source)
            assert before == after

    @pytest.mark.asyncio
    async def test_rerun_of_a_rerun_keeps_the_root(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            source = await seed_failed_source(connection)
            first = await rerun_task(
                connection,
                RerunTask(source_task_id=source, deadline=None),
                policy(),
            )
            assert isinstance(first, RerunEnqueued)
            # Fail the first rerun for real, then rerun it.
            await connection.execute(
                text(
                    'UPDATE horsies_tasks '
                    "SET status = 'RUNNING', "
                    'claimed_by_worker_id = :worker '
                    'WHERE id = CAST(:task_id AS uuid)'
                ),
                {'task_id': first.new_task_id, 'worker': WORKER},
            )
            await connection.execute(
                text(
                    'SELECT outcome FROM horsies_fail_locked_task('
                    "CAST(:task_id AS uuid), :worker, '{}', 'X', 'x')"
                ),
                {'task_id': first.new_task_id, 'worker': WORKER},
            )
            second = await rerun_task(
                connection,
                RerunTask(
                    source_task_id=first.new_task_id, deadline=None
                ),
                policy(),
            )
            assert isinstance(second, RerunEnqueued)
            assert second.source_task_id == first.new_task_id
            assert second.rerun_root_task_id == source

    @pytest.mark.asyncio
    async def test_policy_resolves_current_class_at_enqueue(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            await prepare_move_storage(connection, SECOND_CLASS)
            source = await seed_failed_source(connection)
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=source, deadline=None),
                policy(class_key=SECOND_CLASS),
            )
            assert isinstance(outcome, RerunEnqueued)
            row = await live_row(connection, outcome.new_task_id)
            assert row.retention_class_key == SECOND_CLASS

    @pytest.mark.asyncio
    async def test_unregistered_class_raises_before_any_write(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """A typo'd class must cost a refused call, not a new task that
        runs to completion and then has no history partition to land in."""
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            source = await seed_failed_source(connection)
            before = (
                await connection.execute(
                    text('SELECT count(*) FROM horsies_tasks')
                )
            ).scalar_one()
            with pytest.raises(ValueError, match='unknown retention class'):
                await rerun_task(
                    connection,
                    RerunTask(source_task_id=source, deadline=None),
                    policy(class_key='it_never_registered'),
                )
            after = (
                await connection.execute(
                    text('SELECT count(*) FROM horsies_tasks')
                )
            ).scalar_one()
            assert after == before, 'a refused rerun must write nothing'

    @pytest.mark.asyncio
    async def test_current_policy_can_decline_retention(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            source = await seed_failed_source(connection)
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=source, deadline=None),
                policy(retain=False),
            )
            assert isinstance(outcome, RerunEnqueued)
            row = await live_row(connection, outcome.new_task_id)
            assert row.prepared_rerun_input_disposition == (
                'DECLINED_BY_POLICY'
            )
            assert row.prepared_rerun_input_inline is None
            # The input identity still carries.
            assert bytes(row.input_digest) == sha256(INPUT_PAYLOAD).digest()


class TestTypedRefusals:
    @pytest.mark.asyncio
    async def test_live_source_refuses(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            live = await insert_live_task(
                connection, class_key=CLASS_KEY, worker=WORKER
            )
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=live, deadline=None),
                policy(),
            )
            assert outcome == RerunSourceLive(task_id=live)

    @pytest.mark.asyncio
    async def test_absent_source_classifies(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            ghost = str(uuid4())
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=ghost, deadline=None),
                policy(),
            )
            assert isinstance(outcome, RerunSourceAbsent)
            assert outcome.task_id == ghost

    @pytest.mark.asyncio
    async def test_completed_source_is_not_eligible(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection, class_key=CLASS_KEY, worker=WORKER
            )
            done = (
                await connection.execute(
                    text(
                        'SELECT outcome FROM horsies_complete_task_fused('
                        'CAST(:task_id AS uuid), :worker, NULL, :result, '
                        "'task_done', CAST(:task_id AS text))"
                    ),
                    {
                        'task_id': task_id,
                        'worker': WORKER,
                        'result': '{"ok":true}',
                    },
                )
            ).one()
            assert done.outcome == 'APPLIED'
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=task_id, deadline=None),
                policy(),
            )
            assert outcome == RerunNotEligible(
                task_id=task_id,
                reason=NotEligibleReason.COMPLETED_SOURCE,
            )

    @pytest.mark.asyncio
    async def test_workflow_backing_task_is_not_eligible(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                is_workflow_task=True,
            )
            workflow_id = await create_workflow(
                connection, status='RUNNING'
            )
            await link_workflow_node(
                connection,
                task_id,
                workflow_id=workflow_id,
                node_status='RUNNING',
            )
            await connection.execute(
                text(
                    'SELECT outcome FROM horsies_fail_locked_task('
                    "CAST(:task_id AS uuid), :worker, '{}', 'X', 'x')"
                ),
                {'task_id': task_id, 'worker': WORKER},
            )
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=task_id, deadline=None),
                policy(),
            )
            assert outcome == RerunNotEligible(
                task_id=task_id,
                reason=NotEligibleReason.WORKFLOW_TASK,
            )

    @pytest.mark.asyncio
    async def test_declined_input_is_unavailable_with_disposition(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            source = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                retain=False,
                prepared_disposition='DECLINED_BY_POLICY',
            )
            await connection.execute(
                text(
                    'SELECT outcome FROM horsies_fail_locked_task('
                    "CAST(:task_id AS uuid), :worker, '{}', 'X', 'x')"
                ),
                {'task_id': source, 'worker': WORKER},
            )
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=source, deadline=None),
                policy(),
            )
            assert outcome == RerunInputUnavailable(
                task_id=source,
                disposition='DECLINED_BY_POLICY',
                reference_locator=None,
            )

    @pytest.mark.asyncio
    async def test_corrupt_envelope_fails_closed(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            source = await seed_failed_source(connection)
            await connection.execute(
                text(
                    'UPDATE horsies_task_history '
                    'SET rerun_input_inline = :bad '
                    'WHERE task_id = CAST(:task_id AS uuid)'
                ),
                {'bad': b'tampered', 'task_id': source},
            )
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=source, deadline=None),
                policy(),
            )
            assert isinstance(outcome, RerunInputCorrupt)
            assert 'digest' in outcome.detail


class TestKeyedRerun:
    @pytest.mark.asyncio
    async def test_replay_returns_the_committed_rerun(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            source = await seed_failed_source(connection)
            first = await rerun_task(
                connection,
                RerunTask(
                    source_task_id=source,
                    deadline=None,
                    caller_key='retry-batch-7',
                ),
                policy(),
            )
            assert isinstance(first, RerunEnqueued)
            replay = await rerun_task(
                connection,
                RerunTask(
                    source_task_id=source,
                    deadline=None,
                    caller_key='retry-batch-7',
                ),
                policy(),
            )
            assert replay == RerunKeyReplay(
                existing_task_id=first.new_task_id
            )
            count = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM horsies_tasks '
                        'WHERE rerun_of_task_id = CAST(:source AS uuid)'
                    ),
                    {'source': source},
                )
            ).scalar_one()
            assert count == 1

    @pytest.mark.asyncio
    async def test_key_reserved_by_the_source_conflicts_by_contract(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """The frozen fingerprint covers rerun lineage, so the source's
        own reservation cannot alias its rerun — the conflict is the
        contract working."""
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            source = await seed_failed_source(connection)
            scoped = ScopedIdempotencyKey(
                task_name='it.move', key='original-key'
            )
            claimed = await claim_key_reservation(
                connection,
                key_digest=scoped.digest,
                key_scope_version=1,
                reservation_window_seconds=86400,
                fingerprint_version=1,
                fingerprint=sha256(b'the original command').digest(),
                task_id=source,
            )
            assert isinstance(claimed, ReservationApplied)
            outcome = await rerun_task(
                connection,
                RerunTask(
                    source_task_id=source,
                    deadline=None,
                    caller_key='original-key',
                ),
                policy(),
            )
            assert outcome == RerunKeyConflict(
                task_id=source, reserved_by_task_id=source
            )
            new_rows = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM horsies_tasks '
                        'WHERE rerun_of_task_id IS NOT NULL'
                    )
                )
            ).scalar_one()
            assert new_rows == 0
