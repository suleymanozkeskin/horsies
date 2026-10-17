"""The moves encode an attempt snapshot once and hash a result once.

A counting wrapper replaces the attempt encoder inside the test schema.
A batch move must call it once per moved task and still store snapshot
bytes that decode to the seeded attempts under their stored digest. The
single-row deferred move must write the history row's result digest to
the deferred record.
"""

from __future__ import annotations

from datetime import timedelta
from hashlib import sha256

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.archive.attempts import decode_attempt_snapshot
from horsies.core.history.archive.versions import DecodedArchiveValue
from tests.integration.task_history_harness import (
    HistorySchema,
    create_workflow,
    insert_live_task,
    link_workflow_node,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration]

CLASS_KEY = 'it_snapshot_reuse'
WORKER = 'worker-snapshot-reuse'

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_snapshot_reuse'
)


async def count_encoder_calls(connection: AsyncConnection) -> None:
    """Route every attempt-encoder call through a counter in this schema."""
    await connection.execute(text('CREATE SEQUENCE encoder_calls'))
    await connection.execute(text(
        'ALTER FUNCTION horsies_encode_task_attempts(uuid) '
        'RENAME TO horsies_encode_task_attempts_counted'
    ))
    await connection.execute(text("""
        CREATE FUNCTION horsies_encode_task_attempts(p_task_id uuid)
        RETURNS bytea
        LANGUAGE plpgsql
        STABLE
        STRICT
        AS $function$
        BEGIN
            PERFORM nextval('encoder_calls');
            RETURN horsies_encode_task_attempts_counted(p_task_id);
        END
        $function$
    """))


async def encoder_call_count(connection: AsyncConnection) -> int:
    return int((await connection.execute(text(
        'SELECT CASE WHEN is_called THEN last_value ELSE 0 END '
        'FROM encoder_calls'
    ))).scalar_one())


async def insert_attempts(
    connection: AsyncConnection,
    task_id: str,
    count: int,
) -> None:
    for attempt in range(1, count + 1):
        await connection.execute(
            text("""
                INSERT INTO horsies_task_attempts (
                    task_id, attempt, outcome, will_retry,
                    started_at, finished_at, worker_id
                ) VALUES (
                    CAST(:task_id AS uuid), :attempt, 'FAILED', TRUE,
                    NOW() - interval '2 minutes', NOW() - interval '1 minute',
                    :worker
                )
            """),
            {'task_id': task_id, 'attempt': attempt, 'worker': WORKER},
        )


class TestBatchSnapshotReuse:
    @pytest.mark.asyncio
    async def test_batch_move_encodes_each_snapshot_once(
        self, terminalization_schema: HistorySchema,
    ) -> None:
        attempts_by_task: dict[str, int] = {}
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            for attempt_count in (0, 1, 2):
                task_id = await insert_live_task(
                    connection,
                    class_key=CLASS_KEY,
                    status='PENDING',
                    worker=None,
                    good_until_offset=timedelta(minutes=-5),
                )
                await insert_attempts(connection, task_id, attempt_count)
                attempts_by_task[task_id] = attempt_count
            await count_encoder_calls(connection)

            outcomes = (await connection.execute(text(
                'SELECT * FROM horsies_expire_pending_tasks('
                "10, '{\"error\":{\"code\":\"TASK_EXPIRED\"}}', "
                "'TASK_EXPIRED')"
            ))).all()

            assert {str(row.task_id) for row in outcomes} == set(attempts_by_task)
            assert {row.outcome for row in outcomes} == {'APPLIED'}
            assert await encoder_call_count(connection) == len(attempts_by_task)
            history = (await connection.execute(text(
                'SELECT task_id, attempt_archive_version, attempt_snapshot, '
                'attempt_snapshot_digest FROM horsies_task_history'
            ))).all()
            assert len(history) == len(attempts_by_task)
            for row in history:
                decoded = decode_attempt_snapshot(
                    version=row.attempt_archive_version,
                    codec='json-utf8',
                    content_type='application/json',
                    payload=bytes(row.attempt_snapshot),
                    digest=bytes(row.attempt_snapshot_digest),
                )
                assert isinstance(decoded, DecodedArchiveValue)
                assert [a.attempt for a in decoded.value] == list(
                    range(1, attempts_by_task[str(row.task_id)] + 1),
                )


class TestDeferredDigestReuse:
    @pytest.mark.asyncio
    async def test_deferred_record_carries_the_history_result_digest(
        self, terminalization_schema: HistorySchema,
    ) -> None:
        result = '{"ok":true}'
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                is_workflow_task=True,
            )
            await link_workflow_node(
                connection,
                task_id,
                workflow_id=await create_workflow(connection, status='RUNNING'),
                node_status='RUNNING',
            )
            outcome = (await connection.execute(
                text(
                    'SELECT * FROM horsies_complete_locked_task('
                    'CAST(:task_id AS uuid), :worker, :result)'
                ),
                {'task_id': task_id, 'worker': WORKER, 'result': result},
            )).one()
            assert outcome.outcome == 'APPLIED'
            digests = (await connection.execute(
                text("""
                    SELECT h.result_digest AS history_digest,
                           p.result_digest AS deferred_digest
                    FROM horsies_task_history h
                    JOIN horsies_workflow_phase2_pending p
                        ON p.task_id = h.task_id
                    WHERE h.task_id = CAST(:task_id AS uuid)
                """),
                {'task_id': task_id},
            )).one()
            expected = sha256(result.encode()).digest()
            assert bytes(digests.history_digest) == expected
            assert bytes(digests.deferred_digest) == expected
