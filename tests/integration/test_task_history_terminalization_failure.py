"""The failure-family move against real PostgreSQL.

The failure family carries the envelope-eligible branch: FAILED is not
NEVER_ELIGIBLE, so this suite proves the full disposition ladder against
the confirmed gate semantics — workflow-backing archives NEVER_ELIGIBLE,
a declined snapshot archives DECLINED_BY_POLICY, and a retained prepared
envelope carries byte-identically with its copied digest. It is also the
first family to exercise deferred phase-2 pending creation, and the
staleness guard's single-capture evidence contract.
"""

from __future__ import annotations

from datetime import timedelta, timezone
from hashlib import sha256
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.integration.task_history_harness import (
    HistorySchema,
    insert_live_task,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration]

UTC = timezone.utc
CLASS_KEY = 'it_fail'
WORKER = 'worker-fail-1'

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_fail'
)


async def fail_locked(
    connection: AsyncConnection,
    task_id: str,
    *,
    worker: str = WORKER,
    result: str | None = '{"error":{"code":"BOOM"}}',
    error_code: str | None = 'BOOM',
    failed_reason: str | None = 'exploded',
) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_fail_locked_task('
                'CAST(:task_id AS uuid), :worker, :result, '
                ':error_code, :failed_reason)'
            ),
            {
                'task_id': task_id,
                'worker': worker,
                'result': result,
                'error_code': error_code,
                'failed_reason': failed_reason,
            },
        )
    ).one()


async def fail_stale(
    connection: AsyncConnection,
    task_id: str,
    *,
    stale_after_ms: int = 1_000,
) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_fail_stale_task('
                'CAST(:task_id AS uuid), :stale_ms, :finalizing_ms, '
                "'{\"error\":{\"code\":\"STALE\"}}', 'STALE', 'went silent')"
            ),
            {
                'task_id': task_id,
                'stale_ms': stale_after_ms,
                'finalizing_ms': stale_after_ms,
            },
        )
    ).one()


async def history_disposition(
    connection: AsyncConnection, task_id: str
) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT status, final_failed_reason, error_code, '
                'rerun_input_disposition, rerun_input_inline, '
                'rerun_input_digest '
                'FROM horsies_task_history '
                'WHERE task_id = CAST(:task_id AS uuid)'
            ),
            {'task_id': task_id},
        )
    ).one()


class TestDispositionLadder:
    @pytest.mark.asyncio
    async def test_declined_snapshot_archives_declined_by_policy(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                retain=False,
                prepared_disposition='DECLINED_BY_POLICY',
            )
            outcome = await fail_locked(connection, task_id)
            assert outcome.outcome == 'APPLIED'
            row = await history_disposition(connection, task_id)
            assert row.status == 'FAILED'
            assert row.rerun_input_disposition == 'DECLINED_BY_POLICY'
            assert row.rerun_input_inline is None

    @pytest.mark.asyncio
    async def test_retained_prepared_envelope_carries_byte_identically(
        self, terminalization_schema: HistorySchema
    ) -> None:
        payload = b'{"precious":"failure input"}'
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                retain=True,
                prepared_disposition='INLINE',
                prepared_inline=payload,
            )
            await fail_locked(connection, task_id)
            row = await history_disposition(connection, task_id)
            assert row.rerun_input_disposition == 'INLINE'
            assert bytes(row.rerun_input_inline) == payload
            assert bytes(row.rerun_input_digest) == sha256(payload).digest()

    @pytest.mark.asyncio
    async def test_workflow_backing_failure_is_never_eligible_with_pending(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                retain=True,
                prepared_disposition='INLINE',
                is_workflow_task=True,
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
                    'task_id': task_id,
                },
            )
            outcome = await fail_locked(connection, task_id)
            assert outcome.outcome == 'APPLIED'
            row = await history_disposition(connection, task_id)
            assert row.rerun_input_disposition == 'NEVER_ELIGIBLE'
            assert row.rerun_input_inline is None

            pending = (
                await connection.execute(
                    text(
                        'SELECT workflow_id, workflow_node_row_id, '
                        'recovery_source, history_class, history_anchor, '
                        'result_digest, terminal_status '
                        'FROM horsies_workflow_phase2_pending '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).one()
            assert str(pending.workflow_id) == workflow_id
            assert str(pending.workflow_node_row_id) == node_row_id
            assert pending.recovery_source == 'HISTORY'
            assert pending.history_class == CLASS_KEY
            assert pending.terminal_status == 'FAILED'
            assert bytes(pending.result_digest) == sha256(
                b'{"error":{"code":"BOOM"}}'
            ).digest()
            terminal_at = (
                await connection.execute(
                    text(
                        'SELECT terminal_at FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert pending.history_anchor == terminal_at


class TestDeferredResultFence:
    @pytest.mark.asyncio
    async def test_workflow_failure_without_result_raises_typed(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """pending.result_digest is NOT NULL and sha256(NULL) is NULL: the
        kind-agnostic deferred guard must fence failure kinds before the
        insert, proven here rather than inherited from a reading."""
        from sqlalchemy.exc import DBAPIError

        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                is_workflow_task=True,
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
                    'task_id': task_id,
                },
            )
            with pytest.raises(
                DBAPIError, match='requires a result payload'
            ):
                await fail_locked(connection, task_id, result=None)


class TestFailedReasonOwnership:
    @pytest.mark.asyncio
    async def test_null_reason_clears_rather_than_carries(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection, class_key=CLASS_KEY, worker=WORKER
            )
            await fail_locked(
                connection, task_id, error_code=None, failed_reason=None
            )
            row = await history_disposition(connection, task_id)
            assert row.final_failed_reason is None
            assert row.error_code is None


class TestStalenessGuard:
    @pytest.mark.asyncio
    async def test_silent_runner_is_failed_cross_worker(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker='worker-silent',
                started_at_offset=timedelta(minutes=-10),
            )
            outcome = await fail_stale(connection, task_id)
            assert outcome.outcome == 'APPLIED'
            assert outcome.terminalization_kind == 'FAIL_STALE'
            assert outcome.observed_worker_id == 'worker-silent'
            row = await history_disposition(connection, task_id)
            assert row.status == 'FAILED'

    @pytest.mark.asyncio
    async def test_fresh_heartbeat_refuses_with_the_capture(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                started_at_offset=timedelta(minutes=-10),
            )
            await connection.execute(
                text(
                    'INSERT INTO horsies_heartbeats (task_id, role, sent_at) '
                    "VALUES (CAST(:task_id AS uuid), 'runner', "
                    'statement_timestamp())'
                ),
                {'task_id': task_id},
            )
            refusal = await fail_stale(connection, task_id)
            assert refusal.outcome == 'SOURCE_STATE_CONFLICT'
            assert refusal.guard_kind == 'STALENESS'
            evidence = refusal.observed_guard
            assert set(evidence) == {
                'last_heartbeat_at',
                'started_at',
                'finalizing_at',
                'stale_after_ms',
                'finalizing_stale_after_ms',
                'evaluated_at',
            }
            assert evidence['last_heartbeat_at'] is not None
            live_still = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM horsies_tasks '
                        'WHERE id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert live_still == 1

    @pytest.mark.asyncio
    async def test_absent_task_classifies_through_the_null_claim_path(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            absent = await fail_stale(connection, str(uuid4()))
            assert absent.outcome == 'TASK_ABSENT'
