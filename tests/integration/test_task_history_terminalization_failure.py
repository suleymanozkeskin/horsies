"""The failure-family move against real PostgreSQL.

The failure family carries the envelope-eligible branch: FAILED is not
NEVER_ELIGIBLE, so this suite proves the full disposition ladder against
the confirmed gate semantics — workflow-backing archives NEVER_ELIGIBLE,
a declined snapshot archives DECLINED_BY_POLICY, and a retained input
is framed byte-identically with its digest. It is also the
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

from horsies.core.history.heartbeats.partitioning import (
    CreateHourlyHeartbeatLeaf,
    HeartbeatClassRegistered,
    create_hourly_heartbeat_leaf,
    hourly_leaf_ref,
    register_heartbeat_class,
)
from horsies.core.history.outcomes import LeafCreated
from horsies.core.history.partitions.catalog import database_now
from tests.integration.task_history_harness import (
    HistorySchema,
    create_workflow,
    insert_live_task,
    link_workflow_node,
    prepare_move_storage,
    terminalization_schema_fixture,
)
from tests.rerun_envelope_support import frame_rerun_envelope

pytestmark = [pytest.mark.integration]

UTC = timezone.utc
CLASS_KEY = 'standard_30d'
WORKER = 'worker-fail-1'

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_fail'
)


async def fail_running(
    connection: AsyncConnection,
    task_id: str,
    *,
    worker: str = WORKER,
    error_code: str | None = 'BOOM',
    failed_reason: str | None = 'exploded',
) -> Any:
    """The worker's failure of a RUNNING task (``horsies_finish_failed_attempt``,
    no retry, kind FAIL_RUNNING) under the claim that the row holds."""
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_finish_failed_attempt('
                'CAST(:task_id AS uuid), :worker, '
                '(SELECT claimed_at FROM horsies_tasks '
                'WHERE id = CAST(:task_id AS uuid)), '
                "'NONE', NULL, NULL, NULL, :result, :error_code, :failed_reason, "
                "'FAILED', NULL, :failed_reason, 'REFUSE', TRUE)"
            ),
            {
                'task_id': task_id,
                'worker': worker,
                'result': '{"error":{"code":"BOOM"}}',
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
    """The stale RUNNING pass's failure (``horsies_finish_stale_attempt``, no
    retry, kind FAIL_STALE)."""
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_finish_stale_attempt('
                "CAST(:task_id AS uuid), 'BEAT_AGE', CAST(:stale_ms AS INTEGER), "
                "CAST(:finalizing_ms AS INTEGER), NULL, NULL, 'NONE', NULL, NULL, "
                "'{\"error\":{\"code\":\"STALE\"}}', 'STALE', 'went silent', "
                "'went silent', 'went silent')"
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
            )
            outcome = await fail_running(connection, task_id)
            assert outcome.outcome == 'APPLIED'
            row = await history_disposition(connection, task_id)
            assert row.status == 'FAILED'
            assert row.rerun_input_disposition == 'DECLINED_BY_POLICY'
            assert row.rerun_input_inline is None

    @pytest.mark.asyncio
    async def test_retained_input_is_framed_byte_identically(
        self, terminalization_schema: HistorySchema
    ) -> None:
        input_text = '{"precious":"failure input"}'
        payload = frame_rerun_envelope(input_text, None)
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                retain=True,
                input_text=input_text,
            )
            await fail_running(connection, task_id)
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
                is_workflow_task=True,
            )
            workflow_id = await create_workflow(connection, status='RUNNING')
            node_row_id = await link_workflow_node(
                connection,
                task_id,
                workflow_id=workflow_id,
                node_status='RUNNING',
            )
            outcome = await fail_running(connection, task_id)
            assert outcome.outcome == 'APPLIED'
            row = await history_disposition(connection, task_id)
            assert row.rerun_input_disposition == 'NEVER_ELIGIBLE'
            assert row.rerun_input_inline is None

            pending = (
                await connection.execute(
                    text(
                        'SELECT workflow_id, workflow_node_row_id, '
                        'recovery_source, history_class, history_anchor, '
                        'result_digest, terminalization_kind '
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
            assert pending.terminalization_kind == 'FAIL_RUNNING'
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
            _ = await link_workflow_node(
                connection,
                task_id,
                workflow_id=await create_workflow(
                    connection, status='RUNNING'
                ),
                node_status='RUNNING',
            )
            # The move itself, with no result: the writers of a run refuse
            # a NULL result before they call it.
            with pytest.raises(
                DBAPIError, match='requires a result payload'
            ):
                await connection.execute(
                    text(
                        'SELECT horsies_move_task_to_history('
                        '(SELECT t FROM horsies_tasks AS t '
                        'WHERE t.id = CAST(:task_id AS uuid) FOR UPDATE), '
                        "'FAILED', 'FAIL_RUNNING', NOW(), NULL, 'BOOM', 'exploded')"
                    ),
                    {'task_id': task_id},
                )


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
            await fail_running(
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
            # The heartbeat table has no default partition: the beat's
            # hour needs its leaf.
            registered = await register_heartbeat_class(
                connection, horizon=timedelta(hours=2),
            )
            assert isinstance(registered, HeartbeatClassRegistered)
            landing = await database_now(connection)
            created = await create_hourly_heartbeat_leaf(
                connection,
                CreateHourlyHeartbeatLeaf(
                    leaf=hourly_leaf_ref(
                        landing.replace(minute=0, second=0, microsecond=0),
                    ),
                ),
            )
            assert isinstance(created, LeafCreated)
            await connection.execute(
                text(
                    'INSERT INTO horsies_heartbeats '
                    '(task_id, sender_id, role, sent_at, hostname, pid) '
                    "VALUES (CAST(:task_id AS uuid), :worker, 'runner', "
                    "statement_timestamp(), 'it-host', 4242)"
                ),
                {'task_id': task_id, 'worker': WORKER},
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
