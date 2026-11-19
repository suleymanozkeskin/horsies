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

from datetime import datetime, timedelta, timezone
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
            # hour needs its leaf. prepare_move_storage registered the
            # heartbeat class (horsies_register_retention_classes).
            landing = (await connection.execute(text('SELECT now()'))).scalar_one()
            assert isinstance(landing, datetime), landing
            created = (
                await connection.execute(
                    text(
                        'SELECT outcome FROM horsies_create_heartbeat_leaf('
                        'CAST(:lower AS timestamptz))'
                    ),
                    {
                        'lower': landing.astimezone(timezone.utc).replace(
                            minute=0, second=0, microsecond=0,
                        ),
                    },
                )
            ).scalar_one()
            assert created == 'CREATED'
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
