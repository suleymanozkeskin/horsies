# pyright: reportPrivateUsage=false
"""Regression tests for verified task lifecycle correctness issues."""

from __future__ import annotations

import uuid
import asyncio
from datetime import datetime, timedelta, timezone
from typing import TypedDict, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.task_options import serialize_task_options
from horsies.core.models.tasks import (
    OperationalErrorCode,
    RetryPolicy,
    TaskError,
    TaskOptions,
    TaskResult,
)
from horsies.core.types.result import is_ok
from horsies.core.types.result import Err, Ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import (
    Worker,
    _FINALIZE_STAGE_PHASE2,
    _RequeueOutcome,
)
from horsies.core.workflows.lifecycle import pause_workflow
from tests.integration.conftest import compute_test_enqueue_sha

pytestmark = [pytest.mark.integration]


class _TaskRow(TypedDict):
    status: str
    retry_count: int
    error_code: str | None
    result: str | None
    next_retry_at: datetime | None
    finalizing_at: datetime | None
    finalizing_by_worker_id: str | None


def _make_worker(engine: AsyncEngine) -> Worker:
    sf = async_sessionmaker(engine, expire_on_commit=False)
    cfg = WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
    )
    worker = Worker(session_factory=sf, listener=MagicMock(), cfg=cfg)
    worker._spawn_background = MagicMock()  # type: ignore[method-assign]
    worker._schedule_delayed_notification = MagicMock(  # type: ignore[method-assign]
        return_value=None,
    )
    worker._finalize_workflow_phase = AsyncMock(  # type: ignore[method-assign]
        return_value=Ok(None),
    )
    return worker


async def _insert_owned_running_task(
    session: AsyncSession,
    *,
    worker_id: str,
    task_options: str | None = None,
    max_retries: int = 0,
    started_at: datetime | None = None,
    finalizing_at: datetime | None = None,
) -> str:
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(
        task_name='lifecycle_regression_task',
        task_options=task_options,
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, enqueued_at, created_at, updated_at,
                 claimed, retry_count, max_retries, started_at, enqueue_sha,
                 claimed_by_worker_id, worker_hostname, worker_pid,
                 worker_process_name, task_options, finalizing_at,
                 finalizing_by_worker_id)
            VALUES
                (:id, 'lifecycle_regression_task', 'default', 100, '[]', '{}',
                 'RUNNING', :sent_at, NOW(), NOW(), NOW(),
                 FALSE, 0, :max_retries, :started_at, :enqueue_sha,
                 :worker_id, 'itest-host', 4321,
                 'itest-process', :task_options, :finalizing_at,
                 :finalizing_by_worker_id)
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'enqueue_sha': sha,
            'worker_id': worker_id,
            'max_retries': max_retries,
            'task_options': task_options,
            'started_at': started_at or datetime.now(timezone.utc),
            'finalizing_at': finalizing_at,
            'finalizing_by_worker_id': worker_id if finalizing_at else None,
        },
    )
    await session.commit()
    return task_id


async def _task_row(session: AsyncSession, task_id: str) -> _TaskRow:
    row = (
        await session.execute(
            text("""
                SELECT status, retry_count, error_code, result, next_retry_at,
                       finalizing_at, finalizing_by_worker_id
                FROM horsies_tasks
                WHERE id = :id
            """),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None
    mapping = row._mapping
    return {
        'status': cast(str, mapping['status']),
        'retry_count': cast(int, mapping['retry_count']),
        'error_code': cast(str | None, mapping['error_code']),
        'result': cast(str | None, mapping['result']),
        'next_retry_at': cast(datetime | None, mapping['next_retry_at']),
        'finalizing_at': cast(datetime | None, mapping['finalizing_at']),
        'finalizing_by_worker_id': cast(
            str | None,
            mapping['finalizing_by_worker_id'],
        ),
    }


async def _insert_retry_window_workflow_task(
    session: AsyncSession,
    *,
    worker_id: str,
) -> tuple[str, str, str]:
    workflow_id = str(uuid.uuid4())
    workflow_task_id = str(uuid.uuid4())
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='lifecycle_regression_task')
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, sent_at, created_at, updated_at,
                 depth, root_workflow_id)
            VALUES
                (:workflow_id, 'retry-window-regression', 'RUNNING', 'fail',
                 NOW(), NOW(), NOW(), 0, :workflow_id)
        """),
        {'workflow_id': workflow_id},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs, status,
                 sent_at, enqueued_at, created_at, updated_at, claimed, claimed_at,
                 claimed_by_worker_id, claim_expires_at, retry_count, max_retries,
                 enqueue_sha, is_workflow_task)
            VALUES
                (:task_id, 'lifecycle_regression_task', 'default', 100, '[]', '{}',
                 'CLAIMED', :sent_at, NOW(), NOW(), NOW(), TRUE, NOW(),
                 :worker_id, NOW() + INTERVAL '60 seconds', 1, 3,
                 :enqueue_sha, TRUE)
        """),
        {
            'task_id': task_id,
            'sent_at': sent_at,
            'worker_id': worker_id,
            'enqueue_sha': sha,
        },
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args,
                 task_kwargs, queue_name, priority, dependencies, args_from,
                 workflow_ctx_from, allow_failed_deps, join_type, task_options,
                 status, task_id, is_subworkflow, created_at, started_at)
            VALUES
                (:workflow_task_id, :workflow_id, 0, 'retry_node',
                 'lifecycle_regression_task', '[]', '{}', 'default', 100,
                 ARRAY[]::integer[], NULL, NULL, FALSE, 'all', NULL,
                 'RUNNING', :task_id, FALSE, NOW(), NOW())
        """),
        {
            'workflow_task_id': workflow_task_id,
            'workflow_id': workflow_id,
            'task_id': task_id,
        },
    )
    await session.commit()
    return workflow_id, workflow_task_id, task_id


@pytest.mark.asyncio(loop_scope='function')
async def test_future_failure_on_non_retryable_running_task_is_terminal_failed(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A post-dispatch failure must not blindly requeue non-retryable RUNNING work."""
    worker = _make_worker(engine)
    task_id = await _insert_owned_running_task(
        session,
        worker_id=worker.worker_instance_id,
        max_retries=0,
    )

    outcome = await worker._recover_worker_future_failure(task_id, 'process died')

    assert outcome is _RequeueOutcome.REQUEUED
    row = await _task_row(session, task_id)
    assert row['status'] == 'FAILED'
    assert row['retry_count'] == 0
    assert row['error_code'] == OperationalErrorCode.WORKER_CRASHED.value
    assert row['result'] is not None
    attempts = (
        await session.execute(
            text("""
                SELECT outcome, will_retry, error_code
                FROM horsies_task_attempts
                WHERE task_id = :id
            """),
            {'id': task_id},
        )
    ).fetchall()
    assert attempts == [
        ('FAILED', False, OperationalErrorCode.WORKER_CRASHED.value)
    ]


@pytest.mark.asyncio(loop_scope='function')
async def test_future_failure_on_retryable_running_task_respects_retry_policy(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """WORKER_CRASHED retries only when task retry policy allows that code."""
    retry_policy = RetryPolicy.fixed(
        [1],
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
        jitter=False,
    )
    options = TaskOptions(
        task_name='lifecycle_regression_task',
        retry_policy=retry_policy,
    )
    serialized = serialize_task_options(options)
    assert is_ok(serialized)

    worker = _make_worker(engine)
    task_id = await _insert_owned_running_task(
        session,
        worker_id=worker.worker_instance_id,
        task_options=serialized.ok_value,
        max_retries=1,
    )

    outcome = await worker._recover_worker_future_failure(task_id, 'process died')

    assert outcome is _RequeueOutcome.REQUEUED
    row = await _task_row(session, task_id)
    assert row['status'] == 'PENDING'
    assert row['retry_count'] == 1
    assert row['error_code'] is None
    assert row['next_retry_at'] is not None


@pytest.mark.asyncio(loop_scope='function')
async def test_future_failure_terminal_phase2_error_schedules_finalize_retry(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Crash recovery must not drop phase-2 errors after committing phase 1."""
    worker = _make_worker(engine)
    task_id = await _insert_owned_running_task(
        session,
        worker_id=worker.worker_instance_id,
        max_retries=0,
    )
    phase2_err = worker._make_finalize_error(
        task_id=task_id,
        stage=_FINALIZE_STAGE_PHASE2,
        message='forced phase2 failure',
        retryable=True,
    )
    worker._finalize_workflow_phase = AsyncMock(  # type: ignore[method-assign]
        return_value=Err(phase2_err),
    )
    worker._handle_finalize_error = AsyncMock()  # type: ignore[method-assign]

    outcome = await worker._recover_worker_future_failure(task_id, 'process died')

    assert outcome is _RequeueOutcome.REQUEUED
    row = await _task_row(session, task_id)
    assert row['status'] == 'FAILED'
    assert row['error_code'] == OperationalErrorCode.WORKER_CRASHED.value
    worker._handle_finalize_error.assert_awaited_once_with(phase2_err)


@pytest.mark.asyncio(loop_scope='function')
async def test_finalize_worker_failure_runs_phase2(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Worker-failure terminal paths return a TaskResult so phase 2 still runs."""
    worker = _make_worker(engine)
    task_id = await _insert_owned_running_task(
        session,
        worker_id=worker.worker_instance_id,
    )

    phase2_calls: list[tuple[str, TaskResult[object, TaskError]]] = []

    async def _phase2(
        task_id_arg: str,
        result: TaskResult[object, TaskError],
        **_: object,
    ) -> object:
        phase2_calls.append((task_id_arg, result))
        return Ok(None)

    worker._finalize_workflow_phase = _phase2  # type: ignore[method-assign]
    fut = asyncio.get_running_loop().create_future()
    fut.set_result((False, '', 'bookkeeping failed'))

    result = await worker._finalize_after(
        fut,
        task_id,
        queue_name='default',
        is_workflow_task=False,
        task_name='lifecycle_regression_test',
        executor=MagicMock(),
    )

    assert is_ok(result)
    assert len(phase2_calls) == 1
    assert phase2_calls[0][0] == task_id
    assert phase2_calls[0][1].is_err()
    row = await _task_row(session, task_id)
    assert row['status'] == 'FAILED'
    assert row['error_code'] == OperationalErrorCode.BROKER_ERROR.value


@pytest.mark.asyncio(loop_scope='function')
async def test_reaper_skips_recent_finalizing_task(
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A completed child waiting on parent finalization is not reaped."""
    task_id = await _insert_owned_running_task(
        session,
        worker_id='dead-worker',
        started_at=datetime.now(timezone.utc) - timedelta(minutes=10),
        finalizing_at=datetime.now(timezone.utc),
    )

    result = await broker.mark_stale_tasks_as_failed(
        stale_threshold_ms=1_000,
        finalizing_stale_threshold_ms=300_000,
    )

    assert is_ok(result)
    assert result.ok_value == 0
    row = await _task_row(session, task_id)
    assert row['status'] == 'RUNNING'
    assert row['finalizing_at'] is not None


@pytest.mark.asyncio(loop_scope='function')
async def test_pause_resets_retry_window_claimed_workflow_task(
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Pause remains resumable when a retry-window workflow task is CLAIMED."""
    workflow_id, workflow_task_id, task_id = await _insert_retry_window_workflow_task(
        session,
        worker_id='retry-window-worker',
    )

    result = await pause_workflow(broker, workflow_id)

    assert is_ok(result)
    assert result.ok_value is True
    row = (
        await session.execute(
            text("""
                SELECT w.status AS workflow_status,
                       wt.status AS workflow_task_status,
                       wt.task_id AS workflow_task_task_id,
                       t.status AS task_status,
                       t.error_code
                FROM horsies_workflows w
                JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
                JOIN horsies_tasks t ON t.id = :task_id
                WHERE w.id = :workflow_id
                  AND wt.id = :workflow_task_id
            """),
            {
                'workflow_id': workflow_id,
                'workflow_task_id': workflow_task_id,
                'task_id': task_id,
            },
        )
    ).fetchone()
    assert row is not None
    assert row.workflow_status == 'PAUSED'
    assert row.workflow_task_status == 'READY'
    assert row.workflow_task_task_id is None
    assert row.task_status == 'CANCELLED'
    assert row.error_code == 'TASK_CANCELLED'


@pytest.mark.asyncio(loop_scope='function')
async def test_reaper_recovers_stale_runner_when_owner_worker_state_is_fresh(
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A live parent worker does not prove its child process is healthy."""
    worker_id = f'live-{uuid.uuid4().hex}'
    task_id = await _insert_owned_running_task(
        session,
        worker_id=worker_id,
        started_at=datetime.now(timezone.utc) - timedelta(minutes=10),
    )
    await session.execute(
        text("""
            INSERT INTO horsies_worker_states (
                worker_id, snapshot_at, hostname, pid, processes,
                max_claim_batch, max_claim_per_worker, queues,
                tasks_running, tasks_claimed, worker_started_at
            ) VALUES (
                :worker_id, NOW(), 'itest-host', 9999, 1,
                0, 1, :queues,
                1, 0, NOW() - INTERVAL '1 minute'
            )
        """),
        {'worker_id': worker_id, 'queues': ['default']},
    )
    await session.commit()

    result = await broker.mark_stale_tasks_as_failed(
        stale_threshold_ms=1_000,
        finalizing_stale_threshold_ms=1_000,
    )

    assert is_ok(result)
    assert result.ok_value == 1
    row = await _task_row(session, task_id)
    assert row['status'] == 'FAILED'
    assert row['error_code'] == OperationalErrorCode.WORKER_CRASHED.value
    assert row['result'] is not None


@pytest.mark.asyncio(loop_scope='function')
async def test_reaper_recovers_old_finalizing_task_and_clears_marker(
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A stale finalization handoff is recoverable and stops reporting finalizing."""
    worker_id = f'old-finalizing-{uuid.uuid4().hex}'
    task_id = await _insert_owned_running_task(
        session,
        worker_id=worker_id,
        started_at=datetime.now(timezone.utc) - timedelta(minutes=10),
        finalizing_at=datetime.now(timezone.utc) - timedelta(minutes=10),
    )

    result = await broker.mark_stale_tasks_as_failed(
        stale_threshold_ms=1_000,
        finalizing_stale_threshold_ms=1_000,
    )

    assert is_ok(result)
    assert result.ok_value == 1
    row = await _task_row(session, task_id)
    assert row['status'] == 'FAILED'
    assert row['error_code'] == OperationalErrorCode.WORKER_CRASHED.value
    assert row['finalizing_at'] is None
    assert row['finalizing_by_worker_id'] is None
