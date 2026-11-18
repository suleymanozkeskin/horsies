# pyright: reportPrivateUsage=false
"""Regression tests for verified task lifecycle correctness issues."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import TypedDict, cast

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import (
    OperationalErrorCode,
)
from horsies.core.types.result import is_ok
from horsies.core.workflows.lifecycle import pause_workflow
from tests.integration.conftest import compute_test_command_fingerprint

pytestmark = [pytest.mark.integration]

# The claim generation of every RUNNING row seeded here, which the fences of
# the worker check.
CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)


class _TaskRow(TypedDict):
    status: str
    retry_count: int
    error_code: str | None
    result: str | None
    next_retry_at: datetime | None
    finalizing_at: datetime | None
    finalizing_by_worker_id: str | None
    terminalization_kind: str | None


async def _insert_owned_running_task(
    session: AsyncSession,
    *,
    worker_id: str,
    task_options: str | None = None,
    max_retries: int = 0,
    started_at: datetime | None = None,
    finalizing_at: datetime | None = None,
    is_workflow_task: bool = False,
) -> str:
    """A RUNNING row of ``worker_id``; a workflow task gets its node."""
    task_id = str(uuid.uuid4())
    sent_at, fingerprint = compute_test_command_fingerprint(
        task_name='lifecycle_regression_task',
        task_options=task_options,
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    enqueued_at, created_at, updated_at, retry_count, max_retries,
                    started_at, claimed_by_worker_id, claimed_at, worker_hostname,
                    worker_pid, worker_process_name, task_options, finalizing_at,
                    finalizing_by_worker_id, is_workflow_task, retention_class_key,
                    command_fingerprint_version, command_fingerprint,
                    retain_rerun_input, execution
                ) VALUES (
                    :id, 'lifecycle_regression_task', 'default', 100, '{}', 'RUNNING',
                    :sent_at, NOW(), NOW(), NOW(), 0, :max_retries, :started_at,
                    :worker_id, :claimed_at, 'itest-host', 4321, 'itest-process',
                    :task_options, :finalizing_at, :finalizing_by_worker_id,
                    :is_workflow_task, 'standard_30d',
                    CASE WHEN CAST(:is_workflow_task AS boolean) THEN NULL ELSE 1 END,
                    CASE WHEN CAST(:is_workflow_task AS boolean) THEN NULL ELSE :command_fingerprint END,
                    FALSE, 'ISOLATED'
                )
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'command_fingerprint': fingerprint,
            'worker_id': worker_id,
            'claimed_at': CLAIMED_AT,
            'max_retries': max_retries,
            'task_options': task_options,
            'started_at': started_at or datetime.now(timezone.utc),
            'finalizing_at': finalizing_at,
            'finalizing_by_worker_id': worker_id if finalizing_at else None,
            'is_workflow_task': is_workflow_task,
        },
    )
    if is_workflow_task:
        await _link_running_node(session, task_id)
    await session.commit()
    return task_id


async def _link_running_node(session: AsyncSession, task_id: str) -> None:
    """A RUNNING workflow and its RUNNING node backed by ``task_id``."""
    workflow_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, sent_at, created_at, updated_at,
                 depth, root_workflow_id)
            VALUES (:workflow_id, 'lifecycle-regression', 'RUNNING', 'fail',
                    NOW(), NOW(), NOW(), 0, :workflow_id)
        """),
        {'workflow_id': workflow_id},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                 queue_name, priority, dependencies,
                 allow_failed_deps, join_type, status, task_id,
                 is_subworkflow, created_at, started_at,
                 max_retries, execution, retention_class_key)
            VALUES ('NONE', :node_id, :workflow_id, 0, 'node-0',
                    'lifecycle_regression_task', '{}', 'default', 100,
                    ARRAY[]::integer[], FALSE, 'all', 'RUNNING',
                    CAST(:task_id AS uuid), FALSE, NOW(), NOW(),
                    0, 'ISOLATED', 'standard_30d')
        """),
        {
            'node_id': str(uuid.uuid4()),
            'workflow_id': workflow_id,
            'task_id': task_id,
        },
    )


async def _task_row(session: AsyncSession, task_id: str) -> _TaskRow:
    row = (
        await session.execute(
            text("""
                SELECT status, retry_count, error_code, result, next_retry_at,
                       finalizing_at, finalizing_by_worker_id,
                       terminalization_kind
                FROM itest_task_rows
                WHERE id = CAST(:id AS uuid)
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
        'terminalization_kind': cast(
            str | None,
            mapping['terminalization_kind'],
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
    sent_at, fingerprint = compute_test_command_fingerprint(task_name='lifecycle_regression_task')
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
                (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    enqueued_at, created_at, updated_at, claimed_at,
                    claimed_by_worker_id, claim_expires_at, retry_count, max_retries,
                    is_workflow_task, retention_class_key, command_fingerprint_version,
                    command_fingerprint, retain_rerun_input, execution
                ) VALUES (
                    :task_id, 'lifecycle_regression_task', 'default', 100, '{}',
                    'CLAIMED', :sent_at, NOW(), NOW(), NOW(), NOW(), :worker_id,
                    NOW() + INTERVAL '60 seconds', 1, 3, TRUE, 'standard_30d', NULL,
                    NULL, FALSE, 'ISOLATED'
                )
        """),
        {
            'task_id': task_id,
            'sent_at': sent_at,
            'worker_id': worker_id,
            'command_fingerprint': fingerprint,
        },
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                 queue_name, priority, dependencies, args_from,
                 workflow_ctx_from, allow_failed_deps, join_type, task_options,
                 status, task_id, is_subworkflow, created_at, started_at,
                 max_retries, execution, retention_class_key)
            VALUES
                ('NONE', :workflow_task_id, :workflow_id, 0, 'retry_node',
                 'lifecycle_regression_task', '{}', 'default', 100,
                 ARRAY[]::integer[], NULL, NULL, FALSE, 'all', NULL,
                 'RUNNING', :task_id, FALSE, NOW(), NOW(),
                 0, 'ISOLATED', 'standard_30d')
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

    result = await pause_workflow(broker, uuid.UUID(workflow_id))

    assert is_ok(result)
    assert result.ok_value is True
    row = (
        await session.execute(
            text("""
                SELECT w.status AS workflow_status,
                       wt.status AS workflow_task_status,
                       wt.task_id AS workflow_task_task_id,
                       t.status AS task_status,
                       t.error_code,
                       t.terminalization_kind
                FROM horsies_workflows w
                JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
                JOIN itest_task_rows t ON t.id = CAST(:task_id AS uuid)
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
    assert row.terminalization_kind == 'PAUSE_ABANDON_WORKFLOW'


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
                queue_priorities, queue_max_concurrency, recovery_config,
                tasks_running, tasks_claimed, worker_started_at,
                slots_down, class_capacity
            ) VALUES (
                :worker_id, NOW(), 'itest-host', 9999, 1,
                0, 1, :queues,
                '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
                1, 0, NOW() - INTERVAL '1 minute',
                '[]'::jsonb, '{}'::jsonb
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
    assert row['terminalization_kind'] == 'FAIL_STALE'


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
    assert row['terminalization_kind'] == 'FAIL_STALE'
