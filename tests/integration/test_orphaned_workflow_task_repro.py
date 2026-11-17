"""Focused repro for orphaned workflow task claim-budget leaks."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.types.result import is_ok
from horsies.core.worker.child_pool import _get_child_core_handle, _initialize_worker_pool
from horsies.core.worker.child_runner import (
    _WorkflowResumedBeforeStart,
    _after_missed_start,
    _confirm_until_not_resumed,
)
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.sql import COUNT_IN_FLIGHT_FOR_WORKER_SQL
from horsies.core.worker.worker import Worker
from tests.dispatch_claim_support import claim_at
from tests.integration.conftest import compute_test_command_fingerprint
from tests.broker_resources_support import current_resources, listener_of

# The claim generation of every row seeded here, which the fences of the
# worker check.
CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)


async def _seed_orphaned_claimed_workflow_task(
    session: AsyncSession,
    *,
    worker_id: str,
) -> str:
    task_id = str(uuid4())
    sent_at, fingerprint = compute_test_command_fingerprint(
        task_name='workflow_join_barrier',
        queue_name='default',
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, status, sent_at,
                enqueued_at, created_at, updated_at, claimed_at, claimed_by_worker_id,
                claim_expires_at, retry_count, max_retries, is_workflow_task,
                retention_class_key, command_fingerprint_version, command_fingerprint,
                retain_rerun_input, execution
            ) VALUES (
                :id, 'workflow_join_barrier', 'default', 100, '{}', 'CLAIMED',
                :sent_at, NOW(), NOW(), NOW(), :claimed_at, :worker_id,
                NOW() + INTERVAL '5 minutes', 0, 0, TRUE, 'standard_30d', NULL, NULL,
                FALSE, 'ISOLATED'
            )
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'claimed_at': CLAIMED_AT,
            'worker_id': worker_id,
            'command_fingerprint': fingerprint,
        },
    )
    await session.commit()
    return task_id


async def _task_status(session: AsyncSession, task_id: str) -> str:
    row = (
        await session.execute(
            text(
                'SELECT status FROM itest_task_rows '
                'WHERE id = CAST(:id AS uuid)'
            ),
            {'id': task_id},
        )
    ).one()
    return str(row.status)


async def _terminalization_kind(
    session: AsyncSession,
    task_id: str,
) -> str | None:
    row = (
        await session.execute(
            text(
                'SELECT terminalization_kind FROM itest_task_rows '
                'WHERE id = CAST(:id AS uuid)'
            ),
            {'id': task_id},
        )
    ).one()
    value = row.terminalization_kind
    return str(value) if value is not None else None


async def _in_flight_for_worker(session: AsyncSession, worker_id: str) -> int:
    row = (
        await session.execute(
            COUNT_IN_FLIGHT_FOR_WORKER_SQL,
            {'wid': worker_id},
        )
    ).one()
    return int(row.cnt or 0)


async def _link_runnable_workflow_node(
    session: AsyncSession,
    task_id: str,
) -> None:
    workflow_id = str(uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES
                (:id, 'orphan_guard_workflow', 'RUNNING', 'fail', 0, :id,
                 NOW(), NOW(), NOW(), NOW())
        """),
        {'id': workflow_id},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                 queue_name, priority, dependencies,
                 allow_failed_deps, join_type, is_subworkflow, status,
                 task_id, created_at,
                 max_retries, execution, retention_class_key)
            VALUES
                ('NONE', :id, :workflow_id, 0, 'orphan_guard_node',
                 'workflow_join_barrier', '{}', 'default', 100, '{}',
                 FALSE, 'all', FALSE, 'RUNNING', :task_id, NOW(),
                 0, 'ISOLATED', 'standard_30d')
        """),
        {
            'id': str(uuid4()),
            'workflow_id': workflow_id,
            'task_id': task_id,
        },
    )
    await session.commit()


def _orphan_worker(broker: PostgresBroker, db_url: str, worker_id: str, *, self_heal: bool) -> Worker:
    """A worker of ``worker_id`` whose recovery config sets the orphan
    self-heal to ``self_heal``."""
    worker = Worker(
        session_factory=current_resources(broker).session_factory,
        listener=MagicMock(),
        cfg=WorkerConfig(
            dsn=db_url,
            psycopg_dsn=db_url.replace('+psycopg', ''),
            queues=['default'],
            recovery_config=RecoveryConfig(
                auto_terminate_orphaned_workflow_tasks=self_heal,
            ),
        ),
    )
    worker.worker_instance_id = worker_id
    return worker


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
async def test_the_stop_before_start_cancels_an_orphan_and_frees_budget(
    clean_workflow_tables: None,
    session: AsyncSession,
    broker: PostgresBroker,
    db_url: str,
) -> None:
    """Lever 2: the child's stop before start cancels a genuine orphan.

    A CLAIMED workflow task with no live workflow_task linkage can never reach
    RUNNING. Its start misses with NODE_NOT_LINKED, and the stop before start
    (with the orphan self-heal of the run, from the worker's recovery config)
    gives it a terminal disposition (CANCELLED) instead of leaving it CLAIMED,
    so in-flight budget frees and the reaper no longer requeues it. The
    worker's finalize of ORPHAN_CANCELLED then owes nothing.
    """
    _ = clean_workflow_tables
    worker_id = 'worker-orphan-repro'
    task_id = await _seed_orphaned_claimed_workflow_task(
        session,
        worker_id=worker_id,
    )
    assert await _in_flight_for_worker(session, worker_id) == 1
    worker = _orphan_worker(broker, db_url, worker_id, self_heal=True)
    _initialize_worker_pool(listener_of(broker).database_url)

    stopped = _confirm_until_not_resumed(
        task_id,
        worker_id,
        True,
        CLAIMED_AT,
        orphan_self_heal=worker.cfg.orphan_self_heal(),
    )
    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=datetime.now(timezone.utc),
        ok=False,
        result_json_str='',
        failed_reason='ORPHAN_CANCELLED',
        task_name='workflow_join_barrier',
        queue_name='default',
        is_workflow_task=True,
        claim=claim_at(CLAIMED_AT),
    )

    assert stopped == (False, '', 'ORPHAN_CANCELLED')
    assert is_ok(result)
    assert await _task_status(session, task_id) == 'CANCELLED'
    assert await _terminalization_kind(session, task_id) == 'CANCEL_ORPHAN'
    assert await _in_flight_for_worker(session, worker_id) == 0


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
async def test_the_stop_before_start_does_not_cancel_a_task_with_a_runnable_node_link(
    clean_workflow_tables: None,
    session: AsyncSession,
    broker: PostgresBroker,
) -> None:
    """The live-link guard: a stop before start whose task a runnable node
    links writes nothing. The claim is still startable in a RUNNING
    workflow, so the stop answers that the start runs again."""
    _ = (clean_workflow_tables, broker)
    worker_id = 'worker-orphan-stop-live-link'
    task_id = await _seed_orphaned_claimed_workflow_task(session, worker_id=worker_id)
    await _link_runnable_workflow_node(session, task_id)

    stopped = _after_missed_start(
        _get_child_core_handle(),
        task_id,
        worker_id,
        'NODE_NOT_LINKED',
        CLAIMED_AT,
        orphan_self_heal=True,
    )

    assert stopped == _WorkflowResumedBeforeStart()
    assert await _task_status(session, task_id) == 'CLAIMED'
    assert await _terminalization_kind(session, task_id) is None
    assert await _in_flight_for_worker(session, worker_id) == 1


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_check_failed_disabled_leaves_orphan_claimed(
    clean_workflow_tables: None,
    session: AsyncSession,
    broker: PostgresBroker,
    db_url: str,
) -> None:
    """With self-heal disabled, the orphan stays CLAIMED (inspection mode).

    The flag gates the cancel of the stop before start symmetrically with the
    reaper step: off => the child reports WORKFLOW_CHECK_FAILED with nothing
    written, the finalize writes nothing, and (separately) the reaper does
    not requeue it either.
    """
    _ = clean_workflow_tables
    worker_id = 'worker-orphan-disabled'
    task_id = await _seed_orphaned_claimed_workflow_task(session, worker_id=worker_id)
    worker = _orphan_worker(broker, db_url, worker_id, self_heal=False)
    _initialize_worker_pool(listener_of(broker).database_url)

    stopped = _confirm_until_not_resumed(
        task_id,
        worker_id,
        True,
        CLAIMED_AT,
        orphan_self_heal=worker.cfg.orphan_self_heal(),
    )
    assert stopped == (False, '', 'WORKFLOW_CHECK_FAILED')
    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=datetime.now(timezone.utc),
        ok=False,
        result_json_str='',
        failed_reason='WORKFLOW_CHECK_FAILED',
        task_name='workflow_join_barrier',
        queue_name='default',
        is_workflow_task=True,
        claim=claim_at(CLAIMED_AT),
    )

    assert is_ok(result)
    assert await _task_status(session, task_id) == 'CLAIMED'


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
async def test_finalize_does_not_cancel_a_task_with_a_runnable_node_link(
    clean_workflow_tables: None,
    session: AsyncSession,
    broker: PostgresBroker,
    db_url: str,
) -> None:
    """The live-link guard refusal remains a successful finalize no-op."""
    _ = clean_workflow_tables
    worker_id = 'worker-orphan-live-link'
    task_id = await _seed_orphaned_claimed_workflow_task(
        session,
        worker_id=worker_id,
    )
    await _link_runnable_workflow_node(session, task_id)
    worker = Worker(
        session_factory=current_resources(broker).session_factory,
        listener=MagicMock(),
        cfg=WorkerConfig(
            dsn=db_url,
            psycopg_dsn=db_url.replace('+psycopg', ''),
            queues=['default'],
            recovery_config=RecoveryConfig(),
        ),
    )
    worker.worker_instance_id = worker_id

    result = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=datetime.now(timezone.utc),
        ok=False,
        result_json_str='',
        failed_reason='WORKFLOW_CHECK_FAILED',
        task_name='workflow_join_barrier',
        queue_name='default',
        is_workflow_task=True,
        claim=claim_at(CLAIMED_AT),
    )

    assert is_ok(result)
    assert await _task_status(session, task_id) == 'CLAIMED'
    assert await _terminalization_kind(session, task_id) is None
    assert await _in_flight_for_worker(session, worker_id) == 1


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
async def test_reaper_terminates_orphaned_workflow_task(
    clean_workflow_tables: None,
    session: AsyncSession,
    broker: PostgresBroker,
) -> None:
    """Lever 3b: the reaper self-heals an orphan even without a dispatch cycle."""
    _ = clean_workflow_tables
    worker_id = 'worker-orphan-reaper'
    task_id = await _seed_orphaned_claimed_workflow_task(session, worker_id=worker_id)

    result = await broker.terminate_orphaned_workflow_tasks()

    assert is_ok(result)
    assert result.ok_value == 1
    assert await _task_status(session, task_id) == 'CANCELLED'
    assert await _terminalization_kind(session, task_id) == 'CANCEL_ORPHAN_SWEEP'
    assert await _in_flight_for_worker(session, worker_id) == 0


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
async def test_requeue_skips_orphaned_workflow_task(
    clean_workflow_tables: None,
    session: AsyncSession,
    broker: PostgresBroker,
) -> None:
    """Lever 3a: requeue must not re-dispatch an orphan (the churn engine)."""
    _ = clean_workflow_tables
    worker_id = 'worker-orphan-requeue'
    task_id = await _seed_orphaned_claimed_workflow_task(session, worker_id=worker_id)
    # Make the claim stale so it would otherwise be requeued.
    await session.execute(
        text("""
            UPDATE horsies_tasks
            SET claimed_at = NOW() - INTERVAL '3600 seconds'
            WHERE id = :id
        """),
        {'id': task_id},
    )
    await session.commit()

    result = await broker.requeue_stale_claimed(stale_threshold_ms=1_000)

    assert is_ok(result)
    assert result.ok_value == 0, 'orphan must not be requeued'
    assert await _task_status(session, task_id) == 'CLAIMED'
