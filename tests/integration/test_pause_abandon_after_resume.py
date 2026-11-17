"""A pause abandon must not apply after the workflow was resumed.

A worker reads the workflow as PAUSED, a resume commits, and then the
worker abandons its claim. Without a pause re-check under the workflow
lock, the abandon cancels the task and the node is reset to READY with no
task while the workflow is RUNNING; only workflow recovery enqueues it.
With the re-check, the abandon refuses and the claim is run instead. The
claim-time settle reads the status again under the workflow row lock.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.lifecycle.commands import AbandonOwnedNodes
from horsies.core.lifecycle.fences import OwnedClaimBatch
from horsies.core.lifecycle.outcomes import (
    Applied,
    ObservedWorkflowState,
    SourceStateConflict,
)
from horsies.core.lifecycle.persistence import apply_batch_async
from horsies.core.types.result import is_ok
from horsies.core.utils.url import to_psycopg_url
from horsies.core.worker import child_runner
from horsies.core.worker.child_pool import _get_child_core_handle, _initialize_worker_pool
from horsies._core import CoreChildHandle, CoreHandle
from horsies.core.worker.claiming import ClaimMixin
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.queue_cap_hold import QueueCapHold, start_check_caps
from horsies.core.workflows.lifecycle import resume_workflow
from horsies.core.utils.db import PooledStatements
from tests.integration.conftest import DB_URL, compute_test_command_fingerprint
from tests.broker_resources_support import current_resources, listener_of

pytestmark = [pytest.mark.integration]

WORKER_ID = 'abandon-after-resume-worker'


async def _seed_paused_workflow_with_claimed_node(
    session: AsyncSession,
    claimed_at: datetime,
) -> tuple[str, str]:
    task_id = str(uuid.uuid4())
    workflow_id = str(uuid.uuid4())
    sent_at, fingerprint = compute_test_command_fingerprint(task_name='abandon_after_resume')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    enqueued_at, created_at, updated_at, retry_count, max_retries,
                    claimed_at, claimed_by_worker_id, claim_expires_at,
                    is_workflow_task, retention_class_key, command_fingerprint_version,
                    command_fingerprint, retain_rerun_input, execution
                ) VALUES (
                    :id, 'abandon_after_resume', 'default', 100, '{}', 'CLAIMED',
                    :sent_at, NOW(), NOW(), NOW(), 0, 0, :claimed_at, :worker_id,
                    NOW() + interval '1 minute', TRUE, 'standard_30d', NULL, NULL,
                    FALSE, 'ISOLATED'
                )
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'claimed_at': claimed_at,
            'worker_id': WORKER_ID,
            'command_fingerprint': fingerprint,
        },
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES (:id, 'abandon_after_resume_wf', 'PAUSED', 'fail', 0, :id,
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
            VALUES ('NONE', :id, :workflow_id, 0, 'node_0', 'abandon_after_resume',
                    '{}', 'default', 100, '{}', FALSE, 'all', FALSE,
                    'ENQUEUED', :task_id, NOW(),
                    0, 'ISOLATED', 'standard_30d')
        """),
        {'id': str(uuid.uuid4()), 'workflow_id': workflow_id, 'task_id': task_id},
    )
    await session.commit()
    return workflow_id, task_id


def _claim_filter_probe(broker: PostgresBroker) -> ClaimMixin:
    cfg = WorkerConfig(dsn='unused', psycopg_dsn='unused', queues=['default'])

    class _Probe(ClaimMixin):
        def __init__(self) -> None:
            self.sf = current_resources(broker).session_factory
            self._claim_runner = PooledStatements(self.sf)
            self.cfg = cfg
            self.worker_instance_id = WORKER_ID
            self._queue_cap_hold = QueueCapHold(
                start_check_caps(cfg.prefetch_buffer, cfg.queue_max_concurrency),
            )

        @property
        def core_handle(self) -> CoreHandle:
            return broker._core_handle  # pyright: ignore[reportPrivateUsage]

        async def _dispatch_one(
            self, *args: Any, **kwargs: Any,
        ) -> asyncio.Future[None]:
            # A finalizer that does not end while the test runs.
            return asyncio.get_running_loop().create_future()

    return _Probe()


async def _node_and_task(
    session: AsyncSession,
    workflow_id: str,
    task_id: str,
) -> tuple[str, str | None, str, str | None]:
    """(node status, node task id, workflow status, live task status)."""
    node = (
        await session.execute(
            text("""
                SELECT wt.status, wt.task_id, w.status AS workflow_status
                FROM horsies_workflow_tasks wt
                JOIN horsies_workflows w ON w.id = wt.workflow_id
                WHERE wt.workflow_id = CAST(:wf AS uuid)
            """),
            {'wf': workflow_id},
        )
    ).one()
    live_task = (
        await session.execute(
            text('SELECT status FROM horsies_tasks WHERE id = CAST(:id AS uuid)'),
            {'id': task_id},
        )
    ).scalar_one_or_none()
    node_task = None if node.task_id is None else str(node.task_id)
    return str(node.status), node_task, str(node.workflow_status), live_task


_LOCK_WAIT_POLLS = 200
_LOCK_WAIT_POLL_S = 0.05
_SETTLE_END_S = 10.0


async def _until_a_backend_waits_on_a_lock(
    session_factory: async_sessionmaker[AsyncSession],
    query_part: str,
) -> None:
    """Return once a backend whose query contains ``query_part`` waits on a lock."""
    for _ in range(_LOCK_WAIT_POLLS):
        async with session_factory() as probe:
            waiting = (
                await probe.execute(
                    text("""
                        SELECT count(*) FROM pg_stat_activity
                        WHERE datname = current_database()
                          AND wait_event_type = 'Lock'
                          AND query LIKE '%' || :part || '%'
                          AND pid <> pg_backend_pid()
                    """),
                    {'part': query_part},
                )
            ).scalar_one()
        if waiting > 0:
            return
        await asyncio.sleep(_LOCK_WAIT_POLL_S)
    raise AssertionError(f'no backend waited on a lock running {query_part!r}')


@pytest.mark.asyncio(loop_scope='function')
async def test_claim_filter_dispatches_a_claim_resumed_before_its_settle(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The settle reads the workflow PAUSED, then waits for its row lock; a
    transaction that holds the row makes it RUNNING, as a resume does, and
    commits. The settle reads the status again under the lock and the claim
    is dispatched, with nothing written."""
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    workflow_id, task_id = await _seed_paused_workflow_with_claimed_node(
        session,
        claimed_at,
    )
    session_factory = current_resources(broker).session_factory
    rows = [{'id': task_id, 'is_workflow_task': True, 'claimed_at': claimed_at}]

    async with session_factory() as holder:
        _ = await holder.execute(
            text("""
                UPDATE horsies_workflows SET status = 'RUNNING', updated_at = NOW()
                WHERE id = CAST(:wf AS uuid) AND status = 'PAUSED'
            """),
            {'wf': workflow_id},
        )
        settle = asyncio.ensure_future(
            _claim_filter_probe(broker)._filter_nonrunnable_workflow_tasks(rows),
        )
        await _until_a_backend_waits_on_a_lock(
            session_factory,
            'horsies_workflow_claimed_tasks_settle',
        )
        await holder.commit()
    dispatched = await asyncio.wait_for(settle, _SETTLE_END_S)

    assert [row['id'] for row in dispatched] == [task_id]
    assert await _node_and_task(session, workflow_id, task_id) == (
        'ENQUEUED',
        task_id,
        'RUNNING',
        'CLAIMED',
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_batch_abandon_refuses_after_a_committed_resume(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    workflow_id, task_id = await _seed_paused_workflow_with_claimed_node(
        session,
        claimed_at,
    )
    resumed = await resume_workflow(broker, workflow_id)
    assert is_ok(resumed) and resumed.ok_value is True

    async with current_resources(broker).session_factory() as worker_session:
        outcomes = await apply_batch_async(
            await worker_session.connection(),
            AbandonOwnedNodes(
                fence=OwnedClaimBatch(
                    worker_id=WORKER_ID,
                    claim_generations=((task_id, claimed_at),),
                ),
            ),
        )
        await worker_session.commit()

    match outcomes:
        case [
            SourceStateConflict(
                task_id=refused_id,
                evidence=ObservedWorkflowState(
                    workflow_id=evidence_workflow,
                    workflow_status='RUNNING',
                ),
            ),
        ]:
            assert refused_id == task_id
            assert evidence_workflow == workflow_id
        case _:
            raise AssertionError(f'expected a WORKFLOW_STATUS refusal: {outcomes}')
    assert await _node_and_task(session, workflow_id, task_id) == (
        'ENQUEUED',
        task_id,
        'RUNNING',
        'CLAIMED',
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_batch_abandon_applies_while_still_paused(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    workflow_id, task_id = await _seed_paused_workflow_with_claimed_node(
        session,
        claimed_at,
    )

    async with current_resources(broker).session_factory() as worker_session:
        outcomes = await apply_batch_async(
            await worker_session.connection(),
            AbandonOwnedNodes(
                fence=OwnedClaimBatch(
                    worker_id=WORKER_ID,
                    claim_generations=((task_id, claimed_at),),
                ),
            ),
        )
        await worker_session.commit()

    match outcomes:
        case [Applied(task_id=applied_id)]:
            assert applied_id == task_id
        case _:
            raise AssertionError(f'expected the abandon to apply: {outcomes}')
    _, _, workflow_status, live_task = await _node_and_task(
        session,
        workflow_id,
        task_id,
    )
    assert (workflow_status, live_task) == ('PAUSED', None)


def _set_workflow_running(workflow_id: str) -> None:
    """Commit the resume's status flip from a separate connection."""
    with psycopg.connect(to_psycopg_url(DB_URL), autocommit=True) as resumer:
        resumer.execute(
            "UPDATE horsies_workflows SET status = 'RUNNING' WHERE id = %s",
            (workflow_id,),
        )


class _ResumeBeforeStop:
    """The core handle of the child, with a resume of ``workflow_id`` that
    commits right before the stop before start."""

    def __init__(self, handle: CoreChildHandle, workflow_id: str) -> None:
        self._handle = handle
        self._workflow_id = workflow_id

    def start_attempt(self, *args: Any, **kwargs: Any) -> Any:
        return self._handle.start_attempt(*args, **kwargs)

    def stop_workflow_task_before_start(self, *args: Any, **kwargs: Any) -> Any:
        _set_workflow_running(self._workflow_id)
        return self._handle.stop_workflow_task_before_start(*args, **kwargs)


@pytest.mark.asyncio(loop_scope='function')
async def test_child_starts_a_task_resumed_before_its_abandon(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The start misses on the paused workflow; the resume commits before
    the stop judges the workflow under its lock, so the stop finds the claim
    still startable (RESUMED) and the next attempt starts the task."""
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    workflow_id, task_id = await _seed_paused_workflow_with_claimed_node(
        session,
        claimed_at,
    )
    _initialize_worker_pool(listener_of(broker).database_url)
    handle = _ResumeBeforeStop(_get_child_core_handle(), workflow_id)
    monkeypatch.setattr(child_runner, '_get_child_core_handle', lambda: handle)

    result = child_runner._confirm_until_not_resumed(
        task_id,
        WORKER_ID,
        True,
        claimed_at,
        orphan_self_heal=True,
    )

    assert result is None
    assert await _node_and_task(session, workflow_id, task_id) == (
        'RUNNING',
        task_id,
        'RUNNING',
        'RUNNING',
    )
