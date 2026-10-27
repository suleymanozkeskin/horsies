"""A pause abandon must not apply after the workflow was resumed.

A worker reads the workflow as PAUSED, a resume commits, and then the
worker abandons its claim. Without a pause re-check under the workflow
lock, the abandon cancels the task and the node is reset to READY with no
task while the workflow is RUNNING; only workflow recovery enqueues it.
With the re-check, the abandon refuses and the claim is run instead.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.lifecycle.commands import AbandonOwnedNode, AbandonOwnedNodes
from horsies.core.lifecycle.fences import OwnedClaimBatch
from horsies.core.lifecycle.outcomes import (
    Applied,
    ObservedWorkflowState,
    SourceStateConflict,
)
from horsies.core.lifecycle.persistence import apply_batch_async
from horsies.core.types.result import is_ok
from horsies.core.utils.url import to_psycopg_url
from horsies.core.worker import child_runner, claiming
from horsies.core.worker.child_pool import _initialize_worker_pool
from horsies.core.worker.claiming import ClaimMixin
from horsies.core.worker.config import WorkerConfig
from horsies.core.workflows.lifecycle import resume_workflow
from tests.integration.conftest import DB_URL, compute_test_enqueue_sha
from tests.broker_resources_support import current_resources, listener_of

pytestmark = [pytest.mark.integration]

WORKER_ID = 'abandon-after-resume-worker'


async def _seed_paused_workflow_with_claimed_node(
    session: AsyncSession,
    claimed_at: datetime,
) -> tuple[str, str]:
    task_id = str(uuid.uuid4())
    workflow_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='abandon_after_resume')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, enqueued_at, created_at, updated_at,
                 claimed, retry_count, max_retries, claimed_at,
                 claimed_by_worker_id, claim_expires_at, enqueue_sha,
                 is_workflow_task, retention_class_key,
                 command_fingerprint_version, command_fingerprint,
                 retain_rerun_input, prepared_rerun_input_disposition)
            VALUES
                (:id, 'abandon_after_resume', 'default', 100, '[]', '{}',
                 'CLAIMED', :sent_at, NOW(), NOW(), NOW(),
                 TRUE, 0, 0, :claimed_at,
                 :worker_id, NOW() + interval '1 minute', :sha,
                 TRUE, 'standard_30d',
                 1, sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                 FALSE, 'DECLINED_BY_POLICY')
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'claimed_at': claimed_at,
            'worker_id': WORKER_ID,
            'sha': sha,
        },
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES (:id, 'abandon_after_resume_wf', 'PAUSED', 'FAIL', 0, :id,
                    NOW(), NOW(), NOW(), NOW())
        """),
        {'id': workflow_id},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args,
                 task_kwargs, queue_name, priority, dependencies,
                 allow_failed_deps, join_type, is_subworkflow, status,
                 task_id, created_at)
            VALUES (:id, :workflow_id, 0, 'node_0', 'abandon_after_resume',
                    '[]', '{}', 'default', 100, '{}', FALSE, 'all', FALSE,
                    'ENQUEUED', :task_id, NOW())
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
            self.cfg = cfg
            self.worker_instance_id = WORKER_ID

        async def _dispatch_one(self, *args: Any, **kwargs: Any) -> None:
            return None

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


@pytest.mark.asyncio(loop_scope='function')
async def test_claim_filter_dispatches_a_claim_resumed_before_its_abandon(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The post-claim filter reads PAUSED; the resume commits before the abandon."""
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    workflow_id, task_id = await _seed_paused_workflow_with_claimed_node(
        session,
        claimed_at,
    )
    real_apply_batch = claiming.apply_batch_async

    async def _resume_then_apply(connection: Any, command: Any) -> Any:
        resumed = await resume_workflow(broker, workflow_id)
        assert is_ok(resumed) and resumed.ok_value is True
        return await real_apply_batch(connection, command)

    monkeypatch.setattr(claiming, 'apply_batch_async', _resume_then_apply)
    rows = [{'id': task_id, 'is_workflow_task': True, 'claimed_at': claimed_at}]

    dispatched = await _claim_filter_probe(broker)._filter_nonrunnable_workflow_tasks(
        rows,
    )

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


@pytest.mark.asyncio(loop_scope='function')
async def test_child_starts_a_task_resumed_before_its_abandon(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The child reads PAUSED before start; the resume commits before the abandon."""
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    workflow_id, task_id = await _seed_paused_workflow_with_claimed_node(
        session,
        claimed_at,
    )
    _initialize_worker_pool(listener_of(broker).database_url)
    real_apply_sync = child_runner.apply_sync

    def _resume_then_apply(cursor: Any, command: Any) -> Any:
        if isinstance(command, AbandonOwnedNode):
            _set_workflow_running(workflow_id)
        return real_apply_sync(cursor, command)

    monkeypatch.setattr(child_runner, 'apply_sync', _resume_then_apply)

    result = child_runner._confirm_until_not_resumed(
        task_id,
        WORKER_ID,
        True,
        claimed_at,
    )

    assert result is None
    assert await _node_and_task(session, workflow_id, task_id) == (
        'RUNNING',
        task_id,
        'RUNNING',
        'RUNNING',
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_child_starts_a_task_resumed_before_its_status_read(
    session: AsyncSession,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The transition misses on the paused guard; the resume commits before
    the child reads the workflow status."""
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    workflow_id, task_id = await _seed_paused_workflow_with_claimed_node(
        session,
        claimed_at,
    )
    _initialize_worker_pool(listener_of(broker).database_url)
    real_status_read = child_runner._get_workflow_status_for_task

    def _resume_then_read(cursor: Any, read_task_id: str) -> str | None:
        _set_workflow_running(workflow_id)
        return real_status_read(cursor, read_task_id)

    monkeypatch.setattr(
        child_runner,
        '_get_workflow_status_for_task',
        _resume_then_read,
    )

    result = child_runner._confirm_until_not_resumed(
        task_id,
        WORKER_ID,
        True,
        claimed_at,
    )

    assert result is None
    assert await _node_and_task(session, workflow_id, task_id) == (
        'RUNNING',
        task_id,
        'RUNNING',
        'RUNNING',
    )
