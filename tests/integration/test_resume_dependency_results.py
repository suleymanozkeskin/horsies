"""Regression tests: resume must decode stored dependency results.

``resume_workflow`` and ``cascade_resume_to_children`` previously called
``get_dependency_results`` without ``app``, so every COMPLETED dependency
decoded to a ``RESULT_DESERIALIZATION_ERROR`` (stage='no_app') sentinel and
resumed ``args_from`` consumers received error envelopes for genuinely
successful upstreams.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode
from horsies.core.types.result import is_ok
from horsies.core.workflows.engine import (
    on_workflow_task_complete,
    resume_workflow,
)

from .conftest import start_ok
from tests.integration.conftest import task_name_for

pytestmark = [pytest.mark.integration]


async def _get_linked_task_kwargs(
    session: AsyncSession,
    workflow_id: str,
    task_index: int,
) -> str:
    """Return the raw kwargs JSON of the backing task linked to a node."""
    row = (
        await session.execute(
            text("""
                SELECT t.kwargs
                FROM horsies_workflow_tasks wt
                JOIN horsies_tasks t ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf_id AND wt.task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
    ).fetchone()
    assert row is not None, f'No linked task for node {task_index}'
    return str(row[0])


@pytest.mark.asyncio(loop_scope='function')
async def test_resume_injects_real_dependency_results(
    clean_workflow_tables: None,  # noqa: ARG001
    session: AsyncSession,
    broker: PostgresBroker,
    app: Horsies,
) -> None:
    """A resumed args_from consumer receives the upstream's real result.

    Timeline:
      1. producer -> consumer (args_from). Producer completes with ok=42;
         consumer's node becomes ENQUEUED and its backing task is claimed
         by a worker (but not started).
      2. Pause: the claimed-not-started backing task is cancelled, its
         node returns to READY.
      3. Resume: the engine re-enqueues consumer, fetching dependency
         results from storage — these must decode as ok=42, not as a
         no-app deserialization sentinel.
    """

    @app.task(task_name='resume_dep_producer')
    def producer() -> TaskResult[int, TaskError]:
        return TaskResult(ok=42)

    @app.task(task_name='resume_dep_consumer')
    def consumer(value: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
        return TaskResult(ok=1)

    node_a: TaskNode[int] = TaskNode(fn=producer)
    node_b: TaskNode[int] = TaskNode(
        fn=consumer,
        waits_for=[node_a],
        args_from={'value': node_a},
    )
    spec: Any = app.workflow(
        name='resume_dep_results',
        tasks=[node_a, node_b],
        output=node_b,
        definition_key='tests.resume_dep_results.v1',
    )

    handle = await start_ok(spec, broker)

    # 1. Producer completes -> consumer enqueued with real dep results.
    producer_task_row = (
        await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
    ).fetchone()
    assert producer_task_row is not None
    await on_workflow_task_complete(
        session, producer_task_row[0], TaskResult(ok=42), broker,
        task_name=await task_name_for(session, producer_task_row[0]),
    )
    await session.commit()

    # Simulate a worker claiming (but not starting) the consumer's task so
    # pause sweeps it: only claimed-not-started rows are cancelled by pause.
    await session.execute(
        text("""
            UPDATE horsies_tasks
            SET status = 'CLAIMED', claimed = TRUE,
                claimed_at = NOW(),
                claimed_by_worker_id = 'w-resume-test',
                claim_expires_at = NOW() + INTERVAL '60 seconds',
                updated_at = NOW()
            WHERE id = (
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            )
        """),
        {'wf_id': handle.workflow_id},
    )
    await session.commit()

    # 2. Pause: consumer's backing task cancelled, node back to READY.
    pause_r = await handle.pause_async()
    assert is_ok(pause_r)
    session.expire_all()

    node_b_status = (
        await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
    ).scalar_one()
    assert node_b_status == 'READY', (
        f'Pause must reset un-started ENQUEUED node to READY, got {node_b_status}'
    )

    # 3. Resume re-enqueues the consumer with stored dependency results.
    resume_r = await resume_workflow(broker, handle.workflow_id)
    assert is_ok(resume_r)
    session.expire_all()

    kwargs_json = await _get_linked_task_kwargs(session, handle.workflow_id, 1)
    assert 'RESULT_DESERIALIZATION_ERROR' not in kwargs_json, (
        'Resumed consumer received a no-app sentinel instead of the real '
        f'dependency result: {kwargs_json}'
    )
    assert 'no_app' not in kwargs_json
    assert '42' in kwargs_json, (
        f'Expected producer ok=42 injected into consumer kwargs: {kwargs_json}'
    )
