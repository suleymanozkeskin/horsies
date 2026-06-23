"""Integration regression: subworkflow tasks bind to their queue priority.

Pins the fix for the priority-binding bug: a parameterized subworkflow whose
``build_with`` returns a direct ``WorkflowSpec(...)`` (the documented pattern)
never passes through ``app.workflow()``, so its TaskNodes arrive at the engine
unresolved. Before the fix the engine persisted them at a hardcoded priority
100 regardless of the queue's configured priority; the queue routed correctly
but intra-queue claim order (``ORDER BY priority ASC``) was wrong. The engine
now binds queue/priority through the shared resolver, so the persisted task row
carries the queue's priority.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import uuid
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.queues import CustomQueueConfig, QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import (
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)
from horsies.core.types.result import is_err
from horsies.core.workflows.lifecycle import start_workflow_async

pytestmark = [pytest.mark.integration]

SCRAPING_PRIORITY = 30


def _custom_app(broker: PostgresBroker) -> Horsies:
    """CUSTOM-mode app with a 'scraping' queue at priority 30, on the test broker."""
    app = Horsies(
        AppConfig(
            queue_mode=QueueMode.CUSTOM,
            broker=broker.config,
            custom_queues=[
                CustomQueueConfig(
                    name='scraping', priority=SCRAPING_PRIORITY, max_concurrency=1,
                ),
            ],
        ),
    )
    app._broker = broker
    broker.app = app
    return app


@pytest.mark.asyncio(loop_scope='function')
class TestSubworkflowPriorityBinding:
    async def test_direct_build_with_binds_queue_priority(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """A subworkflow scrape task persists at the queue priority (30), not 100."""
        app = _custom_app(broker)
        suffix = uuid.uuid4().hex[:8]

        @app.task(task_name=f'scrape_{suffix}', queue_name='scraping')
        def scrape_task(*, run_postproc: bool = False) -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)  # pragma: no cover - never executed in this test

        class ClientSync(WorkflowDefinition[int]):
            name = f'client_sync_{suffix}'
            definition_key = f'client-sync-{suffix}'

            @classmethod
            def build_with(cls, app: Horsies, **params: Any) -> WorkflowSpec[int]:
                # Direct WorkflowSpec — bypasses app.workflow() binding, exactly
                # like the documented parameterized-subworkflow pattern.
                scrape = TaskNode(
                    fn=scrape_task, kwargs={'run_postproc': False}, node_id='scrape',
                )
                return WorkflowSpec(name=cls.name, tasks=[scrape], output=scrape)

        parent = app.workflow(
            f'parent_{suffix}',
            [SubWorkflowNode(workflow_def=ClientSync)],
            definition_key=f'parent-{suffix}',
        )
        r = await start_workflow_async(parent, broker)
        assert not is_err(r), r

        parent_id = r.ok_value.workflow_id
        child_id = (
            await session.execute(
                text('SELECT id FROM horsies_workflows WHERE parent_workflow_id = :p'),
                {'p': parent_id},
            )
        ).scalar()
        assert child_id is not None

        row = (
            await session.execute(
                text("""
                    SELECT t.priority AS task_priority,
                           t.queue_name AS task_queue,
                           wt.priority AS wt_priority
                    FROM horsies_workflow_tasks wt
                    JOIN horsies_tasks t ON t.id = wt.task_id
                    WHERE wt.workflow_id = :wf AND wt.node_id = 'scrape'
                """),
                {'wf': str(child_id)},
            )
        ).one()

        # Queue routing was always correct; priority is the regression.
        assert row.task_queue == 'scraping'
        assert row.task_priority == SCRAPING_PRIORITY  # claimable row: was 100
        assert row.wt_priority == SCRAPING_PRIORITY     # workflow-task row: was 100
