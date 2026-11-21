"""The sent_at of a task that the workflow engine creates.

``sent_at`` is the call time of ``.send()``, ``.schedule()`` or ``.start()``.
A task that the engine creates for a workflow node stores the ``sent_at`` of
its workflow row: the call time of the start of that workflow. This holds for
a root task written at the start, for a dependent promoted later, and for the
tasks of a child workflow. The child that a workflow start writes for a
sub-workflow root is part of that start: its row stores the start call time.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import SubWorkflowNode, TaskNode, WorkflowDefinition
from horsies.core.types.result import is_err
from horsies.core.workflows.lifecycle import start_workflow_async

from tests.integration.conftest import finish_task

pytestmark = [pytest.mark.integration]

START_CALL_TIME = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
"""A call time far from the clock, so a task row that took NOW() differs."""


def _task(app: Horsies, prefix: str) -> Any:
    @app.task(task_name=f'{prefix}_{uuid.uuid4().hex[:8]}')
    def fn(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    return fn


async def _task_sent_at(
    session: AsyncSession, workflow_id: str,
) -> dict[int, datetime]:
    rows = (
        await session.execute(
            text("""
                SELECT wt.task_index, t.sent_at
                FROM horsies_workflow_tasks wt
                JOIN horsies_tasks t ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf
            """),
            {'wf': workflow_id},
        )
    ).fetchall()
    return {row.task_index: row.sent_at for row in rows}


async def _workflow_sent_at(session: AsyncSession, workflow_id: str) -> datetime:
    value = (
        await session.execute(
            text('SELECT sent_at FROM horsies_workflows WHERE id = :wf'),
            {'wf': workflow_id},
        )
    ).scalar_one()
    assert isinstance(value, datetime)
    return value


@pytest.mark.asyncio(loop_scope='function')
class TestEngineTaskSentAt:
    async def test_a_root_and_a_promoted_dependent_store_the_start_call_time(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        fn = _task(app, 'sent_at')
        root = TaskNode(fn=fn, kwargs={'value': 0})
        dependent = TaskNode(fn=fn, kwargs={'value': 1}, waits_for=[root])
        spec = app.workflow(
            f'sent_at_{uuid.uuid4().hex[:6]}', [root, dependent],
            definition_key=f'sent-at-{uuid.uuid4().hex[:6]}',
        )
        started = await start_workflow_async(spec, broker, sent_at=START_CALL_TIME)
        assert not is_err(started), started
        workflow_id = str(started.ok_value.workflow_id)

        assert await _workflow_sent_at(session, workflow_id) == START_CALL_TIME
        assert await _task_sent_at(session, workflow_id) == {0: START_CALL_TIME}

        await finish_task(session, broker, workflow_id, 0, TaskResult(ok=0))
        await session.commit()

        # Phase 1 moved the root's task to history; the live table holds the
        # promoted dependent.
        assert await _task_sent_at(session, workflow_id) == {1: START_CALL_TIME}

    async def test_the_child_of_a_subworkflow_root_stores_the_start_call_time(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        child_fn = _task(app, 'sent_at_child')
        suffix = uuid.uuid4().hex[:8]

        class SentAtChild(WorkflowDefinition[int]):
            name = f'sent_at_child_{suffix}'
            definition_key = f'sent-at-child-{suffix}'

            @classmethod
            def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
                _ = (args, params)
                node = TaskNode(fn=child_fn, kwargs={'value': 2})
                return app.workflow(cls.name, [node], output=node)

        spec = app.workflow(
            f'sent_at_parent_{suffix}', [SubWorkflowNode(workflow_def=SentAtChild)],
            definition_key=f'sent-at-parent-{suffix}',
        )
        started = await start_workflow_async(spec, broker, sent_at=START_CALL_TIME)
        assert not is_err(started), started
        child_id = (
            await session.execute(
                text('SELECT id FROM horsies_workflows WHERE parent_workflow_id = :p'),
                {'p': started.ok_value.workflow_id},
            )
        ).scalar_one()

        assert await _workflow_sent_at(session, str(child_id)) == START_CALL_TIME
        assert await _task_sent_at(session, str(child_id)) == {0: START_CALL_TIME}

    async def test_the_child_of_a_child_root_stores_the_start_call_time(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        leaf_fn = _task(app, 'sent_at_leaf')
        suffix = uuid.uuid4().hex[:8]

        class SentAtLeaf(WorkflowDefinition[int]):
            name = f'sent_at_leaf_{suffix}'
            definition_key = f'sent-at-leaf-{suffix}'

            @classmethod
            def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
                _ = (args, params)
                node = TaskNode(fn=leaf_fn, kwargs={'value': 3})
                return app.workflow(cls.name, [node], output=node)

        class SentAtMiddle(WorkflowDefinition[int]):
            name = f'sent_at_middle_{suffix}'
            definition_key = f'sent-at-middle-{suffix}'

            @classmethod
            def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
                _ = (args, params)
                node = SubWorkflowNode(workflow_def=SentAtLeaf)
                return app.workflow(cls.name, [node], output=node)

        spec = app.workflow(
            f'sent_at_top_{suffix}', [SubWorkflowNode(workflow_def=SentAtMiddle)],
            definition_key=f'sent-at-top-{suffix}',
        )
        started = await start_workflow_async(spec, broker, sent_at=START_CALL_TIME)
        assert not is_err(started), started
        leaf_id = (
            await session.execute(
                text('SELECT id FROM horsies_workflows WHERE depth = 2 AND root_workflow_id = :r'),
                {'r': started.ok_value.workflow_id},
            )
        ).scalar_one()

        assert await _workflow_sent_at(session, str(leaf_id)) == START_CALL_TIME
        assert await _task_sent_at(session, str(leaf_id)) == {0: START_CALL_TIME}
