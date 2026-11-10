"""Every path that creates a task row stores the task's execution class.

A send, a delayed send, a send with a deadline, a workflow root at start, a
promoted workflow node, and a child workflow root each write
`horsies_tasks.execution` from the stored task options. The database refuses
a value outside the `Execution` domain, and enqueue refuses stored options
whose class does not validate. The rerun path is covered in
`test_task_history_rerun.py`.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import BrokerInvalidTaskOptions
from horsies.core.models.tasks import (
    DEFAULT_CANCEL_GRACE_MS,
    Execution,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow import SubWorkflowNode, TaskNode, WorkflowDefinition
from horsies.core.types.result import Err, Ok, is_err
from horsies.core.workflows.lifecycle import start_workflow_async
from tests.integration.conftest import complete_task, compute_test_enqueue_sha

pytestmark = [pytest.mark.integration]


async def _stored_class(session: AsyncSession, task_id: str) -> str:
    row = (
        await session.execute(
            text('SELECT execution FROM horsies_tasks WHERE id = CAST(:id AS uuid)'),
            {'id': task_id},
        )
    ).one()
    return str(row.execution)


async def _node_task_id(
    session: AsyncSession,
    workflow_id: str,
    task_index: int,
) -> str:
    task_id = (
        await session.execute(
            text(
                'SELECT task_id FROM horsies_workflow_tasks '
                'WHERE workflow_id = :wf AND task_index = :idx'
            ),
            {'wf': workflow_id, 'idx': task_index},
        )
    ).scalar_one()
    assert task_id is not None, (workflow_id, task_index)
    return str(task_id)


def _sent_id(sent: Any) -> str:
    assert not is_err(sent), sent
    return str(sent.ok_value.task_id)


@pytest.mark.asyncio(loop_scope='function')
class TestSendPaths:
    async def test_a_send_of_each_class_stores_it(
        self,
        clean_workflow_tables: None,  # noqa: ARG002
        app: Horsies,
        session: AsyncSession,
    ) -> None:
        @app.task('exec_store_isolated')
        def isolated(*, n: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=n)

        @app.task(
            'exec_store_async',
            execution=Execution.SHARED_ASYNC,
            timeout_ms=5_000,
        )
        async def shared_async(*, n: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=n)

        @app.task(
            'exec_store_thread',
            execution=Execution.SHARED_THREAD,
            timeout_ms=5_000,
        )
        def shared_thread(*, n: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=n)

        isolated_id = _sent_id(await isolated.send_async(n=1))
        async_id = _sent_id(await shared_async.send_async(n=1))
        thread_id = _sent_id(await shared_thread.send_async(n=1))

        assert await _stored_class(session, isolated_id) == 'ISOLATED'
        assert await _stored_class(session, async_id) == 'SHARED_ASYNC'
        assert await _stored_class(session, thread_id) == 'SHARED_THREAD'
        options = (
            await session.execute(
                text(
                    'SELECT task_options FROM horsies_tasks '
                    'WHERE id = CAST(:id AS uuid)'
                ),
                {'id': async_id},
            )
        ).scalar_one()
        assert json.loads(options)['cancel_grace_ms'] == DEFAULT_CANCEL_GRACE_MS

    async def test_a_delayed_send_stores_the_class(
        self,
        clean_workflow_tables: None,  # noqa: ARG002
        app: Horsies,
        session: AsyncSession,
    ) -> None:
        @app.task(
            'exec_store_delayed',
            execution=Execution.SHARED_THREAD,
            timeout_ms=5_000,
        )
        def delayed(*, n: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=n)

        task_id = _sent_id(await delayed.schedule_async(600, n=1))

        assert await _stored_class(session, task_id) == 'SHARED_THREAD'

    async def test_a_send_with_a_deadline_stores_the_class(
        self,
        clean_workflow_tables: None,  # noqa: ARG002
        app: Horsies,
        session: AsyncSession,
    ) -> None:
        @app.task(
            'exec_store_deadline',
            execution=Execution.SHARED_ASYNC,
            timeout_ms=5_000,
        )
        async def with_deadline(*, n: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=n)

        deadline = datetime.now(timezone.utc) + timedelta(hours=1)
        sent = await with_deadline.with_options(good_until=deadline).send_async(n=1)

        assert await _stored_class(session, _sent_id(sent)) == 'SHARED_ASYNC'

    async def test_enqueue_refuses_options_whose_class_does_not_validate(
        self,
        clean_workflow_tables: None,  # noqa: ARG002
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        options = json.dumps({'execution': 'PARALLEL'})
        sent_at, sha = compute_test_enqueue_sha(
            task_name='exec_store_raw',
            task_options=options,
        )
        task_id = str(uuid.uuid4())

        outcome = await broker.enqueue_async(
            'exec_store_raw',
            task_id=task_id,
            enqueue_sha=sha,
            args_json='[]',
            kwargs_json='{}',
            sent_at=sent_at,
            task_options=options,
        )

        assert isinstance(outcome, Err)
        assert isinstance(outcome.err_value, BrokerInvalidTaskOptions)
        count = (
            await session.execute(
                text('SELECT count(*) FROM horsies_tasks WHERE id = CAST(:id AS uuid)'),
                {'id': task_id},
            )
        ).scalar_one()
        assert count == 0

    async def test_the_database_refuses_a_value_outside_the_domain(
        self,
        clean_workflow_tables: None,  # noqa: ARG002
        app: Horsies,
        session: AsyncSession,
    ) -> None:
        @app.task('exec_store_check')
        def plain(*, n: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=n)

        task_id = _sent_id(await plain.send_async(n=1))

        with pytest.raises(IntegrityError, match='ck_horsies_tasks_execution'):
            await session.execute(
                text(
                    "UPDATE horsies_tasks SET execution = 'PARALLEL' "
                    'WHERE id = CAST(:id AS uuid)'
                ),
                {'id': task_id},
            )
        await session.rollback()


@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowPaths:
    async def test_a_root_and_a_promoted_node_store_their_classes(
        self,
        clean_workflow_tables: None,  # noqa: ARG002
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        @app.task(
            'exec_wf_root',
            execution=Execution.SHARED_THREAD,
            timeout_ms=5_000,
        )
        def root(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        @app.task(
            'exec_wf_next',
            execution=Execution.SHARED_ASYNC,
            timeout_ms=5_000,
        )
        async def after(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        node_root = root.node()(value=1)
        node_next = after.node(waits_for=[node_root])(value=2)
        spec = app.workflow(
            f'exec_wf_{uuid.uuid4().hex[:6]}',
            [node_root, node_next],
            definition_key=f'exec-wf-{uuid.uuid4().hex[:6]}',
        )
        started = await start_workflow_async(spec, broker)
        assert isinstance(started, Ok), started
        workflow_id = started.ok_value.workflow_id

        root_id = await _node_task_id(session, workflow_id, 0)
        assert await _stored_class(session, root_id) == 'SHARED_THREAD'

        await complete_task(session, broker, workflow_id, 0, TaskResult(ok=1))

        next_id = await _node_task_id(session, workflow_id, 1)
        assert await _stored_class(session, next_id) == 'SHARED_ASYNC'

    async def test_a_child_workflow_root_stores_its_class(
        self,
        clean_workflow_tables: None,  # noqa: ARG002
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        suffix = uuid.uuid4().hex[:8]

        @app.task(
            f'exec_child_{suffix}',
            execution=Execution.SHARED_THREAD,
            timeout_ms=5_000,
        )
        def child_task(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        class Child(WorkflowDefinition[int]):
            name = f'exec_child_wf_{suffix}'
            definition_key = f'exec-child-{suffix}'

            @classmethod
            def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
                _ = args, params
                node = TaskNode(fn=child_task, kwargs={'value': 1})
                return app.workflow(name=cls.name, tasks=[node], output=node)

        parent = app.workflow(
            f'exec_parent_{suffix}',
            [SubWorkflowNode(workflow_def=Child)],
            definition_key=f'exec-parent-{suffix}',
        )
        started = await start_workflow_async(parent, broker)
        assert isinstance(started, Ok), started
        child_id = (
            await session.execute(
                text('SELECT id FROM horsies_workflows WHERE parent_workflow_id = :p'),
                {'p': started.ok_value.workflow_id},
            )
        ).scalar_one()

        child_root_id = await _node_task_id(session, str(child_id), 0)
        assert await _stored_class(session, child_root_id) == 'SHARED_THREAD'
