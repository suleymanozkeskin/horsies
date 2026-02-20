"""Regression tests for kwargs-only workflow engine writes and backward compat.

Verifies:
- New workflow writes store empty task_args (kwargs-only contract).
- Old persisted rows with non-empty task_args still dispatch correctly.
- Subworkflow build_with receives only kwargs for new writes.
- Recovery handles both old-format and new-format rows.
"""

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.serde import loads_json
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.workflow import (
    TaskNode,
    SubWorkflowNode,
    WorkflowDefinition,
    OnError,
    WorkflowValidationError,
)
from horsies.core.errors import ErrorCode
from horsies.core.workflows.engine import start_workflow_async, on_workflow_task_complete

from .conftest import make_simple_task


# =============================================================================
# Child workflow for subworkflow kwargs tests
# =============================================================================


class KwargsChildWorkflow(WorkflowDefinition[int]):
    """Child workflow that verifies it receives kwargs, not positional args."""

    name = 'kwargs_child_workflow'

    # Track last received params for test assertions
    last_params: dict[str, Any] = {}

    @classmethod
    def build_with(cls, app: Horsies, **params: Any) -> Any:
        cls.last_params = dict(params)

        @app.task(task_name='kwargs_child_task')
        def child_task(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        raw_value: Any = params.get('value', 0)
        if isinstance(raw_value, dict):
            raw_dict: dict[str, Any] = dict(raw_value)
            if raw_dict.get('__horsies_taskresult__'):
                data_str = raw_dict.get('data')
                if isinstance(data_str, str):
                    from horsies.core.codec.serde import task_result_from_json

                    tr = task_result_from_json(loads_json(data_str).unwrap()).unwrap()
                    raw_value = tr.unwrap() if tr.is_ok() else 0

        node = TaskNode(fn=child_task, kwargs={'value': raw_value})
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestKwargsOnlyWrites:
    """Verify that new workflow writes use kwargs-only persistence."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        await session.execute(
            text(
                'TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'
            )
        )
        await session.commit()
        return session, broker, app

    async def test_new_tasknode_persists_empty_task_args(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """New TaskNode writes store [] for task_args and kwargs in task_kwargs."""
        session, broker, app = setup
        task_fn = make_simple_task(app, 'kwargs_simple')

        node = TaskNode(fn=task_fn, kwargs={'value': 42})
        spec = app.workflow(
            name='kwargs_write_test',
            tasks=[node],
            output=node,
        )

        handle = await start_workflow_async(spec, broker)

        # Verify task_args is empty, task_kwargs has the value
        result = await session.execute(
            text("""
                SELECT task_args, task_kwargs
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None

        task_args = loads_json(row[0]).unwrap() if row[0] else []
        task_kwargs = loads_json(row[1]).unwrap() if row[1] else {}

        assert task_args == [], f'Expected empty task_args, got {task_args}'
        assert isinstance(task_kwargs, dict)
        assert task_kwargs.get('value') == 42

    async def test_new_subworkflow_node_persists_empty_task_args(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """New SubWorkflowNode writes store [] for task_args."""
        session, broker, app = setup

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=KwargsChildWorkflow,
            kwargs={'value': 99},
        )

        spec = app.workflow(
            name='kwargs_subwf_write_test',
            tasks=[node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)

        result = await session.execute(
            text("""
                SELECT task_args, task_kwargs
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None

        task_args = loads_json(row[0]).unwrap() if row[0] else []
        task_kwargs = loads_json(row[1]).unwrap() if row[1] else {}

        assert task_args == [], f'Expected empty task_args, got {task_args}'
        assert isinstance(task_kwargs, dict)
        assert task_kwargs.get('value') == 99

    async def test_positional_args_guard_raises(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """TaskNode with non-empty args fails spec validation immediately."""
        session, broker, app = setup
        task_fn = make_simple_task(app, 'kwargs_guard_task')
        _ = (session, broker)

        # Manually set args to bypass any constructor ban
        node = TaskNode(fn=task_fn, kwargs={'value': 1})
        node.args = (5,)  # Force positional args

        with pytest.raises(WorkflowValidationError) as exc:
            app.workflow(
                name='kwargs_guard_test',
                tasks=[node],
                output=node,
            )

        assert exc.value.code == ErrorCode.WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestOldRowBackwardCompat:
    """Verify that old persisted rows with task_args still dispatch correctly."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        await session.execute(
            text(
                'TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'
            )
        )
        await session.commit()
        return session, broker, app

    async def test_old_positional_row_enqueues_with_args(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Old row with non-empty task_args dispatches correctly when enqueued."""
        session, broker, app = setup
        task_fn = make_simple_task(app, 'old_compat_task')

        # Create workflow with kwargs (new-style)
        node = TaskNode(fn=task_fn, kwargs={'value': 10})
        spec = app.workflow(
            name='old_compat_test',
            tasks=[node],
            output=node,
        )

        handle = await start_workflow_async(spec, broker)

        # Simulate old-format row by injecting task_args directly
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET task_args = :args, task_kwargs = :kwargs
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {
                'wf_id': handle.workflow_id,
                'args': '[10]',  # Old format: positional args
                'kwargs': '{}',  # Empty kwargs
            },
        )
        # Reset to READY so we can re-enqueue
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'READY', task_id = NULL
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        # Delete the task row that was created during start
        await session.execute(
            text("""
                DELETE FROM horsies_tasks WHERE task_name = 'old_compat_task'
            """),
        )
        await session.commit()

        # Run recovery to re-enqueue the READY task
        from horsies.core.workflows.recovery import recover_stuck_workflows

        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered >= 1

        # Verify the task was created in horsies_tasks with the old args
        task_result = await session.execute(
            text("""
                SELECT t.args, t.kwargs
                FROM horsies_tasks t
                JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
                WHERE wt.workflow_id = :wf_id AND wt.task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_row = task_result.fetchone()
        assert task_row is not None

        # Old args should be passed through
        dispatched_args = loads_json(task_row[0]).unwrap() if task_row[0] else []
        assert dispatched_args == [10], f'Expected [10], got {dispatched_args}'


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestSubworkflowKwargsOnly:
    """Verify subworkflow build_with receives kwargs for new writes."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        await session.execute(
            text(
                'TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'
            )
        )
        await session.commit()
        KwargsChildWorkflow.last_params = {}
        return session, broker, app

    async def test_subworkflow_build_with_receives_kwargs(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """When upstream completes, subworkflow build_with gets args_from as kwargs."""
        session, broker, app = setup

        producer = make_simple_task(app, 'kwargs_producer')

        node_a: TaskNode[int] = TaskNode(fn=producer, kwargs={'value': 7})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=KwargsChildWorkflow,
            waits_for=[node_a],
            args_from={'value': node_a},
        )

        spec = app.workflow(
            name='kwargs_subwf_params_test',
            tasks=[node_a, node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)

        # Complete upstream task to trigger subworkflow start
        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_row = wt_result.fetchone()
        assert task_row is not None and task_row[0] is not None

        await on_workflow_task_complete(
            session, task_row[0], TaskResult(ok=7), broker,
        )
        await session.commit()

        # Subworkflow should be RUNNING
        child_row = await session.execute(
            text("""
                SELECT status, sub_workflow_id
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        child_status_row = child_row.fetchone()
        assert child_status_row is not None
        assert child_status_row[0] == 'RUNNING'
        assert child_status_row[1] is not None  # child workflow created

        # Verify build_with received kwargs (value key present)
        assert 'value' in KwargsChildWorkflow.last_params


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestRecoveryKwargsCompat:
    """Verify recovery works with both new (kwargs-only) and old (positional) rows."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        await session.execute(
            text(
                'TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'
            )
        )
        await session.commit()
        return session, broker, app

    async def test_recovery_kwargs_only_workflow(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Recovery handles a stuck workflow with kwargs-only rows."""
        session, broker, app = setup
        task_fn = make_simple_task(app, 'recovery_kwargs')

        node = TaskNode(fn=task_fn, kwargs={'value': 5})
        spec = app.workflow(
            name='recovery_kwargs_test',
            tasks=[node],
            output=node,
        )

        handle = await start_workflow_async(spec, broker)

        # Simulate stuck: all tasks done, workflow not finalized
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = '{"ok": 10}'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL, result = NULL
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        from horsies.core.workflows.recovery import recover_stuck_workflows

        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        wf_result = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        wf_row = wf_result.fetchone()
        assert wf_row is not None
        assert wf_row[0] == 'COMPLETED'

    async def test_recovery_ready_kwargs_task_enqueued(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Recovery re-enqueues a READY kwargs-only task that wasn't dispatched."""
        session, broker, app = setup
        task_fn = make_simple_task(app, 'recovery_ready_kwargs')

        node = TaskNode(fn=task_fn, kwargs={'value': 3})
        spec = app.workflow(
            name='recovery_ready_kwargs_test',
            tasks=[node],
            output=node,
        )

        handle = await start_workflow_async(spec, broker)

        # Simulate crash: set task to READY but clear task_id
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'READY', task_id = NULL
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        from horsies.core.workflows.recovery import recover_stuck_workflows

        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        # Task should now be ENQUEUED
        result = await session.execute(
            text("""
                SELECT status, task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] == 'ENQUEUED'
        assert row[1] is not None

        # Verify the dispatched task has empty args and kwargs with value
        task_result = await session.execute(
            text('SELECT args, kwargs FROM horsies_tasks WHERE id = :tid'),
            {'tid': row[1]},
        )
        task_row = task_result.fetchone()
        assert task_row is not None

        dispatched_args = loads_json(task_row[0]).unwrap() if task_row[0] else []
        dispatched_kwargs = loads_json(task_row[1]).unwrap() if task_row[1] else {}

        assert dispatched_args == [], f'Expected empty args, got {dispatched_args}'
        assert isinstance(dispatched_kwargs, dict)
        assert dispatched_kwargs.get('value') == 3
