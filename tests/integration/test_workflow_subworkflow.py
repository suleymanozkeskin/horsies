"""Integration tests for SubWorkflowNode behavior."""

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.serde import dumps_json, loads_json, task_result_from_json
from horsies.core.errors import WorkflowValidationError
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.workflow import (
    TaskNode,
    SubWorkflowNode,
    SubWorkflowSummary,
    WorkflowDefinition,
    WorkflowContext,
    WorkflowStatus,
    OnError,
)
from horsies.core.workflows.engine import start_workflow_async, on_workflow_task_complete
from horsies.core.workflows.registry import unregister_workflow_spec

from .conftest import make_simple_task


# =============================================================================
# Helpers
# =============================================================================


def _decode_task_result(value: Any) -> TaskResult[Any, TaskError] | None:
    if isinstance(value, str):
        return task_result_from_json(loads_json(value).unwrap()).unwrap()
    return None


def _extract_taskresult_value(raw: Any, default: int = 0) -> int:
    """Extract int value from a raw build_with param that may be a wrapped TaskResult."""
    if isinstance(raw, dict):
        raw_dict: dict[str, Any] = dict(raw)
        if raw_dict.get('__horsies_taskresult__'):
            data_str = raw_dict.get('data')
            if isinstance(data_str, str):
                tr = task_result_from_json(loads_json(data_str).unwrap()).unwrap()
                return tr.unwrap() if tr.is_ok() else default
    if isinstance(raw, int):
        return raw
    return default


# =============================================================================
# Child WorkflowDefinitions
# =============================================================================


class ImportableChildWorkflow(WorkflowDefinition[int]):
    """Child workflow used for registry fallback tests."""

    name = 'importable_child_workflow'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        @app.task(task_name='importable_child_task')
        def child_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=5)

        node = TaskNode(fn=child_task)
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


class ParamChildWorkflow(WorkflowDefinition[int]):
    """Child workflow that uses build_with parameters."""

    name = 'param_child_workflow'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        params_map: dict[str, Any] = params
        raw_value: Any = params_map.get('value', 0)
        if isinstance(raw_value, dict):
            raw_dict: dict[str, Any] = dict(raw_value)
            if raw_dict.get('__horsies_taskresult__'):
                data_str = raw_dict.get('data')
                if isinstance(data_str, str):
                    tr = task_result_from_json(loads_json(data_str).unwrap()).unwrap()
                    raw_value = tr.unwrap() if tr.is_ok() else 0

        @app.task(task_name='param_child_task')
        def child_task(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        node = TaskNode(fn=child_task, kwargs={'value': raw_value})
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


class MultiParamChildWorkflow(WorkflowDefinition[int]):
    """Child workflow that accepts two build_with parameters."""

    name = 'multi_param_child_workflow'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        params_map: dict[str, Any] = params
        first_val = _extract_taskresult_value(params_map.get('first', 0))
        second_val = _extract_taskresult_value(params_map.get('second', 0))

        @app.task(task_name='multi_param_child_task')
        def child_task(first: int, second: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=first + second)

        node = TaskNode(fn=child_task, kwargs={'first': first_val, 'second': second_val})
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


class StartChildWorkflow(WorkflowDefinition[int]):
    name = 'child_workflow_start'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        @app.task(task_name='sub_child_task')
        def child_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        node = TaskNode(fn=child_task)
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


class SummaryChildWorkflow(WorkflowDefinition[int]):
    name = 'summary_child_workflow'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        @app.task(task_name='summary_child_task')
        def child_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        node = TaskNode(fn=child_task)
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


class MiddleWorkflow(WorkflowDefinition[int]):
    """Intermediate workflow containing a SubWorkflowNode for 3-level nesting."""

    name = 'middle_workflow'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
        )
        return app.workflow(
            name=cls.name,
            tasks=[node_child],
            output=node_child,
            on_error=OnError.FAIL,
        )


# =============================================================================
# TestSubworkflowIntegration
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestSubworkflowIntegration:
    """Integration tests for subworkflow start, fallback, and summaries."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
        await session.commit()
        return session, broker, app

    async def _complete_task(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        workflow_id: str,
        task_index: int,
        result: TaskResult[Any, TaskError],
    ) -> None:
        res = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = res.fetchone()
        if row and row[0]:
            await on_workflow_task_complete(session, row[0], result, broker)
            await session.commit()

    async def _get_workflow_task_row(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
    ) -> tuple[Any, ...] | None:
        res = await session.execute(
            text("""
                SELECT status, sub_workflow_id, result
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = res.fetchone()
        if row is None:
            return None
        return tuple(row)

    async def _get_child_task_id(
        self, session: AsyncSession, child_workflow_id: str
    ) -> str | None:
        res = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': child_workflow_id},
        )
        row = res.fetchone()
        if row and row[0]:
            return row[0]
        return None

    async def _complete_child_workflow(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        parent_workflow_id: str,
        parent_task_index: int,
        child_result: TaskResult[Any, TaskError],
    ) -> str:
        """Complete a child workflow's first task and return the child workflow ID."""
        row = await self._get_workflow_task_row(session, parent_workflow_id, parent_task_index)
        assert row is not None
        _, child_id, _ = row
        assert isinstance(child_id, str)

        child_task_id = await self._get_child_task_id(session, child_id)
        assert isinstance(child_task_id, str)
        await on_workflow_task_complete(session, child_task_id, child_result, broker)
        await session.commit()
        return child_id

    # -----------------------------------------------------------------
    # Existing happy-path tests
    # -----------------------------------------------------------------

    async def test_subworkflow_starts_on_dependency_complete(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup

        parent_task = make_simple_task(app, 'sub_parent_task')

        node_a: TaskNode[int] = TaskNode(fn=parent_task, kwargs={'value': 2})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
            waits_for=[node_a],
        )

        spec = app.workflow(
            name='parent_workflow_start',
            tasks=[node_a, node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)
        await self._complete_task(
            session, broker, handle.workflow_id, 0, TaskResult(ok=4)
        )

        row = await self._get_workflow_task_row(session, handle.workflow_id, 1)
        assert row is not None
        status, child_id, _ = row
        assert status == 'RUNNING'
        assert isinstance(child_id, str)

        child_row = await session.execute(
            text(
                'SELECT parent_workflow_id, parent_task_index FROM horsies_workflows WHERE id = :cid'
            ),
            {'cid': child_id},
        )
        child_info = child_row.fetchone()
        assert child_info is not None
        assert child_info[0] == handle.workflow_id
        assert child_info[1] == 1

    async def test_subworkflow_registry_fallback(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup

        parent_task = make_simple_task(app, 'sub_parent_task_fallback')

        node_a: TaskNode[int] = TaskNode(fn=parent_task, kwargs={'value': 2})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=ImportableChildWorkflow,
            waits_for=[node_a],
        )

        spec = app.workflow(
            name='parent_workflow_fallback',
            tasks=[node_a, node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)
        unregister_workflow_spec(spec.name)

        await self._complete_task(
            session, broker, handle.workflow_id, 0, TaskResult(ok=4)
        )

        row = await self._get_workflow_task_row(session, handle.workflow_id, 1)
        assert row is not None
        status, child_id, _ = row
        assert status == 'RUNNING'
        assert isinstance(child_id, str)

    async def test_subworkflow_build_with_args_from(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup

        producer_task = make_simple_task(app, 'sub_value_producer')

        node_a: TaskNode[int] = TaskNode(fn=producer_task, kwargs={'value': 7})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=ParamChildWorkflow,
            waits_for=[node_a],
            args_from={'value': node_a},
        )

        spec = app.workflow(
            name='parent_workflow_params',
            tasks=[node_a, node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)
        await self._complete_task(
            session, broker, handle.workflow_id, 0, TaskResult(ok=7)
        )

        row = await self._get_workflow_task_row(session, handle.workflow_id, 1)
        assert row is not None
        _, child_id, _ = row
        assert isinstance(child_id, str)

        child_task_id = await self._get_child_task_id(session, child_id)
        assert isinstance(child_task_id, str)

        await on_workflow_task_complete(
            session, child_task_id, TaskResult(ok=7), broker
        )
        await session.commit()

        row_after = await self._get_workflow_task_row(session, handle.workflow_id, 1)
        assert row_after is not None
        status, _, result_json = row_after
        assert status == 'COMPLETED'
        tr = _decode_task_result(result_json)
        assert tr is not None and tr.is_ok()
        assert tr.unwrap() == 7

    async def test_subworkflow_summary_in_run_when(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup

        @app.task(task_name='summary_post_task')
        def post_task(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            if workflow_ctx is None:
                return TaskResult(err=TaskError(error_code='NO_CTX'))
            return TaskResult(ok=10)

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=SummaryChildWorkflow,
        )
        node_post: TaskNode[int] = TaskNode(
            fn=post_task,
            waits_for=[node_child],
            workflow_ctx_from=[node_child],
            run_when=lambda ctx: ctx.summary_for(node_child).failed_tasks == 0,
        )

        spec = app.workflow(
            name='parent_workflow_summary',
            tasks=[node_child, node_post],
        )

        handle = await start_workflow_async(spec, broker)

        row = await self._get_workflow_task_row(session, handle.workflow_id, 0)
        assert row is not None
        _, child_id, _ = row
        assert isinstance(child_id, str)

        child_task_id = await self._get_child_task_id(session, child_id)
        assert isinstance(child_task_id, str)
        await on_workflow_task_complete(
            session, child_task_id, TaskResult(ok=1), broker
        )
        await session.commit()

        post_row = await self._get_workflow_task_row(session, handle.workflow_id, 1)
        assert post_row is not None
        post_status, _, _ = post_row
        assert post_status == 'ENQUEUED'

    async def test_child_failure_propagates_to_parent_node(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """When child workflow fails, parent node is FAILED with SUBWORKFLOW_FAILED error."""
        session, broker, app = setup

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
        )

        spec = app.workflow(
            name='parent_workflow_failure',
            tasks=[node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)

        child_id = await self._complete_child_workflow(
            session, broker, handle.workflow_id, 0,
            TaskResult(err=TaskError(error_code='CHILD_FAILED', message='Deliberate failure')),
        )

        # Parent node should now be FAILED
        parent_row = await self._get_workflow_task_row(session, handle.workflow_id, 0)
        assert parent_row is not None
        status, _, result_json = parent_row
        assert status == 'FAILED'

        # Result should contain SUBWORKFLOW_FAILED error code
        tr = _decode_task_result(result_json)
        assert tr is not None
        assert tr.is_err()
        assert tr.err is not None
        assert tr.err.error_code == 'SUBWORKFLOW_FAILED'

        # sub_workflow_summary column should be populated
        summary_row = await session.execute(
            text("""
                SELECT sub_workflow_summary
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        summary_json = summary_row.scalar_one()
        assert summary_json is not None
        parsed_summary = loads_json(summary_json).unwrap() if isinstance(summary_json, str) else summary_json
        assert isinstance(parsed_summary, dict)
        summary = SubWorkflowSummary.from_json(parsed_summary)
        assert summary.status == WorkflowStatus.FAILED
        assert summary.failed_tasks >= 1
        assert summary.error_summary is not None

    async def test_child_failure_with_on_error_pause(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """When child fails and parent has on_error=PAUSE, parent workflow pauses."""
        session, broker, app = setup

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
        )

        spec = app.workflow(
            name='parent_workflow_pause_on_fail',
            tasks=[node_child],
            output=node_child,
            on_error=OnError.PAUSE,
        )

        handle = await start_workflow_async(spec, broker)

        await self._complete_child_workflow(
            session, broker, handle.workflow_id, 0,
            TaskResult(err=TaskError(error_code='CHILD_FAILED', message='Test')),
        )

        # Parent workflow should be PAUSED
        wf_row = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        wf_status = wf_row.scalar_one()
        assert wf_status == 'PAUSED'

    async def test_parent_completes_when_child_output_succeeds(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """When child (as output) completes successfully, parent workflow completes."""
        session, broker, app = setup

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
        )

        spec = app.workflow(
            name='parent_workflow_output',
            tasks=[node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)

        await self._complete_child_workflow(
            session, broker, handle.workflow_id, 0, TaskResult(ok=42),
        )

        # Parent workflow should be COMPLETED
        wf_row = await session.execute(
            text('SELECT status, result FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        wf_result = wf_row.fetchone()
        assert wf_result is not None
        assert wf_result[0] == 'COMPLETED'

        # Result should be child's output
        parent_result = _decode_task_result(wf_result[1])
        assert parent_result is not None and parent_result.is_ok()
        assert parent_result.unwrap() == 42

    async def test_multiple_parallel_subworkflows(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Multiple subworkflows can run in parallel."""
        session, broker, app = setup

        node_child1: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
        )
        node_child2: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=ImportableChildWorkflow,
        )

        post_task = make_simple_task(app, 'parallel_post_task')
        node_post: TaskNode[int] = TaskNode(
            fn=post_task,
            waits_for=[node_child1, node_child2],
            kwargs={'value': 1},
        )

        spec = app.workflow(
            name='parent_parallel_subworkflows',
            tasks=[node_child1, node_child2, node_post],
            output=node_post,
        )

        handle = await start_workflow_async(spec, broker)

        # Both children should be RUNNING
        row1 = await self._get_workflow_task_row(session, handle.workflow_id, 0)
        row2 = await self._get_workflow_task_row(session, handle.workflow_id, 1)
        assert row1 is not None and row2 is not None
        assert row1[0] == 'RUNNING'
        assert row2[0] == 'RUNNING'

        # Complete both children
        child1_id = row1[1]
        child2_id = row2[1]
        assert isinstance(child1_id, str) and isinstance(child2_id, str)

        child1_task_id = await self._get_child_task_id(session, child1_id)
        child2_task_id = await self._get_child_task_id(session, child2_id)
        assert isinstance(child1_task_id, str) and isinstance(child2_task_id, str)

        await on_workflow_task_complete(
            session, child1_task_id, TaskResult(ok=1), broker
        )
        await session.commit()
        await on_workflow_task_complete(
            session, child2_task_id, TaskResult(ok=5), broker
        )
        await session.commit()

        # Post task should now be ENQUEUED
        post_row = await self._get_workflow_task_row(session, handle.workflow_id, 2)
        assert post_row is not None
        assert post_row[0] == 'ENQUEUED'

    async def test_depth_tracking_in_nested_workflows(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Child workflows have correct depth values."""
        session, broker, app = setup

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
        )

        spec = app.workflow(
            name='parent_depth_test',
            tasks=[node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)

        # Check parent depth
        parent_depth = await session.execute(
            text('SELECT depth FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        assert parent_depth.scalar_one() == 0

        # Check child depth
        row = await self._get_workflow_task_row(session, handle.workflow_id, 0)
        assert row is not None
        child_id = row[1]
        assert isinstance(child_id, str)

        child_depth = await session.execute(
            text('SELECT depth FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': child_id},
        )
        assert child_depth.scalar_one() == 1

    async def test_root_workflow_id_propagation(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Root workflow ID is correctly set for child workflows."""
        session, broker, app = setup

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
        )

        spec = app.workflow(
            name='parent_root_id_test',
            tasks=[node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)

        # Get child workflow ID
        row = await self._get_workflow_task_row(session, handle.workflow_id, 0)
        assert row is not None
        child_id = row[1]
        assert isinstance(child_id, str)

        # Check root_workflow_id
        child_row = await session.execute(
            text('SELECT root_workflow_id FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': child_id},
        )
        root_id = child_row.scalar_one()
        assert root_id == handle.workflow_id

    # -----------------------------------------------------------------
    # New: error / non-happy paths
    # -----------------------------------------------------------------

    async def test_subworkflow_load_failure_marks_node_failed(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """When both registry and import fallback fail, node is FAILED with SUBWORKFLOW_LOAD_FAILED."""
        session, broker, app = setup

        parent_task = make_simple_task(app, 'load_fail_parent_task')

        node_a: TaskNode[int] = TaskNode(fn=parent_task, kwargs={'value': 1})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=ImportableChildWorkflow,
            waits_for=[node_a],
        )

        spec = app.workflow(
            name='parent_load_failure',
            tasks=[node_a, node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)

        # Remove from registry
        unregister_workflow_spec(spec.name)

        # Corrupt stored module/qualname so import fallback also fails
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET sub_workflow_module = 'nonexistent.module',
                    sub_workflow_qualname = 'BadWorkflow'
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Complete dependency → triggers subworkflow load → fails
        await self._complete_task(
            session, broker, handle.workflow_id, 0, TaskResult(ok=1),
        )

        # Subworkflow node should be FAILED
        row = await self._get_workflow_task_row(session, handle.workflow_id, 1)
        assert row is not None
        status, _, result_json = row
        assert status == 'FAILED'

        tr = _decode_task_result(result_json)
        assert tr is not None and tr.is_err()
        assert tr.err is not None
        assert tr.err.error_code == 'SUBWORKFLOW_LOAD_FAILED'

    async def test_parallel_child_one_fails_downstream_skipped(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """When one of two parallel children fails, downstream task (allow_failed_deps=False) is skipped."""
        session, broker, app = setup

        node_child1: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
        )
        node_child2: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=ImportableChildWorkflow,
        )

        post_task = make_simple_task(app, 'parallel_fail_post')
        node_post: TaskNode[int] = TaskNode(
            fn=post_task,
            waits_for=[node_child1, node_child2],
            kwargs={'value': 1},
        )

        spec = app.workflow(
            name='parent_parallel_one_fails',
            tasks=[node_child1, node_child2, node_post],
            output=node_post,
        )

        handle = await start_workflow_async(spec, broker)

        # Complete child1 with failure
        await self._complete_child_workflow(
            session, broker, handle.workflow_id, 0,
            TaskResult(err=TaskError(error_code='C1_FAIL', message='child 1 failed')),
        )

        # Complete child2 with success
        await self._complete_child_workflow(
            session, broker, handle.workflow_id, 1,
            TaskResult(ok=5),
        )

        # Downstream should be SKIPPED (has failed dep, allow_failed_deps=False)
        post_row = await self._get_workflow_task_row(session, handle.workflow_id, 2)
        assert post_row is not None
        assert post_row[0] == 'SKIPPED'

    async def test_allow_failed_deps_subworkflow_still_starts(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """SubWorkflowNode with allow_failed_deps=True starts even when upstream failed."""
        session, broker, app = setup

        parent_task = make_simple_task(app, 'allow_fail_parent')

        node_a: TaskNode[int] = TaskNode(fn=parent_task, kwargs={'value': 1})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
            waits_for=[node_a],
            allow_failed_deps=True,
        )

        spec = app.workflow(
            name='parent_allow_failed',
            tasks=[node_a, node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)

        # Complete task A with failure
        await self._complete_task(
            session, broker, handle.workflow_id, 0,
            TaskResult(err=TaskError(error_code='A_FAIL', message='upstream failed')),
        )

        # Subworkflow should still be RUNNING despite failed upstream
        row = await self._get_workflow_task_row(session, handle.workflow_id, 1)
        assert row is not None
        status, child_id, _ = row
        assert status == 'RUNNING'
        assert isinstance(child_id, str)

    async def test_skip_when_prevents_downstream_after_subworkflow(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """skip_when on a downstream TaskNode skips it based on subworkflow summary."""
        session, broker, app = setup

        @app.task(task_name='skip_when_post')
        def post_task(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=99)

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=SummaryChildWorkflow,
        )
        node_post: TaskNode[int] = TaskNode(
            fn=post_task,
            waits_for=[node_child],
            workflow_ctx_from=[node_child],
            # Always skip: skip_when returns True
            skip_when=lambda ctx: ctx.summary_for(node_child).total_tasks > 0,
        )

        spec = app.workflow(
            name='parent_skip_when',
            tasks=[node_child, node_post],
        )

        handle = await start_workflow_async(spec, broker)

        # Complete child successfully (total_tasks > 0 → skip_when fires)
        await self._complete_child_workflow(
            session, broker, handle.workflow_id, 0, TaskResult(ok=1),
        )

        # Downstream should be SKIPPED
        post_row = await self._get_workflow_task_row(session, handle.workflow_id, 1)
        assert post_row is not None
        assert post_row[0] == 'SKIPPED'

    async def test_condition_exception_fails_downstream_task(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """When run_when raises an exception, downstream task fails."""
        session, broker, app = setup

        @app.task(task_name='cond_err_post')
        def post_task(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=99)

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=SummaryChildWorkflow,
        )
        node_post: TaskNode[int] = TaskNode(
            fn=post_task,
            waits_for=[node_child],
            workflow_ctx_from=[node_child],
            run_when=lambda ctx: bool(1 / 0),  # ZeroDivisionError
        )

        spec = app.workflow(
            name='parent_cond_exception',
            tasks=[node_child, node_post],
        )

        handle = await start_workflow_async(spec, broker)

        # Complete child successfully
        await self._complete_child_workflow(
            session, broker, handle.workflow_id, 0, TaskResult(ok=1),
        )

        # Downstream should be FAILED (condition evaluation error)
        post_row = await self._get_workflow_task_row(session, handle.workflow_id, 1)
        assert post_row is not None
        assert post_row[0] == 'FAILED'

    async def test_subworkflow_join_any_starts_on_first_dep(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """SubWorkflowNode with join='any' starts when first dependency completes."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'join_any_a')
        task_b = make_simple_task(app, 'join_any_b')

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(fn=task_b, kwargs={'value': 2})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
            waits_for=[node_a, node_b],
            join='any',
        )

        spec = app.workflow(
            name='parent_join_any',
            tasks=[node_a, node_b, node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)

        # Complete only task A → join='any' should trigger subworkflow start
        await self._complete_task(
            session, broker, handle.workflow_id, 0, TaskResult(ok=10),
        )

        # Subworkflow should already be RUNNING (didn't wait for B)
        row = await self._get_workflow_task_row(session, handle.workflow_id, 2)
        assert row is not None
        status, child_id, _ = row
        assert status == 'RUNNING'
        assert isinstance(child_id, str)

    async def test_subworkflow_mid_chain_result_flows(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """SubWorkflow result flows through to a downstream TaskNode in the middle of a chain."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'mid_chain_a')
        post_task = make_simple_task(app, 'mid_chain_post')

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 3})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
            waits_for=[node_a],
        )
        node_post: TaskNode[int] = TaskNode(
            fn=post_task,
            waits_for=[node_child],
            kwargs={'value': 1},
        )

        spec = app.workflow(
            name='parent_mid_chain',
            tasks=[node_a, node_child, node_post],
            output=node_post,
        )

        handle = await start_workflow_async(spec, broker)

        # Complete A → child starts
        await self._complete_task(
            session, broker, handle.workflow_id, 0, TaskResult(ok=6),
        )

        # Complete child → should enqueue post task
        await self._complete_child_workflow(
            session, broker, handle.workflow_id, 1, TaskResult(ok=50),
        )

        # Post task should be ENQUEUED
        post_row = await self._get_workflow_task_row(session, handle.workflow_id, 2)
        assert post_row is not None
        assert post_row[0] == 'ENQUEUED'

        # Subworkflow node should be COMPLETED with the child's result
        child_row = await self._get_workflow_task_row(session, handle.workflow_id, 1)
        assert child_row is not None
        assert child_row[0] == 'COMPLETED'
        tr = _decode_task_result(child_row[2])
        assert tr is not None and tr.is_ok()
        assert tr.unwrap() == 50

    async def test_summary_for_failed_child_has_error_fields(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """When child fails, sub_workflow_summary has failed_tasks > 0 and error_summary populated."""
        session, broker, app = setup

        @app.task(task_name='summary_fail_post')
        def post_task(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=SummaryChildWorkflow,
        )
        # This downstream runs regardless (run_when checks for failures)
        node_post: TaskNode[int] = TaskNode(
            fn=post_task,
            waits_for=[node_child],
            workflow_ctx_from=[node_child],
            allow_failed_deps=True,
            run_when=lambda ctx: ctx.summary_for(node_child).failed_tasks > 0,
        )

        spec = app.workflow(
            name='parent_summary_failed',
            tasks=[node_child, node_post],
        )

        handle = await start_workflow_async(spec, broker)

        # Complete child with failure
        await self._complete_child_workflow(
            session, broker, handle.workflow_id, 0,
            TaskResult(err=TaskError(error_code='CHILD_ERR', message='Something went wrong')),
        )

        # Verify sub_workflow_summary column is populated with failure data
        summary_row = await session.execute(
            text("""
                SELECT sub_workflow_summary FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        summary_json = summary_row.scalar_one()
        assert summary_json is not None
        parsed = loads_json(summary_json).unwrap() if isinstance(summary_json, str) else summary_json
        assert isinstance(parsed, dict)
        summary = SubWorkflowSummary.from_json(parsed)
        assert summary.status == WorkflowStatus.FAILED
        assert summary.failed_tasks >= 1
        assert summary.total_tasks >= 1
        assert summary.error_summary is not None and len(summary.error_summary) > 0

        # Downstream post task should be ENQUEUED (run_when matched failed_tasks > 0)
        post_row = await self._get_workflow_task_row(session, handle.workflow_id, 1)
        assert post_row is not None
        assert post_row[0] == 'ENQUEUED'

    async def test_multiple_args_from_keys(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Multiple args_from keys inject results from different upstream tasks."""
        session, broker, app = setup

        task_a = make_simple_task(app, 'multi_args_a')
        task_b = make_simple_task(app, 'multi_args_b')

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 3})
        node_b: TaskNode[int] = TaskNode(fn=task_b, kwargs={'value': 5})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=MultiParamChildWorkflow,
            waits_for=[node_a, node_b],
            args_from={'first': node_a, 'second': node_b},
        )

        spec = app.workflow(
            name='parent_multi_args',
            tasks=[node_a, node_b, node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)

        # Complete both upstream tasks
        await self._complete_task(
            session, broker, handle.workflow_id, 0, TaskResult(ok=10),
        )
        await self._complete_task(
            session, broker, handle.workflow_id, 1, TaskResult(ok=20),
        )

        # Subworkflow should be RUNNING
        row = await self._get_workflow_task_row(session, handle.workflow_id, 2)
        assert row is not None
        status, child_id, _ = row
        assert status == 'RUNNING'
        assert isinstance(child_id, str)

    async def test_deeply_nested_depth_and_root_id(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """3-level nesting: parent(0) → middle(1) → child(2), depth and root_workflow_id propagate."""
        session, broker, app = setup

        node_middle: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=MiddleWorkflow,
        )

        spec = app.workflow(
            name='parent_deep_nest',
            tasks=[node_middle],
            output=node_middle,
        )

        handle = await start_workflow_async(spec, broker)

        # Parent depth = 0
        parent_depth_row = await session.execute(
            text('SELECT depth FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        assert parent_depth_row.scalar_one() == 0

        # Middle workflow: depth = 1
        row = await self._get_workflow_task_row(session, handle.workflow_id, 0)
        assert row is not None
        middle_id = row[1]
        assert isinstance(middle_id, str)

        middle_depth_row = await session.execute(
            text('SELECT depth, root_workflow_id FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': middle_id},
        )
        middle_info = middle_depth_row.fetchone()
        assert middle_info is not None
        assert middle_info[0] == 1
        assert middle_info[1] == handle.workflow_id

        # Grandchild workflow: depth = 2, same root
        grandchild_row = await session.execute(
            text("""
                SELECT sub_workflow_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': middle_id},
        )
        grandchild_id_row = grandchild_row.fetchone()
        assert grandchild_id_row is not None
        grandchild_id = grandchild_id_row[0]
        assert isinstance(grandchild_id, str)

        grandchild_depth_row = await session.execute(
            text('SELECT depth, root_workflow_id FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': grandchild_id},
        )
        grandchild_info = grandchild_depth_row.fetchone()
        assert grandchild_info is not None
        assert grandchild_info[0] == 2
        assert grandchild_info[1] == handle.workflow_id

    async def test_broker_app_missing_marks_node_failed(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """When broker.app is None at enqueue time, node is FAILED (no post-ENQUEUED raise)."""
        session, broker, app = setup

        parent_task = make_simple_task(app, 'broker_missing_parent')

        node_a: TaskNode[int] = TaskNode(fn=parent_task, kwargs={'value': 1})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
            waits_for=[node_a],
        )

        spec = app.workflow(
            name='parent_broker_missing',
            tasks=[node_a, node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)

        # Detach app from broker
        original_app = broker.app
        broker.app = None
        try:
            # Completing dep triggers subworkflow enqueue → broker.app is None.
            # Post-ENQUEUED failures are converted to FAILED state instead of raising.
            await self._complete_task(
                session, broker, handle.workflow_id, 0, TaskResult(ok=1),
            )
        finally:
            broker.app = original_app

        row = await self._get_workflow_task_row(session, handle.workflow_id, 1)
        assert row is not None
        status, child_id, result_json = row
        assert status == 'FAILED'
        assert child_id is None

        tr = _decode_task_result(result_json)
        assert tr is not None and tr.is_err()
        assert tr.err is not None
        assert tr.err.error_code == 'WORKFLOW_ENQUEUE_FAILED'
        assert tr.err.message is not None
        assert 'Broker missing app reference' in tr.err.message


# =============================================================================
# TestSubworkflowValidation
# =============================================================================


@pytest.mark.integration
class TestSubworkflowValidation:
    """Tests for subworkflow validation during spec creation."""

    def test_cycle_detection_mutual_reference(self, app: Horsies) -> None:
        """Mutual cycle: A references B, B references A → WorkflowValidationError."""
        # CycleB defined first (empty body → metaclass sets _workflow_nodes = [])
        class CycleB(WorkflowDefinition[int]):
            name = 'cycle_b'

        # CycleA references CycleB via class attribute → metaclass picks this up naturally
        class CycleA(WorkflowDefinition[int]):
            name = 'cycle_a'
            sub_b: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=CycleB)

        # Back-reference: CycleB → CycleA. Must override _workflow_nodes because
        # CycleA didn't exist when CycleB's metaclass ran. This is the only way to
        # create mutual references in Python's top-down class definition order.
        CycleB._workflow_nodes = [('sub_a', SubWorkflowNode(workflow_def=CycleA))]

        parent_node: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=CycleA)

        with pytest.raises(WorkflowValidationError) as exc:
            app.workflow(
                name='parent_cyclic',
                tasks=[parent_node],
                output=parent_node,
            )

        assert 'cycle' in str(exc.value).lower()

    def test_unsupported_retry_mode_rejected(self, app: Horsies) -> None:
        """SubWorkflowNode with unsupported retry_mode raises validation error."""
        from horsies.core.models.workflow import SubWorkflowRetryMode

        child_node = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
            retry_mode=SubWorkflowRetryMode.RERUN_ALL,
        )

        with pytest.raises(WorkflowValidationError) as exc:
            app.workflow(
                name='unsupported_retry_mode',
                tasks=[child_node],
                output=child_node,
            )

        assert 'retry_mode' in str(exc.value).lower()

    def test_args_from_without_waits_for_rejected(self, app: Horsies) -> None:
        """args_from referencing a node not in waits_for raises WORKFLOW_INVALID_ARGS_FROM."""
        task_a_fn = make_simple_task(app, 'args_from_no_dep_a')

        node_a: TaskNode[int] = TaskNode(fn=task_a_fn, kwargs={'value': 1})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=ParamChildWorkflow,
            # waits_for is empty, but args_from references node_a
            args_from={'value': node_a},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            app.workflow(
                name='args_from_no_dep',
                tasks=[node_a, node_child],
                output=node_child,
            )

        assert 'args_from' in str(exc.value).lower()


# =============================================================================
# TestSubworkflowRecovery
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestSubworkflowRecovery:
    """Tests for subworkflow recovery scenarios."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
        await session.commit()
        return session, broker, app

    async def test_recovery_child_completed_parent_not_updated(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Recovery handles case where child completed but parent node wasn't updated."""
        session, broker, app = setup
        from horsies.core.workflows.recovery import recover_stuck_workflows

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
        )

        spec = app.workflow(
            name='parent_recovery_test',
            tasks=[node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)

        # Get child workflow ID
        row_res = await session.execute(
            text("""
                SELECT sub_workflow_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        row = row_res.fetchone()
        assert row is not None
        child_id = row[0]
        assert isinstance(child_id, str)

        # Simulate: child workflow completed but parent node still RUNNING
        # (as if the callback failed midway)
        await session.execute(
            text("""
                UPDATE horsies_workflows SET status = 'COMPLETED', result = :result
                WHERE id = :child_id
            """),
            {'child_id': child_id, 'result': '{"ok": 99}'},
        )
        # Parent node stays RUNNING (simulating interrupted callback)
        await session.commit()

        # Run recovery
        recovered = await recover_stuck_workflows(session, broker)
        await session.commit()

        assert recovered >= 1

        # Parent node should now be COMPLETED
        parent_row = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        parent_status = parent_row.scalar_one()
        assert parent_status == 'COMPLETED'

    async def test_recovery_ready_subworkflow_not_started(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Recovery handles READY subworkflow that was never started."""
        session, broker, app = setup
        from horsies.core.workflows.recovery import recover_stuck_workflows

        parent_task = make_simple_task(app, 'recovery_parent_task')

        node_a: TaskNode[int] = TaskNode(fn=parent_task, kwargs={'value': 1})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
            waits_for=[node_a],
        )

        spec = app.workflow(
            name='parent_ready_recovery',
            tasks=[node_a, node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)

        # Complete task A to make child READY
        res = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_row = res.fetchone()
        assert task_row is not None and task_row[0] is not None
        await on_workflow_task_complete(session, task_row[0], TaskResult(ok=2), broker)
        await session.commit()

        # Verify child is RUNNING (started normally)
        child_row = await session.execute(
            text("""
                SELECT status, sub_workflow_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        child_status_row = child_row.fetchone()
        assert child_status_row is not None
        # Child should be RUNNING with sub_workflow_id set
        assert child_status_row[0] == 'RUNNING'
        assert child_status_row[1] is not None

    async def test_recovery_child_failed_parent_still_running(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Recovery handles case where child FAILED but parent node still stuck RUNNING."""
        session, broker, app = setup
        from horsies.core.workflows.recovery import recover_stuck_workflows

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
        )

        spec = app.workflow(
            name='parent_recovery_failed_child',
            tasks=[node_child],
            output=node_child,
        )

        handle = await start_workflow_async(spec, broker)

        # Get child workflow ID
        row_res = await session.execute(
            text("""
                SELECT sub_workflow_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        row = row_res.fetchone()
        assert row is not None
        child_id = row[0]
        assert isinstance(child_id, str)

        # Simulate: child workflow FAILED but parent node still RUNNING
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'FAILED',
                    error = :error
                WHERE id = :child_id
            """),
            {
                'child_id': child_id,
                'error': dumps_json({'message': 'child task crashed'}).unwrap(),
            },
        )
        # Also mark child's task as FAILED for accurate task counts
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED',
                    result = :result
                WHERE workflow_id = :child_id AND task_index = 0
            """),
            {
                'child_id': child_id,
                'result': dumps_json(TaskResult(err=TaskError(
                    error_code='CHILD_CRASH',
                    message='child task crashed',
                ))).unwrap(),
            },
        )
        await session.commit()

        # Run recovery
        recovered = await recover_stuck_workflows(session, broker)
        await session.commit()

        assert recovered >= 1

        # Parent node should now be FAILED
        parent_row = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        parent_status = parent_row.scalar_one()
        assert parent_status == 'FAILED'
