"""Integration test for the _evaluate_conditions DB-backed fallback path (BUG 3 regression).

Verifies that when the workflow registry misses (simulated via unregister),
the engine falls back to the DB-stored module/qualname, dynamically imports
the WorkflowDefinition, resolves nodes via _resolve_workflow_def_nodes,
and evaluates conditions correctly — all without mutating class-level nodes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.serde import loads_json, task_result_from_json
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.task_send_types import TaskSendError, TaskSendResult
from horsies.core.task_decorator import TaskHandle, TaskFunction, NodeFactory
from horsies.core.types.result import Ok
from horsies.core.models.workflow import (
    TaskNode,
    WorkflowDefinition,
    WorkflowContext,
)
from horsies.core.workflows.engine import on_workflow_task_complete
from horsies.core.workflows.registry import unregister_workflow_spec

from .conftest import start_ok


# =============================================================================
# Module-level mock task functions (needed for declarative WorkflowDefinition)
# =============================================================================


@dataclass
class _MockFn(TaskFunction[Any, Any]):
    """Minimal TaskFunction for integration tests."""

    task_name: str
    task_ok_type: Any = Any

    def __call__(self, *args: Any, **kwargs: Any) -> TaskResult[Any, TaskError]:
        return TaskResult(ok=None)

    def send(self, *args: Any, **kwargs: Any) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    async def send_async(self, *args: Any, **kwargs: Any) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def schedule(self, delay: int, *args: Any, **kwargs: Any) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def retry_send(self, error: TaskSendError) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    async def retry_send_async(self, error: TaskSendError) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def retry_schedule(self, error: TaskSendError) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def node(self, **kwargs: Any) -> NodeFactory[Any, Any]:
        return NodeFactory(fn=self, **kwargs)  # type: ignore[arg-type]


@dataclass
class _MockFnWithCtx(TaskFunction[Any, Any]):
    """MockFn that accepts workflow_ctx (passes E010 validator)."""

    task_name: str
    task_ok_type: Any = Any

    def __call__(
        self,
        *args: Any,
        workflow_ctx: WorkflowContext | None = None,
        **kwargs: Any,
    ) -> TaskResult[Any, TaskError]:
        return TaskResult(ok=None)

    def send(self, *args: Any, **kwargs: Any) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    async def send_async(self, *args: Any, **kwargs: Any) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def schedule(self, delay: int, *args: Any, **kwargs: Any) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def retry_send(self, error: TaskSendError) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    async def retry_send_async(self, error: TaskSendError) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def retry_schedule(self, error: TaskSendError) -> TaskSendResult[TaskHandle[Any]]:
        return Ok(TaskHandle('mock'))

    def node(self, **kwargs: Any) -> NodeFactory[Any, Any]:
        return NodeFactory(fn=self, **kwargs)  # type: ignore[arg-type]


_fn_a = _MockFn(task_name='fallback_task_a')
_fn_b = _MockFnWithCtx(task_name='fallback_task_b')

_fn_a_proceed = _MockFn(task_name='fallback_proceed_task_a')
_fn_b_proceed = _MockFnWithCtx(task_name='fallback_proceed_task_b')


# =============================================================================
# Module-level declarative WorkflowDefinitions (importlib must find them)
#
# Nodes are class-level attributes so get_workflow_nodes() returns them.
# The DB fallback calls get_workflow_nodes() to reconstruct the node graph.
# =============================================================================


# --- Workflow 1: skip_when=True (result > 3) ---

# Module-level node so the condition lambda can capture it.
_skip_node_a: TaskNode[int] = TaskNode(fn=_fn_a)


def _should_skip(ctx: WorkflowContext) -> bool:
    """skip_when: skip if upstream result > 3."""
    result = ctx.result_for(_skip_node_a)
    return result.is_ok() and result.unwrap() > 3


class ConditionFallbackSkipWorkflow(WorkflowDefinition[int]):
    """Declarative workflow with skip_when evaluated via DB fallback."""

    name = 'condition_fallback_skip_wf'

    fetch = _skip_node_a
    process: TaskNode[int] = TaskNode(
        fn=_fn_b,
        waits_for=[_skip_node_a],
        workflow_ctx_from=[_skip_node_a],
        skip_when=_should_skip,
    )


# --- Workflow 2: skip_when=False (result NOT > 100) → task proceeds ---

_proceed_node_a: TaskNode[int] = TaskNode(fn=_fn_a_proceed)


def _should_not_skip(ctx: WorkflowContext) -> bool:
    """skip_when: skip if upstream result > 100 (never true in this test)."""
    result = ctx.result_for(_proceed_node_a)
    return result.is_ok() and result.unwrap() > 100


class ConditionFallbackProceedWorkflow(WorkflowDefinition[int]):
    """Declarative workflow where skip_when returns False via DB fallback."""

    name = 'condition_fallback_proceed_wf'

    fetch = _proceed_node_a
    process: TaskNode[int] = TaskNode(
        fn=_fn_b_proceed,
        waits_for=[_proceed_node_a],
        workflow_ctx_from=[_proceed_node_a],
        skip_when=_should_not_skip,
    )


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestConditionFallbackPath:
    """_evaluate_conditions DB fallback: registry miss -> dynamic import -> condition eval."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        """Clean tables and return fixtures."""
        await session.execute(text(
            'TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'
        ))
        await session.commit()
        return session, broker, app

    async def _complete_task(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
        result: TaskResult[Any, TaskError],
    ) -> None:
        """Simulate task completion via on_workflow_task_complete."""
        res = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = res.fetchone()
        if row and row[0]:
            await on_workflow_task_complete(session, row[0], result)
            await session.commit()

    async def _get_task_status(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
    ) -> str:
        """Get workflow task status."""
        result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = result.fetchone()
        return row[0] if row else 'NOT_FOUND'

    async def _get_task_result(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
    ) -> TaskResult[Any, TaskError] | None:
        """Get stored workflow task result payload."""
        result = await session.execute(
            text("""
                SELECT result FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = result.fetchone()
        if row is None or row[0] is None:
            return None
        parsed = loads_json(row[0]).unwrap()
        return task_result_from_json(parsed).unwrap()

    # ------------------------------------------------------------------
    # Core regression: condition evaluated via DB fallback
    # ------------------------------------------------------------------

    async def test_skip_when_evaluated_via_db_fallback(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Registry miss triggers DB fallback; skip_when=True correctly SKIPs the task.

        BUG 3 regression: the old _node_from_workflow_def mutated class-level
        nodes. This test verifies the full fallback path works end-to-end.
        """
        session, broker, app = setup

        spec = ConditionFallbackSkipWorkflow.build(app)

        # Capture class-level node state after build (back-propagation stamps them)
        nodes = ConditionFallbackSkipWorkflow.get_workflow_nodes()
        assert nodes is not None and len(nodes) == 2
        class_fetch = nodes[0][1]
        class_process = nodes[1][1]
        fetch_node_id_after_build = class_fetch.node_id
        fetch_index_after_build = class_fetch.index

        handle = await start_ok(spec, broker)

        # Force registry miss — engine must fall back to DB lookup
        unregister_workflow_spec(spec.name)

        # Complete task A with result=10 (which is > 3 → skip_when returns True)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # Task B should be SKIPPED via the DB fallback condition evaluation
        status_b = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'SKIPPED', (
            f'Expected SKIPPED but got {status_b} — '
            'DB fallback condition evaluation may have failed'
        )

        result_b = await self._get_task_result(session, handle.workflow_id, 1)
        assert result_b is not None
        assert result_b.is_err()
        assert result_b.unwrap_err().error_code == 'WORKFLOW_CONDITION_SKIP_WHEN_TRUE'

        # BUG 3 assertion: class-level nodes not further mutated by the fallback.
        # build() + back-propagation stamps them once; the fallback resolver
        # must NOT write additional values.
        assert class_fetch.node_id == fetch_node_id_after_build
        assert class_fetch.index == fetch_index_after_build

    async def test_condition_proceeds_via_db_fallback(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Registry miss triggers DB fallback; skip_when=False lets the task proceed."""
        session, broker, app = setup

        spec = ConditionFallbackProceedWorkflow.build(app)
        handle = await start_ok(spec, broker)

        # Force registry miss
        unregister_workflow_spec(spec.name)

        # Complete task A with result=1 (which is NOT > 100 → skip_when False)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=1))

        # Task B should be ENQUEUED (condition evaluated to "proceed")
        status_b = await self._get_task_status(session, handle.workflow_id, 1)
        assert status_b == 'ENQUEUED', (
            f'Expected ENQUEUED but got {status_b} — '
            'DB fallback condition evaluation may have failed'
        )
