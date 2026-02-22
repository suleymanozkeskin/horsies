"""Integration tests for workflow_ctx injection."""

from __future__ import annotations

from typing import Any, TypeGuard

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.serde import dumps_json, loads_json, task_result_from_json
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.errors import ErrorCode
from horsies.core.models.workflow import (
    NodeKey,
    TaskNode,
    WorkflowContext,
    WorkflowMeta,
    WorkflowValidationError,
)
from horsies.core.workflows.engine import on_workflow_task_complete
from horsies.core.worker.current import set_current_app
from horsies.core.worker.worker import _run_task_entry, _initialize_worker_pool

from .conftest import make_simple_task, make_no_ctx_task, make_workflow_spec, start_ok


def _is_str_keyed_dict(value: object) -> TypeGuard[dict[str, Any]]:
    if not isinstance(value, dict):
        return False
    value_keys: list[object] = list(value.keys())
    for key in value_keys:
        if not isinstance(key, str):
            return False
    return True


def _is_str_to_str_dict(value: object) -> TypeGuard[dict[str, str]]:
    if not isinstance(value, dict):
        return False
    value_items: list[tuple[object, object]] = list(value.items())
    for key, val in value_items:
        if not isinstance(key, str) or not isinstance(val, str):
            return False
    return True


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowCtx:
    """Tests for workflow_ctx injection."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        """Clean tables and return fixtures."""
        await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
        await session.commit()
        return session, broker, app

    async def _claim_task(
        self,
        session: AsyncSession,
        task_id: str,
        worker_id: str,
    ) -> None:
        """Helper to claim a task for a worker (required before _run_task_entry)."""
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'CLAIMED',
                    claimed = TRUE,
                    claimed_at = NOW(),
                    claimed_by_worker_id = :worker_id,
                    claim_expires_at = NULL
                WHERE id = :task_id
            """),
            {'task_id': task_id, 'worker_id': worker_id},
        )
        await session.commit()

    async def _complete_task(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
        result: TaskResult[Any, TaskError],
    ) -> None:
        """Helper to simulate task completion."""
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

    async def _get_task_kwargs(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
    ) -> dict[str, Any]:
        """Get kwargs from tasks table for the workflow task."""
        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = wt_result.fetchone()
        if not row or not row[0]:
            return {}

        task_result = await session.execute(
            text('SELECT kwargs FROM horsies_tasks WHERE id = :tid'),
            {'tid': row[0]},
        )
        task_row = task_result.fetchone()
        if not task_row or not task_row[0]:
            return {}

        data = loads_json(task_row[0]).unwrap()
        return data if isinstance(data, dict) else {}

    def _build_ctx_from_kwargs(self, kwargs: dict[str, Any]) -> WorkflowContext:
        """Reconstruct WorkflowContext from injected kwargs."""
        ctx_data_raw = kwargs.get('__horsies_workflow_ctx__')
        if not _is_str_keyed_dict(ctx_data_raw):
            raise AssertionError('workflow_ctx data missing or invalid')

        typed_ctx_data = ctx_data_raw

        results_by_id: dict[str, TaskResult[Any, TaskError]] = {}
        raw_results = typed_ctx_data.get('results_by_id')
        if _is_str_to_str_dict(raw_results):
            for node_id, result_json in raw_results.items():
                results_by_id[node_id] = task_result_from_json(loads_json(result_json).unwrap()).unwrap()

        return WorkflowContext.from_serialized(
            workflow_id=typed_ctx_data.get('workflow_id', ''),
            task_index=typed_ctx_data.get('task_index', 0),
            task_name=typed_ctx_data.get('task_name', ''),
            results_by_id=results_by_id,
        )

    async def test_ctx_injected_and_result_for_works(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Injected workflow_ctx supports result_for(node)."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'ctx_inject_a')

        @app.task(task_name='ctx_inject_b')
        def ctx_reader(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            if workflow_ctx is None:
                return TaskResult(err=TaskError(error_code='NO_CTX', message='missing'))
            result = workflow_ctx.result_for(node_a)
            if result.is_err():
                return TaskResult(err=result.err_value)
            return TaskResult(ok=result.unwrap())

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})
        node_b: TaskNode[int] = TaskNode(
            fn=ctx_reader,
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
        )

        spec = make_workflow_spec(
            broker=broker, name='ctx_inject', tasks=[node_a, node_b]
        )

        handle = await start_ok(spec, broker)

        # Complete A
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 1)
        ctx = self._build_ctx_from_kwargs(kwargs)

        result = ctx.result_for(node_a)
        assert result.is_ok()
        assert result.unwrap() == 10

        # Validate metadata too
        assert ctx.workflow_id == handle.workflow_id
        assert ctx.task_index == 1
        assert ctx.task_name == 'ctx_inject_b'

    async def test_ctx_not_injected_without_ctx_from(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """No workflow_ctx injected when workflow_ctx_from is not set."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'no_ctx_from_a')

        @app.task(task_name='no_ctx_from_b')
        def ctx_reader(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            if workflow_ctx is None:
                return TaskResult(ok=0)
            return TaskResult(ok=1)

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})
        node_b: TaskNode[int] = TaskNode(
            fn=ctx_reader,
            waits_for=[node_a],
        )

        spec = make_workflow_spec(
            broker=broker, name='no_ctx_from', tasks=[node_a, node_b]
        )

        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 1)
        assert '__horsies_workflow_ctx__' not in kwargs

    async def test_workflow_meta_injected_without_ctx_from(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """workflow_meta is injected even when workflow_ctx_from is not used."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'meta_only_a')

        @app.task(task_name='meta_only_b')
        def meta_reader(
            workflow_meta: WorkflowMeta | None = None,
        ) -> TaskResult[int, TaskError]:
            if workflow_meta is None:
                return TaskResult(ok=-1)
            return TaskResult(ok=workflow_meta.task_index)

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})
        node_b: TaskNode[int] = TaskNode(
            fn=meta_reader,
            waits_for=[node_a],
        )

        spec = make_workflow_spec(
            broker=broker, name='meta_only', tasks=[node_a, node_b]
        )
        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 1)
        meta_raw = kwargs.get('__horsies_workflow_meta__')
        assert isinstance(meta_raw, dict)
        assert meta_raw['workflow_id'] == handle.workflow_id
        assert meta_raw['task_index'] == 1
        assert meta_raw['task_name'] == 'meta_only_b'

    async def test_ctx_multiple_deps_result_for_each(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """workflow_ctx includes multiple deps and result_for works for each."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'ctx_multi_a')
        task_b = make_simple_task(app, 'ctx_multi_b')

        @app.task(task_name='ctx_multi_c')
        def ctx_reader(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            if workflow_ctx is None:
                return TaskResult(err=TaskError(error_code='NO_CTX', message='missing'))
            first = workflow_ctx.result_for(node_a).unwrap()
            second = workflow_ctx.result_for(node_b).unwrap()
            return TaskResult(ok=first + second)

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(fn=task_b, kwargs={'value': 2})
        node_c: TaskNode[int] = TaskNode(
            fn=ctx_reader,
            waits_for=[node_a, node_b],
            workflow_ctx_from=[node_a, node_b],
        )

        spec = make_workflow_spec(
            broker=broker, name='ctx_multi', tasks=[node_a, node_b, node_c]
        )

        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=20))

        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 2)
        ctx = self._build_ctx_from_kwargs(kwargs)

        assert ctx.result_for(node_a).unwrap() == 10
        assert ctx.result_for(node_b).unwrap() == 20

    async def test_ctx_skipped_dep_returns_upstream_skipped(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """SKIPPED dep in workflow_ctx_from yields UPSTREAM_SKIPPED sentinel."""
        session, broker, app = setup

        @app.task(task_name='ctx_skipped_failing')
        def failing_task() -> TaskResult[int, TaskError]:
            return TaskResult(err=TaskError(error_code='FAIL', message='Failed'))

        task_b = make_simple_task(app, 'ctx_skipped_b')

        @app.task(task_name='ctx_skipped_c')
        def ctx_reader(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[str, TaskError]:
            if workflow_ctx is None:
                return TaskResult(err=TaskError(error_code='NO_CTX', message='missing'))
            result = workflow_ctx.result_for(node_b)
            if result.is_err():
                return TaskResult(err=result.err_value)
            return TaskResult(ok='unexpected')

        node_a = TaskNode(fn=failing_task)
        node_b = TaskNode(fn=task_b, kwargs={'value': 1}, waits_for=[node_a])
        node_c = TaskNode(
            fn=ctx_reader,
            waits_for=[node_b],
            workflow_ctx_from=[node_b],
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            broker=broker, name='ctx_skipped', tasks=[node_a, node_b, node_c]
        )

        handle = await start_ok(spec, broker)

        # Complete A as FAILED -> B becomes SKIPPED -> C becomes READY
        await self._complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='A_FAILED', message='A failed')),
        )

        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 2)
        ctx = self._build_ctx_from_kwargs(kwargs)

        result = ctx.result_for(node_b)
        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == 'UPSTREAM_SKIPPED'
        assert result.err.data is not None
        assert 'dependency_index' in result.err.data

    async def test_ctx_failed_dep_returns_original_error(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """FAILED dep in workflow_ctx_from yields original TaskError."""
        session, broker, app = setup

        @app.task(task_name='ctx_failed_failing')
        def failing_task() -> TaskResult[int, TaskError]:
            return TaskResult(err=TaskError(error_code='FAIL', message='Failed'))

        @app.task(task_name='ctx_failed_c')
        def ctx_reader(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[str, TaskError]:
            if workflow_ctx is None:
                return TaskResult(err=TaskError(error_code='NO_CTX', message='missing'))
            result = workflow_ctx.result_for(node_a)
            if result.is_err():
                return TaskResult(err=result.err_value)
            return TaskResult(ok='unexpected')

        node_a = TaskNode(fn=failing_task)
        node_c = TaskNode(
            fn=ctx_reader,
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            broker=broker, name='ctx_failed', tasks=[node_a, node_c]
        )

        handle = await start_ok(spec, broker)

        # Complete A as FAILED -> B should be ENQUEUED (allow_failed_deps)
        await self._complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='A_FAILED', message='A failed')),
        )

        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 1)
        ctx = self._build_ctx_from_kwargs(kwargs)

        result = ctx.result_for(node_a)
        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == 'A_FAILED'
        assert result.err.message == 'A failed'

    async def test_ctx_not_injected_without_param(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Tasks without workflow_ctx param reject workflow_ctx_from."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'no_ctx_param_a')
        task_b = make_no_ctx_task(app, 'no_ctx_param_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        node_b = TaskNode(
            fn=task_b,
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
        )

        with pytest.raises(
            WorkflowValidationError,
            match='workflow_ctx_from declared but function missing workflow_ctx param',
        ) as exc_info:
            make_workflow_spec(
                broker=broker, name='no_ctx_param', tasks=[node_a, node_b]
            )
        assert exc_info.value.code == ErrorCode.WORKFLOW_CTX_PARAM_MISSING

    async def test_worker_deserializes_workflow_ctx(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Worker reconstructs WorkflowContext and result_for(node) works in task code."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'ctx_worker_a')

        @app.task(task_name='ctx_worker_b')
        def ctx_worker_b(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            if workflow_ctx is None:
                return TaskResult(err=TaskError(error_code='NO_CTX', message='missing'))
            return TaskResult(ok=workflow_ctx.result_for(node_a).unwrap())

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})
        node_b: TaskNode[int] = TaskNode(
            fn=ctx_worker_b,
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
        )

        spec = make_workflow_spec(
            broker=broker, name='ctx_worker', tasks=[node_a, node_b]
        )
        handle = await start_ok(spec, broker)

        # Complete A to enqueue B
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        task_row_result = await session.execute(
            text("""
                SELECT t.id, t.task_name, t.args, t.kwargs
                FROM horsies_tasks t
                JOIN horsies_workflow_tasks wt ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf_id AND wt.task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        task_row = task_row_result.fetchone()
        assert task_row is not None

        task_id, task_name, args_json, kwargs_json = task_row
        assert isinstance(task_id, str)
        assert isinstance(task_name, str)
        assert isinstance(args_json, str)
        assert isinstance(kwargs_json, str)

        # Claim the task before running (required by ownership check)
        await self._claim_task(session, task_id, 'test-worker')

        set_current_app(app)
        database_url = broker.config.database_url.replace('+asyncpg', '').replace(
            '+psycopg', ''
        )
        _initialize_worker_pool(database_url)
        ok, result_json, worker_failure = _run_task_entry(
            task_name=task_name,
            args_json=args_json,
            kwargs_json=kwargs_json,
            task_id=task_id,
            database_url=database_url,
            master_worker_id='test-worker',
        )
        assert ok is True
        assert worker_failure is None

        task_result = task_result_from_json(loads_json(result_json).unwrap()).unwrap()
        assert task_result.is_ok()
        assert task_result.unwrap() == 10

    async def test_worker_injects_required_workflow_meta(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Worker injects workflow_meta so required parameter tasks run successfully."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'meta_worker_a')

        @app.task(task_name='meta_worker_b')
        def meta_worker_b(
            workflow_meta: WorkflowMeta,
        ) -> TaskResult[str, TaskError]:
            return TaskResult(
                ok=f'{workflow_meta.workflow_id}:{workflow_meta.task_index}:{workflow_meta.task_name}'
            )

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})
        node_b: TaskNode[str] = TaskNode(
            fn=meta_worker_b,
            waits_for=[node_a],
        )

        spec = make_workflow_spec(
            broker=broker, name='meta_worker', tasks=[node_a, node_b]
        )
        handle = await start_ok(spec, broker)

        # Complete A to enqueue B
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        task_row_result = await session.execute(
            text("""
                SELECT t.id, t.task_name, t.args, t.kwargs
                FROM horsies_tasks t
                JOIN horsies_workflow_tasks wt ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf_id AND wt.task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        task_row = task_row_result.fetchone()
        assert task_row is not None

        task_id, task_name, args_json, kwargs_json = task_row
        assert isinstance(task_id, str)
        assert isinstance(task_name, str)
        assert isinstance(args_json, str)
        assert isinstance(kwargs_json, str)

        # Claim the task before running (required by ownership check)
        await self._claim_task(session, task_id, 'test-worker')

        set_current_app(app)
        database_url = broker.config.database_url.replace('+asyncpg', '').replace(
            '+psycopg', ''
        )
        _initialize_worker_pool(database_url)
        ok, result_json, worker_failure = _run_task_entry(
            task_name=task_name,
            args_json=args_json,
            kwargs_json=kwargs_json,
            task_id=task_id,
            database_url=database_url,
            master_worker_id='test-worker',
        )
        assert ok is True
        assert worker_failure is None

        task_result = task_result_from_json(loads_json(result_json).unwrap()).unwrap()
        assert task_result.is_ok()
        assert task_result.unwrap() == f'{handle.workflow_id}:1:meta_worker_b'

    async def test_worker_ctx_missing_id_surfaces_error(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """If a TaskNode has no node_id, result_for raises and is surfaced as TaskError."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'ctx_missing_id_a')

        missing_node: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})

        @app.task(task_name='ctx_missing_id_b')
        def ctx_missing_id_b(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            if workflow_ctx is None:
                return TaskResult(err=TaskError(error_code='NO_CTX', message='missing'))
            # missing_node.node_id is None -> should raise RuntimeError
            return TaskResult(ok=workflow_ctx.result_for(missing_node).unwrap())

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})
        node_b: TaskNode[int] = TaskNode(
            fn=ctx_missing_id_b,
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
        )

        spec = make_workflow_spec(
            broker=broker, name='ctx_missing_id', tasks=[node_a, node_b]
        )
        handle = await start_ok(spec, broker)

        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        task_row_result = await session.execute(
            text("""
                SELECT t.id, t.task_name, t.args, t.kwargs
                FROM horsies_tasks t
                JOIN horsies_workflow_tasks wt ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf_id AND wt.task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        task_row = task_row_result.fetchone()
        assert task_row is not None

        task_id, task_name, args_json, kwargs_json = task_row
        assert isinstance(task_id, str)
        assert isinstance(task_name, str)
        assert isinstance(args_json, str)
        assert isinstance(kwargs_json, str)

        # Claim the task before running (required by ownership check)
        await self._claim_task(session, task_id, 'test-worker')

        set_current_app(app)
        database_url = broker.config.database_url.replace('+asyncpg', '').replace(
            '+psycopg', ''
        )
        _initialize_worker_pool(database_url)
        ok, result_json, worker_failure = _run_task_entry(
            task_name=task_name,
            args_json=args_json,
            kwargs_json=kwargs_json,
            task_id=task_id,
            database_url=database_url,
            master_worker_id='test-worker',
        )
        assert ok is True
        assert worker_failure is None

        task_result = task_result_from_json(loads_json(result_json).unwrap()).unwrap()
        assert task_result.is_err()
        assert task_result.unwrap_err().error_code == 'WORKFLOW_CTX_MISSING_ID'

    async def test_ctx_from_not_in_waits_for_raises_e009(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """workflow_ctx_from referencing a node not in waits_for raises E009."""
        _session, broker, app = setup
        task_a = make_simple_task(app, 'e009_a')

        @app.task(task_name='e009_b')
        def ctx_reader(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(
            fn=ctx_reader,
            # waits_for intentionally omitted — node_a is NOT a dependency
            workflow_ctx_from=[node_a],
        )

        with pytest.raises(
            WorkflowValidationError,
            match='workflow_ctx_from references task not in waits_for',
        ) as exc_info:
            make_workflow_spec(
                broker=broker, name='e009', tasks=[node_a, node_b],
            )
        assert exc_info.value.code == ErrorCode.WORKFLOW_INVALID_CTX_FROM

    async def test_ctx_result_for_unknown_node_raises_key_error(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """result_for() with a valid node_id not in context raises KeyError."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'key_err_a')

        @app.task(task_name='key_err_b')
        def ctx_reader(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(
            fn=ctx_reader,
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
        )

        spec = make_workflow_spec(
            broker=broker, name='key_err', tasks=[node_a, node_b],
        )
        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 1)
        ctx = self._build_ctx_from_kwargs(kwargs)

        # node_c was never part of this workflow — its node_id won't be in context
        task_c = make_simple_task(app, 'key_err_c')
        node_c: TaskNode[int] = TaskNode(fn=task_c, kwargs={'value': 1})
        # Manually assign a node_id so result_for reaches the KeyError branch
        node_c.node_id = 'nonexistent-node-id'

        with pytest.raises(KeyError, match='not in workflow context'):
            ctx.result_for(node_c)

    async def test_ctx_has_result_true_for_present_node(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """has_result() returns True for a node whose result is in context."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'has_res_a')

        @app.task(task_name='has_res_b')
        def ctx_reader(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(
            fn=ctx_reader,
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
        )

        spec = make_workflow_spec(
            broker=broker, name='has_res', tasks=[node_a, node_b],
        )
        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=42))

        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 1)
        ctx = self._build_ctx_from_kwargs(kwargs)

        assert ctx.has_result(node_a) is True

    async def test_ctx_has_result_false_for_absent_and_unset_node(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """has_result() returns False for absent node_id and for node_id=None."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'has_res_false_a')

        @app.task(task_name='has_res_false_b')
        def ctx_reader(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(
            fn=ctx_reader,
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
        )

        spec = make_workflow_spec(
            broker=broker, name='has_res_false', tasks=[node_a, node_b],
        )
        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=1))

        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 1)
        ctx = self._build_ctx_from_kwargs(kwargs)

        # node_id is set but not in context
        absent_node: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})
        absent_node.node_id = 'absent-id'
        assert ctx.has_result(absent_node) is False

        # node_id is None
        unset_node: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})
        assert unset_node.node_id is None
        assert ctx.has_result(unset_node) is False

    async def test_ctx_result_for_node_key(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """result_for() accepts NodeKey and returns the correct result."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'nodekey_a')

        @app.task(task_name='nodekey_b')
        def ctx_reader(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(
            fn=ctx_reader,
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
        )

        spec = make_workflow_spec(
            broker=broker, name='nodekey', tasks=[node_a, node_b],
        )
        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=77))

        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 1)
        ctx = self._build_ctx_from_kwargs(kwargs)

        # Use NodeKey with the same node_id that WorkflowSpec assigned to node_a
        assert node_a.node_id is not None
        key: NodeKey[int] = NodeKey(node_id=node_a.node_id)
        result = ctx.result_for(key)
        assert result.is_ok()
        assert result.unwrap() == 77

    async def test_ctx_empty_ctx_from_no_injection(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """workflow_ctx_from=[] behaves like None — no context injected."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'empty_ctx_a')

        @app.task(task_name='empty_ctx_b')
        def ctx_reader(
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=0)

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(
            fn=ctx_reader,
            waits_for=[node_a],
            workflow_ctx_from=[],
        )

        spec = make_workflow_spec(
            broker=broker, name='empty_ctx', tasks=[node_a, node_b],
        )
        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 1)
        assert '__horsies_workflow_ctx__' not in kwargs

    async def test_worker_skips_ctx_when_function_lacks_param(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Worker silently skips ctx injection when function has no workflow_ctx param."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'skip_ctx_a')

        @app.task(task_name='skip_ctx_b')
        def no_ctx_task(value: int = 0) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value + 100)

        # Build a workflow where node_b has workflow_ctx_from but the function
        # does NOT declare workflow_ctx. Normally WorkflowSpec validation rejects
        # this (E010), so we bypass validation by manually injecting
        # __horsies_workflow_ctx__ into the task's kwargs at the DB level.
        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b: TaskNode[int] = TaskNode(
            fn=no_ctx_task,
            waits_for=[node_a],
            # No workflow_ctx_from — the spec is valid
        )

        spec = make_workflow_spec(
            broker=broker, name='skip_ctx', tasks=[node_a, node_b],
        )
        handle = await start_ok(spec, broker)
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=5))

        # Fetch task B's row and manually inject __horsies_workflow_ctx__ into kwargs
        task_row_result = await session.execute(
            text("""
                SELECT t.id, t.task_name, t.args, t.kwargs
                FROM horsies_tasks t
                JOIN horsies_workflow_tasks wt ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf_id AND wt.task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        task_row = task_row_result.fetchone()
        assert task_row is not None

        task_id, task_name, args_json, kwargs_json = task_row
        assert isinstance(task_id, str)
        assert isinstance(task_name, str)
        assert isinstance(args_json, str)
        assert isinstance(kwargs_json, str)

        # Inject ctx data into kwargs to simulate the engine path
        original_kwargs = loads_json(kwargs_json).unwrap()
        assert isinstance(original_kwargs, dict)
        original_kwargs['__horsies_workflow_ctx__'] = {
            'workflow_id': handle.workflow_id,
            'task_index': 1,
            'task_name': 'skip_ctx_b',
            'results_by_id': {},
        }
        modified_kwargs_json = dumps_json(original_kwargs).unwrap()

        await self._claim_task(session, task_id, 'test-worker')

        set_current_app(app)
        database_url = broker.config.database_url.replace('+asyncpg', '').replace(
            '+psycopg', ''
        )
        _initialize_worker_pool(database_url)
        ok, result_json, worker_failure = _run_task_entry(
            task_name=task_name,
            args_json=args_json,
            kwargs_json=modified_kwargs_json,
            task_id=task_id,
            database_url=database_url,
            master_worker_id='test-worker',
        )
        assert ok is True
        assert worker_failure is None

        task_result = task_result_from_json(loads_json(result_json).unwrap()).unwrap()
        assert task_result.is_ok()
        # value=0 (default) + 100 = 100 — ctx was silently skipped
        assert task_result.unwrap() == 100
