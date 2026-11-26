"""Integration tests for workflow_ctx injection."""

from __future__ import annotations

from datetime import datetime
from typing import Any, TypeGuard
from uuid import UUID

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import dumps_json, loads_json
from horsies.core.codec.typed import decode_task_result
from horsies.core.models.outcome import Failed, Succeeded, TaskOutcome
from horsies.core.models.tasks import OperationalErrorCode, TaskError
from horsies.core.types.result import Err, Ok, Result
from horsies.core.errors import ErrorCode
from horsies.core.models.workflow import (
    NodeKey,
    TaskNode,
    WorkflowContext,
    WorkflowMetadata,
    WorkflowValidationError,
)
from horsies.core.worker.current import set_current_app
from horsies.core.worker.child_runner import _run_task_entry
from horsies.core.worker.child_frames import NoTraceCarrier
from horsies.core.worker.task_loop import TaskLoop

from .conftest import make_simple_task, make_no_ctx_task, make_workflow_spec, start_ok
from tests.integration.conftest import finish_task
from horsies.core.worker.child_runner import RunTheTask
from tests.fixtures.run_return import wire_of


def _is_str_keyed_dict(value: object) -> TypeGuard[dict[str, Any]]:
    if not isinstance(value, dict):
        return False
    value_keys: list[object] = list(value.keys())
    for key in value_keys:
        if not isinstance(key, str):
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
    ) -> datetime:
        """Helper to claim a task for a worker (required before _run_task_entry).

        Returns the claim time, as the claim of a worker returns it.
        """
        claimed_at = (
            await session.execute(
                text("""
                    UPDATE horsies_tasks
                    SET status = 'CLAIMED',
                        claimed_at = NOW(),
                        claimed_by_worker_id = :worker_id,
                        claim_expires_at = NOW() + INTERVAL '1 hour',
                        attempt_claims = 1,
                        first_claimed_at = NOW()
                    WHERE id = :task_id
                    RETURNING claimed_at
                """),
                {'task_id': task_id, 'worker_id': worker_id},
            )
        ).scalar_one()
        await session.commit()
        assert isinstance(claimed_at, datetime)
        return claimed_at

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
            text('SELECT input FROM horsies_tasks WHERE id = :tid'),
            {'tid': row[0]},
        )
        task_row = task_result.fetchone()
        if not task_row or not task_row[0]:
            return {}

        data = loads_json(task_row[0]).unwrap()
        return data if isinstance(data, dict) else {}

    def _build_ctx_from_kwargs(self, kwargs: dict[str, Any]) -> WorkflowContext:
        """Reconstruct WorkflowContext from injected kwargs.

        Phase 6+ shape under ``__h_workflow_ctx__``:
            { 'workflow_id': str,
              'task_index': int,
              'task_name': str,
              'results_by_id': { node_id: <__h_task_result__ envelope> | None },
              'task_name_by_id': { node_id: str },
              'summaries_by_id': { node_id: <summary json> } }
        ``None`` in ``results_by_id`` means the engine couldn't resolve a
        source-task ``task_ok_type`` at encode time; surface that as a
        sentinel ``TaskError`` so the context still holds a typed value.
        """
        ctx_data_raw = kwargs.get('__h_workflow_ctx__')
        if not _is_str_keyed_dict(ctx_data_raw):
            raise AssertionError('workflow_ctx data missing or invalid')

        typed_ctx_data = ctx_data_raw

        results_by_id: dict[str, TaskOutcome[Any, TaskError]] = {}
        raw_results = typed_ctx_data.get('results_by_id')
        if isinstance(raw_results, dict):
            for node_id, envelope in raw_results.items():
                if envelope is None:
                    results_by_id[node_id] = Failed(
                        TaskError(
                            error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                            message=(
                                f'workflow_ctx result for node {node_id!r} unavailable '
                                f'(engine could not encode at emit time)'
                            ),
                            data={'node_id': node_id},
                        ),
                    )
                    continue
                results_by_id[node_id] = decode_task_result(envelope, Any)

        return WorkflowContext.from_serialized(
            workflow_id=typed_ctx_data.get('workflow_id', ''),
            node_index=typed_ctx_data.get('task_index', 0),
            task_name=typed_ctx_data.get('task_name', ''),
            results_by_node_id=results_by_id,
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
            *, workflow_ctx: WorkflowContext | None = None,
        ) -> Result[int, TaskError]:
            if workflow_ctx is None:
                return Err(TaskError(error_code='NO_CTX', message='missing'))
            match workflow_ctx.result_for(node_a):
                case Succeeded(value=value):
                    return Ok(value)
                case Failed(error=error):
                    return Err(error)

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
        await finish_task(session, broker, str(handle.workflow_id), 0, Ok(10))

        kwargs = await self._get_task_kwargs(session, str(handle.workflow_id), 1)
        ctx = self._build_ctx_from_kwargs(kwargs)

        assert ctx.result_for(node_a) == Succeeded(10)

        # Validate metadata too
        assert ctx.workflow_id == str(handle.workflow_id)
        assert ctx.node_index == 1
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
            *, workflow_ctx: WorkflowContext | None = None,
        ) -> Result[int, TaskError]:
            if workflow_ctx is None:
                return Ok(0)
            return Ok(1)

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})
        node_b: TaskNode[int] = TaskNode(
            fn=ctx_reader,
            waits_for=[node_a],
        )

        spec = make_workflow_spec(
            broker=broker, name='no_ctx_from', tasks=[node_a, node_b]
        )

        handle = await start_ok(spec, broker)
        await finish_task(session, broker, str(handle.workflow_id), 0, Ok(10))

        kwargs = await self._get_task_kwargs(session, str(handle.workflow_id), 1)
        assert '__h_workflow_ctx__' not in kwargs

    async def test_workflow_meta_injected_without_ctx_from(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """workflow_meta is injected even when workflow_ctx_from is not used."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'meta_only_a')

        @app.task(task_name='meta_only_b')
        def meta_reader(
            *, workflow_meta: WorkflowMetadata | None = None,
        ) -> Result[int, TaskError]:
            if workflow_meta is None:
                return Ok(-1)
            return Ok(workflow_meta.node_index)

        node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})
        node_b: TaskNode[int] = TaskNode(
            fn=meta_reader,
            waits_for=[node_a],
        )

        spec = make_workflow_spec(
            broker=broker, name='meta_only', tasks=[node_a, node_b]
        )
        handle = await start_ok(spec, broker)
        await finish_task(session, broker, str(handle.workflow_id), 0, Ok(10))

        kwargs = await self._get_task_kwargs(session, str(handle.workflow_id), 1)
        meta_raw = kwargs.get('__h_workflow_meta__')
        assert isinstance(meta_raw, dict)
        assert meta_raw['workflow_id'] == str(handle.workflow_id)
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
            *, workflow_ctx: WorkflowContext | None = None,
        ) -> Result[int, TaskError]:
            if workflow_ctx is None:
                return Err(TaskError(error_code='NO_CTX', message='missing'))
            match (workflow_ctx.result_for(node_a), workflow_ctx.result_for(node_b)):
                case (Succeeded(value=first), Succeeded(value=second)):
                    return Ok(first + second)
                case _:
                    return Err(
                        TaskError(error_code='UPSTREAM_FAILED', message='a dependency failed'),
                    )

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
        await finish_task(session, broker, str(handle.workflow_id), 0, Ok(10))
        await finish_task(session, broker, str(handle.workflow_id), 1, Ok(20))

        kwargs = await self._get_task_kwargs(session, str(handle.workflow_id), 2)
        ctx = self._build_ctx_from_kwargs(kwargs)

        assert ctx.result_for(node_a) == Succeeded(10)
        assert ctx.result_for(node_b) == Succeeded(20)

    async def test_ctx_skipped_dep_returns_upstream_skipped(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """SKIPPED dep in workflow_ctx_from yields UPSTREAM_SKIPPED sentinel."""
        session, broker, app = setup

        @app.task(task_name='ctx_skipped_failing')
        def failing_task() -> Result[int, TaskError]:
            return Err(TaskError(error_code='FAIL', message='Failed'))

        task_b = make_simple_task(app, 'ctx_skipped_b')

        @app.task(task_name='ctx_skipped_c')
        def ctx_reader(
            *, workflow_ctx: WorkflowContext | None = None,
        ) -> Result[str, TaskError]:
            if workflow_ctx is None:
                return Err(TaskError(error_code='NO_CTX', message='missing'))
            match workflow_ctx.result_for(node_b):
                case Succeeded():
                    return Ok('unexpected')
                case Failed(error=error):
                    return Err(error)

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
        await finish_task(
            session,
            broker,
            str(handle.workflow_id),
            0,
            Err(TaskError(error_code='A_FAILED', message='A failed')),
        )

        kwargs = await self._get_task_kwargs(session, str(handle.workflow_id), 2)
        ctx = self._build_ctx_from_kwargs(kwargs)

        match ctx.result_for(node_b):
            case Failed(error=error):
                assert error.error_code == 'UPSTREAM_SKIPPED'
                assert error.data is not None
                assert 'dependency_index' in error.data
            case outcome:
                pytest.fail(f'expected a failed outcome, got {outcome!r}')

    async def test_ctx_failed_dep_returns_original_error(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """FAILED dep in workflow_ctx_from yields original TaskError."""
        session, broker, app = setup

        @app.task(task_name='ctx_failed_failing')
        def failing_task() -> Result[int, TaskError]:
            return Err(TaskError(error_code='FAIL', message='Failed'))

        @app.task(task_name='ctx_failed_c')
        def ctx_reader(
            *, workflow_ctx: WorkflowContext | None = None,
        ) -> Result[str, TaskError]:
            if workflow_ctx is None:
                return Err(TaskError(error_code='NO_CTX', message='missing'))
            match workflow_ctx.result_for(node_a):
                case Succeeded():
                    return Ok('unexpected')
                case Failed(error=error):
                    return Err(error)

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
        await finish_task(
            session,
            broker,
            str(handle.workflow_id),
            0,
            Err(TaskError(error_code='A_FAILED', message='A failed')),
        )

        kwargs = await self._get_task_kwargs(session, str(handle.workflow_id), 1)
        ctx = self._build_ctx_from_kwargs(kwargs)

        match ctx.result_for(node_a):
            case Failed(error=error):
                assert error.error_code == 'A_FAILED'
                assert error.message == 'A failed'
            case outcome:
                pytest.fail(f'expected a failed outcome, got {outcome!r}')

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
            *, workflow_ctx: WorkflowContext | None = None,
        ) -> Result[int, TaskError]:
            if workflow_ctx is None:
                return Err(TaskError(error_code='NO_CTX', message='missing'))
            match workflow_ctx.result_for(node_a):
                case Succeeded(value=value):
                    return Ok(value)
                case Failed(error=error):
                    return Err(error)

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
        await finish_task(session, broker, str(handle.workflow_id), 0, Ok(10))

        task_row_result = await session.execute(
            text("""
                SELECT t.id, t.task_name, t.input
                FROM horsies_tasks t
                JOIN horsies_workflow_tasks wt ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf_id AND wt.task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        task_row = task_row_result.fetchone()
        assert task_row is not None

        task_id, task_name, input_json = task_row
        assert isinstance(task_id, str)
        assert isinstance(task_name, str)
        assert isinstance(input_json, str)

        # Claim the task before running (required by ownership check)
        claimed_at = await self._claim_task(session, task_id, 'test-worker')

        set_current_app(app)
        ok, result_json, worker_failure = wire_of(
            _run_task_entry(
                task_name=task_name,
                input_json=input_json,
                task_id=UUID(task_id),
                master_worker_id='test-worker',
                work=RunTheTask(),
                claimed_at=claimed_at,
                task_loop=TaskLoop(),
                orphan_self_heal=True,
                queue_name='default',
                attempt=1,
                trace=NoTraceCarrier(),
            )
        )
        assert ok is True
        assert worker_failure is None

        task_result = decode_task_result(loads_json(result_json).unwrap(), Any)
        assert task_result == Succeeded(10)

    async def test_worker_injects_required_workflow_meta(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Worker injects workflow_meta so required parameter tasks run successfully."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'meta_worker_a')

        @app.task(task_name='meta_worker_b')
        def meta_worker_b(
            *, workflow_meta: WorkflowMetadata,
        ) -> Result[str, TaskError]:
            return Ok(
                f'{workflow_meta.workflow_id}:{workflow_meta.node_index}:{workflow_meta.task_name}'
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
        await finish_task(session, broker, str(handle.workflow_id), 0, Ok(10))

        task_row_result = await session.execute(
            text("""
                SELECT t.id, t.task_name, t.input
                FROM horsies_tasks t
                JOIN horsies_workflow_tasks wt ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf_id AND wt.task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        task_row = task_row_result.fetchone()
        assert task_row is not None

        task_id, task_name, input_json = task_row
        assert isinstance(task_id, str)
        assert isinstance(task_name, str)
        assert isinstance(input_json, str)

        # Claim the task before running (required by ownership check)
        claimed_at = await self._claim_task(session, task_id, 'test-worker')

        set_current_app(app)
        ok, result_json, worker_failure = wire_of(
            _run_task_entry(
                task_name=task_name,
                input_json=input_json,
                task_id=UUID(task_id),
                master_worker_id='test-worker',
                work=RunTheTask(),
                claimed_at=claimed_at,
                task_loop=TaskLoop(),
                orphan_self_heal=True,
                queue_name='default',
                attempt=1,
                trace=NoTraceCarrier(),
            )
        )
        assert ok is True
        assert worker_failure is None

        task_result = decode_task_result(loads_json(result_json).unwrap(), Any)
        assert task_result == Succeeded(f'{handle.workflow_id}:1:meta_worker_b')

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
            *, workflow_ctx: WorkflowContext | None = None,
        ) -> Result[int, TaskError]:
            if workflow_ctx is None:
                return Err(TaskError(error_code='NO_CTX', message='missing'))
            # missing_node.node_id is None -> should raise RuntimeError
            match workflow_ctx.result_for(missing_node):
                case Succeeded(value=value):
                    return Ok(value)
                case Failed(error=error):
                    return Err(error)

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

        await finish_task(session, broker, str(handle.workflow_id), 0, Ok(10))

        task_row_result = await session.execute(
            text("""
                SELECT t.id, t.task_name, t.input
                FROM horsies_tasks t
                JOIN horsies_workflow_tasks wt ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf_id AND wt.task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        task_row = task_row_result.fetchone()
        assert task_row is not None

        task_id, task_name, input_json = task_row
        assert isinstance(task_id, str)
        assert isinstance(task_name, str)
        assert isinstance(input_json, str)

        # Claim the task before running (required by ownership check)
        claimed_at = await self._claim_task(session, task_id, 'test-worker')

        set_current_app(app)
        ok, result_json, worker_failure = wire_of(
            _run_task_entry(
                task_name=task_name,
                input_json=input_json,
                task_id=UUID(task_id),
                master_worker_id='test-worker',
                work=RunTheTask(),
                claimed_at=claimed_at,
                task_loop=TaskLoop(),
                orphan_self_heal=True,
                queue_name='default',
                attempt=1,
                trace=NoTraceCarrier(),
            )
        )
        assert ok is True
        assert worker_failure is None

        match decode_task_result(loads_json(result_json).unwrap(), Any):
            case Failed(error=error):
                assert error.error_code == 'WORKFLOW_CTX_MISSING_ID'
            case outcome:
                pytest.fail(f'expected a failed outcome, got {outcome!r}')

    async def test_ctx_from_not_in_waits_for_raises_e009(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """workflow_ctx_from referencing a node not in waits_for raises HRS-009."""
        _session, broker, app = setup
        task_a = make_simple_task(app, 'e009_a')

        @app.task(task_name='e009_b')
        def ctx_reader(
            *, workflow_ctx: WorkflowContext | None = None,
        ) -> Result[int, TaskError]:
            return Ok(0)

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
            *, workflow_ctx: WorkflowContext | None = None,
        ) -> Result[int, TaskError]:
            return Ok(0)

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
        await finish_task(session, broker, str(handle.workflow_id), 0, Ok(10))

        kwargs = await self._get_task_kwargs(session, str(handle.workflow_id), 1)
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
            *, workflow_ctx: WorkflowContext | None = None,
        ) -> Result[int, TaskError]:
            return Ok(0)

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
        await finish_task(session, broker, str(handle.workflow_id), 0, Ok(42))

        kwargs = await self._get_task_kwargs(session, str(handle.workflow_id), 1)
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
            *, workflow_ctx: WorkflowContext | None = None,
        ) -> Result[int, TaskError]:
            return Ok(0)

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
        await finish_task(session, broker, str(handle.workflow_id), 0, Ok(1))

        kwargs = await self._get_task_kwargs(session, str(handle.workflow_id), 1)
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
            *, workflow_ctx: WorkflowContext | None = None,
        ) -> Result[int, TaskError]:
            return Ok(0)

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
        await finish_task(session, broker, str(handle.workflow_id), 0, Ok(77))

        kwargs = await self._get_task_kwargs(session, str(handle.workflow_id), 1)
        ctx = self._build_ctx_from_kwargs(kwargs)

        # Use NodeKey with the same node_id that WorkflowSpec assigned to node_a
        assert node_a.node_id is not None
        key: NodeKey[int] = NodeKey(node_id=node_a.node_id)
        assert ctx.result_for(key) == Succeeded(77)

    async def test_ctx_empty_ctx_from_no_injection(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """workflow_ctx_from=[] behaves like None — no context injected."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'empty_ctx_a')

        @app.task(task_name='empty_ctx_b')
        def ctx_reader(
            *, workflow_ctx: WorkflowContext | None = None,
        ) -> Result[int, TaskError]:
            return Ok(0)

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
        await finish_task(session, broker, str(handle.workflow_id), 0, Ok(10))

        kwargs = await self._get_task_kwargs(session, str(handle.workflow_id), 1)
        assert '__h_workflow_ctx__' not in kwargs

    async def test_worker_skips_ctx_when_function_lacks_param(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Worker silently skips ctx injection when function has no workflow_ctx param."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'skip_ctx_a')

        @app.task(task_name='skip_ctx_b')
        def no_ctx_task(*, value: int = 0) -> Result[int, TaskError]:
            return Ok(value + 100)

        # Build a workflow where node_b has workflow_ctx_from but the function
        # does NOT declare workflow_ctx. Normally WorkflowSpec validation rejects
        # this (HRS-010), so we bypass validation by manually injecting
        # __h_workflow_ctx__ into the task's kwargs at the DB level.
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
        await finish_task(session, broker, str(handle.workflow_id), 0, Ok(5))

        # Fetch task B's row and manually inject __h_workflow_ctx__ into kwargs
        task_row_result = await session.execute(
            text("""
                SELECT t.id, t.task_name, t.input
                FROM horsies_tasks t
                JOIN horsies_workflow_tasks wt ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf_id AND wt.task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        task_row = task_row_result.fetchone()
        assert task_row is not None

        task_id, task_name, input_json = task_row
        assert isinstance(task_id, str)
        assert isinstance(task_name, str)
        assert isinstance(input_json, str)

        # Inject ctx data into kwargs to simulate the engine path
        original_kwargs = loads_json(input_json).unwrap()
        assert isinstance(original_kwargs, dict)
        original_kwargs['__h_workflow_ctx__'] = {
            'workflow_id': str(handle.workflow_id),
            'task_index': 1,
            'task_name': 'skip_ctx_b',
            'results_by_id': {},
        }
        modified_kwargs_json = dumps_json(original_kwargs).unwrap()

        claimed_at = await self._claim_task(session, task_id, 'test-worker')

        set_current_app(app)
        ok, result_json, worker_failure = wire_of(
            _run_task_entry(
                task_name=task_name,
                input_json=modified_kwargs_json,
                task_id=UUID(task_id),
                master_worker_id='test-worker',
                work=RunTheTask(),
                claimed_at=claimed_at,
                task_loop=TaskLoop(),
                orphan_self_heal=True,
                queue_name='default',
                attempt=1,
                trace=NoTraceCarrier(),
            )
        )
        assert ok is True
        assert worker_failure is None

        task_result = decode_task_result(loads_json(result_json).unwrap(), Any)
        # value=0 (default) + 100 = 100 — ctx was silently skipped
        assert task_result == Succeeded(100)
