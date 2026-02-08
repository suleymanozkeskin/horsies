"""Integration tests for args_from data injection."""

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.workflow import TaskNode, WorkflowSpec
from horsies.core.workflows.engine import start_workflow_async, on_workflow_task_complete
from horsies.core.codec.serde import loads_json, task_result_from_json
from horsies.core.worker.current import set_current_app
from horsies.core.worker.worker import _run_task_entry, _initialize_worker_pool

from .conftest import (
    make_simple_task,
    make_args_receiver_task,
    make_recovery_task,
    make_multi_args_receiver_task,
    make_failing_task,
    make_workflow_spec,
)


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestArgsFromInjection:
    """Tests for args_from data injection."""

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
        """Get kwargs stored in the tasks table for the workflow task."""
        # First get the task_id
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

        # Then get kwargs from tasks table
        task_result = await session.execute(
            text('SELECT kwargs FROM horsies_tasks WHERE id = :tid'),
            {'tid': row[0]},
        )
        task_row = task_result.fetchone()
        if not task_row or not task_row[0]:
            return {}

        return loads_json(task_row[0])

    async def test_single_args_from_injects_ok_taskresult(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Single kwarg injected from dep: marker, structure, and value."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'single_args_a')
        task_b = make_args_receiver_task(app, 'single_args_b')

        node_a = TaskNode(fn=task_a, args=(5,))
        node_b = TaskNode(
            fn=task_b,
            waits_for=[node_a],
            args_from={'input_result': node_a},
        )

        spec = make_workflow_spec(
            broker=broker, name='single_args', tasks=[node_a, node_b]
        )

        handle = await start_workflow_async(spec, broker)

        # Complete A with result
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=42))

        # Check wrapper: key present with TaskResult marker
        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 1)
        assert 'input_result' in kwargs
        assert kwargs['input_result']['__horsies_taskresult__'] is True

        # Check inner data: full TaskResult with ok value
        injected = loads_json(kwargs['input_result']['data'])
        assert isinstance(injected, dict)
        assert injected.get('ok') == 42
        assert 'err' not in injected or injected['err'] is None

    async def test_multiple_args_from(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Multiple kwargs from multiple deps."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'multi_args_a')
        task_b = make_simple_task(app, 'multi_args_b')
        task_c = make_multi_args_receiver_task(app, 'multi_args_c')

        node_a = TaskNode(fn=task_a, args=(5,))
        node_b = TaskNode(fn=task_b, args=(10,))
        node_c = TaskNode(
            fn=task_c,
            waits_for=[node_a, node_b],
            args_from={'first': node_a, 'second': node_b},
        )

        spec = make_workflow_spec(
            broker=broker, name='multi_args', tasks=[node_a, node_b, node_c]
        )

        handle = await start_workflow_async(spec, broker)

        # Complete both roots
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=100))
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=200))

        # Check C's kwargs
        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 2)
        assert 'first' in kwargs
        assert 'second' in kwargs
        assert kwargs['first']['__horsies_taskresult__'] is True
        assert kwargs['second']['__horsies_taskresult__'] is True

    async def test_args_from_err_result(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Failed TaskResult injected (with allow_failed_deps=True)."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'err_result_a')
        task_b = make_recovery_task(app, 'err_result_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=task_b,
            waits_for=[node_a],
            args_from={'input_result': node_a},
            allow_failed_deps=True,  # Required to receive failed result
        )

        spec = make_workflow_spec(
            broker=broker, name='err_result', tasks=[node_a, node_b]
        )

        handle = await start_workflow_async(spec, broker)

        # Complete A with failure
        await self._complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='TEST_FAIL', message='Test failure')),
        )

        # B should be enqueued (allow_failed_deps=True)
        status_result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        assert status_result.fetchone()[0] == 'ENQUEUED'

        # Get B's kwargs - should contain failed TaskResult
        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 1)
        injected = loads_json(kwargs['input_result']['data'])
        assert 'err' in injected

    async def test_args_from_with_static_kwargs(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """args_from merged with static kwargs."""
        session, broker, app = setup

        # Create a task that accepts both static and injected args
        @app.task(task_name='static_and_injected')
        def static_and_injected(
            static_value: int,
            input_result: TaskResult[int, TaskError] | None = None,
        ) -> TaskResult[int, TaskError]:
            base = input_result.unwrap() if input_result and input_result.is_ok() else 0
            return TaskResult(ok=static_value + base)

        task_a = make_simple_task(app, 'static_a')

        node_a = TaskNode(fn=task_a, args=(5,))
        node_b = TaskNode(
            fn=static_and_injected,
            kwargs={'static_value': 100},  # Static kwarg
            waits_for=[node_a],
            args_from={'input_result': node_a},  # Injected kwarg
        )

        spec = make_workflow_spec(
            broker=broker, name='static_merged', tasks=[node_a, node_b]
        )

        handle = await start_workflow_async(spec, broker)

        # Complete A
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=50))

        # Get B's kwargs - should have both static and injected
        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 1)
        assert kwargs.get('static_value') == 100
        assert 'input_result' in kwargs

    async def test_args_from_same_dep_multiple_kwargs(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Same dep mapped to multiple kwargs (unusual but valid)."""
        session, broker, app = setup

        @app.task(task_name='dual_receiver')
        def dual_receiver(
            first_copy: TaskResult[int, TaskError],
            second_copy: TaskResult[int, TaskError],
        ) -> TaskResult[int, TaskError]:
            if first_copy.is_ok() and second_copy.is_ok():
                return TaskResult(ok=first_copy.unwrap() + second_copy.unwrap())
            return TaskResult(err=TaskError(error_code='FAIL', message='Failed'))

        task_a = make_simple_task(app, 'same_dep_a')

        node_a = TaskNode(fn=task_a, args=(25,))
        node_b = TaskNode(
            fn=dual_receiver,
            waits_for=[node_a],
            # Same node mapped to two different kwargs
            args_from={'first_copy': node_a, 'second_copy': node_a},
        )

        spec = make_workflow_spec(
            broker=broker, name='same_dep', tasks=[node_a, node_b]
        )

        handle = await start_workflow_async(spec, broker)

        # Complete A
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=50))

        # Get B's kwargs - should have both pointing to same result
        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 1)
        assert 'first_copy' in kwargs
        assert 'second_copy' in kwargs

        # Both should have same data
        first_data = loads_json(kwargs['first_copy']['data'])
        second_data = loads_json(kwargs['second_copy']['data'])
        assert first_data == second_data

    async def test_skipped_dep_injects_upstream_skipped_sentinel(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """SKIPPED dep injects sentinel TaskResult with UPSTREAM_SKIPPED error."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'skipped_sentinel_a')
        task_b = make_simple_task(app, 'skipped_sentinel_b')
        task_c = make_recovery_task(app, 'skipped_sentinel_c')

        # A -> B -> C (with allow_failed_deps=True and args_from)
        # When A fails, B is SKIPPED
        # C should receive UPSTREAM_SKIPPED for B (which is SKIPPED)
        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, args=(1,), waits_for=[node_a])
        node_c = TaskNode(
            fn=task_c,
            waits_for=[node_b],
            args_from={'input_result': node_b},
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            broker=broker, name='skipped_sentinel', tasks=[node_a, node_b, node_c]
        )

        handle = await start_workflow_async(spec, broker)

        # Complete A as FAILED
        await self._complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='A_FAILED', message='A failed')),
        )

        # B should be SKIPPED now (not allow_failed_deps)
        # This triggers C to become READY (allow_failed_deps=True)

        # Check C's kwargs contain UPSTREAM_SKIPPED sentinel for B
        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 2)
        assert 'input_result' in kwargs
        assert kwargs['input_result']['__horsies_taskresult__'] is True

        # Parse the injected TaskResult
        injected_data = loads_json(kwargs['input_result']['data'])
        assert 'err' in injected_data
        assert injected_data['err']['error_code'] == 'UPSTREAM_SKIPPED'
        assert injected_data['err']['data']['dependency_index'] == 1  # B's index

    async def test_failed_dep_passes_original_error_not_skipped_sentinel(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """FAILED dep passes its actual error, not UPSTREAM_SKIPPED."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'failed_error_a')
        task_b = make_recovery_task(app, 'failed_error_b')

        # A -> B (with allow_failed_deps=True and args_from)
        # A fails, B should receive A's actual error
        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=task_b,
            waits_for=[node_a],
            args_from={'input_result': node_a},
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            broker=broker, name='failed_error', tasks=[node_a, node_b]
        )

        handle = await start_workflow_async(spec, broker)

        # Complete A as FAILED with specific error
        await self._complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(
                err=TaskError(error_code='ORIGINAL_ERROR', message='Original failure')
            ),
        )

        # Check B's kwargs contain A's original error (not UPSTREAM_SKIPPED)
        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 1)
        assert 'input_result' in kwargs
        assert kwargs['input_result']['__horsies_taskresult__'] is True

        # Parse the injected TaskResult
        injected_data = loads_json(kwargs['input_result']['data'])
        assert 'err' in injected_data
        assert (
            injected_data['err']['error_code'] == 'ORIGINAL_ERROR'
        )  # Original error preserved
        assert injected_data['err']['message'] == 'Original failure'

    async def test_worker_deserializes_args_from_taskresult(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Worker reconstructs TaskResult objects from args_from injection."""
        session, broker, app = setup

        @app.task(task_name='worker_args_from_b')
        def worker_args_from_b(
            input_result: TaskResult[int, TaskError],
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=input_result.unwrap() + 5)

        task_a = make_simple_task(app, 'worker_args_from_a')
        node_a = TaskNode(fn=task_a, args=(5,))
        node_b = TaskNode(
            fn=worker_args_from_b,
            waits_for=[node_a],
            args_from={'input_result': node_a},
        )

        spec = make_workflow_spec(
            broker=broker, name='worker_args_from', tasks=[node_a, node_b]
        )
        handle = await start_workflow_async(spec, broker)

        # Complete A to enqueue B
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # Fetch task row for B
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

        task_result = task_result_from_json(loads_json(result_json))
        assert task_result.is_ok()
        assert task_result.unwrap() == 15

    async def test_failed_dep_skips_downstream_no_injection(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Default allow_failed_deps=False: failed dep skips downstream, no injection."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'skip_no_inject_a')
        task_b = make_args_receiver_task(app, 'skip_no_inject_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=task_b,
            waits_for=[node_a],
            args_from={'input_result': node_a},
            # allow_failed_deps defaults to False
        )

        spec = make_workflow_spec(
            broker=broker, name='skip_no_inject', tasks=[node_a, node_b]
        )

        handle = await start_workflow_async(spec, broker)

        # Complete A as FAILED
        await self._complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='DEP_FAIL', message='Dep failed')),
        )

        # B should be SKIPPED (not ENQUEUED)
        status_result = await session.execute(
            text("""
                SELECT status, task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        row = status_result.fetchone()
        assert row is not None
        assert row[0] == 'SKIPPED'
        # No task was created (never enqueued)
        assert row[1] is None

    async def test_empty_args_from_no_interference(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Empty args_from={} does not interfere with static kwargs."""
        session, broker, app = setup

        @app.task(task_name='empty_args_from_b')
        def static_only(
            static_value: int,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=static_value + 1)

        task_a = make_simple_task(app, 'empty_args_from_a')

        node_a = TaskNode(fn=task_a, args=(5,))
        node_b = TaskNode(
            fn=static_only,
            kwargs={'static_value': 100},
            waits_for=[node_a],
            args_from={},  # Explicitly empty
        )

        spec = make_workflow_spec(
            broker=broker, name='empty_args_from', tasks=[node_a, node_b]
        )

        handle = await start_workflow_async(spec, broker)

        # Complete A
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=10))

        # B should be enqueued with only static kwargs
        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 1)
        assert kwargs.get('static_value') == 100
        # No TaskResult markers injected
        for value in kwargs.values():
            if isinstance(value, dict):
                assert '__horsies_taskresult__' not in value

    async def test_worker_deserializes_err_taskresult(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Worker deserializes err TaskResult from args_from injection."""
        session, broker, app = setup

        @app.task(task_name='worker_err_args_b')
        def worker_err_args_b(
            input_result: TaskResult[int, TaskError],
        ) -> TaskResult[int, TaskError]:
            if input_result.is_err():
                return TaskResult(ok=999)  # Recover with default
            return TaskResult(ok=input_result.unwrap())

        task_a = make_failing_task(app, 'worker_err_args_a')
        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=worker_err_args_b,
            waits_for=[node_a],
            args_from={'input_result': node_a},
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            broker=broker, name='worker_err_args', tasks=[node_a, node_b]
        )
        handle = await start_workflow_async(spec, broker)

        # Complete A as FAILED to enqueue B
        await self._complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='WORKER_ERR', message='Worker err test')),
        )

        # Fetch task row for B
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

        task_result = task_result_from_json(loads_json(result_json))
        assert task_result.is_ok()
        assert task_result.unwrap() == 999  # Recovery value

    async def test_worker_deserializes_upstream_skipped_sentinel(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Worker deserializes UPSTREAM_SKIPPED sentinel from skipped dep."""
        session, broker, app = setup

        @app.task(task_name='worker_skipped_c')
        def worker_skipped_c(
            input_result: TaskResult[int, TaskError],
        ) -> TaskResult[str, TaskError]:
            if input_result.is_err():
                return TaskResult(ok=input_result.unwrap_err().error_code)
            return TaskResult(ok='not_skipped')

        task_a = make_failing_task(app, 'worker_skipped_a')
        task_b = make_simple_task(app, 'worker_skipped_b')

        # A -> B -> C: A fails, B skipped, C receives UPSTREAM_SKIPPED for B
        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, args=(1,), waits_for=[node_a])
        node_c = TaskNode(
            fn=worker_skipped_c,
            waits_for=[node_b],
            args_from={'input_result': node_b},
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            broker=broker,
            name='worker_skipped',
            tasks=[node_a, node_b, node_c],
        )
        handle = await start_workflow_async(spec, broker)

        # Complete A as FAILED → B becomes SKIPPED → C enqueued
        await self._complete_task(
            session,
            handle.workflow_id,
            0,
            TaskResult(err=TaskError(error_code='A_FAIL', message='A failed')),
        )

        # Fetch task row for C (index 2)
        task_row_result = await session.execute(
            text("""
                SELECT t.id, t.task_name, t.args, t.kwargs
                FROM horsies_tasks t
                JOIN horsies_workflow_tasks wt ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf_id AND wt.task_index = 2
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

        task_result = task_result_from_json(loads_json(result_json))
        assert task_result.is_ok()
        assert task_result.unwrap() == 'UPSTREAM_SKIPPED'

    async def test_partial_args_from_only_mapped_deps_injected(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Only explicitly mapped deps appear in kwargs; unmapped deps don't leak."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'partial_map_a')
        task_b = make_simple_task(app, 'partial_map_b')

        @app.task(task_name='partial_map_c')
        def partial_receiver(
            from_a: TaskResult[int, TaskError],
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=from_a.unwrap() + 1)

        node_a = TaskNode(fn=task_a, args=(5,))
        node_b = TaskNode(fn=task_b, args=(10,))
        node_c = TaskNode(
            fn=partial_receiver,
            waits_for=[node_a, node_b],
            args_from={'from_a': node_a},  # Only A mapped, not B
        )

        spec = make_workflow_spec(
            broker=broker, name='partial_map', tasks=[node_a, node_b, node_c]
        )

        handle = await start_workflow_async(spec, broker)

        # Complete both roots
        await self._complete_task(session, handle.workflow_id, 0, TaskResult(ok=100))
        await self._complete_task(session, handle.workflow_id, 1, TaskResult(ok=200))

        # Check C's kwargs: only 'from_a' injected, nothing from B
        kwargs = await self._get_task_kwargs(session, handle.workflow_id, 2)
        assert 'from_a' in kwargs
        assert kwargs['from_a']['__horsies_taskresult__'] is True

        # Exactly one TaskResult marker in kwargs — B's result not leaked
        taskresult_keys = [
            k for k, v in kwargs.items()
            if isinstance(v, dict) and v.get('__horsies_taskresult__')
        ]
        assert taskresult_keys == ['from_a']
