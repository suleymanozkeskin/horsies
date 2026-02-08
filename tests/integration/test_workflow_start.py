"""Integration tests for workflow start/creation."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.workflow import (
    TaskNode,
    WorkflowSpec,
    OnError,
)
from horsies.core.workflows.engine import start_workflow_async, on_workflow_task_complete

from .conftest import (
    make_simple_task,
    make_workflow_spec,
    make_identity_task,
    make_retryable_task,
    make_ctx_receiver_task,
)


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowStart:
    """Tests for start_workflow_async() function."""

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

    async def test_start_creates_workflow_record(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Workflow row created with RUNNING status."""
        session, broker, app = setup
        simple_task = make_simple_task(app, 'simple_a')

        node_a = TaskNode(fn=simple_task, args=(5,))
        spec = make_workflow_spec(broker=broker, name='test_wf', tasks=[node_a])

        handle = await start_workflow_async(spec, broker)

        # Check workflow record
        result = await session.execute(
            text('SELECT id, name, status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[1] == 'test_wf'
        assert row[2] == 'RUNNING'

    async def test_start_creates_workflow_tasks(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """All TaskNodes become workflow_tasks rows."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'task_a')
        task_b = make_simple_task(app, 'task_b')

        node_a = TaskNode(fn=task_a, args=(1,))
        node_b = TaskNode(fn=task_b, args=(2,), waits_for=[node_a])
        spec = make_workflow_spec(
            broker=broker, name='multi_task', tasks=[node_a, node_b]
        )

        handle = await start_workflow_async(spec, broker)

        # Check workflow_tasks count
        result = await session.execute(
            text('SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        count = result.scalar()
        assert count == 2

    async def test_root_tasks_start_ready(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Tasks with no deps start as READY or ENQUEUED with consistent task_id."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'root_task')

        node_a = TaskNode(fn=task_a, args=(5,))
        spec = make_workflow_spec(broker=broker, name='root_test', tasks=[node_a])

        handle = await start_workflow_async(spec, broker)

        result = await session.execute(
            text("""
                SELECT status, task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        status, task_id = row
        # ENQUEUED means task_id was assigned; READY means not yet enqueued
        if status == 'ENQUEUED':
            assert task_id is not None
        else:
            assert status == 'READY'
            assert task_id is None

    async def test_nonroot_tasks_start_pending(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Tasks with deps start as PENDING."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'root')
        task_b = make_simple_task(app, 'child')

        node_a = TaskNode(fn=task_a, args=(1,))
        node_b = TaskNode(fn=task_b, args=(2,), waits_for=[node_a])
        spec = make_workflow_spec(
            broker=broker, name='pending_test', tasks=[node_a, node_b]
        )

        handle = await start_workflow_async(spec, broker)

        # Child task should be PENDING
        result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] == 'PENDING'

    async def test_custom_workflow_id(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Can specify custom workflow_id."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'custom_id_task')

        node_a = TaskNode(fn=task_a, args=(5,))
        spec = make_workflow_spec(broker=broker, name='custom_id', tasks=[node_a])

        custom_id = 'my-custom-workflow-id-123'
        handle = await start_workflow_async(spec, broker, workflow_id=custom_id)

        assert handle.workflow_id == custom_id

        # Verify in database
        result = await session.execute(
            text('SELECT id FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': custom_id},
        )
        row = result.fetchone()
        assert row is not None

    async def test_on_error_stored(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """on_error policy stored in workflow record."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'error_policy_task')

        node_a = TaskNode(fn=task_a, args=(5,))

        # Test FAIL
        spec_fail = make_workflow_spec(
            broker=broker, name='fail_policy', tasks=[node_a], on_error=OnError.FAIL
        )
        handle_fail = await start_workflow_async(spec_fail, broker)

        result = await session.execute(
            text('SELECT on_error FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle_fail.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] == 'fail'

        # Test PAUSE (need fresh node)
        task_b = make_simple_task(app, 'pause_policy_task')
        node_b = TaskNode(fn=task_b, args=(5,))
        spec_pause = make_workflow_spec(
            broker=broker, name='pause_policy', tasks=[node_b], on_error=OnError.PAUSE
        )
        handle_pause = await start_workflow_async(spec_pause, broker)

        result = await session.execute(
            text('SELECT on_error FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle_pause.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] == 'pause'

    async def test_output_task_index_stored(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """output_task_index set when output specified."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'output_a')
        task_b = make_simple_task(app, 'output_b')

        node_a = TaskNode(fn=task_a, args=(1,))
        node_b = TaskNode(fn=task_b, args=(2,), waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker, name='with_output', tasks=[node_a, node_b], output=node_b
        )

        handle = await start_workflow_async(spec, broker)

        result = await session.execute(
            text('SELECT output_task_index FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] == 1  # node_b is at index 1

    async def test_dependencies_stored_as_array(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """dependencies column contains correct indices."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'dep_a')
        task_b = make_simple_task(app, 'dep_b')
        task_c = make_simple_task(app, 'dep_c')

        node_a = TaskNode(fn=task_a, args=(1,))
        node_b = TaskNode(fn=task_b, args=(2,))
        node_c = TaskNode(fn=task_c, args=(3,), waits_for=[node_a, node_b])

        spec = make_workflow_spec(
            broker=broker, name='deps_test', tasks=[node_a, node_b, node_c]
        )

        handle = await start_workflow_async(spec, broker)

        result = await session.execute(
            text("""
                SELECT dependencies FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 2
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        deps = row[0]
        assert sorted(deps) == [0, 1]

    async def test_args_from_stored_as_jsonb(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """args_from mapping stored correctly."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'args_from_a')
        task_b = make_identity_task(app, 'args_from_b')

        node_a = TaskNode(fn=task_a, args=(1,))
        node_b = TaskNode(
            fn=task_b,
            waits_for=[node_a],
            args_from={'value': node_a},
        )

        spec = make_workflow_spec(
            broker=broker, name='args_from_test', tasks=[node_a, node_b]
        )

        handle = await start_workflow_async(spec, broker)

        result = await session.execute(
            text("""
                SELECT args_from FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        args_from = row[0]
        assert args_from == {'value': 0}  # Maps to task_index 0

    async def test_workflow_ctx_from_stored(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """workflow_ctx_from node_ids stored correctly."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'ctx_a')
        task_b = make_ctx_receiver_task(app, 'ctx_b')

        node_a = TaskNode(fn=task_a, args=(1,))
        node_b = TaskNode(
            fn=task_b,
            waits_for=[node_a],
            workflow_ctx_from=[node_a],
        )

        spec = make_workflow_spec(
            broker=broker, name='ctx_from_test', tasks=[node_a, node_b]
        )

        handle = await start_workflow_async(spec, broker)

        result = await session.execute(
            text("""
                SELECT workflow_ctx_from FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        ctx_from = row[0]
        assert ctx_from == [f'{spec.name}:0']

    async def test_allow_failed_deps_stored(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """allow_failed_deps flag stored per task."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'afd_a')
        task_b = make_identity_task(app, 'afd_b')

        node_a = TaskNode(fn=task_a, args=(1,))
        node_b = TaskNode(
            fn=task_b,
            waits_for=[node_a],
            allow_failed_deps=True,
        )

        spec = make_workflow_spec(
            broker=broker, name='allow_failed_test', tasks=[node_a, node_b]
        )

        handle = await start_workflow_async(spec, broker)

        # Check node_a (default False)
        result_a = await session.execute(
            text("""
                SELECT allow_failed_deps FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        row_a = result_a.fetchone()
        assert row_a is not None
        assert row_a[0] is False

        # Check node_b (explicit True)
        result_b = await session.execute(
            text("""
                SELECT allow_failed_deps FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        row_b = result_b.fetchone()
        assert row_b is not None
        assert row_b[0] is True

    async def test_retry_policy_honored_in_workflow_tasks(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Task retry policy is stored and applied when enqueuing workflow tasks."""
        session, broker, app = setup
        retryable = make_retryable_task(app, 'retry_wf_task', max_retries=5)

        node_a = TaskNode(fn=retryable, args=(10,))
        spec = make_workflow_spec(
            broker=broker, name='retry_policy_test', tasks=[node_a]
        )

        handle = await start_workflow_async(spec, broker)

        # 1. Check that task_options is stored in workflow_tasks
        wt_result = await session.execute(
            text("""
                SELECT task_options FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        wt_row = wt_result.fetchone()
        assert wt_row is not None
        assert wt_row[0] is not None  # task_options should be non-NULL
        assert 'retry_policy' in wt_row[0]

        # 2. Check that the actual task in tasks table has max_retries set correctly
        tasks_result = await session.execute(
            text("""
                SELECT t.max_retries, t.task_options
                FROM horsies_tasks t
                JOIN horsies_workflow_tasks wt ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf_id AND wt.task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_row = tasks_result.fetchone()
        assert task_row is not None
        assert task_row[0] == 5  # max_retries should be 5
        assert task_row[1] is not None  # task_options should be non-NULL

    async def test_retry_policy_honored_for_non_root_task(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Retry policy is applied when non-root workflow tasks are enqueued."""
        session, broker, app = setup
        root = make_simple_task(app, 'retry_root')
        retryable = make_retryable_task(app, 'retry_child', max_retries=4)

        node_a = TaskNode(fn=root, args=(10,))
        node_b = TaskNode(fn=retryable, args=(20,), waits_for=[node_a])
        spec = make_workflow_spec(
            broker=broker, name='retry_non_root', tasks=[node_a, node_b]
        )

        handle = await start_workflow_async(spec, broker)

        # Complete A to enqueue B
        res = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        row = res.fetchone()
        assert row is not None
        await on_workflow_task_complete(session, row[0], TaskResult(ok=10))
        await session.commit()

        # Verify B task row has retry settings
        tasks_result = await session.execute(
            text("""
                SELECT t.max_retries, t.task_options
                FROM horsies_tasks t
                JOIN horsies_workflow_tasks wt ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf_id AND wt.task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        task_row = tasks_result.fetchone()
        assert task_row is not None
        assert task_row[0] == 4
        assert task_row[1] is not None

    async def test_duplicate_workflow_id_returns_existing_handle(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Calling start_workflow_async twice with same custom ID returns same handle."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'idempotent_a')

        node_a = TaskNode(fn=task_a, args=(1,))
        spec = make_workflow_spec(
            broker=broker, name='idempotent_wf', tasks=[node_a],
        )

        custom_id = 'idempotent-test-id'
        handle_1 = await start_workflow_async(spec, broker, workflow_id=custom_id)

        # Second call with same ID — must reuse existing workflow
        task_b = make_simple_task(app, 'idempotent_b')
        node_b = TaskNode(fn=task_b, args=(2,))
        spec_2 = make_workflow_spec(
            broker=broker, name='idempotent_wf_2', tasks=[node_b],
        )
        handle_2 = await start_workflow_async(spec_2, broker, workflow_id=custom_id)

        assert handle_1.workflow_id == handle_2.workflow_id

        # Only one workflow row exists
        result = await session.execute(
            text('SELECT COUNT(*) FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': custom_id},
        )
        assert result.scalar() == 1

        # Task count matches the first spec, not the second
        task_count = await session.execute(
            text('SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = :wf_id'),
            {'wf_id': custom_id},
        )
        assert task_count.scalar() == 1

    async def test_output_task_index_null_when_no_output(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """output_task_index is NULL when output= is not specified."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'no_output_task')

        node_a = TaskNode(fn=task_a, args=(1,))
        spec = make_workflow_spec(
            broker=broker, name='no_output_wf', tasks=[node_a],
        )

        handle = await start_workflow_async(spec, broker)

        result = await session.execute(
            text('SELECT output_task_index FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] is None

    async def test_good_until_stored_in_task_options(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """good_until datetime serialized into task_options JSONB as ISO string."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'good_until_task')

        deadline = datetime(2099, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        node_a = TaskNode(fn=task_a, args=(1,), good_until=deadline)
        spec = make_workflow_spec(
            broker=broker, name='good_until_wf', tasks=[node_a],
        )

        handle = await start_workflow_async(spec, broker)

        result = await session.execute(
            text("""
                SELECT task_options FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] is not None
        options = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        assert 'good_until' in options
        assert options['good_until'] == deadline.isoformat()

    async def test_multiple_root_tasks_all_enqueued(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """All root tasks (no deps) get READY/ENQUEUED, not just the first."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'multi_root_a')
        task_b = make_simple_task(app, 'multi_root_b')
        task_c = make_simple_task(app, 'multi_root_c')

        node_a = TaskNode(fn=task_a, args=(1,))
        node_b = TaskNode(fn=task_b, args=(2,))
        node_c = TaskNode(fn=task_c, args=(3,))
        spec = make_workflow_spec(
            broker=broker, name='multi_root_wf', tasks=[node_a, node_b, node_c],
        )

        handle = await start_workflow_async(spec, broker)

        result = await session.execute(
            text("""
                SELECT task_index, status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id
                ORDER BY task_index
            """),
            {'wf_id': handle.workflow_id},
        )
        rows = result.fetchall()
        assert len(rows) == 3
        for idx, (task_index, status) in enumerate(rows):
            assert task_index == idx
            assert status in {'READY', 'ENQUEUED'}


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestNodeBuilder:
    """Tests for TaskFunction.node() typed builder method."""

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

    async def test_node_builder_creates_valid_tasknode(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """The .node() method creates a valid TaskNode that can be used in workflows."""
        session, broker, app = setup

        @app.task(task_name='node_builder_task')
        def compute(value: int, multiplier: float = 1.0) -> TaskResult[int, TaskError]:
            return TaskResult(ok=int(value * multiplier))

        # Use .node() to create TaskNode
        node_a = compute.node(node_id='compute_a')(value=10, multiplier=2.5)

        spec = make_workflow_spec(
            broker=broker,
            name='node_builder_test',
            tasks=[node_a],
            output=node_a,
        )

        handle = await start_workflow_async(spec, broker)

        # Verify workflow was created
        result = await session.execute(
            text('SELECT id, name, status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[1] == 'node_builder_test'
        assert row[2] == 'RUNNING'

        # Verify task was created with correct args
        task_result = await session.execute(
            text("""
                SELECT task_kwargs FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_row = task_result.fetchone()
        assert task_row is not None
        kwargs = json.loads(task_row[0])
        assert kwargs['value'] == 10
        assert kwargs['multiplier'] == 2.5

    async def test_node_builder_with_waits_for(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """The .node() method respects waits_for dependencies."""
        session, broker, app = setup

        @app.task(task_name='node_builder_root')
        def root_task(x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x * 2)

        @app.task(task_name='node_builder_child')
        def child_task(y: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=y + 1)

        # Create nodes using .node() with dependencies
        root = root_task.node(node_id='root')(x=5)
        child = child_task.node(
            waits_for=[root],
            node_id='child',
        )(y=10)

        spec = make_workflow_spec(
            broker=broker,
            name='node_deps_test',
            tasks=[root, child],
        )

        handle = await start_workflow_async(spec, broker)

        # Verify root is READY/ENQUEUED, child is PENDING
        root_result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND node_id = 'root'
            """),
            {'wf_id': handle.workflow_id},
        )
        root_row = root_result.fetchone()
        assert root_row is not None
        assert root_row[0] in ('READY', 'ENQUEUED')

        child_result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND node_id = 'child'
            """),
            {'wf_id': handle.workflow_id},
        )
        child_row = child_result.fetchone()
        assert child_row is not None
        assert child_row[0] == 'PENDING'

    async def test_node_builder_preserves_node_options(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """The .node() method preserves all node options like allow_failed_deps."""
        session, broker, app = setup

        @app.task(task_name='node_opts_task')
        def opts_task(val: str) -> TaskResult[str, TaskError]:
            return TaskResult(ok=val.upper())

        # Create node with various options
        node_a = opts_task.node(
            node_id='opts_node',
            allow_failed_deps=True,
            join='all',
        )(val='hello')

        spec = make_workflow_spec(
            broker=broker,
            name='node_opts_test',
            tasks=[node_a],
        )

        handle = await start_workflow_async(spec, broker)

        # Verify allow_failed_deps was persisted
        result = await session.execute(
            text("""
                SELECT allow_failed_deps FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND node_id = 'opts_node'
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] is True

    async def test_node_builder_with_args_from(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """.node(args_from=...) stores index-based mapping in DB."""
        session, broker, app = setup

        @app.task(task_name='nb_producer')
        def producer(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        @app.task(task_name='nb_consumer')
        def consumer(
            upstream: TaskResult[int, TaskError],
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=upstream.unwrap() + 1)

        root = producer.node(node_id='prod')(value=42)
        # upstream is provided at runtime via args_from, not at build time
        child: TaskNode[Any] = consumer.node(
            node_id='cons',
            waits_for=[root],
            args_from={'upstream': root},
        )()  # type: ignore[call-arg]

        spec = make_workflow_spec(
            broker=broker,
            name='nb_args_from_test',
            tasks=[root, child],
        )

        handle = await start_workflow_async(spec, broker)

        result = await session.execute(
            text("""
                SELECT args_from FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND node_id = 'cons'
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        args_from = row[0]
        # Maps kwarg name → task_index of producer
        assert args_from == {'upstream': 0}

    async def test_node_builder_with_workflow_ctx_from(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """.node(workflow_ctx_from=...) stores ctx_from node references."""
        session, broker, app = setup

        @app.task(task_name='nb_ctx_src')
        def ctx_source(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        ctx_receiver = make_ctx_receiver_task(app, 'nb_ctx_recv')

        src = ctx_source.node(node_id='ctx_src')(value=10)
        recv = ctx_receiver.node(
            node_id='ctx_recv',
            waits_for=[src],
            workflow_ctx_from=[src],
        )()

        spec = make_workflow_spec(
            broker=broker,
            name='nb_ctx_from_test',
            tasks=[src, recv],
        )

        handle = await start_workflow_async(spec, broker)

        result = await session.execute(
            text("""
                SELECT workflow_ctx_from FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND node_id = 'ctx_recv'
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        ctx_from = row[0]
        # Uses explicit node_id, not auto-generated {spec.name}:{index}
        assert ctx_from == ['ctx_src']

    async def test_node_builder_with_good_until(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """.node(good_until=...) serialized into task_options JSONB."""
        session, broker, app = setup

        @app.task(task_name='nb_good_until')
        def timed_task(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        deadline = datetime(2099, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        node = timed_task.node(
            node_id='timed',
            good_until=deadline,
        )(value=1)

        spec = make_workflow_spec(
            broker=broker,
            name='nb_good_until_test',
            tasks=[node],
        )

        handle = await start_workflow_async(spec, broker)

        result = await session.execute(
            text("""
                SELECT task_options FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND node_id = 'timed'
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] is not None
        options = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        assert options['good_until'] == deadline.isoformat()
