"""Integration tests for error handling policies."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.schema_mode import SchemaMode
from horsies.core.models.workflow import (
    TaskNode,
    WorkflowSpec,
    OnError,
    SubWorkflowNode,
    WorkflowDefinition,
)
from horsies.core.types.result import is_err, is_ok
from horsies.core.workflows.engine import (
    start_workflow_async,
)
from horsies.core.workflows.start_types import WorkflowStartErrorCode

from .conftest import (
    make_simple_task,
    make_workflow_spec,
    start_ok,
)

UNREACHABLE_DATABASE_URL = 'postgresql://postgres:none@127.0.0.1:1/none'
"""A port where no server listens: each core call fails at the connect."""


# =============================================================================
# Shared helpers (deduplicated from per-class methods)
# =============================================================================

_SetupTuple = tuple[AsyncSession, PostgresBroker, Horsies]


async def _get_workflow_status(
    session: AsyncSession,
    workflow_id: str,
) -> str:
    """Get workflow status."""
    result = await session.execute(
        text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
        {'wf_id': workflow_id},
    )
    row = result.fetchone()
    return row[0] if row else 'NOT_FOUND'


async def _get_task_status(
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


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestEnqueuedFailureConversion:
    """Post-ENQUEUED failures should convert to FAILED, not raise and strand state."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> _SetupTuple:
        await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
        await session.commit()
        return session, broker, app


    async def test_resume_enqueues_ready_tasks(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Resume enqueues tasks that were READY at pause time."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'resume_ready_a')
        task_b = make_simple_task(app, 'resume_ready_b')

        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_b = TaskNode(function=task_b, kwargs={'value': 2}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker,
            name='resume_ready',
            tasks=[node_a, node_b],
            on_error=OnError.PAUSE,
        )

        handle = await start_ok(spec, broker)

        # Manually set workflow to PAUSED
        await session.execute(
            text("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = :wf_id"),
            {'wf_id': handle.workflow_id},
        )
        # Manually set B to READY (simulating it was blocked during enqueue)
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'READY'
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Verify B is READY
        assert await _get_task_status(session, str(handle.workflow_id), 1) == 'READY'

        # Resume workflow
        resume_r = await handle.resume_async()
        assert is_ok(resume_r)
        resumed = resume_r.ok_value
        assert resumed is True

        # B should now be ENQUEUED
        b_status = await _get_task_status(session, str(handle.workflow_id), 1)
        assert b_status == 'ENQUEUED', f'Expected ENQUEUED but got {b_status}'

        # Verify task_id was created
        wt_result = await session.execute(
            text(
                'SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = :wf_id AND task_index = 1'
            ),
            {'wf_id': handle.workflow_id},
        )
        wt_row = wt_result.fetchone()
        assert wt_row is not None
        assert wt_row[0] is not None, 'task_id should be set after resume'

    async def test_resume_noop_when_not_paused(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Resume returns False and does nothing when workflow is not PAUSED."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'resume_noop_a')

        node_a = TaskNode(function=task_a, kwargs={'value': 1})

        spec = make_workflow_spec(
            broker=broker,
            name='resume_noop',
            tasks=[node_a],
            on_error=OnError.PAUSE,
        )

        handle = await start_ok(spec, broker)

        # Workflow is RUNNING
        assert await _get_workflow_status(session, str(handle.workflow_id)) == 'RUNNING'

        # Resume should be no-op (returns False)
        resume_r = await handle.resume_async()
        assert is_ok(resume_r)
        resumed = resume_r.ok_value
        assert resumed is False

        # Status unchanged
        assert await _get_workflow_status(session, str(handle.workflow_id)) == 'RUNNING'


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestValidationGuards:
    """Tests for workflow validation guards."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> _SetupTuple:
        """Clean tables and return fixtures."""
        await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
        await session.commit()
        return session, broker, app

    async def test_direct_workflow_spec_fails_without_queue(
        self,
        setup: _SetupTuple,
    ) -> None:
        """WorkflowSpec without resolved queue returns Err(VALIDATION_FAILED)."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'unresolved_queue')

        # Create TaskNode without resolving queue (bypasses app.workflow())
        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        # Direct construction - queue is None
        from horsies.core.models.workflow import WorkflowSpec

        spec = WorkflowSpec(
            name='unresolved',
            tasks=[node_a],
            definition_key='tests.integration.unresolved.v1',
            broker=broker,
        )

        r = await start_workflow_async(spec, broker)
        assert is_err(r)
        assert r.err_value.code == WorkflowStartErrorCode.VALIDATION_FAILED
        assert 'unresolved queue' in r.err_value.message
        assert r.err_value.retryable is False

    async def test_direct_workflow_spec_fails_without_priority(
        self,
        setup: _SetupTuple,
    ) -> None:
        """WorkflowSpec without resolved priority returns Err(VALIDATION_FAILED)."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'unresolved_priority')

        # Create TaskNode with queue but no priority
        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_a.queue = 'default'  # Set queue manually
        # priority is still None

        from horsies.core.models.workflow import WorkflowSpec

        spec = WorkflowSpec(
            name='unresolved_prio',
            tasks=[node_a],
            definition_key='tests.integration.unresolved_prio.v1',
            broker=broker,
        )

        r = await start_workflow_async(spec, broker)
        assert is_err(r)
        assert r.err_value.code == WorkflowStartErrorCode.VALIDATION_FAILED
        assert 'unresolved priority' in r.err_value.message
        assert r.err_value.retryable is False


# =============================================================================
# WorkflowStartResult error-path regression tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowStartErrors:
    """Regression tests for WorkflowStartResult error paths."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> _SetupTuple:
        """Clean tables and return fixtures."""
        await session.execute(
            text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'),
        )
        await session.commit()
        return session, broker, app

    async def test_start_broker_not_configured(self) -> None:
        """WorkflowSpec.start_async without broker returns Err(BROKER_NOT_CONFIGURED)."""
        def task_fn(*, value: int) -> None:
            _ = value

        task_fn.task_name = 'no_broker_task'  # type: ignore[attr-defined]
        task_fn.queue = 'default'  # type: ignore[attr-defined]
        node = TaskNode(function=task_fn, kwargs={'value': 1})
        node.queue = 'default'
        node.priority = 100
        spec = WorkflowSpec(
            name='no_broker',
            tasks=[node],
            definition_key='tests.integration.no_broker.v1',
        )

        r = await spec.start_async()
        assert is_err(r)
        assert r.err_value.code == WorkflowStartErrorCode.BROKER_NOT_CONFIGURED
        assert r.err_value.retryable is False
        assert r.err_value.workflow_name == 'no_broker'
        assert r.err_value.workflow_id  # always populated

    async def test_start_unresolved_queue(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Unresolved queue returns Err(VALIDATION_FAILED)."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'val_q')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        spec = WorkflowSpec(
            name='val_q_wf',
            tasks=[node_a],
            definition_key='tests.integration.val_q_wf.v1',
            broker=broker,
        )

        r = await start_workflow_async(spec, broker)
        assert is_err(r)
        assert r.err_value.code == WorkflowStartErrorCode.VALIDATION_FAILED
        assert 'unresolved queue' in r.err_value.message
        assert r.err_value.retryable is False
        assert r.err_value.workflow_name == 'val_q_wf'
        assert r.err_value.workflow_id  # always populated

    async def test_start_missing_definition_key(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Missing definition_key returns Err(VALIDATION_FAILED)."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'missing_def_key')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_a.queue = 'default'
        node_a.priority = 100
        spec = WorkflowSpec(name='missing_def_key_wf', tasks=[node_a], broker=broker)

        r = await start_workflow_async(spec, broker)
        assert is_err(r)
        assert r.err_value.code == WorkflowStartErrorCode.VALIDATION_FAILED
        assert 'definition_key' in r.err_value.message
        assert r.err_value.retryable is False

    async def test_start_unresolved_priority(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Unresolved priority returns Err(VALIDATION_FAILED)."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'val_p')
        node_a = TaskNode(function=task_a, kwargs={'value': 1})
        node_a.queue = 'default'
        spec = WorkflowSpec(
            name='val_p_wf',
            tasks=[node_a],
            definition_key='tests.integration.val_p_wf.v1',
            broker=broker,
        )

        r = await start_workflow_async(spec, broker)
        assert is_err(r)
        assert r.err_value.code == WorkflowStartErrorCode.VALIDATION_FAILED
        assert 'unresolved priority' in r.err_value.message
        assert r.err_value.retryable is False

    async def test_start_schema_init_failure(
        self,
        setup: _SetupTuple,
    ) -> None:
        """Schema init failure returns Err(ENQUEUE_FAILED).

        The start runs in the core, which installs the schema once per handle
        before the first statement. The broker installs on a database that
        refuses the connection."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'schema_fail')
        spec = make_workflow_spec('schema_fail_wf', [TaskNode(function=task_a, kwargs={'value': 1})], broker)
        unreachable = PostgresBroker(
            PostgresConfig(database_url=SecretStr(UNREACHABLE_DATABASE_URL)),
        )

        try:
            r = await start_workflow_async(spec, unreachable)
        finally:
            _ = await unreachable.close_async()

        assert is_err(r)
        assert r.err_value.code == WorkflowStartErrorCode.ENQUEUE_FAILED
        assert r.err_value.retryable is True
        assert 'Schema initialization failed' in r.err_value.message
        assert 'connection refused' in r.err_value.message.lower()
        assert r.err_value.workflow_id  # always populated

    async def test_start_db_transient_error(
        self,
        setup: _SetupTuple,
    ) -> None:
        """DB transient error returns Err(ENQUEUE_FAILED, retryable=True).

        The start runs in the core; its database refuses the connection."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'db_err')
        spec = make_workflow_spec('db_err_wf', [TaskNode(function=task_a, kwargs={'value': 1})], broker)
        unreachable = PostgresBroker(
            PostgresConfig(database_url=SecretStr(UNREACHABLE_DATABASE_URL)),
            schema_mode=SchemaMode.ASSUME_INITIALIZED,
        )

        try:
            r = await start_workflow_async(spec, unreachable)
        finally:
            _ = await unreachable.close_async()

        assert is_err(r)
        assert r.err_value.code == WorkflowStartErrorCode.ENQUEUE_FAILED
        assert r.err_value.retryable is True
        assert r.err_value.workflow_id  # always populated

    async def test_start_rollback_integrity(
        self,
        setup: _SetupTuple,
    ) -> None:
        """A start that owes a child start runs in one transaction: a failure
        of the child start after the start statement leaves no row.

        The start runs in the core. The child of the broken root cannot be
        built, so its node fails, and the start owes the start of the child
        that waits for it (allow_failed_dependencies): the start statement and that
        child start run in one transaction. A trigger refuses the insert of
        the second child's workflow row."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'rollback_test')
        suffix = uuid.uuid4().hex[:8]

        class RollbackBroken(WorkflowDefinition[int]):
            name = f'rollback_broken_{suffix}'
            definition_key = f'rollback-broken-{suffix}'

            @classmethod
            def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
                _ = (app, args, params)
                raise RuntimeError('the child cannot be built')

        class RollbackChild(WorkflowDefinition[int]):
            name = f'rollback_child_{suffix}'
            definition_key = f'rollback-child-{suffix}'

            @classmethod
            def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
                _ = (args, params)
                node = TaskNode(function=task_a, kwargs={'value': 1})
                return app.workflow(cls.name, [node], output=node)

        broken = SubWorkflowNode(workflow_def=RollbackBroken)
        started_after = SubWorkflowNode(
            workflow_def=RollbackChild, waits_for=[broken], allow_failed_dependencies=True,
        )
        spec = app.workflow(
            'rollback_wf', [broken, started_after], definition_key=f'rollback-wf-{suffix}',
        )
        await session.execute(
            text(f"""
                CREATE FUNCTION it_refuse_child_start() RETURNS trigger
                LANGUAGE plpgsql AS $body$
                BEGIN
                    IF NEW.name = '{RollbackChild.name}' THEN
                        RAISE EXCEPTION 'child start refused by the test';
                    END IF;
                    RETURN NEW;
                END
                $body$
            """),
        )
        await session.execute(
            text(
                'CREATE TRIGGER it_refuse_child_start BEFORE INSERT ON horsies_workflows '
                'FOR EACH ROW EXECUTE FUNCTION it_refuse_child_start()'
            ),
        )
        await session.commit()

        try:
            r = await start_workflow_async(spec, broker)
        finally:
            await session.execute(
                text('DROP TRIGGER it_refuse_child_start ON horsies_workflows'),
            )
            await session.execute(text('DROP FUNCTION it_refuse_child_start()'))
            await session.commit()

        assert is_err(r)
        assert r.err_value.code == WorkflowStartErrorCode.ENQUEUE_FAILED
        assert 'child start refused by the test' in r.err_value.message

        # Verify no rows persisted
        wf_check = await session.execute(
            text(
                'SELECT count(*) FROM horsies_workflows '
                "WHERE name IN ('rollback_wf', :broken, :child)"
            ),
            {'broken': RollbackBroken.name, 'child': RollbackChild.name},
        )
        assert wf_check.scalar() == 0

    async def test_start_sync_core_boundary_error_is_enqueue_failed(
        self,
        setup: _SetupTuple,
    ) -> None:
        """A core boundary error of the blocking start (the call reached its
        bound) returns Err(ENQUEUE_FAILED) with the error, retryable."""
        from unittest.mock import MagicMock, patch

        from horsies._core import CoreCallTimedOut
        from horsies.core.workflows.engine import start_workflow

        session, broker, app = setup
        task_a = make_simple_task(app, 'sync_fail')

        spec = make_workflow_spec(
            'sync_fail_wf',
            [TaskNode(function=task_a, kwargs={'value': 1})],
            broker,
        )

        timed_out = CoreCallTimedOut('CoreHandle.start_workflow', 1800, 'outcome_unknown')
        core_handle = MagicMock()
        core_handle.start_workflow = MagicMock(side_effect=timed_out)
        with patch.object(broker, '_core_handle', core_handle):
            r = await asyncio.to_thread(start_workflow, spec, broker)

        assert is_err(r)
        assert r.err_value.code == WorkflowStartErrorCode.ENQUEUE_FAILED
        assert r.err_value.retryable is True
        assert r.err_value.workflow_name == 'sync_fail_wf'
        assert r.err_value.workflow_id  # always populated
        assert 'CoreCallTimedOut' in r.err_value.message
        core_handle.start_workflow.assert_called_once()
        core_handle.start_workflow_async.assert_not_called()

