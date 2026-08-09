"""Integration tests for workflow recovery logic."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import dumps_json, loads_json
from horsies.core.codec.typed import decode_task_result, encode_task_result, encode_value
from horsies.core.models.tasks import TaskResult, TaskError, OperationalErrorCode
from horsies.core.models.workflow import (
    TaskNode,
    SubWorkflowNode,
    WorkflowDefinition,
    WorkflowSpec,
    OnError,
)
from horsies.core.workflows.engine import (
    on_workflow_task_complete,
    _queue_parent_propagation,  # pyright: ignore[reportPrivateUsage]
    pop_pending_parent_propagations,
)
from horsies.core.workflows.recovery import (
    GLOBAL_SCAN_ROW_CAP,
    recover_stuck_workflows,
    _run_recovery_candidate,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.workflows.phase2_recovery import drive_phase2_recovery

from .conftest import make_simple_task, make_failing_task, make_workflow_spec, start_ok
from horsies.core.models.workflow import SuccessPolicy, SuccessCase
from tests.integration.history_seeding import force_terminal
from tests.integration.conftest import task_name_for


async def _run_phase2_recovery(
    session: AsyncSession,
    broker: PostgresBroker,
    *,
    grace_ms: int = 0,
) -> int:
    """Drive the reaper's phase-2 recovery pass over seeded evidence.

    The crashed-worker case is the outbox consumer's, not the scan's:
    terminalization records the owed progression as it moves the task,
    and this is what the reaper does with those records. The driver owns
    a transaction per record, so the seeded state is committed first and
    the caller's next read starts a fresh snapshot.

    Returns the count of nodes advanced.
    """
    await session.commit()
    summary = await drive_phase2_recovery(
        broker.session_factory,
        broker,
        grace_ms=grace_ms,
        max_rows=GLOBAL_SCAN_ROW_CAP,
        quarantine_after_attempts=25,
    )
    return summary.applied


def _strict_result_json(result: TaskResult[Any, TaskError]) -> str:
    """Build the strict-serde wire envelope for ``horsies_workflow_tasks.result``.

    Tests in this file seed terminal task rows directly (simulating crash /
    recovery states); they must match what production writes so the engine's
    strict envelope decode at ``on_workflow_task_complete`` / recovery
    finalization accepts the payload. ``ok_type=Any`` is sufficient — these
    tests substring-check the serialized output and don't pin the ok-payload
    type-narrowing on read.
    """
    envelope = encode_task_result(result, Any)
    serialized = dumps_json(envelope)
    return serialized.unwrap()


def _strict_error_json(error: TaskError) -> str:
    """Build the bare TaskError JSON for the ``horsies_workflows.error`` column.

    Production writes this column via
    ``dumps_json(encode_value(err, TaskError))`` — bare TaskError dict, no
    ``__h_task_result__`` envelope wrapper.
    """
    return dumps_json(encode_value(error, TaskError)).unwrap()


def _workflow(
    app: Horsies,
    *,
    name: str,
    tasks: list[Any],
    **kwargs: Any,
) -> Any:
    return app.workflow(
        name=name,
        tasks=tasks,
        definition_key=f'tests.integration.{name}.v1',
        **kwargs,
    )


class RecoveryChildWorkflow(WorkflowDefinition[int]):
    """Minimal child workflow for subworkflow recovery tests."""

    name = 'recovery_child_workflow'
    definition_key = 'tests.recovery_child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> WorkflowSpec:
        @app.task(task_name='recovery_child_task')
        def child_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=42)

        node = TaskNode(fn=child_task)
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowRecovery:
    """Tests for recover_stuck_workflows()."""

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

    async def _insert_probe_workflow(self, session: AsyncSession, wf_id: str) -> None:
        """Insert a minimal RUNNING workflow row used as a SAVEPOINT probe."""
        await session.execute(
            text("""
                INSERT INTO horsies_workflows
                    (id, name, status, on_error, depth, root_workflow_id,
                     created_at, updated_at)
                VALUES (:id, 'savepoint_probe', 'RUNNING', 'FAIL', 0, :id,
                        NOW(), NOW())
            """),
            {'id': wf_id},
        )

    async def _probe_exists(self, session: AsyncSession, wf_id: str) -> bool:
        row = (
            await session.execute(
                text('SELECT 1 FROM horsies_workflows WHERE id = :id'),
                {'id': wf_id},
            )
        ).fetchone()
        return row is not None

    async def test_run_recovery_candidate_failure_rolls_back_savepoint(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A failing candidate rolls back its DB writes and restores the
        in-memory propagation queue, leaving the surrounding pass intact."""
        session, _broker, _app = setup

        _queue_parent_propagation(session, 'baseline-child')
        probe_id = str(uuid.uuid4())

        async def _poison() -> bool:
            await self._insert_probe_workflow(session, probe_id)
            _queue_parent_propagation(session, 'candidate-child')
            raise RuntimeError("reserved key '__h_task_result__'")

        result = await _run_recovery_candidate(
            session,
            case='unit_probe',
            action=_poison,
        )

        assert result is False
        # SAVEPOINT rollback dropped the probe row...
        assert not await self._probe_exists(session, probe_id)
        # ...and the queue is restored to the pre-candidate snapshot.
        assert session.info['horsies_pending_parent_propagations'] == [
            'baseline-child',
        ]
        await session.rollback()

    async def test_run_recovery_candidate_success_commits_savepoint(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A succeeding candidate keeps its DB writes and queued propagations."""
        session, _broker, _app = setup

        probe_id = str(uuid.uuid4())

        async def _succeed() -> bool:
            await self._insert_probe_workflow(session, probe_id)
            _queue_parent_propagation(session, 'candidate-child')
            return True

        result = await _run_recovery_candidate(
            session,
            case='unit_probe',
            action=_succeed,
        )

        assert result is True
        assert await self._probe_exists(session, probe_id)
        assert pop_pending_parent_propagations(session) == ['candidate-child']
        await session.rollback()

    async def test_recover_ready_not_enqueued(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """READY tasks with NULL task_id get enqueued."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_ready_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(broker=broker, name='recover_ready', tasks=[node_a])

        handle = await start_ok(spec, broker)

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

        # Run recovery
        recovered = await recover_stuck_workflows(session)
        await session.commit()

        # Should have recovered 1 task
        assert recovered == 1

        # Task should now have task_id
        result = await session.execute(
            text("""
                SELECT task_id, status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row[0] is not None  # task_id set
        assert row[1] == 'ENQUEUED'

    async def test_recover_completed_not_marked(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """All done, no failures -> COMPLETED with result persisted."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_completed_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='recover_completed', tasks=[node_a]
        )

        handle = await start_ok(spec, broker)

        # Simulate: task completed but workflow not updated
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = :result
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {
                'wf_id': handle.workflow_id,
                'result': _strict_result_json(TaskResult(ok=10)),
            },
        )
        # Workflow still RUNNING with no result
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL, result = NULL
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Run recovery
        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        # Workflow should be COMPLETED with result
        result = await session.execute(
            text('SELECT status, result FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row[0] == 'COMPLETED'
        assert row[1] is not None  # result persisted

    async def test_recover_failed_not_marked(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """All done, has failures -> FAILED with result and error persisted."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'recover_failed_a')

        node_a = TaskNode(fn=task_a)
        spec = make_workflow_spec(
            broker=broker, name='recover_failed', tasks=[node_a], on_error=OnError.FAIL
        )

        handle = await start_ok(spec, broker)

        # Simulate: task failed but workflow not updated (error not set on workflow)
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED', result = :result
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {
                'wf_id': handle.workflow_id,
                'result': _strict_result_json(
                    TaskResult(err=TaskError(error_code='TEST_ERROR', message='Test failure')),
                ),
            },
        )
        # Workflow still RUNNING with no error
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL, result = NULL, error = NULL
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Run recovery
        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        # Workflow should be FAILED with result and error
        result = await session.execute(
            text('SELECT status, result, error FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row[0] == 'FAILED'
        assert row[1] is not None  # result persisted
        assert row[2] is not None  # error derived from failed task
        assert 'TEST_ERROR' in row[2]  # error contains task error

    async def test_recover_failed_recomputes_first_failed_error(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Recovery finalization uses the first failed task error by index."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'recover_first_error_a')
        task_b = make_failing_task(app, 'recover_first_error_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b)
        spec = make_workflow_spec(
            broker=broker,
            name='recover_first_error',
            tasks=[node_a, node_b],
            on_error=OnError.FAIL,
        )

        handle = await start_ok(spec, broker)

        # Simulate: both tasks failed, but workflow.error currently has the
        # later failure from prior per-task failure handling.
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED',
                    result = :result
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {
                'wf_id': handle.workflow_id,
                'result': _strict_result_json(
                    TaskResult(err=TaskError(error_code='FIRST_ERROR', message='First failure')),
                ),
            },
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED',
                    result = :result
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {
                'wf_id': handle.workflow_id,
                'result': _strict_result_json(
                    TaskResult(err=TaskError(error_code='SECOND_ERROR', message='Second failure')),
                ),
            },
        )
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL, result = NULL, error = :error
                WHERE id = :wf_id
            """),
            {
                'wf_id': handle.workflow_id,
                'error': _strict_error_json(
                    TaskError(error_code='SECOND_ERROR', message='Second failure'),
                ),
            },
        )
        await session.commit()

        # Run recovery
        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        # Workflow should be FAILED with the deterministic first failed task error.
        result = await session.execute(
            text('SELECT status, result, error FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row[0] == 'FAILED'
        assert row[1] is not None  # result persisted
        assert row[2] is not None
        assert 'FIRST_ERROR' in row[2]
        assert 'SECOND_ERROR' not in row[2]

    async def test_recover_paused_not_touched(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """PAUSED workflows not modified by recovery."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'recover_paused_a')
        task_b = make_simple_task(app, 'recover_paused_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, kwargs={'value': 1}, waits_for=[node_a])

        spec = make_workflow_spec(
            broker=broker,
            name='recover_paused',
            tasks=[node_a, node_b],
            on_error=OnError.PAUSE,
        )

        handle = await start_ok(spec, broker)

        # Set workflow to PAUSED with pending tasks
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'PAUSED'
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        # B stays PENDING
        await session.commit()

        # Run recovery
        await recover_stuck_workflows(session)
        await session.commit()

        # Workflow should still be PAUSED (not touched)
        result = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        assert result.fetchone()[0] == 'PAUSED'

        # B should still be PENDING (not skipped by recovery)
        task_result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        assert task_result.fetchone()[0] == 'PENDING'

    async def test_recover_idempotent(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Running recovery twice is safe."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_idempotent_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='recover_idempotent', tasks=[node_a]
        )

        handle = await start_ok(spec, broker)

        # Simulate stuck READY task
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'READY', task_id = NULL
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Run recovery twice
        recovered1 = await recover_stuck_workflows(session)
        await session.commit()

        recovered2 = await recover_stuck_workflows(session)
        await session.commit()

        # First run should recover, second should find nothing
        assert recovered1 == 1
        assert recovered2 == 0

        # Task should be ENQUEUED (not double-enqueued)
        result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        assert result.fetchone()[0] == 'ENQUEUED'

    async def test_recover_completed_sets_timestamp(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Recovery sets completed_at when finalizing a stuck workflow."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_notify_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(broker=broker, name='recover_notify', tasks=[node_a])

        handle = await start_ok(spec, broker)

        # Simulate: all tasks done, workflow not updated
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = :result
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {
                'wf_id': handle.workflow_id,
                'result': _strict_result_json(TaskResult(ok=10)),
            },
        )
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Run recovery (this sends NOTIFY internally)
        recovered = await recover_stuck_workflows(session)
        await session.commit()

        # Verify workflow is now completed (NOTIFY would have been sent)
        assert recovered == 1
        result = await session.execute(
            text('SELECT status, completed_at FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row[0] == 'COMPLETED'
        assert row[1] is not None  # completed_at set

    async def test_recover_respects_success_policy(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Recovery uses success_policy to determine COMPLETED vs FAILED."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_sp_a')
        task_b = make_failing_task(app, 'recover_sp_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b)

        # Success policy: workflow succeeds if A completes
        policy = SuccessPolicy(cases=[SuccessCase(required=[node_a])])

        spec = make_workflow_spec(
            broker=broker,
            name='recover_sp',
            tasks=[node_a, node_b],
            success_policy=policy,
        )

        handle = await start_ok(spec, broker)

        # Simulate: both tasks terminal, A completed, B failed
        # Workflow stuck in RUNNING
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = :result
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {
                'wf_id': handle.workflow_id,
                'result': _strict_result_json(TaskResult(ok=2)),
            },
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED', result = :result
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {
                'wf_id': handle.workflow_id,
                'result': _strict_result_json(
                    TaskResult(err=TaskError(error_code='TEST', message='')),
                ),
            },
        )
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Run recovery
        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        # Workflow should be COMPLETED (success case [A] is satisfied)
        # even though B failed
        result = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        assert result.fetchone()[0] == 'COMPLETED'

    async def test_recover_crashed_worker_workflow_task(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.7: workflow_task RUNNING but underlying task already FAILED (worker crash)."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_crash_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='recover_crash', tasks=[node_a]
        )

        handle = await start_ok(spec, broker)

        # Simulate worker crash: tasks.status = FAILED, workflow_tasks.status = RUNNING
        # First get the task_id
        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        # Set tasks row to FAILED (as reaper would)
        await force_terminal(
            session,
            task_id,
            status='FAILED',
            result_json=_strict_result_json(
                    TaskResult(
                        err=TaskError(
                            error_code=OperationalErrorCode.WORKER_CRASHED,
                            message='Worker died',
                        ),
                    ),
                ),
        )

        # Set workflow_tasks to RUNNING (simulating crash before on_workflow_task_complete)
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'RUNNING'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Run recovery
        recovered = await _run_phase2_recovery(session, broker)
        await session.commit()

        assert recovered == 1

        # workflow_tasks should now be FAILED
        wt_check = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        assert wt_check.fetchone()[0] == 'FAILED'

        # Workflow should be FAILED (single task failed)
        wf_check = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        assert wf_check.fetchone()[0] == 'FAILED'

    async def test_finalizing_grace_skips_recent_then_recovers_aged(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.7 grace: a just-terminal task is left for its in-flight
        finalizer; the same task aged past the grace is recovered."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'grace_crash_a')
        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='grace_crash', tasks=[node_a]
        )
        handle = await start_ok(spec, broker)

        wt_result = await session.execute(
            text(
                'SELECT task_id FROM horsies_workflow_tasks '
                'WHERE workflow_id = :wf_id AND task_index = 0'
            ),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        async def _terminalize_with_node_running() -> None:
            # The crash shape: the task terminalized, its node left
            # behind. Terminalization records the owed progression as it
            # moves the task, so the evidence exists from here on.
            await force_terminal(
                session,
                task_id,
                status='FAILED',
                result_json=_strict_result_json(
                    TaskResult(
                        err=TaskError(
                            error_code=OperationalErrorCode.WORKER_CRASHED,
                            message='Worker died',
                        ),
                    ),
                ),
            )
            await session.execute(
                text(
                    "UPDATE horsies_workflow_tasks SET status = 'RUNNING' "
                    'WHERE workflow_id = :wf_id AND task_index = 0'
                ),
                {'wf_id': handle.workflow_id},
            )
            await session.commit()

        async def _age_evidence(age_secs: float) -> None:
            # Time passing, not a second terminalization: nothing in
            # production terminalizes an already-terminal task, so the
            # recorded evidence is what ages.
            await session.execute(
                text(
                    'UPDATE horsies_workflow_phase2_pending '
                    'SET created_at = NOW() - make_interval('
                    '    secs => CAST(:age AS double precision)) '
                    'WHERE task_id = CAST(:tid AS uuid)'
                ),
                {'age': age_secs, 'tid': task_id},
            )
            await session.commit()

        async def _wt_status() -> str:
            row = (
                await session.execute(
                    text(
                        'SELECT status FROM horsies_workflow_tasks '
                        'WHERE workflow_id = :wf_id AND task_index = 0'
                    ),
                    {'wf_id': handle.workflow_id},
                )
            ).fetchone()
            assert row is not None
            return row[0]

        grace_ms = 60_000

        # Just terminal: within grace -> not recovered, left RUNNING,
        # because the healthy finalizer is presumed still in flight.
        await _terminalize_with_node_running()
        recovered = await _run_phase2_recovery(
            session, broker, grace_ms=grace_ms
        )
        await session.commit()
        assert recovered == 0
        assert await _wt_status() == 'RUNNING'

        # Aged 120s, past the 60s grace -> recovered.
        await _age_evidence(120.0)
        recovered_aged = await _run_phase2_recovery(
            session, broker, grace_ms=grace_ms
        )
        await session.commit()
        assert recovered_aged == 1
        assert await _wt_status() == 'FAILED'

    async def test_recover_crashed_worker_dependent_propagation(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.7: After crash recovery of A (FAILED), dependent B gets SKIPPED."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_dep_a')
        task_b = make_simple_task(app, 'recover_dep_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        node_b = TaskNode(fn=task_b, kwargs={'value': 1}, waits_for=[node_a])
        spec = make_workflow_spec(
            broker=broker, name='recover_dep', tasks=[node_a, node_b]
        )

        handle = await start_ok(spec, broker)

        # Get task_id for node_a
        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        # Simulate worker crash on task A
        await force_terminal(
            session,
            task_id,
            status='FAILED',
            result_json=_strict_result_json(
                    TaskResult(
                        err=TaskError(
                            error_code=OperationalErrorCode.WORKER_CRASHED,
                            message='Worker died',
                        ),
                    ),
                ),
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'RUNNING'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Run recovery
        recovered = await _run_phase2_recovery(session, broker)
        await session.commit()

        assert recovered == 1

        # Task A should be FAILED
        wt_a = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        assert wt_a.fetchone()[0] == 'FAILED'

        # Task B should be SKIPPED (dependency failed, allow_failed_deps=False)
        wt_b = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        assert wt_b.fetchone()[0] == 'SKIPPED'

    # RETIRED: three cases recovered a terminal task with NO stored
    # result. On any path that owes phase-2 progression the move refuses
    # that state at the source — 'deferred workflow terminalization
    # requires a result payload' — because the outbox carries the
    # result's digest as a NOT NULL column. The four deferring families
    # all produce a payload; the four that do not defer advance their
    # node inline or have no node. The invariant those cases were
    # unknowingly probing is now pinned directly, in
    # test_task_history_terminalization_move.py.

    async def test_recover_crashed_worker_idempotent(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Running recovery twice: second run should recover 0 for Case 1.7."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_idem_crash_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='recover_idem_crash', tasks=[node_a]
        )

        handle = await start_ok(spec, broker)

        # Get task_id
        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        # Simulate crash
        await force_terminal(
            session,
            task_id,
            status='FAILED',
            result_json=_strict_result_json(
                    TaskResult(
                        err=TaskError(
                            error_code=OperationalErrorCode.WORKER_CRASHED,
                            message='Worker died',
                        ),
                    ),
                ),
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'RUNNING'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # First recovery
        recovered1 = await _run_phase2_recovery(session, broker)
        await session.commit()

        # Second recovery
        recovered2 = await _run_phase2_recovery(session, broker)
        await session.commit()

        assert recovered1 == 1
        assert recovered2 == 0

    async def test_recover_crashed_worker_unregistered_task_err_fast_path(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.7: err-only result for an unregistered task must surface
        via the recovery err-fast-path.

        Pre-fix ``_decode_recovered_task_result`` required the source task's
        ``task_ok_type`` for *any* decode path. When recovery ran in a
        process that hadn't imported the task (cross-process recovery), the
        real failure was discarded in favor of a synthetic
        ``WORKER_CRASHED``. The err-fast-path now uses
        ``decode_task_error`` (no ok_type) so the real err survives.
        """
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_unregistered_err_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='recover_unregistered_err', tasks=[node_a]
        )

        handle = await start_ok(spec, broker)

        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        # Store a real TASK_EXCEPTION (err-only) result, then simulate
        # crash via workflow_tasks=RUNNING.
        real_err_result = TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.TASK_EXCEPTION,
                message='real failure to preserve through recovery',
            ),
        )
        await force_terminal(
            session,
            task_id,
            status='FAILED',
            result_json=_strict_result_json(real_err_result),
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'RUNNING'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Simulate cross-process recovery: the task isn't registered in
        # the recovery worker's app.tasks. Pre-fix this discarded the
        # real err and produced a synthetic WORKER_CRASHED.
        app.tasks.unregister('recover_unregistered_err_a')
        assert app.tasks.get('recover_unregistered_err_a') is None

        recovered = await _run_phase2_recovery(session, broker)
        await session.commit()

        assert recovered == 1

        wt_check = await session.execute(
            text("""
                SELECT status, result FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        status, result_json = wt_check.fetchone()
        assert status == 'FAILED'

        recovered_result = decode_task_result(
            loads_json(result_json).unwrap(), Any,
        )
        assert recovered_result.is_err()
        assert recovered_result.err is not None
        # Critical: the real err survives, not the synthetic WORKER_CRASHED.
        assert recovered_result.err.error_code == (
            OperationalErrorCode.TASK_EXCEPTION
        )
        assert recovered_result.err.message == (
            'real failure to preserve through recovery'
        )

    # ── Case 0: PENDING tasks with all deps terminal ──

    async def test_recover_pending_deps_succeeded(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 0: PENDING task whose deps all COMPLETED gets enqueued."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_pend_ok_a')
        task_b = make_simple_task(app, 'recover_pend_ok_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
        spec = make_workflow_spec(
            broker=broker, name='recover_pend_ok', tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)

        # Simulate: A completed but B stuck at PENDING (race condition)
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = :result
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {
                'wf_id': handle.workflow_id,
                'result': _strict_result_json(TaskResult(ok=2)),
            },
        )
        await session.commit()

        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        # B should now be ENQUEUED with task_id set
        result = await session.execute(
            text("""
                SELECT status, task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] == 'ENQUEUED'
        assert row[1] is not None

    async def test_recover_pending_deps_failed_skip(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 0: PENDING task with failed dep and allow_failed_deps=False gets SKIPPED."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'recover_pend_fail_a')
        task_b = make_simple_task(app, 'recover_pend_fail_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(fn=task_b, kwargs={'value': 1}, waits_for=[node_a])
        spec = make_workflow_spec(
            broker=broker, name='recover_pend_fail', tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)

        # Simulate: A failed but B stuck at PENDING
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED',
                    result = :result
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {
                'wf_id': handle.workflow_id,
                'result': _strict_result_json(
                    TaskResult(err=TaskError(error_code='TEST_FAIL', message='fail')),
                ),
            },
        )
        await session.commit()

        recovered = await recover_stuck_workflows(session)
        await session.commit()

        # 2 recoveries: Case 0 skips B, then Case 2+3 finalizes workflow (all tasks terminal)
        assert recovered == 2

        result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        assert result.fetchone()[0] == 'SKIPPED'

        # Workflow should also be FAILED (cascading finalization)
        wf_result = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        assert wf_result.fetchone()[0] == 'FAILED'

    async def test_recover_pending_deps_failed_allow_continue(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 0: PENDING task with failed dep and allow_failed_deps=True gets ENQUEUED."""
        session, broker, app = setup
        task_a = make_failing_task(app, 'recover_pend_allow_a')
        task_b = make_simple_task(app, 'recover_pend_allow_b')

        node_a = TaskNode(fn=task_a)
        node_b = TaskNode(
            fn=task_b, kwargs={'value': 1}, waits_for=[node_a], allow_failed_deps=True,
        )
        spec = make_workflow_spec(
            broker=broker, name='recover_pend_allow', tasks=[node_a, node_b],
        )

        handle = await start_ok(spec, broker)

        # Simulate: A failed but B stuck at PENDING
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED',
                    result = :result
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {
                'wf_id': handle.workflow_id,
                'result': _strict_result_json(
                    TaskResult(err=TaskError(error_code='TEST_FAIL', message='fail')),
                ),
            },
        )
        await session.commit()

        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        result = await session.execute(
            text("""
                SELECT status, task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] == 'ENQUEUED'
        assert row[1] is not None

    # ── Case 1.5: READY SubWorkflowNodes not started ──

    async def test_recover_subworkflow_ready_without_broker_leaves_ready(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.5: READY subworkflow without a broker stays ready for broker retry."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_sub_nb_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=RecoveryChildWorkflow,
            waits_for=[node_a],
        )

        spec = _workflow(app, 
            name='recover_sub_no_broker',
            tasks=[node_a, node_child],
            output=node_child,
        )

        handle = await start_ok(spec, broker)

        # Simulate: A completed, SubWorkflowNode stuck at READY with NULL sub_workflow_id
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = :result
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {
                'wf_id': handle.workflow_id,
                'result': _strict_result_json(TaskResult(ok=2)),
            },
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'READY', sub_workflow_id = NULL
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Recovery without broker
        recovered = await recover_stuck_workflows(session, broker=None)
        await session.commit()

        assert recovered == 0

        result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        assert result.fetchone()[0] == 'READY'

    async def test_recover_subworkflow_ready_with_broker(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.5: READY subworkflow with broker starts the child workflow."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_sub_wb_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=RecoveryChildWorkflow,
            waits_for=[node_a],
        )

        spec = _workflow(app, 
            name='recover_sub_with_broker',
            tasks=[node_a, node_child],
            output=node_child,
        )

        handle = await start_ok(spec, broker)

        # Simulate: A completed, SubWorkflowNode stuck at READY with NULL sub_workflow_id
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = :result
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {
                'wf_id': handle.workflow_id,
                'result': _strict_result_json(TaskResult(ok=2)),
            },
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'READY', sub_workflow_id = NULL
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Recovery with broker — should start the child workflow
        recovered = await recover_stuck_workflows(session, broker)
        await session.commit()

        assert recovered == 1

        result = await session.execute(
            text("""
                SELECT status, sub_workflow_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] in ('ENQUEUED', 'RUNNING')
        assert row[1] is not None  # sub_workflow_id set

    # ── Case 1.6: Child completed but parent node not updated ──

    async def test_recover_child_completed_parent_not_updated(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.6: Child workflow COMPLETED but parent node still RUNNING."""
        session, broker, app = setup

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=RecoveryChildWorkflow,
        )

        spec = _workflow(app, 
            name='recover_child_done',
            tasks=[node_child],
            output=node_child,
        )

        handle = await start_ok(spec, broker)

        # Get the child workflow ID
        wt_result = await session.execute(
            text("""
                SELECT sub_workflow_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        child_id = wt_result.fetchone()[0]
        assert child_id is not None

        # Get child's task_id and complete it normally
        child_task_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :child_id AND task_index = 0
            """),
            {'child_id': child_id},
        )
        child_task_id = child_task_result.fetchone()[0]
        assert child_task_id is not None

        await on_workflow_task_complete(
            session, child_task_id, TaskResult(ok=42), broker,
            task_name=await task_name_for(session, child_task_id),
        )
        await session.commit()

        # Now simulate a crash: revert parent node back to RUNNING
        # (as if the on_subworkflow_complete callback was interrupted)
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'RUNNING'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        # Also revert parent workflow to RUNNING
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Recovery should detect the completed child and update the parent node
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
        assert parent_row.fetchone()[0] == 'COMPLETED'

    async def test_recover_child_cancelled_parent_not_updated(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.6: Child workflow CANCELLED but parent node still RUNNING."""
        session, broker, app = setup

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=RecoveryChildWorkflow,
        )

        spec = _workflow(app, 
            name='recover_child_cancelled',
            tasks=[node_child],
            output=node_child,
        )

        handle = await start_ok(spec, broker)

        # Get the child workflow ID
        wt_result = await session.execute(
            text("""
                SELECT sub_workflow_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        child_id = wt_result.fetchone()[0]
        assert child_id is not None

        # Simulate a missed callback: child is terminal CANCELLED
        # while parent node is still RUNNING.
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'CANCELLED',
                    completed_at = NOW(),
                    updated_at = NOW()
                WHERE id = :child_id
            """),
            {'child_id': child_id},
        )
        await session.commit()

        # Recovery should detect the cancelled child and update the parent node.
        recovered = await recover_stuck_workflows(session, broker)
        await session.commit()

        assert recovered >= 1

        # Parent node should now be FAILED (non-COMPLETED child status maps to failure).
        parent_row = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        assert parent_row.fetchone()[0] == 'FAILED'

    # ── Case 1.7: FAILED task with missing result ──

    async def test_recover_success_policy_not_satisfied(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 2+3: All tasks terminal but no success case met -> FAILED."""
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_sp_fail_a')
        task_b = make_failing_task(app, 'recover_sp_fail_b')

        node_a = TaskNode(fn=task_a, kwargs={'value': 1})
        node_b = TaskNode(fn=task_b)

        # Both A and B are required — B failing means no case is satisfied
        policy = SuccessPolicy(cases=[SuccessCase(required=[node_a, node_b])])

        spec = make_workflow_spec(
            broker=broker,
            name='recover_sp_fail',
            tasks=[node_a, node_b],
            success_policy=policy,
        )

        handle = await start_ok(spec, broker)

        # Simulate: A completed, B failed, workflow stuck RUNNING
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = :result
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {
                'wf_id': handle.workflow_id,
                'result': _strict_result_json(TaskResult(ok=2)),
            },
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'FAILED',
                    result = :result
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {
                'wf_id': handle.workflow_id,
                'result': _strict_result_json(
                    TaskResult(err=TaskError(error_code='TASK_FAIL', message='failed')),
                ),
            },
        )
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL, error = NULL
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        recovered = await recover_stuck_workflows(session)
        await session.commit()

        assert recovered == 1

        result = await session.execute(
            text('SELECT status, error FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] == 'FAILED'
        assert row[1] is not None
        # Error should be from the first failed required task (TASK_FAIL)
        assert 'TASK_FAIL' in row[1]


class TestQuarantineAfterAttempts:
    """The attempt bound on unresolvable retained evidence.

    A retaining disposition increments the pending row's attempt count;
    at the bound the transition runs and discovery stops retrying the
    row either way. An integrity-retained row — the constructible
    unresolvable shape — also refuses the quarantine copy, because the
    copy verifies against the same pending fields consumption verified
    against: the refusal is reported verbatim, the evidence stays where
    it is, and the population stays countable on the health surface.
    The repoint half of the transition is proven by the detach-horizon
    quarantine suite; this proves the bound, the exclusion, and the
    refusal reporting. Disabling the transition turns the refusal
    assertions red for the stated reason: nothing would attempt the
    relocation at all.
    """

    @pytest.mark.asyncio
    async def test_an_unresolvable_row_stops_retrying_at_the_bound(
        self, broker: PostgresBroker, session: AsyncSession, app: Horsies
    ) -> None:
        task_a = make_simple_task(app, 'quarantine_bound_a')
        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='quarantine_bound', tasks=[node_a]
        )
        handle = await start_ok(spec, broker)

        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        await force_terminal(
            session,
            task_id,
            status='COMPLETED',
            result_json=_strict_result_json(TaskResult(ok=5)),
        )
        # A tampered evidence digest: consumption's integrity check
        # retains the row every pass, forever — the constructible
        # unresolvable shape the bound exists for.
        await session.execute(
            text("""
                UPDATE horsies_workflow_phase2_pending
                SET result_digest = decode(repeat('ab', 32), 'hex')
                WHERE task_id = CAST(:t AS uuid)
            """),
            {'t': task_id},
        )
        await session.commit()

        bound = 3
        summary = None
        for expected_attempts in (1, 2, 3):
            summary = await drive_phase2_recovery(
                broker.session_factory,
                broker,
                grace_ms=0,
                max_rows=GLOBAL_SCAN_ROW_CAP,
                quarantine_after_attempts=bound,
            )
            assert summary.retained == 1
            attempts = (
                await session.execute(
                    text(
                        'SELECT attempt_count, last_failure_class '
                        'FROM horsies_workflow_phase2_pending '
                        'WHERE task_id = CAST(:t AS uuid)'
                    ),
                    {'t': task_id},
                )
            ).one()
            assert attempts.attempt_count == expected_attempts
            assert attempts.last_failure_class is not None

        # The bound pass attempted the transition; the copy verification
        # re-failed on the same tampered digest and refused, verbatim —
        # the evidence stays where it is, named rather than relocated.
        assert summary is not None
        assert summary.quarantined == 0
        assert len(summary.quarantine_refusals) == 1
        assert 'COPY_VERIFICATION_FAILED' in summary.quarantine_refusals[0]
        pending = (
            await session.execute(
                text(
                    'SELECT recovery_source, attempt_count '
                    'FROM horsies_workflow_phase2_pending '
                    'WHERE task_id = CAST(:t AS uuid)'
                ),
                {'t': task_id},
            )
        ).one()
        assert pending.recovery_source == 'HISTORY'
        assert pending.attempt_count == bound

        # Discovery no longer selects the row; the standing population
        # is a count, not silence.
        after = await drive_phase2_recovery(
            broker.session_factory,
            broker,
            grace_ms=0,
            max_rows=GLOBAL_SCAN_ROW_CAP,
            quarantine_after_attempts=bound,
        )
        assert after.considered == 0
        assert after.over_attempt_bound == 1
