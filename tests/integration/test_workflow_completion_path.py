"""Integration tests: completion-path statement budget and equivalence.

Pins the phase 2 of a workflow task (``horsies_workflow_node_finish``) and
its behaviour: phase 2 of a node with no sub-workflow node below it is one
statement, a second phase 2 of a node writes nothing, on_error fail/pause
semantics hold, and a paused workflow still stores the result without
propagating.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import uuid
from typing import Any

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import OnError, TaskNode
from horsies.core.types.result import is_err
from horsies.core.workflows.lifecycle import start_workflow_async
from horsies.core.workflows.node_finish import CapacityWake, FactsUnknown, finish_node
from horsies.core.workflows.steps import NodeFinishDisposition

from tests.integration.conftest import finish_phase_one, finish_phase_two, finish_task
from tests.broker_resources_support import current_resources

pytestmark = [pytest.mark.integration]


def _chain_spec(
    app: Horsies, *, on_error: OnError = OnError.FAIL,
) -> Any:
    """A -> B: completion of A exercises one still-pending-dependent check."""

    @app.task(task_name=f'completion_root_{uuid.uuid4().hex[:8]}')
    def root_task(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task(task_name=f'completion_dep_{uuid.uuid4().hex[:8]}')
    def dep_task(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    node_a = TaskNode(fn=root_task, kwargs={'value': 1})
    node_b = TaskNode(fn=dep_task, kwargs={'value': 2}, waits_for=[node_a])
    return app.workflow(
        f'completion_path_{uuid.uuid4().hex[:8]}', [node_a, node_b],
        on_error=on_error,
        definition_key=f'completion-path-{uuid.uuid4().hex[:8]}',
    )


async def _node_row(
    session: AsyncSession, workflow_id: str, task_index: int,
) -> Any:
    return (
        await session.execute(
            text("""
                SELECT status, result FROM horsies_workflow_tasks
                WHERE workflow_id = :wf AND task_index = :idx
            """),
            {'wf': workflow_id, 'idx': task_index},
        )
    ).one()


async def _workflow_row(session: AsyncSession, workflow_id: str) -> Any:
    return (
        await session.execute(
            text('SELECT status, error FROM horsies_workflows WHERE id = :wf'),
            {'wf': workflow_id},
        )
    ).one()


async def _phase_two_statements(
    session: AsyncSession,
    broker: PostgresBroker,
    workflow_id: str,
    task_index: int,
    result: TaskResult[Any, TaskError],
) -> int:
    """Phase 1 of the node's task, then phase 2; the statements that phase 2
    sent on the broker's engine."""
    done = await finish_phase_one(session, broker, workflow_id, task_index, result)
    counts = {'n': 0}

    def _count(conn: Any, cursor: Any, stmt: Any, params: Any, ctx: Any, many: Any) -> None:
        counts['n'] += 1

    sync_engine = current_resources(broker).engine.sync_engine
    event.listen(sync_engine, 'before_cursor_execute', _count)
    try:
        await finish_phase_two(broker, done)
    finally:
        event.remove(sync_engine, 'before_cursor_execute', _count)
    return counts['n']


@pytest.mark.asyncio(loop_scope='function')
class TestCompletionStatementBudget:
    """Statement pins for phase 2 of a node with no sub-workflow node below it."""

    async def test_success_completion_statement_count(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Regression for the per-statement completion path: success with
        one still-pending dependent was 10 statements (each one RTT under
        the workflow lock), then 5 with the merged node write; phase 2 is
        one statement now, with dependents and without."""
        spec = _chain_spec(app)
        start_r = await start_workflow_async(spec, broker)
        assert not is_err(start_r), start_r
        wf_id = str(start_r.ok_value.workflow_id)

        # B waits only on A, so phase 2 of A promotes B.
        promote_stmts = await _phase_two_statements(session, broker, wf_id, 0, TaskResult(ok=1))
        # B has no dependents; its phase 2 completes the workflow.
        finalize_stmts = await _phase_two_statements(session, broker, wf_id, 1, TaskResult(ok=2))
        await session.commit()

        assert promote_stmts == 1, f'promotion completion ran {promote_stmts} statements'
        assert finalize_stmts == 1, f'finalizing completion ran {finalize_stmts} statements'

    async def test_failed_completion_statement_count(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """on_error=FAIL failure path: was 14 statements (third lock +
        on_error read on top of the success 10), then up to 18 with B's skip
        and the completion; phase 2 is one statement now."""
        spec = _chain_spec(app)
        start_r = await start_workflow_async(spec, broker)
        assert not is_err(start_r), start_r
        wf_id = str(start_r.ok_value.workflow_id)

        failed: TaskResult[int, TaskError] = TaskResult(
            err=TaskError(error_code='BENCH_FAIL', message='forced'),
        )
        # The failure policy, B's skip and the completion (B is SKIPPED, so
        # the workflow is FAILED) run inside the one call.
        stmts = await _phase_two_statements(session, broker, wf_id, 0, failed)
        await session.commit()

        assert stmts == 1, f'failed completion ran {stmts} statements'


@pytest.mark.asyncio(loop_scope='function')
class TestCompletionEquivalence:
    """Behavioral equivalence with the pre-merge path."""

    async def test_terminal_cas_is_idempotent(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        spec = _chain_spec(app)
        start_r = await start_workflow_async(spec, broker)
        assert not is_err(start_r), start_r
        wf_id = str(start_r.ok_value.workflow_id)

        await finish_task(session, broker, wf_id, 0, TaskResult(ok=1))
        await session.commit()
        first = await _node_row(session, wf_id, 0)
        assert first.status == 'COMPLETED'

        # A second phase 2 of the node (its task is in history now, so it
        # replays in the transaction form) writes nothing.
        await finish_task(session, broker, wf_id, 0, TaskResult(ok=999))
        await session.commit()
        second = await _node_row(session, wf_id, 0)
        assert second.status == 'COMPLETED'
        assert second.result == first.result  # payload unchanged

    async def test_on_error_fail_records_error_and_resolves_dag(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        spec = _chain_spec(app, on_error=OnError.FAIL)
        start_r = await start_workflow_async(spec, broker)
        assert not is_err(start_r), start_r
        wf_id = str(start_r.ok_value.workflow_id)

        failed: TaskResult[int, TaskError] = TaskResult(
            err=TaskError(error_code='BENCH_FAIL', message='forced'),
        )
        await finish_task(session, broker, wf_id, 0, failed)
        await session.commit()

        wf = await _workflow_row(session, wf_id)
        dep = await _node_row(session, wf_id, 1)
        assert wf.error is not None  # failure recorded by the handler
        assert dep.status == 'SKIPPED'  # propagation continued
        assert wf.status == 'FAILED'  # DAG fully resolved in the same call

    async def test_on_error_pause_stops_propagation(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        spec = _chain_spec(app, on_error=OnError.PAUSE)
        start_r = await start_workflow_async(spec, broker)
        assert not is_err(start_r), start_r
        wf_id = str(start_r.ok_value.workflow_id)

        failed: TaskResult[int, TaskError] = TaskResult(
            err=TaskError(error_code='BENCH_FAIL', message='forced'),
        )
        await finish_task(session, broker, wf_id, 0, failed)
        await session.commit()

        wf = await _workflow_row(session, wf_id)
        dep = await _node_row(session, wf_id, 1)
        assert wf.status == 'PAUSED'
        assert dep.status == 'PENDING'  # untouched, resumable

    async def test_paused_workflow_stores_result_without_promoting(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """The CAS write lands under PAUSED (frozen by the held lock), but
        the dependent stays PENDING for resume."""
        spec = _chain_spec(app)
        start_r = await start_workflow_async(spec, broker)
        assert not is_err(start_r), start_r
        wf_id = str(start_r.ok_value.workflow_id)

        await session.execute(
            text("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = :wf"),
            {'wf': wf_id},
        )
        await session.commit()

        await finish_task(session, broker, wf_id, 0, TaskResult(ok=1))
        await session.commit()

        root = await _node_row(session, wf_id, 0)
        dep = await _node_row(session, wf_id, 1)
        assert root.status == 'COMPLETED'
        assert root.result is not None
        assert dep.status == 'PENDING'

    async def test_non_workflow_task_is_noop(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """A task that no node links and that has no phase-2 record: the
        step writes nothing and names no workflow."""
        committed = await finish_node(
            current_resources(broker).session_factory,
            broker,
            str(uuid.uuid4()),
            FactsUnknown(),
            CapacityWake.SEND,
        )

        assert committed.outcome.disposition is NodeFinishDisposition.PENDING_ABSENT
        assert committed.outcome.workflow_status is None
        assert committed.parent_advances == ()
