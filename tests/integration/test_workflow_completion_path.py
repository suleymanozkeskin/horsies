"""Integration tests: completion-path statement budget and equivalence.

Pins the merged locate+lock+CAS completion shape (COMPLETE_WORKFLOW_TASK_SQL)
and its behavioral equivalence with the old per-statement path: per-completion
statement count stays flat, the terminal CAS stays idempotent, on_error
fail/pause semantics survive, and a paused workflow still stores the result
without propagating.
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

from tests.integration.conftest import complete_task

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


@pytest.mark.asyncio(loop_scope='function')
class TestCompletionStatementBudget:
    """Round-trip pins for the merged completion statement."""

    async def test_success_completion_statement_count(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Regression for the per-statement completion path: success with
        one still-pending dependent was 10 statements (each one RTT under
        the workflow lock); the merged path is 5."""
        spec = _chain_spec(app)
        start_r = await start_workflow_async(spec, broker)
        assert not is_err(start_r), start_r
        wf_id = start_r.ok_value.workflow_id

        counts = {'n': 0}

        def _count(conn: Any, cursor: Any, stmt: Any, params: Any, ctx: Any, many: Any) -> None:
            counts['n'] += 1

        event.listen(broker.async_engine.sync_engine, 'before_cursor_execute', _count)
        try:
            # A has no still-pending dependent gate here: B waits only on A,
            # so this completion PROMOTES B (the 7-statement promote branch).
            await complete_task(session, broker, wf_id, 0, TaskResult(ok=1))
            promote_stmts = counts['n']
            counts['n'] = 0
            # B's completion has zero dependents and finalizes the workflow.
            await complete_task(session, broker, wf_id, 1, TaskResult(ok=2))
            finalize_stmts = counts['n']
        finally:
            event.remove(broker.async_engine.sync_engine, 'before_cursor_execute', _count)
            await session.commit()

        # complete_task adds 2 lookups of its own (task_id + task_name).
        # Old shape: promote ≈ 15+2, finalize ≈ 8+2 + terminal resolution.
        assert promote_stmts <= 13, f'promotion completion ran {promote_stmts} statements'
        assert finalize_stmts <= 13, f'finalizing completion ran {finalize_stmts} statements'

    async def test_failed_completion_statement_count(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """on_error=FAIL failure path: was 14 statements (third lock +
        on_error read on top of the success 10); merged path drops both."""
        spec = _chain_spec(app)
        start_r = await start_workflow_async(spec, broker)
        assert not is_err(start_r), start_r
        wf_id = start_r.ok_value.workflow_id

        counts = {'n': 0}

        def _count(conn: Any, cursor: Any, stmt: Any, params: Any, ctx: Any, many: Any) -> None:
            counts['n'] += 1

        failed: TaskResult[int, TaskError] = TaskResult(
            err=TaskError(error_code='BENCH_FAIL', message='forced'),
        )
        event.listen(broker.async_engine.sync_engine, 'before_cursor_execute', _count)
        try:
            await complete_task(session, broker, wf_id, 0, failed)
            stmts = counts['n']
        finally:
            event.remove(broker.async_engine.sync_engine, 'before_cursor_execute', _count)
            await session.commit()

        # Includes the failure handler's two writes plus B's skip cascade
        # and the final completion resolution (B is SKIPPED -> workflow
        # FAILED in the same call). The old path was 7 statements heavier.
        assert stmts <= 18, f'failed completion ran {stmts} statements'


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
        wf_id = start_r.ok_value.workflow_id

        await complete_task(session, broker, wf_id, 0, TaskResult(ok=1))
        await session.commit()
        first = await _node_row(session, wf_id, 0)
        assert first.status == 'COMPLETED'

        # Second completion with a different payload must not win the CAS.
        await complete_task(session, broker, wf_id, 0, TaskResult(ok=999))
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
        wf_id = start_r.ok_value.workflow_id

        failed: TaskResult[int, TaskError] = TaskResult(
            err=TaskError(error_code='BENCH_FAIL', message='forced'),
        )
        await complete_task(session, broker, wf_id, 0, failed)
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
        wf_id = start_r.ok_value.workflow_id

        failed: TaskResult[int, TaskError] = TaskResult(
            err=TaskError(error_code='BENCH_FAIL', message='forced'),
        )
        await complete_task(session, broker, wf_id, 0, failed)
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
        wf_id = start_r.ok_value.workflow_id

        await session.execute(
            text("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = :wf"),
            {'wf': wf_id},
        )
        await session.commit()

        await complete_task(session, broker, wf_id, 0, TaskResult(ok=1))
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
        from horsies.core.workflows.engine import on_workflow_task_complete

        await on_workflow_task_complete(
            session, str(uuid.uuid4()), TaskResult(ok=1), broker,
            task_name='completion_unknown',
        )
        await session.rollback()  # nothing to commit; must not raise
