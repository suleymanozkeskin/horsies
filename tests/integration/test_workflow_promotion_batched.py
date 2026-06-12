"""Integration tests: batched dependent promotion (per-level pipeline).

Pins the flat-statement fan-out shape and the batching correctness
constraints: statement count flat in fan-out F, skip cascades resolve
across levels, mixed fast/slow dependents both promote, join modes
evaluate identically, and a payload-build failure fails only its node.
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
from horsies.core.models.workflow import TaskNode, WorkflowContext
from horsies.core.types.result import is_err
from horsies.core.workflows.lifecycle import start_workflow_async

from tests.integration.conftest import complete_task

pytestmark = [pytest.mark.integration]


def _make_task(app: Horsies, prefix: str) -> Any:
    @app.task(task_name=f'{prefix}_{uuid.uuid4().hex[:8]}')
    def fn(value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    return fn


async def _statuses(session: AsyncSession, wf_id: str) -> dict[int, str]:
    rows = (
        await session.execute(
            text("""
                SELECT task_index, status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf ORDER BY task_index
            """),
            {'wf': wf_id},
        )
    ).fetchall()
    return {r.task_index: r.status for r in rows}


async def _start(app: Horsies, broker: PostgresBroker, nodes: list[Any], name: str) -> str:
    spec = app.workflow(
        f'{name}_{uuid.uuid4().hex[:6]}', nodes,
        definition_key=f'{name}-{uuid.uuid4().hex[:6]}',
    )
    r = await start_workflow_async(spec, broker)
    assert not is_err(r), r
    return r.ok_value.workflow_id


@pytest.mark.asyncio(loop_scope='function')
class TestBatchedPromotionShape:
    async def test_fanout_statement_count_flat_in_f(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Regression for the O(7F) promotion: one completion promoting F
        dependents ran 7 statements per dependent under the workflow lock
        (F=119 ≈ 36.8s at 33-45ms RTT). The batched pipeline must issue
        the same flat count for any F."""
        root_fn = _make_task(app, 'fanout_root')
        dep_fn = _make_task(app, 'fanout_dep')

        counts_by_f: dict[int, int] = {}
        for fan_out in (5, 20):
            root = TaskNode(fn=root_fn, kwargs={'value': 0})
            nodes: list[Any] = [
                root,
                *[
                    TaskNode(fn=dep_fn, kwargs={'value': i}, waits_for=[root])
                    for i in range(fan_out)
                ],
            ]
            wf_id = await _start(app, broker, nodes, 'batched_fanout')

            counter = {'n': 0}

            def _count(conn: Any, cursor: Any, stmt: Any, params: Any, ctx: Any, many: Any) -> None:
                counter['n'] += 1

            event.listen(broker.async_engine.sync_engine, 'before_cursor_execute', _count)
            try:
                await complete_task(session, broker, wf_id, 0, TaskResult(ok=0))
            finally:
                event.remove(broker.async_engine.sync_engine, 'before_cursor_execute', _count)
            await session.commit()
            counts_by_f[fan_out] = counter['n']

            statuses = await _statuses(session, wf_id)
            assert all(
                statuses[i] == 'ENQUEUED' for i in range(1, fan_out + 1)
            ), statuses

        # Flatness is the claim, not just smallness.
        assert counts_by_f[5] == counts_by_f[20], counts_by_f
        # 8 engine statements + complete_task's 2 helper lookups.
        assert counts_by_f[20] <= 12, counts_by_f

    async def test_promoted_dependents_are_claimable_and_linked(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Row-shape equivalence with the sequential path."""
        root_fn = _make_task(app, 'shape_root')
        dep_fn = _make_task(app, 'shape_dep')
        root = TaskNode(fn=root_fn, kwargs={'value': 0})
        nodes: list[Any] = [
            root,
            *[
                TaskNode(fn=dep_fn, kwargs={'value': i}, waits_for=[root])
                for i in range(3)
            ],
        ]
        wf_id = await _start(app, broker, nodes, 'batched_shape')
        await complete_task(session, broker, wf_id, 0, TaskResult(ok=0))
        await session.commit()

        rows = (
            await session.execute(
                text("""
                    SELECT wt.task_index, wt.status, wt.started_at,
                           t.id AS task_id, t.status AS task_status,
                           t.enqueue_sha, t.kwargs
                    FROM horsies_workflow_tasks wt
                    JOIN horsies_tasks t ON t.id = wt.task_id
                    WHERE wt.workflow_id = :wf AND wt.task_index > 0
                    ORDER BY wt.task_index
                """),
                {'wf': wf_id},
            )
        ).fetchall()
        assert len(rows) == 3
        for row in rows:
            assert row.status == 'ENQUEUED'
            assert row.started_at is not None
            assert row.task_status == 'PENDING'  # claimable
            assert row.enqueue_sha
            assert '__h_workflow_meta__' in row.kwargs


@pytest.mark.asyncio(loop_scope='function')
class TestBatchedPromotionSemantics:
    async def test_skip_cascade_resolves_across_levels(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """A(fails) -> B -> C -> D: each level's skip feeds the next."""
        fn = _make_task(app, 'cascade')
        node_a = TaskNode(fn=fn, kwargs={'value': 0})
        node_b = TaskNode(fn=fn, kwargs={'value': 1}, waits_for=[node_a])
        node_c = TaskNode(fn=fn, kwargs={'value': 2}, waits_for=[node_b])
        node_d = TaskNode(fn=fn, kwargs={'value': 3}, waits_for=[node_c])
        wf_id = await _start(app, broker, [node_a, node_b, node_c, node_d], 'batched_cascade')

        failed: TaskResult[int, TaskError] = TaskResult(
            err=TaskError(error_code='CASCADE_FAIL', message='forced'),
        )
        await complete_task(session, broker, wf_id, 0, failed)
        await session.commit()

        statuses = await _statuses(session, wf_id)
        assert statuses == {0: 'FAILED', 1: 'SKIPPED', 2: 'SKIPPED', 3: 'SKIPPED'}
        wf_status = (
            await session.execute(
                text('SELECT status FROM horsies_workflows WHERE id = :wf'),
                {'wf': wf_id},
            )
        ).scalar()
        assert wf_status == 'FAILED'  # DAG fully resolved in one call

    async def test_any_join_promotes_on_first_success(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        fn = _make_task(app, 'anyjoin')
        node_a = TaskNode(fn=fn, kwargs={'value': 0})
        node_b = TaskNode(fn=fn, kwargs={'value': 1})
        node_c = TaskNode(fn=fn, kwargs={'value': 2}, waits_for=[node_a, node_b], join='any')
        wf_id = await _start(app, broker, [node_a, node_b, node_c], 'batched_any')

        await complete_task(session, broker, wf_id, 0, TaskResult(ok=0))
        await session.commit()
        statuses = await _statuses(session, wf_id)
        assert statuses[2] == 'ENQUEUED'  # one success suffices

    async def test_quorum_impossible_skips(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        fn = _make_task(app, 'quorum')
        node_a = TaskNode(fn=fn, kwargs={'value': 0})
        node_b = TaskNode(fn=fn, kwargs={'value': 1})
        node_c = TaskNode(
            fn=fn, kwargs={'value': 2},
            waits_for=[node_a, node_b], join='quorum', min_success=2,
        )
        wf_id = await _start(app, broker, [node_a, node_b, node_c], 'batched_quorum')

        failed: TaskResult[int, TaskError] = TaskResult(
            err=TaskError(error_code='Q_FAIL', message='forced'),
        )
        await complete_task(session, broker, wf_id, 0, failed)
        await session.commit()
        statuses = await _statuses(session, wf_id)
        # 1 of 2 deps failed; threshold 2 unreachable -> SKIPPED.
        assert statuses[2] == 'SKIPPED'

    async def test_allow_failed_deps_promotes_despite_failure(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        fn = _make_task(app, 'allowfail')
        node_a = TaskNode(fn=fn, kwargs={'value': 0})
        node_b = TaskNode(
            fn=fn, kwargs={'value': 1}, waits_for=[node_a], allow_failed_deps=True,
        )
        wf_id = await _start(app, broker, [node_a, node_b], 'batched_allowfail')

        failed: TaskResult[int, TaskError] = TaskResult(
            err=TaskError(error_code='AF_FAIL', message='forced'),
        )
        await complete_task(session, broker, wf_id, 0, failed)
        await session.commit()
        statuses = await _statuses(session, wf_id)
        assert statuses[1] == 'ENQUEUED'

    async def test_mixed_fast_and_ctx_dependents_both_promote(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """A ctx_from dependent takes the per-node slow path in the same
        level as a fast TaskNode dependent; both must land ENQUEUED."""

        @app.task(task_name=f'mixed_ctx_{uuid.uuid4().hex[:8]}')
        def ctx_fn(
            value: int, workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        fn = _make_task(app, 'mixed')
        root = TaskNode(fn=fn, kwargs={'value': 0})
        fast = TaskNode(fn=fn, kwargs={'value': 1}, waits_for=[root])
        slow = TaskNode(
            fn=ctx_fn, kwargs={'value': 2},
            waits_for=[root], workflow_ctx_from=[root],
        )
        wf_id = await _start(app, broker, [root, fast, slow], 'batched_mixed')

        await complete_task(session, broker, wf_id, 0, TaskResult(ok=0))
        await session.commit()
        statuses = await _statuses(session, wf_id)
        assert statuses[1] == 'ENQUEUED'
        assert statuses[2] == 'ENQUEUED'

    async def test_build_failure_fails_only_its_node(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Per-node failure isolation under the bulk INSERT: a dependent
        with corrupt decorator-attached task_options fails alone; its
        sibling still enqueues."""
        fn = _make_task(app, 'isolation')

        @app.task(task_name=f'isolation_corrupt_{uuid.uuid4().hex[:8]}')
        def corrupt_fn(value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        corrupt_fn.task_options_json = '{not-json'  # type: ignore[attr-defined]

        root = TaskNode(fn=fn, kwargs={'value': 0})
        good = TaskNode(fn=fn, kwargs={'value': 1}, waits_for=[root])
        bad = TaskNode(fn=corrupt_fn, kwargs={'value': 2}, waits_for=[root])
        wf_id = await _start(app, broker, [root, good, bad], 'batched_isolation')

        await complete_task(session, broker, wf_id, 0, TaskResult(ok=0))
        await session.commit()

        statuses = await _statuses(session, wf_id)
        assert statuses[1] == 'ENQUEUED'
        assert statuses[2] == 'FAILED'
        # The failed node carries the serialization error, not a task row.
        row = (
            await session.execute(
                text("""
                    SELECT task_id, result FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf AND task_index = 2
                """),
                {'wf': wf_id},
            )
        ).one()
        assert row.task_id is None
        assert 'task_options' in str(row.result)
