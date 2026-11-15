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

import psycopg
import psycopg.sql
import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import loads_json
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import OnError, TaskNode, WorkflowContext
from horsies.core.types.result import is_err
from horsies.core.workflows.lifecycle import start_workflow_async
from horsies.core.workflows.sql import PROMOTE_DEPENDENTS_BATCH_SQL

from tests.integration.conftest import DB_URL, complete_task

pytestmark = [pytest.mark.integration]


def _make_task(app: Horsies, prefix: str) -> Any:
    @app.task(task_name=f'{prefix}_{uuid.uuid4().hex[:8]}')
    def fn(*, value: int) -> TaskResult[int, TaskError]:
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
        engine: AsyncEngine,
        session: AsyncSession,
    ) -> None:
        """Regression for the O(7F) promotion: one completion promoting F
        dependents ran 7 statements per dependent under the workflow lock
        (F=119 ≈ 36.8s at 33-45ms RTT). The batched pipeline must issue
        the same flat count for any F.

        ``complete_task`` runs phase 2 on ``session``, so the statements are
        counted on its engine."""
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

            event.listen(engine.sync_engine, 'before_cursor_execute', _count)
            try:
                await complete_task(session, broker, wf_id, 0, TaskResult(ok=0))
            finally:
                event.remove(engine.sync_engine, 'before_cursor_execute', _count)
            await session.commit()
            counts_by_f[fan_out] = counter['n']

            statuses = await _statuses(session, wf_id)
            assert all(
                statuses[i] == 'ENQUEUED' for i in range(1, fan_out + 1)
            ), statuses

        # Flatness is the claim, not just smallness. complete_task's lookup
        # of the task, then phase 2: the node write, the dependents read,
        # the promotion write, the dependency results, the insert
        # executemany, the completion check.
        assert counts_by_f == {5: 7, 20: 7}, counts_by_f

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
            *, value: int, workflow_ctx: WorkflowContext | None = None,
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
        def corrupt_fn(*, value: int) -> TaskResult[int, TaskError]:
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

    async def test_pause_policy_build_failure_reverts_siblings_to_ready(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Under on_error=PAUSE, a payload-build failure pauses the
        workflow and the level's CAS'd-but-uninserted siblings revert to
        READY: a paused workflow gains NO new runnable task rows, the
        siblings are in the recoverable state (resume step 3 re-enqueues
        READY nodes), and nothing is stranded ENQUEUED without a task
        row. Stronger than the sequential path, whose post-pause state
        depended on processing order."""
        fn = _make_task(app, 'pause_corner')

        @app.task(task_name=f'pause_corrupt_{uuid.uuid4().hex[:8]}')
        def corrupt_fn(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        corrupt_fn.task_options_json = '{not-json'  # type: ignore[attr-defined]

        root = TaskNode(fn=fn, kwargs={'value': 0})
        good = TaskNode(fn=fn, kwargs={'value': 1}, waits_for=[root])
        bad = TaskNode(fn=corrupt_fn, kwargs={'value': 2}, waits_for=[root])
        spec = app.workflow(
            f'batched_pause_{uuid.uuid4().hex[:6]}', [root, good, bad],
            on_error=OnError.PAUSE,
            definition_key=f'batched-pause-{uuid.uuid4().hex[:6]}',
        )
        start_r = await start_workflow_async(spec, broker)
        assert not is_err(start_r), start_r
        wf_id = start_r.ok_value.workflow_id

        await complete_task(session, broker, wf_id, 0, TaskResult(ok=0))
        await session.commit()

        statuses = await _statuses(session, wf_id)
        assert statuses == {0: 'COMPLETED', 1: 'READY', 2: 'FAILED'}
        wf_status = (
            await session.execute(
                text('SELECT status FROM horsies_workflows WHERE id = :wf'),
                {'wf': wf_id},
            )
        ).scalar()
        assert wf_status == 'PAUSED'

        # No runnable task row was created under the paused workflow, and
        # the reverted sibling is not stranded (task_id NULL + READY is
        # the recovery-covered shape, with started_at cleared).
        good_row = (
            await session.execute(
                text("""
                    SELECT task_id, started_at FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf AND task_index = 1
                """),
                {'wf': wf_id},
            )
        ).one()
        assert good_row.task_id is None
        assert good_row.started_at is None

        # Resume re-enqueues the READY sibling through the normal path.
        from horsies.core.workflows.lifecycle import resume_workflow

        resume_r = await resume_workflow(broker, wf_id)
        assert not is_err(resume_r), resume_r
        statuses_after = await _statuses(session, wf_id)
        assert statuses_after[1] == 'ENQUEUED'
        good_task = (
            await session.execute(
                text("""
                    SELECT t.status FROM horsies_tasks t
                    JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
                    WHERE wt.workflow_id = :wf AND wt.task_index = 1
                """),
                {'wf': wf_id},
            )
        ).scalar()
        assert good_task == 'PENDING'  # claimable now that it resumed


async def _listen_all(channels: tuple[str, ...]) -> psycopg.AsyncConnection[Any]:
    connection = await psycopg.AsyncConnection.connect(
        DB_URL.replace('postgresql+psycopg://', 'postgresql://'),
        autocommit=True,
    )
    for channel in channels:
        await connection.execute(
            psycopg.sql.SQL('LISTEN {}').format(psycopg.sql.Identifier(channel)),
        )
    return connection


_NOTIFY_WAIT_S = 0.5
"""How long a test listens for notifications after the commit."""
_TASK_CHANNELS = ('task_new', 'task_queue_default', 'horsies_task_status')
_WORKFLOW_CHANNELS = ('workflow_done', 'horsies_workflow_status')


async def _node_rows(session: AsyncSession, wf_id: str) -> dict[int, Any]:
    rows = (
        await session.execute(
            text("""
                SELECT wt.task_index, wt.status, wt.task_id, wt.started_at,
                       wt.completed_at, t.enqueued_at, t.created_at,
                       t.updated_at, t.status AS task_status
                FROM horsies_workflow_tasks wt
                LEFT JOIN horsies_tasks t ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf
                ORDER BY wt.task_index
            """),
            {'wf': wf_id},
        )
    ).fetchall()
    return {row.task_index: row for row in rows}


@pytest.mark.asyncio(loop_scope='function')
class TestPromotionWrite:
    """One update moves a dependent from PENDING to ENQUEUED. A READY row
    written and overwritten in one transaction is seen by no other
    transaction, and no trigger is on horsies_workflow_tasks, so the
    committed rows, the timestamps and the NOTIFYs are those of the READY
    write followed by the ENQUEUED write."""

    async def test_promoted_dependents_carry_the_transaction_time_and_the_insert_notifies(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        root_fn = _make_task(app, 'notify_root')
        dep_fn = _make_task(app, 'notify_dep')
        root = TaskNode(fn=root_fn, kwargs={'value': 0})
        nodes: list[Any] = [
            root,
            *[
                TaskNode(fn=dep_fn, kwargs={'value': i}, waits_for=[root])
                for i in range(3)
            ],
        ]
        wf_id = await _start(app, broker, nodes, 'promotion_notify')
        listener = await _listen_all((*_TASK_CHANNELS, *_WORKFLOW_CHANNELS))
        try:
            await complete_task(session, broker, wf_id, 0, TaskResult(ok=0))
            received = [
                (notify.channel, notify.payload)
                async for notify in listener.notifies(timeout=_NOTIFY_WAIT_S)
            ]
        finally:
            await listener.close()

        rows = await _node_rows(session, wf_id)
        promoted_at = rows[0].completed_at
        assert promoted_at is not None
        expected: list[tuple[str, str]] = []
        for index in (1, 2, 3):
            row = rows[index]
            assert row.status == 'ENQUEUED'
            assert row.task_status == 'PENDING'
            # NOW() is the start of the transaction that completed the root.
            assert row.started_at == promoted_at
            assert row.enqueued_at == promoted_at
            assert row.created_at == promoted_at
            assert row.updated_at == promoted_at
            assert row.completed_at is None
            expected.extend((channel, str(row.task_id)) for channel in _TASK_CHANNELS)
        assert sorted(received) == sorted(expected)

    async def test_the_promotion_write_moves_only_pending_nodes(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """B and D are enqueued, C is skip-on-ready, D is no longer PENDING:
        B goes ENQUEUED at the transaction time, C goes READY with no
        timestamp, and D is neither written nor returned."""
        wf_id = await self._start_root_with_three_dependents(app, broker)
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks SET status = 'SKIPPED'
                WHERE workflow_id = :wf AND task_index = 3
            """),
            {'wf': wf_id},
        )

        returned = await self._promote(session, wf_id)
        now = (await session.execute(text('SELECT NOW()'))).scalar_one()
        rows = await _node_rows(session, wf_id)
        await session.rollback()

        assert {index: row.status for index, row in returned.items()} == {
            1: 'ENQUEUED',
            2: 'READY',
        }
        # The enqueued row carries the payload of its task insert.
        assert returned[1].task_name.startswith('promotion_write_')
        assert returned[1].queue_name == 'default'
        assert (rows[1].status, rows[1].started_at) == ('ENQUEUED', now)
        assert (rows[2].status, rows[2].started_at) == ('READY', None)
        assert (rows[3].status, rows[3].started_at) == ('SKIPPED', None)

    async def test_the_promotion_write_leaves_a_node_ready_when_the_workflow_is_not_running(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """A caller that does not hold the workflow row lock (the skip
        cascade of recovery) can see a pause land after its dependents
        read. The READY write had no workflow guard and the ENQUEUED write
        had one, so such a node ended READY; it still does."""
        wf_id = await self._start_root_with_three_dependents(app, broker)
        await session.execute(
            text("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = :wf"),
            {'wf': wf_id},
        )

        returned = await self._promote(session, wf_id)
        rows = await _node_rows(session, wf_id)
        await session.rollback()

        assert {index: row.status for index, row in returned.items()} == {
            1: 'READY',
            2: 'READY',
            3: 'READY',
        }
        for index in (1, 2, 3):
            assert (rows[index].status, rows[index].started_at) == ('READY', None)

    async def _start_root_with_three_dependents(
        self, app: Horsies, broker: PostgresBroker
    ) -> str:
        fn = _make_task(app, 'promotion_write')
        root = TaskNode(fn=fn, kwargs={'value': 0})
        nodes: list[Any] = [
            root,
            *[TaskNode(fn=fn, kwargs={'value': i}, waits_for=[root]) for i in range(3)],
        ]
        return await _start(app, broker, nodes, 'promotion_write')

    async def _promote(self, session: AsyncSession, wf_id: str) -> dict[int, Any]:
        """Nodes 1 and 3 to enqueue, node 2 skip-on-ready."""
        rows = (
            await session.execute(
                PROMOTE_DEPENDENTS_BATCH_SQL,
                {'wf_id': wf_id, 'idxs': [1, 2, 3], 'enqueue_idxs': [1, 3]},
            )
        ).fetchall()
        return {row.task_index: row for row in rows}


@pytest.mark.asyncio(loop_scope='function')
class TestMultiLevelDag:
    async def test_fan_out_and_fan_in_across_levels_complete_the_workflow(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """A -> {B, C, D}; E waits for B and C and takes B's result; F waits
        for D and E; F -> {G, H}; I waits for G and H.

        Each phase 2 enqueues exactly the nodes whose dependencies all
        completed, at the time of its own transaction, each with one task
        row; the last one completes the workflow."""
        fn = _make_task(app, 'levels')

        @app.task(task_name=f'levels_receiver_{uuid.uuid4().hex[:8]}')
        def receiver(
            *, upstream: TaskResult[int, TaskError]
        ) -> TaskResult[int, TaskError]:
            return upstream

        a = TaskNode(fn=fn, kwargs={'value': 0})
        b = TaskNode(fn=fn, kwargs={'value': 1}, waits_for=[a])
        c = TaskNode(fn=fn, kwargs={'value': 2}, waits_for=[a])
        d = TaskNode(fn=fn, kwargs={'value': 3}, waits_for=[a])
        e = TaskNode(fn=receiver, waits_for=[b, c], args_from={'upstream': b})
        f = TaskNode(fn=fn, kwargs={'value': 5}, waits_for=[d, e])
        g = TaskNode(fn=fn, kwargs={'value': 6}, waits_for=[f])
        h = TaskNode(fn=fn, kwargs={'value': 7}, waits_for=[f])
        i = TaskNode(fn=fn, kwargs={'value': 8}, waits_for=[g, h])
        wf_id = await _start(app, broker, [a, b, c, d, e, f, g, h, i], 'levels')

        # (completed node, nodes its phase 2 enqueues)
        steps: list[tuple[int, set[int]]] = [
            (0, {1, 2, 3}),
            (1, set()),
            (2, {4}),
            (3, set()),
            (4, {5}),
            (5, {6, 7}),
            (6, set()),
            (7, {8}),
            (8, set()),
        ]
        enqueued: set[int] = {0}
        for completed, promoted in steps:
            before = await _node_rows(session, wf_id)
            await complete_task(
                session, broker, wf_id, completed, TaskResult(ok=completed * 10)
            )
            after = await _node_rows(session, wf_id)
            changed = {
                index
                for index, row in after.items()
                if row.status == 'ENQUEUED' and before[index].status != 'ENQUEUED'
            }
            assert changed == promoted, (completed, changed)
            for index in promoted:
                assert after[index].started_at == after[completed].completed_at
                assert after[index].task_status == 'PENDING'
            enqueued |= promoted
            pending = {index for index, row in after.items() if row.status == 'PENDING'}
            assert pending == set(range(9)) - enqueued, (completed, pending)

        rows = await _node_rows(session, wf_id)
        assert {row.status for row in rows.values()} == {'COMPLETED'}
        assert len({row.task_id for row in rows.values()}) == 9
        task_rows = (
            await session.execute(
                text("""
                    SELECT count(*) FROM horsies_tasks t
                    JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
                    WHERE wt.workflow_id = :wf
                """),
                {'wf': wf_id},
            )
        ).scalar_one()
        assert task_rows == 9
        kwargs_of_e = (
            await session.execute(
                text("""
                    SELECT t.kwargs FROM horsies_tasks t
                    JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
                    WHERE wt.workflow_id = :wf AND wt.task_index = 4
                """),
                {'wf': wf_id},
            )
        ).scalar_one()
        match loads_json(kwargs_of_e).unwrap():
            case {'upstream': {'inner': {'ok': upstream_ok}}}:
                assert upstream_ok == 10  # B's result
            case other:
                pytest.fail(f'the kwargs of E carry no result of B: {other!r}')
        wf_status = (
            await session.execute(
                text('SELECT status FROM horsies_workflows WHERE id = :wf'),
                {'wf': wf_id},
            )
        ).scalar_one()
        assert wf_status == 'COMPLETED'
