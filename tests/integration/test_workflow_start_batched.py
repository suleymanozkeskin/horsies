"""Integration tests: batched workflow start (bulk node + root-task inserts).

Pins the flat-round-trip start shape and its behavioral equivalence with
the old per-row path: fast roots land ENQUEUED + linked + claimable,
dependents stay PENDING, statement count stays flat in N, and the
idempotent-restart and whole-transaction-rollback contracts survive.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import json
import uuid
from typing import Any

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode
from horsies.core.types.result import is_err
from horsies.core.workflows.lifecycle import start_workflow_async

pytestmark = [pytest.mark.integration]


def _fanout_spec(app: Horsies, n_roots: int, *, with_dependent: bool = False) -> Any:
    """N independent roots (the run_all_scrapers shape), optionally plus
    one node depending on root 0."""

    @app.task(task_name=f'batched_root_{uuid.uuid4().hex[:8]}')
    def root_task(value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    nodes: list[Any] = [
        TaskNode(fn=root_task, kwargs={'value': i}) for i in range(n_roots)
    ]
    if with_dependent:
        nodes.append(TaskNode(fn=root_task, kwargs={'value': -1}, waits_for=[nodes[0]]))
    return app.workflow(
        f'batched_start_{uuid.uuid4().hex[:8]}', nodes,
        definition_key=f'batched-start-{uuid.uuid4().hex[:8]}',
    )


@pytest.mark.asyncio(loop_scope='function')
class TestBatchedStartShape:
    """Round-trip count and row shapes."""

    async def test_statement_count_flat_in_root_count(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        engine: AsyncEngine,
    ) -> None:
        """Regression for the O(4N) start: 119 roots took ~477 sequential
        statements (~16s at 33ms RTT). The batched path must issue a flat,
        small statement count regardless of N."""
        counts: dict[str, int] = {'n': 0}

        def _count(conn: Any, cursor: Any, stmt: Any, params: Any, ctx: Any, many: Any) -> None:
            counts['n'] += 1

        spec = _fanout_spec(app, 50)
        event.listen(broker.async_engine.sync_engine, 'before_cursor_execute', _count)
        try:
            result = await start_workflow_async(spec, broker)
        finally:
            event.remove(broker.async_engine.sync_engine, 'before_cursor_execute', _count)
        assert not is_err(result), result
        # workflow insert + bulk node insert + bulk task insert + overhead.
        # The old shape was ~201 statements for N=50 (1 + 50 + 3*50).
        assert counts['n'] <= 10, f'start issued {counts["n"]} statements for 50 roots'

    async def test_fast_roots_enqueued_linked_and_dependent_pending(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Row-shape equivalence with the old path."""
        spec = _fanout_spec(app, 3, with_dependent=True)
        result = await start_workflow_async(spec, broker)
        assert not is_err(result), result
        wf_id = result.ok_value.workflow_id

        rows = (
            await session.execute(
                text("""
                    SELECT wt.task_index, wt.status, wt.task_id, wt.started_at,
                           t.id AS real_task_id, t.status AS task_status,
                           t.kwargs AS task_kwargs, t.enqueue_sha, t.max_retries
                    FROM horsies_workflow_tasks wt
                    LEFT JOIN horsies_tasks t ON t.id = wt.task_id
                    WHERE wt.workflow_id = :wf
                    ORDER BY wt.task_index
                """),
                {'wf': wf_id},
            )
        ).mappings().all()
        assert len(rows) == 4

        for row in rows[:3]:  # fast roots
            assert row['status'] == 'ENQUEUED'
            assert row['task_id'] is not None
            assert row['started_at'] is not None
            assert row['real_task_id'] == row['task_id']  # linked
            assert row['task_status'] == 'PENDING'  # claimable
            assert row['enqueue_sha']
            kwargs = json.loads(row['task_kwargs'])
            meta = kwargs['__h_workflow_meta__']
            assert meta['workflow_id'] == wf_id
            assert meta['task_index'] == row['task_index']

        dependent = rows[3]
        assert dependent['status'] == 'PENDING'
        assert dependent['task_id'] is None

    async def test_idempotent_restart_returns_existing_handle(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        spec = _fanout_spec(app, 3)
        wf_id = str(uuid.uuid4())
        first = await start_workflow_async(spec, broker, workflow_id=wf_id)
        assert not is_err(first)
        second = await start_workflow_async(spec, broker, workflow_id=wf_id)
        assert not is_err(second)
        assert second.ok_value.workflow_id == wf_id

        n_nodes = (
            await session.execute(
                text('SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = :wf'),
                {'wf': wf_id},
            )
        ).scalar()
        n_tasks = (
            await session.execute(
                text("""
                    SELECT COUNT(*) FROM horsies_tasks t
                    JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
                    WHERE wt.workflow_id = :wf
                """),
                {'wf': wf_id},
            )
        ).scalar()
        assert n_nodes == 3  # no duplicates from the second start
        assert n_tasks == 3

    async def test_serialization_failure_rolls_back_everything(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A node whose kwargs fail to encode mid-build fails the whole
        start (VALIDATION_FAILED) with no partial rows — params are built
        before any INSERT executes, so the transaction has nothing to
        keep. (User input cannot reach this state — spec construction
        already validates — so the encoder is forced to fail.)"""
        from horsies.core.codec.json_io import SerializationError
        from horsies.core.workflows import lifecycle as lifecycle_mod

        spec = _fanout_spec(app, 2)
        real_encode = lifecycle_mod._encode_node_kwargs_or_raise
        calls = {'n': 0}

        def _flaky_encode(task: Any) -> Any:
            calls['n'] += 1
            if calls['n'] == 2:
                raise SerializationError('forced encode failure (node 2)')
            return real_encode(task)

        monkeypatch.setattr(
            lifecycle_mod, '_encode_node_kwargs_or_raise', _flaky_encode,
        )

        result = await start_workflow_async(spec, broker)
        assert is_err(result)
        assert calls['n'] == 2

        leftover = (
            await session.execute(
                text("SELECT COUNT(*) FROM horsies_workflows WHERE name LIKE 'batched_start_%'"),
            )
        ).scalar()
        assert leftover == 0
