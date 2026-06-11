"""Equivalence tests for the v8 completion-path query rewrites.

Pins that the two-step terminal-set resolution (payload-free edge read +
_compute_terminal_indexes + fetch-by-indexes) and the single-pass form of
GET_CHILD_WORKFLOW_INFO_SQL return exactly what the correlated forms they
replaced returned, across randomized DAG shapes. Seeded RNG: deterministic
across runs.
"""

from __future__ import annotations

import random
import uuid
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.workflows.engine import _compute_terminal_indexes
from horsies.core.workflows.sql import (
    GET_CHILD_WORKFLOW_INFO_SQL,
    GET_TASK_RESULTS_BY_INDEXES_SQL,
    GET_WORKFLOW_DAG_EDGES_SQL,
)

# The exact forms replaced in the v8 rewrite, kept here as the oracle.
OLD_TERMINAL_SQL = text("""
    SELECT wt.node_id, wt.task_index, wt.task_name, wt.result,
           wt.sub_definition_key
    FROM horsies_workflow_tasks wt
    WHERE wt.workflow_id = :wf_id
      AND NOT EXISTS (
          SELECT 1 FROM horsies_workflow_tasks other
          WHERE other.workflow_id = wt.workflow_id
            AND other.dependencies @> ARRAY[wt.task_index]
      )
""")

OLD_CHILD_INFO_SQL = text("""
    SELECT w.status, w.result, w.error, w.parent_workflow_id, w.parent_task_index,
           (SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = w.id) as total,
           (SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = w.id AND status = 'COMPLETED') as completed,
           (SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = w.id AND status = 'FAILED') as failed,
           (SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = w.id AND status = 'SKIPPED') as skipped
    FROM horsies_workflows w
    WHERE w.id = :child_id
""")

STATUSES = ('PENDING', 'READY', 'ENQUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED')


async def _seed_random_workflow(
    session: AsyncSession,
    rng: random.Random,
    n_tasks: int,
) -> str:
    """Insert a workflow with a random acyclic DAG and random statuses."""
    wf_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES (:id, 'equiv_wf', 'RUNNING', 'FAIL', 0, :id,
                    NOW(), NOW(), NOW(), NOW())
        """),
        {'id': wf_id},
    )
    for i in range(n_tasks):
        # Acyclic by construction: deps drawn from earlier indexes only.
        max_deps = min(i, 4)
        n_deps = rng.randint(0, max_deps)
        deps = sorted(rng.sample(range(i), n_deps)) if n_deps else []
        deps_literal = '{' + ','.join(map(str, deps)) + '}'
        await session.execute(
            text("""
                INSERT INTO horsies_workflow_tasks
                    (id, workflow_id, task_index, node_id, task_name,
                     task_args, task_kwargs, queue_name, priority,
                     dependencies, allow_failed_deps, join_type,
                     is_subworkflow, status, created_at)
                VALUES (:id, :wf, :idx, :node, 'equiv_task', '[]', '{}',
                        'default', 100, :deps, FALSE, 'all',
                        FALSE, :status, NOW())
            """),
            {
                'id': str(uuid.uuid4()),
                'wf': wf_id,
                'idx': i,
                'node': f'n{i}',
                'deps': deps_literal,
                'status': rng.choice(STATUSES),
            },
        )
    await session.commit()
    return wf_id


async def _cleanup_workflow(session: AsyncSession, wf_id: str) -> None:
    await session.execute(
        text('DELETE FROM horsies_workflow_tasks WHERE workflow_id = :wf'),
        {'wf': wf_id},
    )
    await session.execute(
        text('DELETE FROM horsies_workflows WHERE id = :wf'), {'wf': wf_id},
    )
    await session.commit()


def _rows_set(rows: Any) -> set[tuple[Any, ...]]:
    return {tuple(r) for r in rows}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_terminal_two_step_matches_correlated_form(
    session: AsyncSession,
) -> None:
    """Property: identical terminal sets on 20 random DAGs (seeded)."""
    rng = random.Random(0xC0FFEE)
    for trial in range(20):
        n_tasks = rng.randint(1, 60)
        wf_id = await _seed_random_workflow(session, rng, n_tasks)
        try:
            old = await session.execute(OLD_TERMINAL_SQL, {'wf_id': wf_id})
            edges = await session.execute(
                GET_WORKFLOW_DAG_EDGES_SQL, {'wf_id': wf_id},
            )
            terminal_indexes = _compute_terminal_indexes(edges.fetchall())
            new = await session.execute(
                GET_TASK_RESULTS_BY_INDEXES_SQL,
                {'wf_id': wf_id, 'idxs': terminal_indexes},
            )
            old_set = _rows_set(old.fetchall())
            new_set = _rows_set(new.fetchall())
            assert new_set == old_set, (
                f'trial {trial}: terminal sets diverge '
                f'(only-old={old_set - new_set}, only-new={new_set - old_set})'
            )
            assert len(new_set) >= 1, 'a non-empty DAG always has a terminal node'
        finally:
            await _cleanup_workflow(session, wf_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_child_info_single_pass_matches_correlated_form(
    session: AsyncSession,
) -> None:
    """Property: identical info rows on 10 random status mixes (seeded)."""
    rng = random.Random(0xBEEF)
    for trial in range(10):
        n_tasks = rng.randint(1, 40)
        wf_id = await _seed_random_workflow(session, rng, n_tasks)
        try:
            old = (
                await session.execute(OLD_CHILD_INFO_SQL, {'child_id': wf_id})
            ).fetchone()
            new = (
                await session.execute(
                    GET_CHILD_WORKFLOW_INFO_SQL, {'child_id': wf_id},
                )
            ).fetchone()
            assert old is not None and new is not None
            assert tuple(new) == tuple(old), f'trial {trial}: rows diverge'
        finally:
            await _cleanup_workflow(session, wf_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_terminal_two_step_handles_zero_task_workflow(
    session: AsyncSession,
) -> None:
    """The ANY([]) driver edge: a task-less workflow (corrupt-state defense)
    yields an empty terminal set and the by-indexes fetch binds an empty
    list cleanly — no rows, no error."""
    wf_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES (:id, 'empty_wf', 'RUNNING', 'FAIL', 0, :id,
                    NOW(), NOW(), NOW(), NOW())
        """),
        {'id': wf_id},
    )
    await session.commit()
    try:
        edges = await session.execute(
            GET_WORKFLOW_DAG_EDGES_SQL, {'wf_id': wf_id},
        )
        terminal_indexes = _compute_terminal_indexes(edges.fetchall())
        assert terminal_indexes == []
        rows = await session.execute(
            GET_TASK_RESULTS_BY_INDEXES_SQL,
            {'wf_id': wf_id, 'idxs': terminal_indexes},
        )
        assert rows.fetchall() == []
    finally:
        await _cleanup_workflow(session, wf_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_child_info_counts_zero_for_taskless_workflow(
    session: AsyncSession,
) -> None:
    """The COUNT(wt.id) pin: a task-less workflow row (builder-unreachable,
    corrupt-state defense) must report total=0, not 1 from the LEFT JOIN's
    null row."""
    wf_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES (:id, 'taskless_wf', 'RUNNING', 'FAIL', 0, :id,
                    NOW(), NOW(), NOW(), NOW())
        """),
        {'id': wf_id},
    )
    await session.commit()
    try:
        row = (
            await session.execute(
                GET_CHILD_WORKFLOW_INFO_SQL, {'child_id': wf_id},
            )
        ).fetchone()
        assert row is not None
        assert row.total == 0
        assert row.completed == 0 and row.failed == 0 and row.skipped == 0
    finally:
        await _cleanup_workflow(session, wf_id)
