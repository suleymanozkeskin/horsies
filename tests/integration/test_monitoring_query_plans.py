"""Plan tests for the monitoring read path (schema v16).

Each test EXPLAINs the statement production executes — compiled from the
same builders ``list_tasks`` / ``task_facets`` use, or the worker's claim
SQL verbatim — and asserts the index the plan must ride. Any drift that
falls off the index (an ordering wrapper the btree cannot satisfy, a
projection change, a claim predicate rewrite) fails here, not on an
adopter's dashboard.

Seeded tables fit in a few pages, where the planner prefers a seq scan and
would hide the index choice a production-sized heap produces on its own;
``enable_seqscan = off`` forces the choice the way the retention plan tests
do. ANALYZE is committed before the EXPLAINs so statements plan on fresh
statistics.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.worker.sql import CLAIM_SQL
from horsies.monitoring.queries import (
    facet_statement,
    list_rows_statement,
)
from horsies.core.models.task_pg import TaskModel

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]


async def _seed_tasks(session: AsyncSession) -> None:
    """~500 running rows plus a deep PENDING backlog, committed and analyzed.

    The non-PENDING half is running rather than terminal because the live
    table no longer holds terminal rows; what the planner needs from it is
    a second status and a spread of task names and enqueue instants, which
    running rows supply exactly as terminal ones did.

    The backlog is deliberately large (2,000 rows): with only a handful of
    PENDING rows the sort above the low-cardinality status index costs the
    same as the ordered claim index and the planner's pick between them is
    arbitrary — the deep-backlog case is the one the ordered index exists
    for.
    """
    await session.execute(
        text('TRUNCATE horsies_task_attempts, horsies_tasks CASCADE')
    )
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, enqueued_at, created_at, updated_at, claimed,
             retry_count, max_retries, started_at, enqueue_sha,
             retention_class_key, command_fingerprint_version,
             command_fingerprint, retain_rerun_input,
             prepared_rerun_input_disposition)
        SELECT gen_random_uuid(),
               'plan_task_' || (g % 5), 'default', 100, '[]', '{}',
               'RUNNING', NOW(), NOW() - (g || ' seconds')::interval,
               NOW(), NOW(), TRUE, 0, 0, NOW(), 'plan-test-sha',
               'standard_30d', 1,
               sha256(convert_to(gen_random_uuid()::text, 'UTF8')),
               FALSE, 'DECLINED_BY_POLICY'
        FROM generate_series(1, 500) AS g
    """))
    await session.execute(text("""
        INSERT INTO horsies_tasks
            (id, task_name, queue_name, priority, args, kwargs,
             status, sent_at, enqueued_at, created_at, updated_at, claimed,
             retry_count, max_retries, enqueue_sha,
             retention_class_key, command_fingerprint_version,
             command_fingerprint, retain_rerun_input,
             prepared_rerun_input_disposition)
        SELECT gen_random_uuid(),
               'plan_task_pending', 'default', 100, '[]', '{}',
               'PENDING', NOW(), NOW() - (g || ' seconds')::interval,
               NOW(), NOW(), FALSE, 0, 0, 'plan-test-sha',
               'standard_30d', 1,
               sha256(convert_to(gen_random_uuid()::text, 'UTF8')),
               FALSE, 'DECLINED_BY_POLICY'
        FROM generate_series(1, 2000) AS g
    """))
    await session.commit()
    await session.execute(text('ANALYZE horsies_tasks'))
    # ANALYZE's pg_statistic writes are MVCC-transactional; commit them so
    # later rollbacks cannot revert the gathered statistics.
    await session.commit()


async def _explain(
    session: AsyncSession,
    statement_sql: str,
    params: dict[str, object] | None = None,
) -> str:
    """EXPLAIN (no execution) with seq scans disabled, rolled back."""
    await session.execute(text('SET enable_seqscan = off'))
    rows = (
        await session.execute(text('EXPLAIN ' + statement_sql), params or {})
    ).fetchall()
    plan = '\n'.join(str(row[0]) for row in rows)
    await session.rollback()
    return plan


def _compiled(statement: Any) -> str:
    """Render a SQLAlchemy statement with literal binds for EXPLAIN."""
    return str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={'literal_binds': True},
        )
    )


async def test_list_default_sort_plans_on_enqueued_at_index(
    broker: PostgresBroker,  # noqa: ARG001 - ensures schema migrations are applied
    session: AsyncSession,
) -> None:
    """Both directions of the default sort ride idx_horsies_tasks_enqueued_at.

    The regression this pins: a ``nulls_last`` wrapper on the NOT NULL sort
    column changes the requested ordering away from what the btree provides
    and silently degrades the list to a full-table sort.
    """
    await _seed_tasks(session)

    for sort_dir in ('desc', 'asc'):
        statement = _compiled(
            list_rows_statement(
                scope=[], sort_by='enqueued_at', sort_dir=sort_dir,
                offset=0, limit=50,
            )
        )
        plan = await _explain(session, statement)
        assert 'idx_horsies_tasks_enqueued_at' in plan, (sort_dir, plan)
        assert 'Sort' not in plan, (sort_dir, plan)


async def test_task_name_facet_plans_index_only(
    broker: PostgresBroker,  # noqa: ARG001 - ensures schema migrations are applied
    session: AsyncSession,
) -> None:
    """The unfiltered task-name facet is an index-only scan on v16's index."""
    await _seed_tasks(session)

    statement = _compiled(facet_statement(TaskModel.task_name, []))
    plan = await _explain(session, statement)
    assert 'idx_horsies_tasks_task_name' in plan, plan
    assert 'Index Only Scan' in plan, plan


async def test_claim_statement_still_plans_on_claim_pending_index(
    broker: PostgresBroker,  # noqa: ARG001 - ensures schema migrations are applied
    session: AsyncSession,
) -> None:
    """The hot-path claim is unaffected by the v16 monitoring indexes.

    Neither ``enqueued_at`` nor ``task_name`` can serve the claim's
    predicate (queue_name + status) or its ordering (priority leads); this
    pins that the planner keeps idx_horsies_tasks_claim_pending.
    """
    await _seed_tasks(session)

    plan = await _explain(
        session,
        CLAIM_SQL.text,
        {
            'queue': 'default',
            'lim': 20,
            'worker_id': 'plan-test-worker',
            'claim_lease_ms': 30_000,
        },
    )
    assert 'idx_horsies_tasks_claim_pending' in plan, plan
    assert 'idx_horsies_tasks_enqueued_at' not in plan, plan
    assert 'idx_horsies_tasks_task_name' not in plan, plan
