"""Plan tests for the monitoring read path.

Each test reads the plans of the statements inside a database function
through auto_explain (``horsies_monitoring_task_list``, the worker's claim
statement) and asserts the index the plan must ride. Any drift that falls
off the index (an ordering wrapper the btree cannot satisfy, a projection
change, a claim predicate rewrite) fails here, not on an adopter's
dashboard.

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
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.worker.sql import HORSIES_CLAIM_SQL

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

# Every column of horsies_tasks, in the order the seed statements give values.
_TASK_COLUMNS = (
    'id, task_name, queue_name, priority, input, task_options, execution, '
    'retention_class_key, sent_at, created_at, command_fingerprint_version, '
    'command_fingerprint, idempotency_key_digest, status, enqueued_at, '
    'claimed_at, claimed_by_worker_id, claim_expires_at, started_at, '
    'finalizing_at, finalizing_by_worker_id, worker_pid, worker_hostname, '
    'worker_process_name, retry_count, max_retries, next_retry_at, good_until, '
    'is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, '
    'rerun_root_task_id'
)


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
    await session.execute(text(f"""
        INSERT INTO horsies_tasks ({_TASK_COLUMNS})
        SELECT gen_random_uuid(),
               'plan_task_' || (g % 5), 'default', 100, '{{}}', NULL,
               'ISOLATED', 'standard_30d', NOW(), NOW(), 1,
               sha256(convert_to(gen_random_uuid()::text, 'UTF8')), NULL,
               'RUNNING', NOW() - (g || ' seconds')::interval, NULL, NULL, NULL,
               NOW(), NULL, NULL,
               NULL, NULL, NULL,
               0, 0, NULL, NULL, FALSE,
               NOW(), FALSE, NULL, NULL
        FROM generate_series(1, 500) AS g
    """))
    await session.execute(text(f"""
        INSERT INTO horsies_tasks ({_TASK_COLUMNS})
        SELECT gen_random_uuid(),
               'plan_task_pending', 'default', 100, '{{}}', NULL,
               'ISOLATED', 'standard_30d', NOW(), NOW(), 1,
               sha256(convert_to(gen_random_uuid()::text, 'UTF8')), NULL,
               'PENDING', NOW() - (g || ' seconds')::interval, NULL, NULL, NULL,
               NULL, NULL, NULL,
               NULL, NULL, NULL,
               0, 0, NULL, NULL, FALSE,
               NOW(), FALSE, NULL, NULL
        FROM generate_series(1, 2000) AS g
    """))
    await session.commit()
    await session.execute(text('ANALYZE horsies_tasks'))
    # ANALYZE's pg_statistic writes are MVCC-transactional; commit them so
    # later rollbacks cannot revert the gathered statistics.
    await session.commit()


async def _nested_plans(
    session: AsyncSession, statement: Any, params: dict[str, object],
) -> str:
    """Run ``statement`` and return the plans of the statements inside the
    functions it calls, from auto_explain, with seq scans disabled. Rolled
    back."""
    connection = await session.connection()
    raw = await connection.get_raw_connection()
    driver = raw.driver_connection
    assert driver is not None
    notices: list[str] = []

    def _collect(diagnostic: Any) -> None:
        notices.append(str(diagnostic.message_primary or ''))

    driver.add_notice_handler(_collect)
    try:
        await session.execute(text('SET LOCAL enable_seqscan = off'))
        await session.execute(text("LOAD 'auto_explain'"))
        for setting in (
            'auto_explain.log_min_duration = 0',
            'auto_explain.log_nested_statements = on',
            "auto_explain.log_level = 'notice'",
        ):
            await session.execute(text(f'SET LOCAL {setting}'))
        await session.execute(statement, params)
    finally:
        await session.rollback()
        driver.remove_notice_handler(_collect)
    return '\n'.join(notices)


_DEFAULT_SORT_LIST_SQL = text("""
    SELECT * FROM horsies_monitoring_task_list(
        now() - interval '1 day', now(), '{}', '{}', '{}', '{}', '{}', '{}', FALSE,
        '{}', '{}', 'enqueued_at', CAST(:direction AS text), 0, 50
    )
""")


async def test_list_default_sort_plans_on_enqueued_at_index(
    broker: PostgresBroker,  # noqa: ARG001 - ensures schema migrations are applied
    session: AsyncSession,
) -> None:
    """Both directions of the default sort read the live side on idx_horsies_tasks_enqueued_at.

    The regression this pins: a ``NULLS LAST`` on the NOT NULL sort column
    changes the requested ordering away from what the btree provides and
    degrades the live side to a full-table sort. The task id tie-break is
    an incremental sort on the index order (presorted key ``enqueued_at``):
    it sorts only the rows of one ``enqueued_at``.
    """
    await _seed_tasks(session)

    for direction in ('DESC', 'ASC'):
        plans = await _nested_plans(session, _DEFAULT_SORT_LIST_SQL, {'direction': direction})
        page_plan = next(
            plan for plan in plans.split('duration:') if 'WITH live AS' in plan
        )
        assert 'idx_horsies_tasks_enqueued_at' in page_plan, (direction, page_plan)
        assert 'Presorted Key: t.enqueued_at' in page_plan, (direction, page_plan)
        assert 'Seq Scan on horsies_tasks' not in page_plan, (direction, page_plan)


async def test_claim_streams_plan_on_the_class_indexes(
    broker: PostgresBroker,  # noqa: ARG001 - ensures schema migrations are applied
    session: AsyncSession,
) -> None:
    """Each claim stream of a worker with isolated children rides a class
    index: the pending arm horsies_tasks_claim_pending, the expired arm one of
    the two expired indexes. The monitoring enqueued_at index does not
    appear, and no stream scans the heap.
    """
    await _seed_tasks(session)

    plans = await _nested_plans(
        session,
        HORSIES_CLAIM_SQL,
        {
            'p_worker_id': 'plan-test-worker',
            'p_queues': '["default"]',
            'p_queue_priority': '{"default": 100}',
            'p_queue_max_concurrency': '{}',
            'p_hard_cap_mode': True,
            'p_budgets': '{"ISOLATED": 20}',
            'p_routes': (
                '{"ISOLATED": "ISOLATED", "SHARED_ASYNC": "ISOLATED", '
                '"SHARED_THREAD": "ISOLATED"}'
            ),
            'p_prefetch_buffer': 0,
            'p_max_claim_per_worker': 0,
            'p_max_claim_batch': 0,
            'p_cluster_wide_cap': None,
            'p_lease_ms': 30_000,
            'p_lock_keys': '[]',
        },
    )
    statements = plans.split('duration:')
    # Phase 1 (horsies_claim_plan) reads the head of each stream; phase 2
    # (horsies_claim_candidates) locks the planned rows from the same streams.
    phases = {
        'plan': next(plan for plan in statements if 'SELECT s.queue_slot' in plan),
        'take': next(plan for plan in statements if 'WITH planned AS' in plan),
    }
    for name, plan in phases.items():
        assert (
            plan.count('Index Scan using horsies_tasks_claim_pending ') == 1
        ), (name, plan)
        assert (
            'horsies_tasks_claim_expired_order' in plan
            or 'horsies_tasks_claim_expired_at' in plan
        ), (name, plan)
        for absent in (
            'Seq Scan on horsies_tasks',
            'idx_horsies_tasks_enqueued_at',
        ):
            assert absent not in plan, (name, absent, plan)
