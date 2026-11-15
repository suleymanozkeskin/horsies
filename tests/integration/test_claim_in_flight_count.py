"""The in-flight count of the claim (``horsies_claim_in_flight``).

A task that finishes leaves dead CLAIMED and RUNNING entries in
``ix_horsies_tasks_status``. The count is a plain scan of that index, and a
plain index scan marks the dead entries that it passes, so later scans skip
them. A bitmap scan marks none. Each test runs the worker's claim statement
(``HORSIES_CLAIM_SQL``) and reads what the count did:

- with statistics that show most rows in flight, the count reads the status
  index, where the default settings give a sequential scan;
- after a drain that leaves only dead RUNNING entries, one claim marks all of
  them.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from typing import Any, Final

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.schemas.migrations import CLAIM_IN_FLIGHT_ROWS_SQL
from horsies.core.worker.sql import HORSIES_CLAIM_SQL
from tests.integration.autocommit_support import QUEUE, TASK_NAME, WORKER_ID
from tests.integration.conftest import compute_test_enqueue_sha

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

ROWS: Final = 2_000
STATUS_INDEX_SCAN: Final = 'Index Scan using ix_horsies_tasks_status on horsies_tasks'
CLAIM_PARAMS: Final[dict[str, object]] = {
    'p_worker_id': 'in-flight-count-worker',
    'p_queues': json.dumps([QUEUE]),
    'p_queue_priority': json.dumps({QUEUE: 100}),
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
}
RUNNING_ROWS_SQL: Final = "SELECT task_name FROM horsies_tasks WHERE status = 'RUNNING'"


@pytest_asyncio.fixture(loop_scope='function')
async def tasks_without_autovacuum(
    broker: PostgresBroker,  # noqa: ARG001 - ensures schema migrations are applied
    session: AsyncSession,
) -> AsyncGenerator[None]:
    """Autovacuum off for horsies_tasks, so only the statements of the test
    touch its dead index entries."""
    _ = await session.execute(text('ALTER TABLE horsies_tasks SET (autovacuum_enabled = off)'))
    await session.commit()
    yield
    await session.rollback()
    _ = await session.execute(text('ALTER TABLE horsies_tasks RESET (autovacuum_enabled)'))
    await session.commit()


async def _insert_pending(session: AsyncSession) -> None:
    """``ROWS`` PENDING tasks of ``QUEUE``, committed."""
    sent_at, sha = compute_test_enqueue_sha(task_name=TASK_NAME, queue_name=QUEUE)
    _ = await session.execute(text('TRUNCATE horsies_task_attempts, horsies_tasks CASCADE'))
    _ = await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, enqueued_at, created_at, updated_at,
                 claimed, retry_count, max_retries,
                 enqueue_sha, is_workflow_task, retention_class_key,
                 command_fingerprint_version, command_fingerprint,
                 retain_rerun_input, prepared_rerun_input_disposition)
            SELECT g.id, :task_name, :queue, 100, '[]', '{}',
                   'PENDING', :sent_at, NOW(), NOW(), NOW(),
                   FALSE, 0, 0,
                   :sha, FALSE, 'standard_30d',
                   1, sha256(convert_to(CAST(g.id AS text), 'UTF8')),
                   FALSE, 'DECLINED_BY_POLICY'
            FROM (SELECT gen_random_uuid() AS id FROM generate_series(1, :rows)) AS g
        """),
        {'task_name': TASK_NAME, 'queue': QUEUE, 'sent_at': sent_at, 'sha': sha, 'rows': ROWS},
    )
    await session.commit()


async def _start(session: AsyncSession, rows: int) -> None:
    """Move ``rows`` PENDING tasks to RUNNING, owned by ``WORKER_ID``, committed."""
    _ = await session.execute(
        text("""
            UPDATE horsies_tasks
            SET status = 'RUNNING', claimed = FALSE, claimed_at = NOW(),
                claimed_by_worker_id = :worker_id, started_at = NOW()
            WHERE id IN (
                SELECT id FROM horsies_tasks WHERE status = 'PENDING' ORDER BY id LIMIT :rows
            )
        """),
        {'worker_id': WORKER_ID, 'rows': rows},
    )
    await session.commit()


async def _nested_plans(session: AsyncSession) -> str:
    """Run one claim through ``HORSIES_CLAIM_SQL`` with the session's default
    planner settings and return the plans of the statements inside the claim
    functions, from auto_explain. Rolled back."""
    connection = await session.connection()
    raw = await connection.get_raw_connection()
    driver = raw.driver_connection
    assert driver is not None
    notices: list[str] = []

    def _collect(diagnostic: Any) -> None:
        notices.append(str(diagnostic.message_primary or ''))

    driver.add_notice_handler(_collect)
    try:
        _ = await session.execute(text("LOAD 'auto_explain'"))
        for setting in (
            'auto_explain.log_min_duration = 0',
            'auto_explain.log_nested_statements = on',
            "auto_explain.log_level = 'notice'",
        ):
            _ = await session.execute(text(f'SET LOCAL {setting}'))
        _ = await session.execute(HORSIES_CLAIM_SQL, CLAIM_PARAMS)
    finally:
        await session.rollback()
        driver.remove_notice_handler(_collect)
    return '\n'.join(notices)


async def _claim_once(session: AsyncSession) -> None:
    """Run one claim through ``HORSIES_CLAIM_SQL``. Rolled back: the marks on
    dead index entries are hints, not part of the transaction."""
    try:
        _ = (await session.execute(HORSIES_CLAIM_SQL, CLAIM_PARAMS)).all()
    finally:
        await session.rollback()


async def _bitmap_running_entries(session: AsyncSession) -> float:
    """The entries that a bitmap scan of ``ix_horsies_tasks_status`` returns
    for RUNNING. A bitmap scan skips the entries that are marked dead and
    marks none itself. Rolled back."""
    try:
        for setting in ('enable_seqscan', 'enable_indexscan', 'enable_indexonlyscan'):
            _ = await session.execute(text(f'SET LOCAL {setting} = off'))
        plan = (
            await session.execute(text(f'EXPLAIN (ANALYZE, FORMAT JSON) {RUNNING_ROWS_SQL}'))
        ).scalar_one()
    finally:
        await session.rollback()
    nodes: list[dict[str, Any]] = [plan[0]['Plan']]
    while nodes:
        node = nodes.pop()
        if node['Node Type'] == 'Bitmap Index Scan':
            assert node['Index Name'] == 'ix_horsies_tasks_status', node
            return float(node['Actual Rows'])
        nodes.extend(node.get('Plans', []))
    raise AssertionError(f'no bitmap index scan in {plan}')


async def test_the_count_reads_the_status_index_when_most_rows_are_in_flight(
    session: AsyncSession,
    tasks_without_autovacuum: None,  # noqa: ARG001
) -> None:
    await _insert_pending(session)
    await _start(session, ROWS * 9 // 10)
    _ = await session.execute(text('ANALYZE horsies_tasks'))
    await session.commit()
    default_plan = '\n'.join(
        str(row[0])
        for row in await session.execute(text(f'EXPLAIN {CLAIM_IN_FLIGHT_ROWS_SQL}'))
    )
    await session.rollback()

    plans = await _nested_plans(session)

    assert 'Seq Scan on horsies_tasks' in default_plan, default_plan
    counts = [plan for plan in plans.split('duration:') if 'WITH in_flight AS' in plan]
    assert len(counts) == 1, plans
    assert STATUS_INDEX_SCAN in counts[0], counts[0]
    assert 'Seq Scan on horsies_tasks' not in counts[0], counts[0]
    assert 'Bitmap Heap Scan on horsies_tasks' not in counts[0], counts[0]


async def test_a_claim_marks_the_dead_running_entries(
    session: AsyncSession,
    tasks_without_autovacuum: None,  # noqa: ARG001
) -> None:
    """Every task ran and was deleted, so each RUNNING entry is dead."""
    await _insert_pending(session)
    await _start(session, ROWS)
    _ = await session.execute(text('DELETE FROM horsies_tasks'))
    await session.commit()
    before = await _bitmap_running_entries(session)

    await _claim_once(session)

    assert before == ROWS
    assert await _bitmap_running_entries(session) == 0
