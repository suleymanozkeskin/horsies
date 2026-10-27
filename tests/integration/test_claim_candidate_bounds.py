"""Claim candidate bounds, dispatch order, and the non-runnable lookup.

- A claim locks only the candidates its queue cap and batch limit let it
  return; the rest stay claimable by other workers while it is open.
- Rows come back in the global dispatch rank (queue priority, task
  priority, enqueue time, id), across the pending and expired arms.
- Candidate selection reuses a generic plan; the parent does not.
- The bounded non-runnable lookup returns what the plain join returns.
"""

from __future__ import annotations

import json
import uuid
from collections import Counter
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import TextClause, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.worker.sql import (
    GET_NONRUNNABLE_WORKFLOW_TASK_IDS_SQL,
    HORSIES_CLAIM_SQL,
)
from tests.broker_resources_support import current_resources

pytestmark = [pytest.mark.integration]

QUEUE = 'bounds_q'
SEEDED_PENDING = 50
CLAIM_PROCESSES = 10
LEASE_MS = 60_000

PLAIN_NON_RUNNABLE_JOIN_SQL = text("""
    SELECT t.id, w.status
    FROM horsies_tasks t
    JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE t.id = ANY(CAST(:ids AS uuid[]))
      AND w.status IN ('PAUSED', 'CANCELLED')
""")


async def _seed_task(
    session: AsyncSession,
    *,
    queue: str,
    priority: int,
    age_secs: int,
    expired_claim: bool = False,
    is_workflow_task: bool = False,
) -> str:
    """Insert one claimable task: PENDING, or CLAIMED with a lapsed lease."""
    task_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
              (id, task_name, queue_name, priority, status, enqueued_at,
               claimed, claimed_by_worker_id, claimed_at, claim_expires_at,
               is_workflow_task, enqueue_sha, created_at, updated_at,
               retry_count, max_retries,
               retention_class_key, command_fingerprint_version,
               command_fingerprint, retain_rerun_input,
               prepared_rerun_input_disposition)
            VALUES
              (:id, 'bounds', :q, :p, :status,
               now() - (:age * interval '1 second'),
               :claimed, :owner, :claimed_at, :expires_at,
               :is_wf, :sha, now(), now(), 0, 0,
               'standard_30d', 1,
               sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
               false, 'DECLINED_BY_POLICY')
        """),
        {
            'id': task_id,
            'q': queue,
            'p': priority,
            'age': age_secs,
            'status': 'CLAIMED' if expired_claim else 'PENDING',
            'claimed': expired_claim,
            'owner': 'lapsed-worker' if expired_claim else None,
            'claimed_at': None,
            'expires_at': None,
            'is_wf': is_workflow_task,
            'sha': '0' * 64,
        },
    )
    if expired_claim:
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET claimed_at = now() - interval '2 minutes',
                    claim_expires_at = now() - interval '1 minute'
                WHERE id = CAST(:id AS uuid)
            """),
            {'id': task_id},
        )
    return task_id


def _claim_params(
    *,
    queues: list[str],
    queue_priority: dict[str, int],
    queue_max_concurrency: dict[str, int],
    max_claim_batch: int,
) -> dict[str, object]:
    return {
        'p_worker_id': f'bounds-{uuid.uuid4().hex[:8]}',
        'p_queues': json.dumps(queues),
        'p_queue_priority': json.dumps(queue_priority),
        'p_queue_max_concurrency': json.dumps(queue_max_concurrency),
        'p_hard_cap_mode': True,
        'p_processes': CLAIM_PROCESSES,
        'p_prefetch_buffer': 0,
        'p_max_claim_per_worker': 0,
        'p_max_claim_batch': max_claim_batch,
        'p_cluster_wide_cap': None,
        'p_lease_ms': LEASE_MS,
        'p_lock_keys': '[]',
    }


@asynccontextmanager
async def _open_claim(
    broker: PostgresBroker,
    params: dict[str, object],
) -> AsyncGenerator[list[str], None]:
    """Run horsies_claim and keep its transaction open while the caller looks."""
    async with current_resources(broker).engine.connect() as connection:
        transaction = await connection.begin()
        try:
            rows = (await connection.execute(HORSIES_CLAIM_SQL, params)).all()
            yield [str(row.id) for row in rows]
        finally:
            await transaction.rollback()


async def _claimable_by_another_worker(connection: AsyncConnection) -> int:
    result = await connection.execute(
        text("""
            SELECT count(*) FROM (
                SELECT id FROM horsies_tasks
                WHERE queue_name = :q AND status = 'PENDING'
                FOR UPDATE SKIP LOCKED
            ) unlocked
        """),
        {'q': QUEUE},
    )
    return int(result.scalar_one())


@pytest.mark.parametrize(
    ('max_claim_batch', 'queue_cap', 'returned'),
    [(2, None, 2), (0, 3, 3)],
    ids=['batch_limit', 'queue_cap'],
)
@pytest.mark.asyncio(loop_scope='function')
async def test_claim_locks_only_the_rows_it_returns(
    clean_workflow_tables: None,  # noqa: ARG001
    broker: PostgresBroker,
    session: AsyncSession,
    max_claim_batch: int,
    queue_cap: int | None,
    returned: int,
) -> None:
    for _ in range(SEEDED_PENDING):
        await _seed_task(session, queue=QUEUE, priority=100, age_secs=10)
    await session.commit()
    caps = {} if queue_cap is None else {QUEUE: queue_cap}
    params = _claim_params(
        queues=[QUEUE],
        queue_priority={},
        queue_max_concurrency=caps,
        max_claim_batch=max_claim_batch,
    )

    async with _open_claim(broker, params) as claimed_ids:
        assert len(claimed_ids) == returned
        async with current_resources(broker).engine.connect() as other_worker:
            async with other_worker.begin():
                assert (
                    await _claimable_by_another_worker(other_worker)
                    == SEEDED_PENDING - returned
                )


@pytest.mark.asyncio(loop_scope='function')
async def test_claim_returns_rows_in_dispatch_order(
    clean_workflow_tables: None,  # noqa: ARG001
    broker: PostgresBroker,
    session: AsyncSession,
) -> None:
    """Insertion runs opposite to the rank, across both eligibility arms."""
    low_queue_task = await _seed_task(
        session,
        queue='bounds_low',
        priority=1,
        age_secs=50,
    )
    late_task = await _seed_task(
        session,
        queue='bounds_high',
        priority=50,
        age_secs=1,
    )
    early_task = await _seed_task(
        session,
        queue='bounds_high',
        priority=50,
        age_secs=30,
    )
    expired_urgent_task = await _seed_task(
        session,
        queue='bounds_high',
        priority=10,
        age_secs=5,
        expired_claim=True,
    )
    await session.commit()
    params = _claim_params(
        queues=['bounds_low', 'bounds_high'],
        queue_priority={'bounds_high': 1, 'bounds_low': 2},
        queue_max_concurrency={},
        max_claim_batch=0,
    )

    async with _open_claim(broker, params) as claimed_ids:
        assert claimed_ids == [
            expired_urgent_task,
            early_task,
            late_task,
            low_queue_task,
        ]


@pytest.mark.asyncio(loop_scope='function')
async def test_only_candidate_selection_forces_a_generic_plan(
    broker: PostgresBroker,
) -> None:
    async with current_resources(broker).engine.connect() as connection:
        settings = dict(
            (
                await connection.execute(
                    text("""
            SELECT proname, coalesce(array_to_string(proconfig, ','), '')
            FROM pg_proc
            WHERE proname IN ('horsies_claim', 'horsies_claim_candidates')
        """)
                )
            )
            .tuples()
            .all()
        )
    assert settings == {
        'horsies_claim': '',
        'horsies_claim_candidates': 'plan_cache_mode=force_generic_plan',
    }


async def _seed_workflow_task(session: AsyncSession, workflow_status: str) -> str:
    """A claimed workflow task linked to a workflow in workflow_status."""
    task_id = await _seed_task(
        session,
        queue=QUEUE,
        priority=100,
        age_secs=1,
        is_workflow_task=True,
    )
    workflow_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES (:id, 'bounds_wf', :status, 'FAIL', 0, :id,
                    NOW(), NOW(), NOW(), NOW())
        """),
        {'id': workflow_id, 'status': workflow_status},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args,
                 task_kwargs, queue_name, priority, dependencies,
                 allow_failed_deps, join_type, is_subworkflow, status,
                 task_id, created_at)
            VALUES (:id, :workflow_id, 0, 'node_0', 'bounds', '[]', '{}',
                    :q, 100, '{}', FALSE, 'all', FALSE, 'ENQUEUED',
                    :task_id, NOW())
        """),
        {
            'id': str(uuid.uuid4()),
            'workflow_id': workflow_id,
            'q': QUEUE,
            'task_id': task_id,
        },
    )
    return task_id


async def _lookup_rows(
    session: AsyncSession,
    statement: TextClause,
    ids: list[str | None],
) -> Counter[tuple[str, str]]:
    result = await session.execute(statement, {'ids': ids})
    return Counter((str(row[0]), str(row[1])) for row in result.all())


@pytest.mark.asyncio(loop_scope='function')
async def test_non_runnable_lookup_matches_the_plain_join(
    clean_workflow_tables: None,  # noqa: ARG001
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
) -> None:
    """Duplicate, null, absent, and runnable ids give the plain join's rows."""
    paused = await _seed_workflow_task(session, 'PAUSED')
    cancelled = await _seed_workflow_task(session, 'CANCELLED')
    running = await _seed_workflow_task(session, 'RUNNING')
    plain = await _seed_task(session, queue=QUEUE, priority=100, age_secs=1)
    await session.commit()
    ids: list[str | None] = [
        paused,
        paused,
        cancelled,
        running,
        plain,
        None,
        str(uuid.uuid4()),
    ]

    bounded = await _lookup_rows(session, GET_NONRUNNABLE_WORKFLOW_TASK_IDS_SQL, ids)
    reference = await _lookup_rows(session, PLAIN_NON_RUNNABLE_JOIN_SQL, ids)

    assert bounded == reference
    assert bounded == Counter({(paused, 'PAUSED'): 1, (cancelled, 'CANCELLED'): 1})


@pytest.mark.asyncio(loop_scope='function')
async def test_non_runnable_lookup_is_empty_without_stopped_workflows(
    clean_workflow_tables: None,  # noqa: ARG001
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
) -> None:
    running = await _seed_workflow_task(session, 'RUNNING')
    await session.commit()

    bounded = await _lookup_rows(
        session,
        GET_NONRUNNABLE_WORKFLOW_TASK_IDS_SQL,
        [running],
    )

    assert bounded == Counter()
