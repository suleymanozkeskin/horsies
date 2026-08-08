"""Integration tests: the horsies_claim SQL function (TIER 1).

Pins the claim ordering contract and cap enforcement against the REAL function
(the per-queue claim loop + budget math now live in SQL, so these cannot be
exercised by mocking the session).

Ordering contract within the global rank `qprio, priority, enqueued_at, id`:
  - distinct queue priority      -> higher-priority queue wins
  - equal queue priority,
      equal task priority        -> FIFO (older enqueued_at wins)
      explicit lower task prio   -> task priority wins (workflow-node priority)
Cap enforcement:
  - per-queue max_concurrency and cluster_wide_cap never over-claim, incl. under
    concurrent claimers.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import asyncio
import uuid
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.worker.claiming import ClaimMixin
from horsies.core.worker.config import WorkerConfig

pytestmark = [pytest.mark.integration]


async def _seed(
    session: AsyncSession,
    *,
    queue: str,
    priority: int,
    age_secs: float,
) -> str:
    """Insert one PENDING task with explicit priority and enqueue age.

    Returns the minted id, which identity supplies rather than the caller;
    ordering assertions compare against the returned value.
    """
    task_id = str(uuid.uuid4())
    await session.execute(
        text(
            """
            INSERT INTO horsies_tasks
              (id, task_name, queue_name, priority, status, enqueued_at,
               is_workflow_task, enqueue_sha, created_at, updated_at,
               retry_count, max_retries,
               retention_class_key, command_fingerprint_version,
               command_fingerprint, retain_rerun_input,
               prepared_rerun_input_disposition)
            VALUES
              (:id,'bench',:q,:p,'PENDING',
               now() - (:age * interval '1 second'),
               false, :sha, now(), now(), 0, 0,
               'standard_30d', 1,
               sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
               false, 'DECLINED_BY_POLICY')
            """
        ),
        {'id': task_id, 'q': queue, 'p': priority, 'age': age_secs, 'sha': '0' * 64},
    )
    return task_id


async def _seed_n(
    session: AsyncSession, *, queue: str, priority: int, n: int
) -> None:
    await session.execute(
        text(
            """
            INSERT INTO horsies_tasks
              (id, task_name, queue_name, priority, status, enqueued_at,
               is_workflow_task, enqueue_sha, created_at, updated_at,
               retry_count, max_retries,
               retention_class_key, command_fingerprint_version,
               command_fingerprint, retain_rerun_input,
               prepared_rerun_input_disposition)
            SELECT gen_random_uuid(),'bench',:q,:p,'PENDING',now(),
                   false,:sha,now(),now(),0,0,
                   'standard_30d',1,
                   sha256(convert_to(gen_random_uuid()::text,'UTF8')),
                   false,'DECLINED_BY_POLICY'
            FROM generate_series(1,:n)
            """
        ),
        {'q': queue, 'p': priority, 'sha': '0' * 64, 'n': n},
    )


async def _claimed(session: AsyncSession) -> list[tuple[str, str, int]]:
    rows = (await session.execute(text(
        "SELECT id, queue_name, priority FROM horsies_tasks "
        "WHERE status='CLAIMED' ORDER BY queue_name, priority"
    ))).all()
    return [(r[0], r[1], r[2]) for r in rows]


def _probe(broker: PostgresBroker, **cfg_kwargs: Any) -> ClaimMixin:
    cfg = WorkerConfig(dsn='unused', psycopg_dsn='unused', **cfg_kwargs)

    class _P(ClaimMixin):
        def __init__(self) -> None:
            self.sf = broker.session_factory
            self.cfg = cfg
            self.worker_instance_id = f'probe-{uuid.uuid4().hex[:8]}'

        async def _dispatch_one(self, *args: Any, **kwargs: Any) -> None:
            return None

    return _P()


@pytest.mark.asyncio
async def test_distinct_queue_priority_wins(
    clean_workflow_tables: None, broker: PostgresBroker, session: AsyncSession
) -> None:
    """Budget=1: the higher-priority queue's task is claimed."""
    await _seed(session, queue='q_hi', priority=1, age_secs=0)
    await _seed(session, queue='q_lo', priority=10, age_secs=10)
    await session.commit()

    probe = _probe(
        broker, queues=['q_hi', 'q_lo'], processes=1, prefetch_buffer=0,
        queue_priorities={'q_hi': 1, 'q_lo': 10},
    )
    await probe._claim_and_dispatch_all()

    claimed = await _claimed(session)
    assert len(claimed) == 1
    assert claimed[0][1] == 'q_hi'


@pytest.mark.asyncio
async def test_equal_queue_priority_fifo(
    clean_workflow_tables: None, broker: PostgresBroker, session: AsyncSession
) -> None:
    """Equal queue priority + equal task priority: older enqueued task wins,
    regardless of queue name."""
    older_id = await _seed(session, queue='q2', priority=100, age_secs=30)
    await _seed(session, queue='q1', priority=100, age_secs=5)
    await session.commit()

    probe = _probe(
        broker, queues=['q1', 'q2'], processes=1, prefetch_buffer=0,
        queue_priorities={'q1': 100, 'q2': 100},
    )
    await probe._claim_and_dispatch_all()

    claimed = await _claimed(session)
    assert len(claimed) == 1
    assert claimed[0][0] == older_id


@pytest.mark.asyncio
async def test_equal_queue_priority_explicit_task_priority_wins(
    clean_workflow_tables: None, broker: PostgresBroker, session: AsyncSession
) -> None:
    """Equal queue priority: an explicit lower task priority (e.g. a workflow
    node) preempts an older equal-importance task in the band."""
    await _seed(session, queue='q1', priority=100, age_secs=30)
    urgent_id = await _seed(session, queue='q2', priority=1, age_secs=5)
    await session.commit()

    probe = _probe(
        broker, queues=['q1', 'q2'], processes=1, prefetch_buffer=0,
        queue_priorities={'q1': 100, 'q2': 100},
    )
    await probe._claim_and_dispatch_all()

    claimed = await _claimed(session)
    assert len(claimed) == 1
    assert claimed[0][0] == urgent_id


@pytest.mark.asyncio
async def test_per_queue_cap_never_over_claims(
    clean_workflow_tables: None, broker: PostgresBroker, session: AsyncSession
) -> None:
    """Concurrent claimers never exceed per-queue max_concurrency (hard cap)."""
    await _seed_n(session, queue='a', priority=50, n=300)
    await _seed_n(session, queue='b', priority=50, n=300)
    await session.commit()

    def mk() -> ClaimMixin:
        return _probe(
            broker, queues=['a', 'b'], processes=100, prefetch_buffer=0,
            queue_priorities={'a': 1, 'b': 2},
            queue_max_concurrency={'a': 3, 'b': 5},
        )

    probes = [mk() for _ in range(6)]

    async def loop(p: ClaimMixin) -> None:
        for _ in range(20):
            await p._claim_and_dispatch_all()
            await asyncio.sleep(0.005)

    await asyncio.gather(*[loop(p) for p in probes])

    rows = (await session.execute(text(
        "SELECT queue_name, count(*) FROM horsies_tasks "
        "WHERE status='CLAIMED' GROUP BY queue_name"
    ))).all()
    got = {r[0]: r[1] for r in rows}
    assert got.get('a', 0) <= 3, got
    assert got.get('b', 0) <= 5, got


@pytest.mark.asyncio
async def test_cluster_cap_never_over_claims(
    clean_workflow_tables: None, broker: PostgresBroker, session: AsyncSession
) -> None:
    """Concurrent claimers never exceed cluster_wide_cap."""
    await _seed_n(session, queue='a', priority=50, n=300)
    await _seed_n(session, queue='b', priority=50, n=300)
    await session.commit()

    def mk() -> ClaimMixin:
        return _probe(
            broker, queues=['a', 'b'], processes=100, prefetch_buffer=0,
            queue_priorities={'a': 1, 'b': 2}, cluster_wide_cap=7,
        )

    probes = [mk() for _ in range(6)]

    async def loop(p: ClaimMixin) -> None:
        for _ in range(20):
            await p._claim_and_dispatch_all()
            await asyncio.sleep(0.005)

    await asyncio.gather(*[loop(p) for p in probes])

    total = (await session.execute(text(
        "SELECT count(*) FROM horsies_tasks WHERE status='CLAIMED'"
    ))).scalar_one()
    assert total <= 7, total


@pytest.mark.asyncio
async def test_cap_only_queue_is_serviced_and_capped(
    clean_workflow_tables: None, broker: PostgresBroker, session: AsyncSession
) -> None:
    """C7: a queue with max_concurrency but NO queue_priorities is serviced
    (not silently dropped) and claimed only up to its cap. Before the fix,
    cap enforcement keyed on queue_priorities, so an empty priority map left
    the cap unenforced."""
    await _seed_n(session, queue='capped', priority=50, n=20)
    await session.commit()

    probe = _probe(
        broker, queues=['capped'], processes=100, prefetch_buffer=0,
        queue_max_concurrency={'capped': 4},
    )
    for _ in range(5):
        await probe._claim_and_dispatch_all()

    claimed = await _claimed(session)
    # Serviced: the queue is claimed at all (not dropped from the pass).
    assert len(claimed) > 0, 'cap-only queue was never serviced'
    # Capped: never exceeds max_concurrency despite 20 eligible tasks.
    assert len(claimed) == 4, claimed


@pytest.mark.asyncio
async def test_cap_without_priorities_never_over_claims(
    clean_workflow_tables: None, broker: PostgresBroker, session: AsyncSession
) -> None:
    """C7: concurrent claimers honor per-queue caps even with NO
    queue_priorities configured. Before the fix, a cap-only config took no
    advisory lock, so concurrent claimers over-claimed past the cap."""
    await _seed_n(session, queue='a', priority=50, n=300)
    await _seed_n(session, queue='b', priority=50, n=300)
    await session.commit()

    def mk() -> ClaimMixin:
        return _probe(
            broker, queues=['a', 'b'], processes=100, prefetch_buffer=0,
            queue_max_concurrency={'a': 3, 'b': 5},
        )

    probes = [mk() for _ in range(6)]

    async def loop(p: ClaimMixin) -> None:
        for _ in range(20):
            await p._claim_and_dispatch_all()
            await asyncio.sleep(0.005)

    await asyncio.gather(*[loop(p) for p in probes])

    rows = (await session.execute(text(
        "SELECT queue_name, count(*) FROM horsies_tasks "
        "WHERE status='CLAIMED' GROUP BY queue_name"
    ))).all()
    got = {r[0]: r[1] for r in rows}
    assert got.get('a', 0) <= 3, got
    assert got.get('b', 0) <= 5, got
