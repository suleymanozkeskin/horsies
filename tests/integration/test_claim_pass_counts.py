"""Integration tests: merged claim-pass cap accounting.

Pins CLAIM_PASS_COUNTS_SQL's equivalence with the five single-purpose
count statements it replaces (which remain in use by health snapshots),
and the flat statement count of an empty capped claim pass.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import uuid
from typing import Any

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.worker.claiming import ClaimMixin
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.sql import (
    CLAIM_PASS_COUNTS_SQL,
    COUNT_CLAIMED_FOR_WORKER_SQL,
    COUNT_GLOBAL_IN_FLIGHT_SQL,
    COUNT_IN_FLIGHT_FOR_WORKER_SQL,
    COUNT_QUEUE_IN_FLIGHT_HARD_SQL,
    COUNT_QUEUE_IN_FLIGHT_SOFT_SQL,
    COUNT_RUNNING_FOR_WORKER_SQL,
)
from tests.integration.conftest import compute_test_enqueue_sha

pytestmark = [pytest.mark.integration]

WID = f'counts-worker-{uuid.uuid4().hex[:8]}'
OTHER = f'counts-other-{uuid.uuid4().hex[:8]}'


async def _seed(
    session: AsyncSession,
    *,
    status: str,
    queue: str,
    worker: str | None,
    lease: str | None,
) -> None:
    """lease: 'live' (now()+1h), 'expired' (now()-1h), or None (NULL)."""
    sent_at, sha = compute_test_enqueue_sha(task_name='counts_seed')
    lease_sql = {
        'live': "NOW() + INTERVAL '1 hour'",
        'expired': "NOW() - INTERVAL '1 hour'",
        None: 'NULL',
    }[lease]
    await session.execute(
        text(f"""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs, status,
                 sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, enqueue_sha, claimed_by_worker_id,
                 claim_expires_at)
            VALUES
                (:id, 'counts_seed', :queue, 100, '[]', '{{}}', :status,
                 :sent_at, NOW(), NOW(), FALSE, 0, 0, :sha, :worker,
                 {lease_sql})
        """),
        {
            'id': str(uuid.uuid4()),
            'queue': queue,
            'status': status,
            'sent_at': sent_at,
            'sha': sha,
            'worker': worker,
        },
    )


@pytest.mark.asyncio(loop_scope='function')
class TestClaimPassCounts:
    async def test_merged_counts_match_single_purpose_statements(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Every column of the merged statement equals its original:
        my CLAIMED (live lease only), my RUNNING, my in-flight, global
        in-flight, and per-queue hard/soft counts — across owned/foreign
        rows, live/expired/NULL leases, and multiple queues."""
        rows: list[dict[str, Any]] = [
            # mine: claimed live, claimed expired (excluded), claimed NULL
            # lease (included), running
            dict(status='CLAIMED', queue='q_a', worker=WID, lease='live'),
            dict(status='CLAIMED', queue='q_a', worker=WID, lease='expired'),
            dict(status='CLAIMED', queue='q_b', worker=WID, lease=None),
            dict(status='RUNNING', queue='q_b', worker=WID, lease=None),
            # others: claimed live, claimed expired, running, plus an
            # unclaimed PENDING row that must count nowhere
            dict(status='CLAIMED', queue='q_a', worker=OTHER, lease='live'),
            dict(status='CLAIMED', queue='q_b', worker=OTHER, lease='expired'),
            dict(status='RUNNING', queue='q_a', worker=OTHER, lease=None),
            dict(status='PENDING', queue='q_a', worker=None, lease=None),
        ]
        for row in rows:
            await _seed(session, **row)
        await session.commit()

        merged = (
            await session.execute(
                CLAIM_PASS_COUNTS_SQL,
                {'wid': WID, 'capped_queues': ['q_a', 'q_b']},
            )
        ).one()

        async def scalar(stmt: Any, **params: Any) -> int:
            res = await session.execute(stmt, params)
            row = res.fetchone()
            return int(row.cnt) if row else 0

        assert merged.my_claimed == await scalar(
            COUNT_CLAIMED_FOR_WORKER_SQL, wid=WID,
        )
        assert merged.my_running == await scalar(
            COUNT_RUNNING_FOR_WORKER_SQL, wid=WID,
        )
        assert merged.my_in_flight == await scalar(
            COUNT_IN_FLIGHT_FOR_WORKER_SQL, wid=WID,
        )
        assert merged.global_in_flight == await scalar(
            COUNT_GLOBAL_IN_FLIGHT_SQL,
        )
        for queue in ('q_a', 'q_b'):
            assert (merged.queue_hard_counts or {}).get(queue, 0) == await scalar(
                COUNT_QUEUE_IN_FLIGHT_HARD_SQL, q=queue,
            ), queue
            assert (merged.queue_soft_counts or {}).get(queue, 0) == await scalar(
                COUNT_QUEUE_IN_FLIGHT_SOFT_SQL, q=queue,
            ), queue

        # Concrete expected values, so a shared bug in old+new cannot hide:
        # mine claimed = live + NULL lease = 2; mine running = 1.
        assert merged.my_claimed == 2
        assert merged.my_running == 1
        assert merged.my_in_flight == 3
        # global = all CLAIMED-unexpired + RUNNING = 2 (mine) + 1 + 1 + 1 = 5
        assert merged.global_in_flight == 5

    async def test_empty_capped_pass_statement_count(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
    ) -> None:
        """Regression for the claim-pass round-trip count. History: the
        per-count pass over Q=3 capped queues ran 11 statements (3 locks +
        2 worker counts + 3 queue counts + 3 claims); the merged accounting cut
        it to 7; TIER 1 (horsies_claim) collapses lock acquisition, counts and
        the windowed claim into ONE statement."""
        queues = [f'cnt_q_{i}' for i in range(3)]
        cfg = WorkerConfig(
            dsn='unused', psycopg_dsn='unused', queues=queues,
            queue_priorities={q: i for i, q in enumerate(queues)},
            queue_max_concurrency={q: 4 for q in queues},
            processes=4,
        )

        class _Probe(ClaimMixin):
            def __init__(self) -> None:
                self.sf = broker.session_factory
                self.cfg = cfg
                self.worker_instance_id = f'probe-{uuid.uuid4().hex[:8]}'

            async def _dispatch_one(self, *args: Any, **kwargs: Any) -> None:
                raise AssertionError('empty pass must not dispatch')

        probe = _Probe()
        counter = {'n': 0}

        def _count(conn: Any, cursor: Any, stmt: Any, params: Any, ctx: Any, many: Any) -> None:
            counter['n'] += 1

        event.listen(broker.async_engine.sync_engine, 'before_cursor_execute', _count)
        try:
            claimed = await probe._claim_and_dispatch_all()
        finally:
            event.remove(broker.async_engine.sync_engine, 'before_cursor_execute', _count)

        assert claimed is False
        assert counter['n'] == 1, f'empty capped pass ran {counter["n"]} statements'
