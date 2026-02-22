"""Integration tests: expired CLAIMED tasks must not block cap-based reclaim.

Verifies that cap-check SQL correctly excludes expired claims from in-flight
counts, allowing CLAIM_SQL's reclaim branch to pick them up even when
cluster_wide_cap or per-worker limits are otherwise saturated.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.sql import (
    CLAIM_SQL,
    COUNT_CLAIMED_FOR_WORKER_SQL,
    COUNT_GLOBAL_IN_FLIGHT_SQL,
    COUNT_IN_FLIGHT_FOR_WORKER_SQL,
    COUNT_QUEUE_IN_FLIGHT_HARD_SQL,
)
from horsies.core.worker.worker import Worker


INSERT_TEST_TASK_SQL = text("""
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, args, kwargs,
        status, sent_at, created_at, updated_at, claimed,
        retry_count, max_retries
    ) VALUES (
        :id, :task_name, :queue_name, 100, '[]', '{}',
        'PENDING', NOW(), NOW(), NOW(), FALSE, 0, 0
    )
""")


async def _seed_task(session: AsyncSession, queue_name: str = 'default') -> str:
    """Insert a minimal PENDING task row and return its id."""
    task_id = str(uuid.uuid4())
    await session.execute(INSERT_TEST_TASK_SQL, {
        'id': task_id,
        'task_name': 'cap_test_task',
        'queue_name': queue_name,
    })
    await session.commit()
    return task_id


# =============================================================================
# Expired claims must not consume cap budget
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestExpiredClaimsExcludedFromCap:
    """Expired CLAIMED tasks must not count toward in-flight cap checks."""

    async def test_expired_claim_not_counted_global(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
    ) -> None:
        """Expired CLAIMED task must not appear in COUNT_GLOBAL_IN_FLIGHT_SQL."""
        task_id = await _seed_task(session)

        # Set task as CLAIMED with expired lease (past timestamp)
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'CLAIMED',
                    claimed = TRUE,
                    claimed_at = NOW() - INTERVAL '10 minutes',
                    claimed_by_worker_id = 'dead-worker',
                    claim_expires_at = NOW() - INTERVAL '5 minutes',
                    updated_at = NOW()
                WHERE id = :task_id
            """),
            {'task_id': task_id},
        )
        await session.commit()

        # Count global in-flight — expired claim must be excluded
        result = (
            await session.execute(COUNT_GLOBAL_IN_FLIGHT_SQL)
        ).fetchone()
        assert result is not None
        in_flight_count = int(result[0])
        assert in_flight_count == 0, (
            f'Expired CLAIMED task should not count as in-flight, got {in_flight_count}'
        )

    async def test_expired_claim_not_counted_per_worker(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
    ) -> None:
        """Expired CLAIMED task must not count in per-worker cap checks."""
        task_id = await _seed_task(session)

        worker_id = 'test-worker-cap'
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'CLAIMED',
                    claimed = TRUE,
                    claimed_at = NOW() - INTERVAL '10 minutes',
                    claimed_by_worker_id = :wid,
                    claim_expires_at = NOW() - INTERVAL '5 minutes',
                    updated_at = NOW()
                WHERE id = :task_id
            """),
            {'task_id': task_id, 'wid': worker_id},
        )
        await session.commit()

        # Per-worker CLAIMED count — expired must be excluded
        claimed_result = (
            await session.execute(
                COUNT_CLAIMED_FOR_WORKER_SQL,
                {'wid': worker_id},
            )
        ).fetchone()
        assert claimed_result is not None
        assert int(claimed_result[0]) == 0, 'Expired claim must not count'

        # Per-worker in-flight count — expired must be excluded
        inflight_result = (
            await session.execute(
                COUNT_IN_FLIGHT_FOR_WORKER_SQL,
                {'wid': worker_id},
            )
        ).fetchone()
        assert inflight_result is not None
        assert int(inflight_result[0]) == 0, 'Expired claim must not count as in-flight'

    async def test_active_claim_still_counted(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
    ) -> None:
        """Active (non-expired) CLAIMED task must still count toward cap."""
        task_id = await _seed_task(session)

        # Set task as CLAIMED with future lease (still active)
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'CLAIMED',
                    claimed = TRUE,
                    claimed_at = NOW(),
                    claimed_by_worker_id = 'active-worker',
                    claim_expires_at = NOW() + INTERVAL '5 minutes',
                    updated_at = NOW()
                WHERE id = :task_id
            """),
            {'task_id': task_id},
        )
        await session.commit()

        # Count global in-flight — active claim MUST be counted
        result = (
            await session.execute(COUNT_GLOBAL_IN_FLIGHT_SQL)
        ).fetchone()
        assert result is not None
        in_flight_count = int(result[0])
        assert in_flight_count == 1, (
            f'Active CLAIMED task must count as in-flight, got {in_flight_count}'
        )

    async def test_expired_claim_not_counted_in_queue_hard_cap(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
    ) -> None:
        """Queue-scoped in-flight count must exclude expired CLAIMED rows."""
        expired_default_id = await _seed_task(session, queue_name='default')
        active_default_id = await _seed_task(session, queue_name='default')
        active_other_queue_id = await _seed_task(session, queue_name='critical')

        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'CLAIMED',
                    claimed = TRUE,
                    claimed_at = NOW() - INTERVAL '10 minutes',
                    claimed_by_worker_id = 'dead-worker',
                    claim_expires_at = NOW() - INTERVAL '5 minutes',
                    updated_at = NOW()
                WHERE id = :task_id
            """),
            {'task_id': expired_default_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'CLAIMED',
                    claimed = TRUE,
                    claimed_at = NOW(),
                    claimed_by_worker_id = 'live-worker-default',
                    claim_expires_at = NOW() + INTERVAL '5 minutes',
                    updated_at = NOW()
                WHERE id = :task_id
            """),
            {'task_id': active_default_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'CLAIMED',
                    claimed = TRUE,
                    claimed_at = NOW(),
                    claimed_by_worker_id = 'live-worker-critical',
                    claim_expires_at = NOW() + INTERVAL '5 minutes',
                    updated_at = NOW()
                WHERE id = :task_id
            """),
            {'task_id': active_other_queue_id},
        )
        await session.commit()

        default_result = (
            await session.execute(
                COUNT_QUEUE_IN_FLIGHT_HARD_SQL,
                {'q': 'default'},
            )
        ).fetchone()
        assert default_result is not None
        assert int(default_result[0]) == 1, (
            'Expected only active default-queue claim to count in hard cap'
        )

        critical_result = (
            await session.execute(
                COUNT_QUEUE_IN_FLIGHT_HARD_SQL,
                {'q': 'critical'},
            )
        ).fetchone()
        assert critical_result is not None
        assert int(critical_result[0]) == 1, (
            'Expected active critical-queue claim to count in hard cap'
        )


# =============================================================================
# Behavioral reclaim-under-cap: advisory lock → cap check → CLAIM_SQL
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestReclaimUnderCap:
    """Exercises reclaim-under-cap behavior through the worker claim loop.

    Scenario:
    1. PENDING task exists
    2. A second task is CLAIMED with an expired lease (dead worker)
    3. cluster_wide_cap=1 means only 1 slot available
    4. Worker claim loop must still claim dispatchable work (expired claims excluded from cap)
    """

    async def test_worker_claim_loop_claims_pending_under_cap_with_expired_claim_present(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
    ) -> None:
        """With cap=1 and an expired claim, worker still claims the pending task."""
        # Seed PENDING task and force it to be first in claim order.
        pending_task_id = await _seed_task(session)
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET sent_at = NOW() - INTERVAL '30 minutes'
                WHERE id = :task_id
            """),
            {'task_id': pending_task_id},
        )

        # Seed second task as expired CLAIMED (dead owner), later sent_at than pending.
        expired_task_id = await _seed_task(session)
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'CLAIMED',
                    claimed = TRUE,
                    claimed_at = NOW() - INTERVAL '10 minutes',
                    claimed_by_worker_id = 'dead-worker',
                    claim_expires_at = NOW() - INTERVAL '5 minutes',
                    sent_at = NOW() - INTERVAL '20 minutes',
                    updated_at = NOW()
                WHERE id = :task_id
            """),
            {'task_id': expired_task_id},
        )
        await session.commit()

        cfg = WorkerConfig(
            dsn=broker.config.database_url,
            psycopg_dsn=broker.listener.database_url,
            queues=['default'],
            processes=1,
            max_claim_batch=1,
            max_claim_per_worker=1,
            cluster_wide_cap=1,
        )
        worker = Worker(
            session_factory=broker.session_factory,
            listener=MagicMock(),
            cfg=cfg,
        )

        dispatched_ids: list[str] = []

        async def _fake_dispatch(
            task_id: str,
            task_name: str,
            args_json: str | None,
            kwargs_json: str | None,
        ) -> None:
            _ = (task_name, args_json, kwargs_json)
            dispatched_ids.append(task_id)

        worker._dispatch_one = AsyncMock(side_effect=_fake_dispatch)  # type: ignore[method-assign]

        claimed_any = await worker._claim_and_dispatch_all()
        assert claimed_any is True
        assert pending_task_id in dispatched_ids, (
            f'Expected pending task {pending_task_id} to be claimed and dispatched, '
            f'got dispatched={dispatched_ids}'
        )

        pending_row = (
            await session.execute(
                text("""
                    SELECT status, claimed_by_worker_id
                    FROM horsies_tasks
                    WHERE id = :task_id
                """),
                {'task_id': pending_task_id},
            )
        ).fetchone()
        assert pending_row is not None
        assert pending_row[0] == 'CLAIMED'
        assert pending_row[1] == worker.worker_instance_id

    async def test_expired_claim_reclaimed_by_claim_sql(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
    ) -> None:
        """CLAIM_SQL's reclaim branch picks up expired CLAIMED task directly."""
        # Seed a task and mark it CLAIMED with expired lease
        expired_task_id = await _seed_task(session)
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'CLAIMED',
                    claimed = TRUE,
                    claimed_at = NOW() - INTERVAL '10 minutes',
                    claimed_by_worker_id = 'dead-worker',
                    claim_expires_at = NOW() - INTERVAL '5 minutes',
                    updated_at = NOW()
                WHERE id = :task_id
            """),
            {'task_id': expired_task_id},
        )
        await session.commit()

        live_worker_id = 'reclaim-worker'

        # Run CLAIM_SQL — should reclaim the expired task
        claim_result = await session.execute(
            CLAIM_SQL,
            {
                'queue': 'default',
                'lim': 1,
                'worker_id': live_worker_id,
                'claim_expires_at': datetime.now(timezone.utc) + timedelta(seconds=60),
            },
        )
        claimed_rows = claim_result.fetchall()
        claimed_ids = [row[0] for row in claimed_rows]

        assert expired_task_id in claimed_ids, (
            f'Expired CLAIMED task {expired_task_id} was not reclaimed. '
            f'Claimed: {claimed_ids}'
        )

        await session.commit()

        # Verify the task is now claimed by the live worker
        verify_result = await session.execute(
            text("""
                SELECT status, claimed_by_worker_id
                FROM horsies_tasks
                WHERE id = :task_id
            """),
            {'task_id': expired_task_id},
        )
        row = verify_result.fetchone()
        assert row is not None
        assert row[0] == 'CLAIMED', f'Expected CLAIMED, got {row[0]}'
        assert row[1] == live_worker_id, (
            f'Expected owner {live_worker_id}, got {row[1]}'
        )
