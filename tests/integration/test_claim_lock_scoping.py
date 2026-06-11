"""Integration tests: per-queue claim advisory lock scoping.

Deterministic (no two-worker timing races): one session holds a queue's
advisory xact lock directly, then a real claim pass either completes
(disjoint capped queue — must not contend) or blocks (same capped queue —
must contend) under a short asyncio timeout.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker

pytestmark = [pytest.mark.integration]

ACQUIRE_XACT_LOCK_SQL = text('SELECT pg_advisory_xact_lock(CAST(:key AS BIGINT))')

_PASS_TIMEOUT_S = 3.0
_BLOCK_PROOF_TIMEOUT_S = 1.0


def _make_worker(engine: AsyncEngine, *, queues: dict[str, int]) -> Worker:
    """Worker claiming the given capped queues (name -> max_concurrency)."""
    cfg = WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=list(queues),
        queue_priorities={name: 1 for name in queues},
        queue_max_concurrency=dict(queues),
    )
    return Worker(
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
        listener=MagicMock(),
        cfg=cfg,
    )


@pytest.mark.asyncio(loop_scope='function')
class TestClaimLockScoping:
    """Capped queues contend only with themselves."""

    async def test_disjoint_capped_queue_does_not_block(
        self,
        clean_workflow_tables: None,
        engine: AsyncEngine,
    ) -> None:
        """Holding capped queue A's lock must not block a pass over capped
        queue B — the regression this scoping exists to prevent (the old
        single global key serialized them)."""
        holder = _make_worker(engine, queues={'lock_qa': 1})
        passer = _make_worker(engine, queues={'lock_qb': 1})
        sf = async_sessionmaker(engine, expire_on_commit=False)

        async with sf() as holding_session:
            await holding_session.execute(
                ACQUIRE_XACT_LOCK_SQL,
                {'key': holder._advisory_key_for_queue('lock_qa')},
            )
            # Real claim pass over queue B while A's lock is held elsewhere.
            await asyncio.wait_for(
                passer._claim_and_dispatch_all(),
                timeout=_PASS_TIMEOUT_S,
            )
            await holding_session.rollback()

    async def test_same_capped_queue_blocks(
        self,
        clean_workflow_tables: None,
        engine: AsyncEngine,
    ) -> None:
        """Holding the SAME capped queue's lock must block the pass —
        proves the cap-accounting serialization is still real."""
        passer = _make_worker(engine, queues={'lock_qc': 1})
        sf = async_sessionmaker(engine, expire_on_commit=False)

        async with sf() as holding_session:
            await holding_session.execute(
                ACQUIRE_XACT_LOCK_SQL,
                {'key': passer._advisory_key_for_queue('lock_qc')},
            )
            pass_task = asyncio.create_task(passer._claim_and_dispatch_all())
            try:
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        asyncio.shield(pass_task),
                        timeout=_BLOCK_PROOF_TIMEOUT_S,
                    )
            finally:
                # Release the lock so the blocked pass can finish cleanly.
                await holding_session.rollback()
                await asyncio.wait_for(pass_task, timeout=_PASS_TIMEOUT_S)

    async def test_global_cap_still_blocks_across_queues(
        self,
        clean_workflow_tables: None,
        engine: AsyncEngine,
    ) -> None:
        """cluster_wide_cap keeps the single global key: a pass over any
        queue blocks while the global key is held (the invariant is
        global, so per-queue scoping must not apply)."""
        cfg = WorkerConfig(
            dsn='postgresql+psycopg://u:p@localhost/db',
            psycopg_dsn='postgresql://u:p@localhost/db',
            queues=['lock_qd'],
            cluster_wide_cap=5,
        )
        passer = Worker(
            session_factory=async_sessionmaker(engine, expire_on_commit=False),
            listener=MagicMock(),
            cfg=cfg,
        )
        sf = async_sessionmaker(engine, expire_on_commit=False)

        async with sf() as holding_session:
            await holding_session.execute(
                ACQUIRE_XACT_LOCK_SQL,
                {'key': passer._advisory_key_global()},
            )
            pass_task = asyncio.create_task(passer._claim_and_dispatch_all())
            try:
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        asyncio.shield(pass_task),
                        timeout=_BLOCK_PROOF_TIMEOUT_S,
                    )
            finally:
                await holding_session.rollback()
                await asyncio.wait_for(pass_task, timeout=_PASS_TIMEOUT_S)
