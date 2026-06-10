"""Claim pipeline: budgeted, capped, ordered claiming per queue.

ClaimMixin runs the claim-and-dispatch pass: compute the worker budget,
serialize accounting under the cluster advisory mutex when caps demand
it, claim per-queue batches with CLAIM_SQL, filter non-runnable
workflow tasks, and hand rows to dispatch. Worker-internal mixin: the
``TYPE_CHECKING`` block declares the slice of ``Worker`` it relies on.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Optional

from horsies.core.defaults import DEFAULT_CLAIM_LEASE_MS
from horsies.core.logging import get_logger
from horsies.core.worker.runtime import _parse_timeout_ms
from horsies.core.worker.sql import (
    CANCEL_CANCELLED_WORKFLOW_TASKS_SQL,
    CLAIM_ADVISORY_LOCK_SQL,
    CLAIM_SQL,
    COUNT_CLAIMED_FOR_WORKER_SQL,
    COUNT_GLOBAL_IN_FLIGHT_SQL,
    COUNT_IN_FLIGHT_FOR_WORKER_SQL,
    COUNT_QUEUE_IN_FLIGHT_HARD_SQL,
    COUNT_QUEUE_IN_FLIGHT_SOFT_SQL,
    COUNT_RUNNING_FOR_WORKER_SQL,
    COUNT_RUNNING_IN_QUEUE_SQL,
    GET_NONRUNNABLE_WORKFLOW_TASK_IDS_SQL,
    RESET_PAUSED_WORKFLOW_TASKS_SQL,
    SKIP_CANCELLED_WORKFLOW_TASKS_SQL,
    UNCLAIM_PAUSED_TASKS_SQL,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from horsies.core.worker.config import WorkerConfig

logger = get_logger('worker')


class ClaimMixin:
    """Budgeted claim pass, queue caps, lease math, advisory claim mutex."""

    if TYPE_CHECKING:
        # Worker state this mixin reads.
        sf: async_sessionmaker[AsyncSession]
        cfg: WorkerConfig
        worker_instance_id: str

        # Cross-concern methods provided by sibling mixins / Worker.
        async def _dispatch_one(
            self,
            task_id: str,
            task_name: str,
            args_json: Optional[str],
            kwargs_json: Optional[str],
            queue_name: str = 'default',
            is_workflow_task: bool = True,
            timeout_ms: Optional[int] = None,
        ) -> None: ...

    async def _claim_and_dispatch_all(self) -> bool:
        """
        Claim tasks subject to:
          - max_claim_per_worker guard (prevents over-claiming)
          - queue priorities (CUSTOM mode)
          - per-queue max_concurrency (CUSTOM mode)
          - worker global concurrency (processes)
        Returns True if anything was claimed.
        """
        # Guard: Check if we've already claimed too many tasks
        # Default depends on mode:
        # - Hard cap (prefetch_buffer=0): default to processes
        # - Soft cap (prefetch_buffer>0): default to processes + prefetch_buffer
        if self.cfg.max_claim_per_worker > 0:
            # User explicitly set a limit - use it
            max_claimed = self.cfg.max_claim_per_worker
        elif self.cfg.prefetch_buffer > 0:
            # Soft cap mode: allow claiming up to processes + prefetch_buffer
            max_claimed = self.cfg.processes + self.cfg.prefetch_buffer
        else:
            # Hard cap mode: limit to processes
            max_claimed = self.cfg.processes
        # Cluster-wide, lock-guarded claim to avoid races. One short transaction.
        # CLAIM_SQL RETURNING provides dispatch payload directly (no separate load query).
        claimed_rows: list[dict[str, Any]] = []

        # Queue order: if custom priorities provided, sort by priority; otherwise keep given order
        if self.cfg.queue_priorities:
            ordered_queues = sorted(
                [q for q in self.cfg.queues if q in self.cfg.queue_priorities],
                key=lambda q: self.cfg.queue_priorities.get(q, 100),
            )
        else:
            ordered_queues = list(self.cfg.queues)

        # Open one transaction; serialize claim passes only when a
        # multi-worker read-then-act invariant exists (cluster/queue caps).
        # Without caps, CLAIM_SQL's FOR UPDATE SKIP LOCKED already makes
        # concurrent claiming safe, and the lock would only cap cluster
        # claim throughput at 1/claim-pass-latency.
        async with self.sf() as s:
            if self._claim_pass_needs_serialization():
                await s.execute(
                    CLAIM_ADVISORY_LOCK_SQL,
                    {'key': self._advisory_key_global()},
                )

            # Compute local budget and optional global remaining
            # Hard cap mode (prefetch_buffer=0): count RUNNING + CLAIMED for strict enforcement
            # Soft cap mode (prefetch_buffer>0): count only RUNNING, allow prefetch with lease
            hard_cap_mode = self.cfg.prefetch_buffer == 0
            claimed_count = await self._count_claimed_for_worker(s)
            if claimed_count >= max_claimed:
                await s.commit()
                return False
            remaining_claim_allowance = max(0, int(max_claimed) - int(claimed_count))

            if hard_cap_mode:
                # Hard cap: count both RUNNING and CLAIMED for this worker
                local_in_flight = await self._count_in_flight_for_worker(s)
                max_local_capacity = self.cfg.processes
            else:
                # Soft cap: queue/global caps count only RUNNING, but local
                # prefetch budget must include already CLAIMED rows so a worker
                # cannot hoard beyond processes + prefetch_buffer.
                local_in_flight = await self._count_only_running_for_worker(s)
                max_local_capacity = self.cfg.processes + self.cfg.prefetch_buffer
            local_available = max(
                0,
                int(max_local_capacity)
                - int(local_in_flight)
                - (0 if hard_cap_mode else int(claimed_count)),
            )
            budget_remaining = local_available

            global_remaining: Optional[int] = None
            if self.cfg.cluster_wide_cap is not None:
                # Hard cap mode: count RUNNING + CLAIMED globally
                # (Note: prefetch_buffer must be 0 when cluster_wide_cap is set, enforced by config validation)
                res = await s.execute(COUNT_GLOBAL_IN_FLIGHT_SQL)
                row = res.fetchone()
                if row:
                    in_flight_global = int(row.cnt)
                else:
                    in_flight_global = 0
                global_remaining = max(
                    0, int(self.cfg.cluster_wide_cap) - in_flight_global
                )

            # Total claim budget for this pass: local budget capped by global remaining (if any)
            total_remaining = min(budget_remaining, remaining_claim_allowance)
            if global_remaining is not None:
                total_remaining = min(total_remaining, global_remaining)
            if total_remaining <= 0:
                # Nothing to claim globally or locally
                await s.commit()
                return False

            for qname in ordered_queues:
                if total_remaining <= 0:
                    break

                # Compute queue remaining in cluster (only if custom-configured)
                q_remaining: Optional[int] = None
                if (
                    self.cfg.queue_priorities
                    and qname in self.cfg.queue_max_concurrency
                ):
                    # Hard cap mode: count RUNNING + CLAIMED for this queue
                    # Soft cap mode: count only RUNNING
                    if hard_cap_mode:
                        resq = await s.execute(
                            COUNT_QUEUE_IN_FLIGHT_HARD_SQL,
                            {'q': qname},
                        )
                    else:
                        resq = await s.execute(
                            COUNT_QUEUE_IN_FLIGHT_SOFT_SQL,
                            {'q': qname},
                        )
                    row = resq.fetchone()
                    if row:
                        in_flight_q = int(row.cnt)
                    else:
                        in_flight_q = 0
                    max_q = int(self.cfg.queue_max_concurrency.get(qname, 0))
                    q_remaining = max(0, max_q - in_flight_q)

                # Determine how many we may claim from this queue.
                # A positive max_claim_batch is an explicit fairness cap. The
                # default 0 means fill the remaining worker/queue budget.

                if self.cfg.max_claim_batch > 0:
                    per_queue_cap = self.cfg.max_claim_batch
                elif self.cfg.queue_priorities:
                    # Strict priority mode: try to fill remaining budget from this queue
                    per_queue_cap = total_remaining
                else:
                    per_queue_cap = total_remaining

                if q_remaining is not None:
                    per_queue_cap = min(per_queue_cap, q_remaining)
                to_claim = min(total_remaining, per_queue_cap)
                if to_claim <= 0:
                    continue

                batch_rows = await self._claim_batch_locked(s, qname, to_claim)
                if not batch_rows:
                    continue
                claimed_rows.extend(batch_rows)
                total_remaining -= len(batch_rows)

            await s.commit()

        if not claimed_rows:
            return False

        # Post-claim guard: filter out tasks for non-runnable workflow states.
        claimed_rows = await self._filter_nonrunnable_workflow_tasks(claimed_rows)

        for row in claimed_rows:
            await self._dispatch_one(
                row['id'],
                row['task_name'],
                row['args'],
                row['kwargs'],
                row.get('queue_name') or 'default',
                bool(row.get('is_workflow_task', False)),
                timeout_ms=_parse_timeout_ms(row.get('task_options'), row['id']),
            )
        return len(claimed_rows) > 0

    async def _filter_nonrunnable_workflow_tasks(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Filter out tasks belonging to non-runnable workflows (PAUSED/CANCELLED).

        Post-claim guard:
        - PAUSED workflow: cancel claimed task row, reset workflow_task to READY
        - CANCELLED workflow: hard-cancel task + mark workflow_task SKIPPED

        Returns the filtered list of rows that should be dispatched.
        """
        if not rows:
            return rows

        workflow_rows = [
            row for row in rows if bool(row.get('is_workflow_task', True))
        ]
        if not workflow_rows:
            return rows

        task_ids = [row['id'] for row in workflow_rows]
        paused_task_ids: set[str] = set()
        cancelled_task_ids: set[str] = set()

        # Find tasks belonging to non-runnable workflows.
        async with self.sf() as s:
            res = await s.execute(
                GET_NONRUNNABLE_WORKFLOW_TASK_IDS_SQL,
                {'ids': task_ids},
            )
            for row in res.fetchall():
                task_id = row.id
                wf_status = row.status
                if wf_status == 'PAUSED':
                    paused_task_ids.add(task_id)
                elif wf_status == 'CANCELLED':
                    cancelled_task_ids.add(task_id)

            if paused_task_ids:
                # Unclaim paused-workflow tasks so they can be picked up on resume.
                paused_res = await s.execute(
                    UNCLAIM_PAUSED_TASKS_SQL,
                    {'ids': list(paused_task_ids), 'wid': self.worker_instance_id},
                )
                unclaimed_paused_task_ids = [
                    str(task_id) for task_id in paused_res.scalars().all()
                ]
                # Keep workflow_task metadata consistent with unclaimed tasks.
                if unclaimed_paused_task_ids:
                    await s.execute(
                        RESET_PAUSED_WORKFLOW_TASKS_SQL,
                        {'ids': unclaimed_paused_task_ids},
                    )

            if cancelled_task_ids:
                # Cancel this worker's claimed task rows so they are no longer claimable.
                cancelled_res = await s.execute(
                    CANCEL_CANCELLED_WORKFLOW_TASKS_SQL,
                    {'ids': list(cancelled_task_ids), 'wid': self.worker_instance_id},
                )
                cancelled_owned_task_ids = [
                    str(task_id) for task_id in cancelled_res.scalars().all()
                ]
                # Ensure workflow_task rows no longer sit in enqueueable states.
                if cancelled_owned_task_ids:
                    await s.execute(
                        SKIP_CANCELLED_WORKFLOW_TASKS_SQL,
                        {'ids': cancelled_owned_task_ids},
                    )

            if paused_task_ids or cancelled_task_ids:
                await s.commit()

        blocked_task_ids = paused_task_ids | cancelled_task_ids
        return [row for row in rows if row['id'] not in blocked_task_ids]

    def _claim_pass_needs_serialization(self) -> bool:
        """Whether the claim pass must hold the cluster advisory lock.

        Serialization is required only for read-then-act cap accounting:
        a cluster_wide_cap, or an active per-queue max_concurrency (CUSTOM
        mode with a configured queue this worker claims from). Workers in a
        capped cluster must share the same cap config — a mixed fleet
        already breaks cap semantics regardless of locking.
        """
        if self.cfg.cluster_wide_cap is not None:
            return True
        if not self.cfg.queue_priorities:
            return False
        return any(
            queue_name in self.cfg.queue_max_concurrency
            for queue_name in self.cfg.queues
        )

    def _advisory_key_global(self) -> int:
        """Compute a stable 64-bit advisory lock key for claim serialization.

        PostgreSQL advisory locks are scoped to the current database, so a
        fixed key serializes all Horsies claim passes per database. A
        DSN-derived key (pre-0.1.7) silently split the lock when workers
        reached the same database through different DSN spellings (host vs
        IP, PgBouncer vs direct), letting cap accounting race.
        """
        h = hashlib.sha256(b'horsies:claim:v1').digest()
        return int.from_bytes(h[:8], byteorder='big', signed=True)

    def _claim_lease_ms(self) -> int:
        """Return the bounded claim lease duration in milliseconds.

        Uses explicit claim_lease_ms when configured (soft-cap or user override),
        otherwise falls back to DEFAULT_CLAIM_LEASE_MS for crash-recovery safety.
        The database computes the actual expiry timestamp.
        """
        return int(
            self.cfg.claim_lease_ms
            if self.cfg.claim_lease_ms is not None
            else DEFAULT_CLAIM_LEASE_MS
        )

    def _compute_claim_expires_at(self) -> datetime:
        """Compatibility helper: local view of the configured claim lease.

        SQL writes use DB time; this remains for diagnostics/tests that need to
        inspect the effective lease duration without touching the database.
        """
        return datetime.now(timezone.utc) + timedelta(milliseconds=self._claim_lease_ms())

    async def _claim_batch_locked(
        self,
        s: AsyncSession,
        queue: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Claim up to *limit* tasks and return dispatch-ready row dicts.

        CLAIM_SQL RETURNING provides id/task_name/args/kwargs atomically,
        eliminating the previous claim-commit → separate-load gap.
        """
        res = await s.execute(
            CLAIM_SQL,
            {
                'queue': queue,
                'lim': limit,
                'worker_id': self.worker_instance_id,
                # Lease expiry is computed server-side (now() + lease);
                # no local-clock timestamp is passed.
                'claim_lease_ms': self._claim_lease_ms(),
            },
        )
        cols = res.keys()
        return [dict(zip(cols, row)) for row in res.fetchall()]

    async def _count_claimed_for_worker(self, session: AsyncSession | None = None) -> int:
        """Count only CLAIMED tasks for this worker (not yet RUNNING)."""
        if session is not None:
            res = await session.execute(
                COUNT_CLAIMED_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row.cnt) if row else 0
        async with self.sf() as s:
            res = await s.execute(
                COUNT_CLAIMED_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row.cnt) if row else 0

    async def _count_only_running_for_worker(
        self, session: AsyncSession | None = None
    ) -> int:
        """Count only RUNNING tasks for this worker (excludes CLAIMED)."""
        if session is not None:
            res = await session.execute(
                COUNT_RUNNING_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row.cnt) if row else 0
        async with self.sf() as s:
            res = await s.execute(
                COUNT_RUNNING_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row.cnt) if row else 0

    async def _count_in_flight_for_worker(
        self, session: AsyncSession | None = None
    ) -> int:
        """Count RUNNING + CLAIMED tasks for this worker (hard cap mode)."""
        if session is not None:
            res = await session.execute(
                COUNT_IN_FLIGHT_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row.cnt) if row else 0
        async with self.sf() as s:
            res = await s.execute(
                COUNT_IN_FLIGHT_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row.cnt) if row else 0

    async def _count_running_in_queue(self, queue_name: str) -> int:
        """Count RUNNING tasks in a given queue across the cluster."""
        async with self.sf() as s:
            res = await s.execute(
                COUNT_RUNNING_IN_QUEUE_SQL,
                {'q': queue_name},
            )
            row = res.fetchone()
            return int(row.cnt) if row else 0
