"""Claim pipeline: budgeted, capped, ordered claiming per queue and class.

ClaimMixin runs the claim-and-dispatch pass: pass the capacity of each
budget of the claim plan and the plan's routes to horsies_claim, which
serializes cap accounting under scoped advisory locks when caps demand it
and claims one stream per (queue, execution class); filter non-runnable
workflow tasks; return rows whose class does not fit their registered task;
and hand the rest to dispatch. Worker-internal mixin: the ``TYPE_CHECKING``
block declares the slice of ``Worker`` it relies on.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Optional, assert_never

from horsies.core.defaults import DEFAULT_CLAIM_LEASE_MS
from horsies.core.lifecycle.commands import AbandonOwnedNodes, CancelOwnedNodes
from horsies.core.lifecycle.fences import OwnedClaimBatch
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    ObservedWorkflowState,
    SourceStateConflict,
    TaskAbsent,
    TerminalizationOutcome,
)
from horsies.core.lifecycle.persistence import apply_batch_async
from horsies.core.logging import get_logger
from horsies.core.models.tasks import Execution
from horsies.core.worker.claim_plan import (
    BudgetCapacity,
    budgets_json,
    routes_json,
)
from horsies.core.worker.row_class import (
    RowClassFits,
    RowClassMismatch,
    TaskNotRegistered,
    check_row_class,
    parse_row_class,
)
from horsies.core.worker.stored_timeout import (
    NoTimeout,
    StoredTimeout,
    read_stored_timeout,
)
from horsies.core.worker.sql import (
    CLAIM_SQL,
    COUNT_CLAIMED_FOR_WORKER_SQL,
    COUNT_IN_FLIGHT_FOR_WORKER_SQL,
    COUNT_RUNNING_FOR_WORKER_SQL,
    GET_NONRUNNABLE_WORKFLOW_TASK_IDS_SQL,
    HORSIES_CLAIM_SQL,
    NOTIFY_TASK_QUEUE_SQL,
    RESET_PAUSED_WORKFLOW_TASKS_SQL,
    RETURN_CLAIMED_TASK_AS_ISOLATED_SQL,
    SKIP_CANCELLED_WORKFLOW_TASKS_SQL,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from horsies.core.worker.config import WorkerConfig

logger = get_logger('worker')


def _newly_terminalized_task_ids(
    outcomes: list[TerminalizationOutcome],
) -> list[str]:
    """Rows whose coupled workflow-task write belongs to this transaction."""
    task_ids: list[str] = []
    for outcome in outcomes:
        match outcome:
            case Applied(task_id=task_id):
                task_ids.append(task_id)
            case (
                AlreadyApplied()
                | LostClaim()
                | SourceStateConflict()
                | TaskAbsent()
            ):
                continue
            case _ as unreachable:
                assert_never(unreachable)
    return task_ids


def _resumed_before_abandon_task_ids(
    outcomes: list[TerminalizationOutcome],
) -> set[str]:
    """Claims the pause abandon refused because the workflow runs again.

    A refusal whose workflow is CANCELLED stays blocked: the cancellation
    path owns that row.
    """
    task_ids: set[str] = set()
    for outcome in outcomes:
        match outcome:
            case SourceStateConflict(
                task_id=task_id,
                evidence=ObservedWorkflowState(workflow_status=status),
            ) if status != 'CANCELLED':
                task_ids.add(task_id)
            case (
                Applied()
                | AlreadyApplied()
                | LostClaim()
                | SourceStateConflict()
                | TaskAbsent()
            ):
                continue
            case _ as unreachable:
                assert_never(unreachable)
    return task_ids


@dataclass(frozen=True, slots=True)
class ClaimPass:
    """What one claim pass did.

    ``claimed`` counts the rows that the claim statement took. ``dropped`` counts
    the rows of those that the post-claim checks gave up: rows of a paused or
    cancelled workflow, and rows returned to the queue because their class
    does not fit the registered task. They used claim budget and hold no slot.
    """

    claimed: int
    dropped: int

    def left_unannounced_work(self, *, limited: bool) -> bool:
        """Whether ready work can remain that no wake would announce.

        A dropped row used budget that ready work could have had, and it frees
        no slot, so no capacity release follows. A pass under ``max_claim_batch``
        can stop with free slots. In both cases the NOTIFY of that work was
        already consumed. A row of a paused workflow cannot be claimed again
        until a resume, and a row returned for its class is ISOLATED, which
        always fits, so a loop on this ends.

        Args:
            limited: Whether ``max_claim_batch`` caps the pass.
        """
        return self.dropped > 0 or (limited and self.claimed > 0)


class ClaimMixin:
    """Budgeted claim pass, queue caps, lease math, advisory claim mutex."""

    if TYPE_CHECKING:
        # Worker state this mixin reads.
        sf: async_sessionmaker[AsyncSession]
        cfg: WorkerConfig
        worker_instance_id: str
        _registered_classes: Mapping[str, Execution]

        # Cross-concern methods provided by sibling mixins / Worker.
        def _claim_slot_count(self) -> int: ...
        def _shared_async_seats_in_service(self) -> int: ...
        def _shared_thread_seats_in_service(self) -> int: ...
        async def _dispatch_one(
            self,
            task_id: str,
            task_name: str,
            args_json: Optional[str],
            kwargs_json: Optional[str],
            queue_name: str = 'default',
            is_workflow_task: bool = True,
            timeout: StoredTimeout = NoTimeout(),
            claimed_at: Optional[datetime] = None,
            *,
            execution: Execution,
            task_options: str | None,
        ) -> None: ...

    async def _claim_and_dispatch_all(self) -> ClaimPass:
        """
        Claim tasks subject to:
          - the budget of each execution class: the classes that the claim
            plan routes to a budget count against its capacity in service
            (``_claim_capacity``)
          - max_claim_per_worker guard (prevents over-claiming)
          - queue priorities (CUSTOM mode)
          - per-queue max_concurrency (CUSTOM mode)

        Raises:
            Exception: DB errors propagate to the main loop, which
                backoff-retries transient connection errors and crashes the
                worker on anything else. Rows already committed CLAIMED when
                a later step fails are recovered by claim-lease expiry; with
                the worker still alive, the claimer heartbeat renews their
                leases until max_claim_renew_age_ms caps it, so recovery
                waits out renewal-cap + lease (the reaper's stale-claimed
                path needs a dead claimer heartbeat and does not apply).
        """
        # TIER 1: the per-pass critical section (advisory-lock acquisition + cap
        # counts + budget math + windowed claim) is one server-side statement
        # (horsies_claim). The xact-scoped advisory lock is held only across that
        # statement plus this transaction's COMMIT — never across a client round
        # trip, so a stalled worker cannot freeze the cluster mid-pass. Budget
        # and cap semantics are computed in-function; see
        # schemas/migrations.py::horsies_claim.
        params = self._claim_params()
        async with self.sf() as s:
            res = await s.execute(HORSIES_CLAIM_SQL, params)
            cols = res.keys()
            claimed_rows: list[dict[str, Any]] = [
                dict(zip(cols, row)) for row in res.fetchall()
            ]
            await s.commit()

        if not claimed_rows:
            return ClaimPass(claimed=0, dropped=0)

        # Post-claim guard: filter out tasks for non-runnable workflow states.
        dispatchable_rows = await self._filter_nonrunnable_workflow_tasks(claimed_rows)
        fitting_rows = await self._return_rows_of_another_class(dispatchable_rows)

        for row in fitting_rows:
            await self._dispatch_one(
                row['id'],
                row['task_name'],
                row['args'],
                row['kwargs'],
                row.get('queue_name') or 'default',
                bool(row.get('is_workflow_task', False)),
                timeout=read_stored_timeout(row.get('task_options')),
                claimed_at=row.get('claimed_at'),
                execution=parse_row_class(str(row['id']), row['execution']),
                task_options=row.get('task_options'),
            )
        return ClaimPass(
            claimed=len(claimed_rows),
            dropped=len(claimed_rows) - len(fitting_rows),
        )

    def _claim_params(self) -> dict[str, Any]:
        """The parameters of HORSIES_CLAIM_SQL for one pass. Does no I/O.

        Per-queue caps are enforced for every queue in this pass that carries
        a max_concurrency entry — independent of queue_priorities — so
        exactly those go to the function and its cap filter matches 1:1.
        """
        ordered_queues = self._ordered_claim_queues()
        lock_keys = self._claim_lock_keys(ordered_queues)
        capped_queues = [
            qname for qname in ordered_queues if qname in self.cfg.queue_max_concurrency
        ]
        return {
            'p_worker_id': self.worker_instance_id,
            'p_queues': json.dumps(ordered_queues, allow_nan=False),
            'p_queue_priority': json.dumps(
                {q: self.cfg.queue_priorities.get(q, 100) for q in ordered_queues},
                allow_nan=False,
            ),
            'p_queue_max_concurrency': json.dumps(
                {q: int(self.cfg.queue_max_concurrency[q]) for q in capped_queues},
                allow_nan=False,
            ),
            'p_hard_cap_mode': self.cfg.prefetch_buffer == 0,
            'p_budgets': json.dumps(
                budgets_json(self._claim_capacity()),
                allow_nan=False,
            ),
            'p_routes': json.dumps(
                routes_json(self.cfg.claim_plan()),
                allow_nan=False,
            ),
            'p_prefetch_buffer': int(self.cfg.prefetch_buffer),
            'p_max_claim_per_worker': int(self.cfg.max_claim_per_worker),
            'p_max_claim_batch': int(self.cfg.max_claim_batch),
            'p_cluster_wide_cap': (
                int(self.cfg.cluster_wide_cap)
                if self.cfg.cluster_wide_cap is not None
                else None
            ),
            'p_lease_ms': int(self._claim_lease_ms()),
            'p_lock_keys': json.dumps(lock_keys, allow_nan=False),
        }

    def _claim_capacity(self) -> tuple[BudgetCapacity, ...]:
        """The capacity in service of each budget of the claim plan.

        Usage is not counted here: the claim function counts this worker's
        RUNNING and live CLAIMED rows of each budget. Reads only; a pass
        before the slot pool exists is a defect (``AssertionError``).
        """
        return tuple(
            BudgetCapacity(
                budget=budget,
                in_service=self._capacity_in_service(budget),
            )
            for budget in self.cfg.claim_plan().budgets()
        )

    def _capacity_in_service(self, budget: Execution) -> int:
        """The children or seats of ``budget`` that take a task now.

        This is the one place that maps a budget class to the children that
        serve it: the isolated slots that are not down for ``ISOLATED``, the
        seats of the shared async children in service for ``SHARED_ASYNC``
        (``_shared_async_seats_in_service``), and the seats of the shared
        thread children in service for ``SHARED_THREAD``
        (``_shared_thread_seats_in_service``). The plan's budget classes come
        from ``claim_plan_for_children`` (``WorkerConfig.claim_plan``).
        """
        match budget:
            case Execution.ISOLATED:
                return self._claim_slot_count()
            case Execution.SHARED_ASYNC:
                return self._shared_async_seats_in_service()
            case Execution.SHARED_THREAD:
                return self._shared_thread_seats_in_service()
            case _ as unreachable:
                assert_never(unreachable)

    async def _return_rows_of_another_class(
        self,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Return each row whose class does not fit its registered task.

        Such a row goes back to PENDING as ISOLATED, fenced to its claim
        generation (``_return_row_as_isolated``). Rows that fit, and rows of
        a task that this worker did not register, come back in their order.

        Raises:
            Exception: DB errors propagate to the main loop. This runs after
                the claim committed; rows not yet returned or dispatched are
                recovered by lease expiry.
        """
        fitting: list[dict[str, Any]] = []
        for row in rows:
            row_class = parse_row_class(str(row['id']), row['execution'])
            check = check_row_class(
                row_class,
                row['task_name'],
                self._registered_classes,
            )
            match check:
                case RowClassMismatch():
                    await self._return_row_as_isolated(row, check)
                case RowClassFits() | TaskNotRegistered():
                    fitting.append(row)
                case _ as unreachable:
                    assert_never(unreachable)
        return fitting

    async def _return_row_as_isolated(
        self,
        row: dict[str, Any],
        mismatch: RowClassMismatch,
    ) -> None:
        """Write ISOLATED on ``row`` and return it to PENDING, then wake the
        workers of its queue. Fenced to this worker and the row's claim
        generation: a row whose claim this worker lost is not touched, and is
        not dispatched either. Logs one warning per row.

        Raises:
            Exception: DB errors propagate to the caller.
        """
        queue_name = row.get('queue_name') or 'default'
        async with self.sf() as s:
            res = await s.execute(
                RETURN_CLAIMED_TASK_AS_ISOLATED_SQL,
                {
                    'id': row['id'],
                    'wid': self.worker_instance_id,
                    'claimed_at': row['claimed_at'],
                },
            )
            returned = (getattr(res, 'rowcount', 0) or 0) > 0
            if returned:
                await s.execute(
                    NOTIFY_TASK_QUEUE_SQL,
                    {'c2': f'task_queue_{queue_name}', 'p': f'class:{row["id"]}'},
                )
            await s.commit()
        logger.warning(
            'Task %s (%s) stores execution %s, and the registered function '
            'is %s on this worker; %s',
            row['id'],
            row['task_name'],
            mismatch.row_class.value,
            mismatch.registered_class.value,
            (
                'wrote ISOLATED on the row and returned it to the queue'
                if returned
                else 'the claim was lost, so the row was left as it is'
            ),
        )

    async def _filter_nonrunnable_workflow_tasks(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Filter out tasks belonging to non-runnable workflows (PAUSED/CANCELLED).

        Post-claim guard:
        - PAUSED workflow: cancel claimed task row, reset workflow_task to READY
        - CANCELLED workflow: hard-cancel task + mark workflow_task SKIPPED

        Returns the filtered list of rows that should be dispatched.

        Raises:
            Exception: DB errors propagate to the main loop (runs after the
                claim transaction committed; the claimed batch is recovered
                by lease expiry if this step fails).
        """
        if not rows:
            return rows

        workflow_rows = [
            row for row in rows if bool(row.get('is_workflow_task', True))
        ]
        if not workflow_rows:
            return rows

        task_ids = [row['id'] for row in workflow_rows]
        # The claim generation each row was dispatched from. This guard runs
        # after the claim transaction committed, so it fences the abandon to
        # that generation (C10); without it a lapsed-and-re-claimed row is
        # still CLAIMED by this worker and would be cancelled out from under
        # its new claim.
        claimed_at_by_task_id: dict[str, Any] = {
            row['id']: row.get('claimed_at') for row in workflow_rows
        }
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
                paused_ids = list(paused_task_ids)
                paused_outcomes = await apply_batch_async(
                    await s.connection(),
                    AbandonOwnedNodes(
                        fence=OwnedClaimBatch(
                            worker_id=self.worker_instance_id,
                            claim_generations=tuple(
                                (
                                    task_id,
                                    claimed_at_by_task_id.get(task_id),
                                )
                                for task_id in paused_ids
                            ),
                        ),
                    ),
                )
                unclaimed_paused_task_ids = _newly_terminalized_task_ids(
                    paused_outcomes,
                )
                # A resume that committed after the lookup makes the abandon
                # refuse; those claims are runnable again and are dispatched.
                paused_task_ids -= _resumed_before_abandon_task_ids(
                    paused_outcomes,
                )
                # Keep workflow_task metadata consistent with unclaimed tasks.
                if unclaimed_paused_task_ids:
                    await s.execute(
                        RESET_PAUSED_WORKFLOW_TASKS_SQL,
                        {'ids': unclaimed_paused_task_ids},
                    )

            if cancelled_task_ids:
                # Cancel this worker's claimed task rows so they are no longer claimable.
                cancelled_ids = list(cancelled_task_ids)
                cancelled_outcomes = await apply_batch_async(
                    await s.connection(),
                    CancelOwnedNodes(
                        fence=OwnedClaimBatch(
                            worker_id=self.worker_instance_id,
                            claim_generations=tuple(
                                (
                                    task_id,
                                    claimed_at_by_task_id.get(task_id),
                                )
                                for task_id in cancelled_ids
                            ),
                        ),
                    ),
                )
                cancelled_owned_task_ids = _newly_terminalized_task_ids(
                    cancelled_outcomes,
                )
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

    def _ordered_claim_queues(self) -> list[str]:
        """Queues this pass claims from, in claim order.

        CUSTOM mode (any priority OR cap configured) sorts by priority and
        keeps queues that carry a priority OR a max_concurrency cap; a
        cap-only queue sorts into the lowest priority bucket (100) so its
        cap is enforced rather than the queue being silently starved.
        DEFAULT mode keeps the configured order. The claim loop and
        `_claim_lock_keys` must consume the SAME list so the pass never
        locks a queue it will not claim from.
        """
        if self.cfg.queue_priorities or self.cfg.queue_max_concurrency:
            return sorted(
                [
                    q
                    for q in self.cfg.queues
                    if q in self.cfg.queue_priorities
                    or q in self.cfg.queue_max_concurrency
                ],
                key=lambda q: self.cfg.queue_priorities.get(q, 100),
            )
        return list(self.cfg.queues)

    def _claim_lock_keys(self, ordered_queues: list[str]) -> list[int]:
        """Advisory keys the claim pass must hold, in acquisition order.

        Serialization exists only for read-then-act cap accounting:
        - cluster_wide_cap: the invariant is global, so the single global
          key (per-queue scoping cannot help; it also subsumes any queue
          caps).
        - per-queue max_concurrency: one key per capped queue in THIS
          pass, sorted by key value so concurrent workers acquiring
          multiple keys cannot deadlock. Workers claiming disjoint capped
          queues no longer contend.
        - no caps: empty (SKIP LOCKED alone is safe).

        Workers in a capped cluster must share the same cap config — a
        mixed fleet already breaks cap semantics regardless of locking.
        Version skew has the same shape: during a rolling deploy, old
        workers (global key) and new workers (queue keys) do not contend,
        so a queue cap can briefly overshoot by one pass's batch.
        """
        if self.cfg.cluster_wide_cap is not None:
            return [self._advisory_key_global()]
        if not self.cfg.queue_max_concurrency:
            return []
        return sorted(
            self._advisory_key_for_queue(queue_name)
            for queue_name in ordered_queues
            if queue_name in self.cfg.queue_max_concurrency
        )

    def _advisory_key_for_queue(self, queue_name: str) -> int:
        """Stable 64-bit advisory key scoping claim serialization to one queue.

        Same fixed-namespace discipline as `_advisory_key_global` (a
        DSN-derived ingredient would silently split the lock between
        workers using different DSN spellings of the same database).
        """
        basis = b'horsies:claim:queue:v1:' + queue_name.encode('utf-8')
        h = hashlib.sha256(basis).digest()
        return int.from_bytes(h[:8], byteorder='big', signed=True)

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

        DB errors propagate to the claim pass (the caller owns the
        transaction).
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

    async def _count_claimed_for_worker(self, session: AsyncSession) -> int:
        """Count only CLAIMED tasks for this worker (not yet RUNNING).

        DB errors propagate to the caller's containment (claim pass or
        health snapshot).
        """
        res = await session.execute(
            COUNT_CLAIMED_FOR_WORKER_SQL,
            {'wid': self.worker_instance_id},
        )
        row = res.fetchone()
        return int(row.cnt) if row else 0

    async def _count_only_running_for_worker(self, session: AsyncSession) -> int:
        """Count only RUNNING tasks for this worker (excludes CLAIMED).

        DB errors propagate to the caller's containment (claim pass or
        health snapshot).
        """
        res = await session.execute(
            COUNT_RUNNING_FOR_WORKER_SQL,
            {'wid': self.worker_instance_id},
        )
        row = res.fetchone()
        return int(row.cnt) if row else 0

    async def _count_in_flight_for_worker(self, session: AsyncSession) -> int:
        """Count RUNNING + CLAIMED tasks for this worker (hard cap mode).

        DB errors propagate to the claim pass.
        """
        res = await session.execute(
            COUNT_IN_FLIGHT_FOR_WORKER_SQL,
            {'wid': self.worker_instance_id},
        )
        row = res.fetchone()
        return int(row.cnt) if row else 0
