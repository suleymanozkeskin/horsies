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

import asyncio
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Final, Optional, assert_never

from horsies.core.defaults import DEFAULT_CLAIM_LEASE_MS
from horsies.core.lifecycle.outcomes import OutcomeDecodeError
from horsies.core.logging import get_logger
from horsies.core.models.tasks import Execution
from horsies.core.utils.db import (
    SingleStatement,
    StatementRunner,
    rows_or_raise,
    run_in_autocommit_result,
)
from horsies.core.worker.claim_plan import (
    BudgetCapacity,
    ClaimPlan,
    NotServed,
    ServedBy,
    budgets_json,
    routes_json,
)
from horsies.core.worker.dispatch_claim import DispatchClaim, dispatch_claim_of
from horsies.core.worker.queue_cap_hold import (
    HeldRow,
    QueueCapHold,
    read_queue_starts,
)
from horsies.core.worker.release import (
    HeldClaim,
    ReleaseOutcome,
    ReleasePlacement,
    release_claims_in_core,
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
    read_claimed_options,
)
from horsies.core.worker.sql import (
    CLAIM_SQL,
    COUNT_CLAIMED_FOR_WORKER_SQL,
    COUNT_IN_FLIGHT_FOR_WORKER_SQL,
    COUNT_QUEUE_STARTS_SQL,
    COUNT_RUNNING_FOR_WORKER_SQL,
    HORSIES_CLAIM_SQL,
    SETTLE_CLAIMED_WORKFLOW_TASKS_BOUND,
    SETTLE_CLAIMED_WORKFLOW_TASKS_SQL,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from horsies._core import CoreHandle
    from horsies.core.worker.config import WorkerConfig
    from horsies.core.worker.runtime import _RequeueOutcome

logger = get_logger('worker')


def blocked_claims_of(
    task_ids: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
) -> set[str]:
    """The blocked task ids of one ``horsies_workflow_claimed_tasks_settle``
    call, from its rows. Pure.

    ``task_ids`` is the input of the call, in order. The function answers
    each input task once, in input order, with ``DISPATCH`` or ``BLOCKED``.

    Raises:
        OutcomeDecodeError: another row count, a row that names another
            task, or another disposition (the client and the installed
            function disagree).
    """
    if len(rows) != len(task_ids):
        raise OutcomeDecodeError(
            f'horsies_workflow_claimed_tasks_settle returned {len(rows)} rows '
            f'for {len(task_ids)} claims; expected one row per claim'
        )
    blocked: set[str] = set()
    for position, (expected, row) in enumerate(zip(task_ids, rows, strict=True), start=1):
        task_id = str(row['task_id'])
        if task_id != expected:
            raise OutcomeDecodeError(
                f'horsies_workflow_claimed_tasks_settle row {position} names task '
                f'{task_id}; the input holds {expected} there'
            )
        match row['disposition']:
            case 'DISPATCH':
                pass
            case 'BLOCKED':
                blocked.add(task_id)
            case other:
                raise OutcomeDecodeError(
                    f'horsies_workflow_claimed_tasks_settle returned disposition '
                    f'{other!r} for task {task_id}; expected DISPATCH or BLOCKED'
                )
    return blocked


# The most claim passes in a row that run again with no wait because each
# took fewer rows than it planned. A transaction that holds planned rows for
# longer leaves the worker to its next wake: a NOTIFY, a capacity release, or
# the poll interval.
SHORT_PASS_REPASS_LIMIT: Final = 3


@dataclass(frozen=True, slots=True)
class ClaimRows:
    """The rows of one ``horsies_claim`` call.

    ``tasks`` holds the rows that the call took, in dispatch order.
    ``short_of_plan`` counts the rows that its plan chose and its take did
    not get: another transaction held or took them. 0 when the take got
    every planned row.
    """

    tasks: list[dict[str, Any]]
    short_of_plan: int


def read_claim_rows(
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
) -> ClaimRows:
    """Split the rows of ``HORSIES_CLAIM_SQL`` by their ``row_kind``.

    A ``'CLAIMED'`` row is a task. At most one ``'SHORT'`` row carries
    ``short_by``, 1 or more. Another ``row_kind``, a second ``'SHORT'`` row,
    or a ``short_by`` below 1 is a defect of the claim function
    (``AssertionError``). Does no I/O.
    """
    tasks: list[dict[str, Any]] = []
    short_rows: list[int] = []
    for values in rows:
        row = dict(zip(columns, values))
        match row['row_kind']:
            case 'CLAIMED':
                tasks.append(row)
            case 'SHORT':
                short_rows.append(int(row['short_by']))
            case other:
                raise AssertionError(f'horsies_claim returned row_kind {other!r}')
    assert len(short_rows) <= 1, f'horsies_claim returned SHORT rows {short_rows}'
    assert all(
        short >= 1 for short in short_rows
    ), f'horsies_claim returned short_by {short_rows}'
    return ClaimRows(tasks=tasks, short_of_plan=sum(short_rows))


def claim_time_of(row: Mapping[str, Any]) -> datetime:
    """The claim time of a task row that ``horsies_claim`` took.

    The claim sets ``claimed_at = now()`` and returns it, and every fence
    of the dispatch uses it. A row without a claim time is a defect of the
    claim function (``AssertionError``). Does no I/O.
    """
    match row:
        case {'claimed_at': datetime() as claimed_at}:
            return claimed_at
        case _:
            raise AssertionError(
                f'horsies_claim returned task {row.get("id")!r} without a '
                f'claim time: claimed_at is {row.get("claimed_at", "missing")!r}',
            )


@dataclass(frozen=True, slots=True)
class ClaimPass:
    """What one claim pass did.

    ``claimed`` counts the rows that the claim statement took. ``dropped`` counts
    the rows of those that the post-claim checks gave up: rows of a paused or
    cancelled workflow, and rows returned to the queue because their class
    does not fit the registered task. They used claim budget and hold no slot.
    ``short_of_plan`` counts the rows that the claim planned and did not take
    (``ClaimRows.short_of_plan``).
    """

    claimed: int
    dropped: int
    short_of_plan: int

    def left_unannounced_work(
        self,
        *,
        limited: bool,
        short_passes_before: int,
    ) -> bool:
        """Whether ready work can remain that no wake would announce.

        A dropped row used budget that ready work could have had, and it frees
        no slot, so no capacity release follows. A pass under ``max_claim_batch``
        can stop with free slots. A pass that took fewer rows than it planned
        can stop with free slots while rows that its plan did not choose are
        ready. In each case the NOTIFY of that work was already consumed. A row
        of a paused workflow cannot be claimed again until a resume, and a row
        returned for its class is ISOLATED, which always fits, so a loop on
        these ends. A short take asks for another pass only while fewer than
        ``SHORT_PASS_REPASS_LIMIT`` passes before it in a row were short.

        Args:
            limited: Whether ``max_claim_batch`` caps the pass.
            short_passes_before: How many passes in a row before this one took
                fewer rows than they planned (``short_passes_after``).
        """
        return (
            self.dropped > 0
            or (limited and self.claimed > 0)
            or (
                self.short_of_plan > 0 and short_passes_before < SHORT_PASS_REPASS_LIMIT
            )
        )

    def short_passes_after(self, short_passes_before: int) -> int:
        """How many passes in a row, ending with this one, took fewer rows
        than they planned."""
        return short_passes_before + 1 if self.short_of_plan > 0 else 0


def route_budget(plan: ClaimPlan, execution: Execution, task_id: str) -> Execution:
    """The budget class of ``plan`` whose children run claimed task
    ``task_id`` of class ``execution``.

    The claim reads only classes that ``plan`` serves, so a class that it
    does not serve is a defect: ``AssertionError``. Pure.
    """
    match plan.route(execution):
        case ServedBy(budget=budget):
            return budget
        case NotServed():
            raise AssertionError(
                f'task {task_id}: a {execution.value} row was claimed by a '
                'worker that does not serve that class',
            )
        case _ as unreachable:
            assert_never(unreachable)


def _end_start_when_done(
    hold: QueueCapHold,
    finalizer: asyncio.Future[Any],
    task_id: str,
) -> None:
    """Forget the started row ``task_id`` when its finalizer ends."""

    def _ended(_done: asyncio.Future[Any]) -> None:
        hold.end(task_id)

    finalizer.add_done_callback(_ended)


class ClaimMixin:
    """Budgeted claim pass, queue caps, lease math, advisory claim mutex."""

    if TYPE_CHECKING:
        # Worker state this mixin reads.
        sf: async_sessionmaker[AsyncSession]
        cfg: WorkerConfig
        _claim_runner: StatementRunner
        worker_instance_id: str
        _registered_classes: Mapping[str, Execution]
        _queue_cap_hold: QueueCapHold

        @property
        def core_handle(self) -> CoreHandle: ...

        # Cross-concern methods provided by sibling mixins / Worker.
        def _claim_slot_count(self) -> int: ...
        def _shared_async_seats_in_service(self) -> int: ...
        def _shared_thread_seats_in_service(self) -> int: ...
        def _free_children(self, budget: Execution) -> int: ...
        async def _dispatch_one(
            self,
            task_id: str,
            task_name: str,
            args_json: Optional[str],
            kwargs_json: Optional[str],
            queue_name: str = 'default',
            is_workflow_task: bool = True,
            timeout: StoredTimeout = NoTimeout(),
            *,
            claim: DispatchClaim,
            execution: Execution,
        ) -> asyncio.Future[Any]: ...
        async def _requeue_claimed_task(
            self,
            task_id: str,
            reason: str,
            claim: DispatchClaim,
        ) -> _RequeueOutcome: ...

    async def _claim_and_dispatch_all(self) -> ClaimPass:
        """
        Claim tasks subject to:
          - the budget of each execution class: the classes that the claim
            plan routes to a budget count against its capacity in service
            (``_claim_capacity``)
          - max_claim_per_worker guard (prevents over-claiming)
          - queue priorities (CUSTOM mode)
          - per-queue max_concurrency (CUSTOM mode)

        In soft-cap mode a row of a capped queue is held, not started; each
        pass starts the held rows that its queue's count leaves room for
        (``_start_held_rows``, ``queue_cap_hold``). This also runs on a pass
        that claims nothing.

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
        # (horsies_claim). It runs in autocommit, as its own transaction, so
        # the xact-scoped advisory lock ends with the statement — never held
        # across a client round trip, so a stalled worker cannot freeze the
        # cluster mid-pass. Budget and cap semantics are computed in-function;
        # see horsies_claim in the baseline SQL.
        # The claim loop's held connection: one checkout for the passes that
        # follow each other, given back after an idle period.
        claim_result = rows_or_raise(
            await self._claim_runner.run(
                SingleStatement(
                    statement=HORSIES_CLAIM_SQL,
                    parameters=self._claim_params(),
                ),
            ),
        )
        claim_rows = read_claim_rows(claim_result.columns, claim_result.rows)

        claimed_rows = claim_rows.tasks
        fitting_rows: list[dict[str, Any]] = []
        if claimed_rows:
            # Post-claim guard: filter out tasks for non-runnable workflow states.
            dispatchable_rows = await self._filter_nonrunnable_workflow_tasks(
                claimed_rows,
            )
            fitting_rows = await self._return_rows_of_another_class(dispatchable_rows)

        for row in fitting_rows:
            queue_name = str(row['queue_name'])
            if self._queue_cap_hold.holds(queue_name):
                task_id = str(row['id'])
                self._queue_cap_hold.hold(
                    HeldRow(
                        task_id=task_id,
                        queue_name=queue_name,
                        budget=route_budget(
                            self.cfg.claim_plan(),
                            parse_row_class(task_id, row['execution']),
                            task_id,
                        ),
                        row=row,
                    ),
                )
            else:
                _ = await self._dispatch_claim_row(row)
        await self._start_held_rows()
        return ClaimPass(
            claimed=len(claimed_rows),
            dropped=len(claimed_rows) - len(fitting_rows),
            short_of_plan=claim_rows.short_of_plan,
        )

    async def _dispatch_claim_row(self, row: Mapping[str, Any]) -> asyncio.Future[Any]:
        """Hand one claimed row to dispatch, and return its finalizer task."""
        # The row's input is the task's keyword arguments as one JSON
        # object text; the dispatch carries it as the kwargs text, and
        # no positional arguments.
        return await self._dispatch_one(
            row['id'],
            row['task_name'],
            None,
            row['input'],
            row.get('queue_name') or 'default',
            bool(row.get('is_workflow_task', False)),
            timeout=read_claimed_options(row.get('task_options')),
            claim=dispatch_claim_of(row),
            execution=parse_row_class(str(row['id']), row['execution']),
        )

    async def _start_held_rows(self) -> None:
        """Start the held rows of the capped queues that have room, while a
        child of the row's route is free.

        Sends one statement (``COUNT_QUEUE_STARTS_SQL``) when rows are held,
        none otherwise. The free capacity of each budget is read after the
        count, and nothing awaits between that read and the submits, so each
        started row goes to a child that is free at its submit. Each started
        row leaves the started set when its finalizer ends.

        Raises:
            Exception: a DB error of the count, as in
                ``_claim_and_dispatch_all``. The held rows stay held.
        """
        hold = self._queue_cap_hold
        queues = hold.held_queues()
        if not queues:
            return
        counted = rows_or_raise(
            await self._claim_runner.run(
                SingleStatement(
                    statement=COUNT_QUEUE_STARTS_SQL,
                    parameters={
                        'queues': list(queues),
                        'wid': self.worker_instance_id,
                        'started_ids': list(hold.started_ids()),
                    },
                ),
            ),
        )
        starts = read_queue_starts(counted.columns, counted.rows)
        for held in hold.admit(starts, self._free_capacity()):
            finalizer = await self._dispatch_claim_row(held.row)
            _end_start_when_done(hold, finalizer, held.task_id)

    def _free_capacity(self) -> dict[Execution, int]:
        """The free capacity of each budget of the claim plan now: its
        children or seats that take a task at once (``_free_children``).
        Reads only."""
        return {
            budget: self._free_children(budget)
            for budget in self.cfg.claim_plan().budgets()
        }

    async def _return_held_rows(self) -> None:
        """Return each held row to its queue, at the stop of the worker.

        Runs after the claim loop ended. Never raises:
        ``_requeue_claimed_task`` logs a DB error, and the lease of a row it
        could not return expires.
        """
        for held in self._queue_cap_hold.drain():
            _ = await self._requeue_claimed_task(
                held.task_id,
                'the worker stopped while the row waited for its queue cap',
                dispatch_claim_of(held.row),
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

        Such rows go back to PENDING as ISOLATED in one statement, fenced to
        their claim generations (``_return_rows_as_isolated``). Rows that
        fit, and rows of a task that this worker did not register, come back
        in their order.

        Raises:
            Exception: DB errors propagate to the main loop. This runs after
                the claim committed; rows not yet returned or dispatched are
                recovered by lease expiry.
        """
        fitting: list[dict[str, Any]] = []
        mismatched: list[tuple[dict[str, Any], RowClassMismatch]] = []
        for row in rows:
            row_class = parse_row_class(str(row['id']), row['execution'])
            check = check_row_class(
                row_class,
                row['task_name'],
                self._registered_classes,
            )
            match check:
                case RowClassMismatch():
                    mismatched.append((row, check))
                case RowClassFits() | TaskNotRegistered():
                    fitting.append(row)
                case _ as unreachable:
                    assert_never(unreachable)
        await self._return_rows_as_isolated(mismatched)
        return fitting

    async def _return_rows_as_isolated(
        self,
        mismatched: Sequence[tuple[dict[str, Any], RowClassMismatch]],
    ) -> None:
        """Write ISOLATED on each row and return it to PENDING, in one
        ``horsies_release_claims`` statement that also wakes each queue of a
        returned row once. Fenced to this worker and each row's claim
        generation: a row whose claim this worker lost is not touched, and is
        not dispatched either. Logs one warning per row. No rows sends
        nothing.

        Runs in the core (``release_claims_in_core``).

        Raises:
            CoreOperationFailed: the release failed; the worker loop retries
                a retryable one as a connection error.
            CoreError: the core of this process stopped or did not start.
        """
        if not mismatched:
            return
        released = await release_claims_in_core(
            self.core_handle,
            worker_id=self.worker_instance_id,
            claims=tuple(
                HeldClaim(task_id=str(row['id']), claimed_at=claim_time_of(row))
                for row, _ in mismatched
            ),
            placement=ReleasePlacement.ISOLATED,
        )
        for (row, mismatch), release in zip(mismatched, released, strict=True):
            match release.outcome:
                case ReleaseOutcome.RELEASED:
                    disposition = 'wrote ISOLATED on the row and returned it to the queue'
                case ReleaseOutcome.NOT_HELD:
                    disposition = 'the claim was lost, so the row was left as it is'
                case _ as unreachable:
                    assert_never(unreachable)
            logger.warning(
                'Task %s (%s) stores execution %s, and the registered function '
                'is %s on this worker; %s',
                row['id'],
                row['task_name'],
                mismatch.row_class.value,
                mismatch.registered_class.value,
                disposition,
            )

    async def _filter_nonrunnable_workflow_tasks(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Settle the claimed tasks of PAUSED and CANCELLED workflows, and
        return the rows to dispatch, in their order.

        One ``horsies_workflow_claimed_tasks_settle`` per
        ``SETTLE_CLAIMED_WORKFLOW_TASKS_BOUND`` workflow tasks, each in
        autocommit. Under the lock of each workflow row the function reads
        the workflow status again:
        - PAUSED: this worker's claim at its claim generation is abandoned,
          and the node of an abandoned task goes back to READY with no task;
        - CANCELLED: the claim is cancelled, and the node of a cancelled
          task is SKIPPED.
        A task of a PAUSED or CANCELLED workflow is never dispatched, also
        when its move found the row moved on (that row belongs to its
        current owner). A resume that committed before the lock makes the
        workflow RUNNING, and its tasks are dispatched. Plain tasks send
        nothing.

        Raises:
            OutcomeDecodeError: rows outside the function's contract.
            Exception: what the pool and the driver raise; it propagates to
                the main loop. The statements before a failed one stay
                committed; the claims of the failed one are recovered by
                lease expiry (a lost reply may have committed its moves).
        """
        if not rows:
            return rows

        workflow_rows = [
            row for row in rows if bool(row.get('is_workflow_task', True))
        ]
        if not workflow_rows:
            return rows

        blocked_task_ids: set[str] = set()
        for start in range(
            0, len(workflow_rows), SETTLE_CLAIMED_WORKFLOW_TASKS_BOUND,
        ):
            chunk = workflow_rows[start:start + SETTLE_CLAIMED_WORKFLOW_TASKS_BOUND]
            task_ids = [str(row['id']) for row in chunk]
            # The claim generation each row was dispatched from: the moves
            # are fenced to it, so a lapsed-and-re-claimed row of this
            # worker is not ended under its new claim.
            settled = rows_or_raise(
                await run_in_autocommit_result(
                    self.sf,
                    SingleStatement(
                        statement=SETTLE_CLAIMED_WORKFLOW_TASKS_SQL,
                        parameters={
                            'ids': task_ids,
                            'claimed_ats': [claim_time_of(row) for row in chunk],
                            'worker_id': self.worker_instance_id,
                        },
                    ),
                ),
            )
            blocked_task_ids |= blocked_claims_of(task_ids, settled.mappings())

        return [row for row in rows if str(row['id']) not in blocked_task_ids]

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

        CLAIM_SQL RETURNING provides id/task_name/input atomically,
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
