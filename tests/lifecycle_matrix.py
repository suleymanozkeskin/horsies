"""Authoritative transition matrix for the sixteen terminal writers.

T01-T16 are the sixteen terminal writer contracts of the runtime. Each one is
implemented by a baseline SQL function that moves the `horsies_tasks` row to
history (`tests/unit/test_lifecycle_matrix.py` names the function of each
row). This module declares what each one does; tests execute the declaration
rather than reading it. `module` and `statement` name the original writer of
the contract; the characterization tests use them as test ids.

Two test layers consume it:

- static checks assert the declared guards and shape against the body of the
  baseline function, so a writer whose SQL changes without its row changing
  fails here;
- characterization tests parametrize over the rows, so every writer is pinned
  in both directions.

Scope of a row: what the *function* does. Behavior contributed by its caller —
workflow phase 2 — is named where it exists but is characterized at the
operation level, not asserted from the function text.

Notification: a move deletes the live row, so no status trigger fires; the
move sends `task_done` and `horsies_task_status` for every row it moves, and
every writer gets both without asking. `EmitsNotify.FUSED_CAPACITY_WAKE`
marks the functions that also send the capacity wake when their caller asks
for it.

Source of truth for the guards is the function text; the static checks keep
this matrix and the baseline functions aligned.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Fence(Enum):
    """Concurrency guard the statement applies to a claimed row."""

    NONE = 'NONE'
    """No ownership predicate. Cross-worker by design."""

    WORKER = 'WORKER'
    """Matches `claimed_by_worker_id` only."""

    WORKER_AND_GENERATION = 'WORKER_AND_GENERATION'
    """Full ClaimedOwnerFence: worker plus `claimed_at` generation."""

    WORKER_AND_GENERATION_PAIRWISE = 'WORKER_AND_GENERATION_PAIRWISE'
    """As above, with generations supplied per task through `unnest`."""

    PRIOR_LOCKED_SELECT = 'PRIOR_LOCKED_SELECT'
    """The function locks the row (`FOR UPDATE`), then matches worker and
    `claimed_at` generation. The client command carries the worker in its
    fence (`PriorLockedRead`) and the generation beside it."""


class Guard(Enum):
    """Non-ownership precondition beyond the source-status check."""

    NONE = 'NONE'
    DEADLINE = 'DEADLINE'
    """`good_until` has passed."""

    STALENESS = 'STALENESS'
    """Runner heartbeat and finalizing marker are both stale."""

    WORKFLOW_STATUS = 'WORKFLOW_STATUS'
    """Containing workflow is in a specific state, verified in-statement."""

    WORKFLOW_LINK_ABSENT = 'WORKFLOW_LINK_ABSENT'
    """No workflow-task row in a runnable status (the orphan predicate)."""

    WORKFLOW_LINK_STATE = 'WORKFLOW_LINK_STATE'
    """Workflow-task row is in a specific state."""


class Attempt(Enum):
    """Attempt-row behavior accompanying the transition."""

    NONE = 'NONE'
    FUSED_UPSERT = 'FUSED_UPSERT'
    """The statement itself upserts the attempt."""


class Shape(Enum):
    """Row cardinality and locking discipline."""

    SINGLE = 'SINGLE'
    SET_WISE = 'SET_WISE'
    SET_WISE_SKIP_LOCKED = 'SET_WISE_SKIP_LOCKED'
    """Set-wise, and the discovery skips rows another transaction holds."""

    FUSED = 'FUSED'
    """One function locks the row, upserts its attempt, moves it to history
    and sends the capacity wake."""


class EmitsNotify(Enum):
    """NOTIFY sent by the function beyond the status events of the move."""

    MOVE_EVENTS_ONLY = 'MOVE_EVENTS_ONLY'
    """Only `task_done` and `horsies_task_status`, for each moved row."""

    FUSED_CAPACITY_WAKE = 'FUSED_CAPACITY_WAKE'


class Driver(Enum):
    ASYNC_SQLALCHEMY = 'ASYNC_SQLALCHEMY'
    SYNC_PSYCOPG = 'SYNC_PSYCOPG'


@dataclass(frozen=True, slots=True)
class TerminalWriter:
    """One row of the transition matrix."""

    writer_id: str
    module: str
    statement: str
    """Module-level SQL constant, or the enclosing function for raw SQL."""

    source_statuses: tuple[str, ...]
    """Task statuses the statement accepts. Empty means caller-supplied."""

    target_status: str
    fence: Fence
    guards: tuple[Guard, ...]
    attempt: Attempt
    writes_result: bool
    shape: Shape
    emits_notify: EmitsNotify
    driver: Driver
    coupled_write: str | None
    """Non-task row the same transaction must change, if any."""

    operation: str
    """Target operation in the consolidated command model."""


MATRIX: tuple[TerminalWriter, ...] = (
    TerminalWriter(
        writer_id='T01',
        module='python/horsies/monitoring/task_actions.py',
        statement='_CANCEL_TASK_SQL',
        source_statuses=(),
        target_status='CANCELLED',
        fence=Fence.NONE,
        guards=(),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.MOVE_EVENTS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='CancelFromMonitoring',
    ),
    TerminalWriter(
        writer_id='T02',
        module='python/horsies/core/worker/sql.py',
        statement='UNCLAIM_PAUSED_TASKS_SQL',
        source_statuses=('CLAIMED',),
        target_status='CANCELLED',
        fence=Fence.WORKER_AND_GENERATION_PAIRWISE,
        guards=(),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SET_WISE,
        emits_notify=EmitsNotify.MOVE_EVENTS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write='horsies_workflow_tasks -> READY',
        operation='AbandonForWorkflowPause',
    ),
    TerminalWriter(
        writer_id='T03',
        module='python/horsies/core/worker/sql.py',
        statement='CANCEL_CANCELLED_WORKFLOW_TASKS_SQL',
        source_statuses=('CLAIMED',),
        target_status='CANCELLED',
        fence=Fence.WORKER_AND_GENERATION_PAIRWISE,
        guards=(),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SET_WISE,
        emits_notify=EmitsNotify.MOVE_EVENTS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write='horsies_workflow_tasks -> SKIPPED',
        operation='CancelForWorkflow',
    ),
    TerminalWriter(
        writer_id='T04',
        module='python/horsies/core/worker/sql.py',
        statement='MARK_TASK_FAILED_WORKER_SQL',
        source_statuses=('RUNNING',),
        target_status='FAILED',
        fence=Fence.PRIOR_LOCKED_SELECT,
        guards=(),
        attempt=Attempt.FUSED_UPSERT,
        writes_result=True,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.FUSED_CAPACITY_WAKE,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='FailRunning',
    ),
    TerminalWriter(
        writer_id='T05',
        module='python/horsies/core/worker/sql.py',
        statement='MARK_TASK_FAILED_SQL',
        source_statuses=('RUNNING',),
        target_status='FAILED',
        fence=Fence.PRIOR_LOCKED_SELECT,
        guards=(),
        attempt=Attempt.FUSED_UPSERT,
        writes_result=True,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.FUSED_CAPACITY_WAKE,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='FailRunning',
    ),
    TerminalWriter(
        writer_id='T06',
        module='python/horsies/core/worker/sql.py',
        statement='MARK_TASK_COMPLETED_SQL',
        source_statuses=('RUNNING',),
        target_status='COMPLETED',
        fence=Fence.PRIOR_LOCKED_SELECT,
        guards=(),
        attempt=Attempt.FUSED_UPSERT,
        writes_result=True,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.MOVE_EVENTS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='CompleteRunning',
    ),
    TerminalWriter(
        writer_id='T07',
        module='python/horsies/core/worker/sql.py',
        statement='FINALIZE_TASK_COMPLETED_SQL',
        source_statuses=('RUNNING',),
        target_status='COMPLETED',
        fence=Fence.WORKER_AND_GENERATION,
        guards=(),
        attempt=Attempt.FUSED_UPSERT,
        writes_result=True,
        shape=Shape.FUSED,
        emits_notify=EmitsNotify.FUSED_CAPACITY_WAKE,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='CompleteRunning fast path',
    ),
    TerminalWriter(
        writer_id='T08',
        module='python/horsies/core/worker/sql.py',
        statement='TERMINATE_ORPHANED_WORKFLOW_TASK_SQL',
        source_statuses=('CLAIMED',),
        target_status='CANCELLED',
        fence=Fence.WORKER_AND_GENERATION,
        guards=(Guard.WORKFLOW_LINK_ABSENT,),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.MOVE_EVENTS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='CancelOrphan',
    ),
    TerminalWriter(
        writer_id='T09',
        module='python/horsies/core/workflows/sql.py',
        statement='CANCEL_CLAIMED_TASKS_FOR_PAUSED_WORKFLOWS_SQL',
        source_statuses=('CLAIMED',),
        target_status='CANCELLED',
        fence=Fence.NONE,
        guards=(Guard.WORKFLOW_STATUS, Guard.WORKFLOW_LINK_STATE),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SET_WISE_SKIP_LOCKED,
        emits_notify=EmitsNotify.MOVE_EVENTS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write='horsies_workflow_tasks -> READY',
        operation='AbandonForWorkflowPause',
    ),
    TerminalWriter(
        writer_id='T10',
        module='python/horsies/core/worker/child_runner.py',
        statement='_handle_workflow_stop_before_start',
        source_statuses=('CLAIMED',),
        target_status='CANCELLED',
        fence=Fence.WORKER_AND_GENERATION,
        guards=(),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.MOVE_EVENTS_ONLY,
        driver=Driver.SYNC_PSYCOPG,
        coupled_write='horsies_workflow_tasks -> READY',
        operation='AbandonForWorkflowPause',
    ),
    TerminalWriter(
        writer_id='T11',
        module='python/horsies/core/worker/child_runner.py',
        statement='_handle_workflow_stop_before_start',
        source_statuses=('CLAIMED', 'PENDING'),
        target_status='CANCELLED',
        fence=Fence.WORKER_AND_GENERATION,
        guards=(),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.MOVE_EVENTS_ONLY,
        driver=Driver.SYNC_PSYCOPG,
        coupled_write='horsies_workflow_tasks -> SKIPPED',
        operation='CancelForWorkflow',
    ),
    TerminalWriter(
        writer_id='T12',
        module='python/horsies/core/worker/child_runner.py',
        statement='_EXPIRE_CLAIMED_TASK_BEFORE_START_SQL',
        source_statuses=('CLAIMED',),
        target_status='EXPIRED',
        fence=Fence.WORKER,
        guards=(Guard.DEADLINE,),
        attempt=Attempt.NONE,
        writes_result=True,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.MOVE_EVENTS_ONLY,
        driver=Driver.SYNC_PSYCOPG,
        coupled_write=None,
        operation='ExpireClaimed',
    ),
    TerminalWriter(
        writer_id='T13',
        module='python/horsies/core/brokers/postgres.py',
        statement='MARK_STALE_TASK_FAILED_SQL',
        source_statuses=('RUNNING',),
        target_status='FAILED',
        fence=Fence.NONE,
        guards=(Guard.STALENESS,),
        attempt=Attempt.FUSED_UPSERT,
        writes_result=True,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.MOVE_EVENTS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='FailStaleRunning',
    ),
    TerminalWriter(
        writer_id='T14',
        module='python/horsies/core/brokers/postgres.py',
        statement='EXPIRE_PENDING_TASKS_SQL',
        source_statuses=('PENDING',),
        target_status='EXPIRED',
        fence=Fence.NONE,
        guards=(Guard.DEADLINE,),
        attempt=Attempt.NONE,
        writes_result=True,
        shape=Shape.SET_WISE_SKIP_LOCKED,
        emits_notify=EmitsNotify.MOVE_EVENTS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='ExpirePendingBatch',
    ),
    TerminalWriter(
        writer_id='T15',
        module='python/horsies/core/brokers/postgres.py',
        statement='TERMINATE_ORPHANED_CLAIMED_WORKFLOW_TASKS_SQL',
        source_statuses=('CLAIMED', 'PENDING'),
        target_status='CANCELLED',
        fence=Fence.NONE,
        guards=(Guard.WORKFLOW_LINK_ABSENT,),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SET_WISE_SKIP_LOCKED,
        emits_notify=EmitsNotify.MOVE_EVENTS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='CancelOrphanBatch',
    ),
    TerminalWriter(
        writer_id='T16',
        module='python/horsies/core/models/workflow/handle.py',
        statement='MARK_ENQUEUED_NOT_STARTED_TASKS_CANCELLED_SQL',
        source_statuses=('PENDING', 'CLAIMED', 'RUNNING'),
        target_status='CANCELLED',
        fence=Fence.NONE,
        guards=(Guard.WORKFLOW_STATUS, Guard.WORKFLOW_LINK_STATE),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SET_WISE_SKIP_LOCKED,
        emits_notify=EmitsNotify.MOVE_EVENTS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write='horsies_workflow_tasks -> SKIPPED',
        operation='CancelForWorkflow',
    ),
)


BY_ID: dict[str, TerminalWriter] = {row.writer_id: row for row in MATRIX}

TERMINAL_STATUSES: frozenset[str] = frozenset(
    {'COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED'},
)
