"""Authoritative transition matrix for the sixteen terminal writers.

T01-T16 are every statement in the runtime package that can move a
`horsies_tasks` row to a terminal status. This module declares what each one
does; tests execute the declaration rather than reading it.

Two test layers consume it:

- static checks assert the declared guards and shape against the statement
  text, so a writer whose SQL changes without its row changing fails here;
- characterization tests parametrize over the rows, so every writer is pinned
  in both directions before the terminalization consolidation refactors it.

Scope of a row: what the *statement* does. Behavior contributed by its caller —
attempt insertion issued separately in the same transaction, queue-capacity
wakes, workflow phase 2 — is named where it exists but is characterized at the
operation level, not asserted from the statement text.

Notification: `task_done` and `horsies_task_status` are emitted by triggers on
any terminal status change (`schemas/triggers.py`), so every writer gets both
without asking. `EmitsNotify.FUSED_CAPACITY_WAKE` marks the one statement that
additionally emits a NOTIFY itself.

Source of truth for the guards is the statement text; this matrix was derived
from it and the static checks keep the two aligned.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Fence(Enum):
    """Concurrency guard the statement applies to a claimed row."""

    NONE = 'NONE'
    """No ownership predicate. Cross-worker by design."""

    CALLER_ROW_LOCK = 'CALLER_ROW_LOCK'
    """No predicate in the statement; the caller holds the row lock."""

    WORKER = 'WORKER'
    """Matches `claimed_by_worker_id` only."""

    WORKER_AND_GENERATION = 'WORKER_AND_GENERATION'
    """Full ClaimedOwnerFence: worker plus `claimed_at` generation."""

    WORKER_AND_GENERATION_PAIRWISE = 'WORKER_AND_GENERATION_PAIRWISE'
    """As above, with generations supplied per task through `unnest`."""

    PRIOR_LOCKED_SELECT = 'PRIOR_LOCKED_SELECT'
    """Statement matches on worker; the generation fence is carried by the
    `FOR UPDATE` select the caller issued immediately before."""


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
    CALLER_INSERTS = 'CALLER_INSERTS'
    """Caller writes the attempt separately in the same transaction."""

    FUSED_UPSERT = 'FUSED_UPSERT'
    """The statement itself upserts the attempt."""


class Shape(Enum):
    """Row cardinality and locking discipline."""

    SINGLE = 'SINGLE'
    SET_WISE = 'SET_WISE'
    SET_WISE_SKIP_LOCKED = 'SET_WISE_SKIP_LOCKED'
    FUSED_CTE = 'FUSED_CTE'


class EmitsNotify(Enum):
    """NOTIFY emitted by the statement itself, beyond the triggers."""

    TRIGGERS_ONLY = 'TRIGGERS_ONLY'
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
        module='horsies/monitoring/task_actions.py',
        statement='_CANCEL_TASK_SQL',
        source_statuses=(),
        target_status='CANCELLED',
        fence=Fence.CALLER_ROW_LOCK,
        guards=(),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.TRIGGERS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='CancelFromMonitoring',
    ),
    TerminalWriter(
        writer_id='T02',
        module='horsies/core/worker/sql.py',
        statement='UNCLAIM_PAUSED_TASKS_SQL',
        source_statuses=('CLAIMED',),
        target_status='CANCELLED',
        fence=Fence.WORKER_AND_GENERATION_PAIRWISE,
        guards=(),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SET_WISE,
        emits_notify=EmitsNotify.TRIGGERS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write='horsies_workflow_tasks -> READY',
        operation='AbandonForWorkflowPause',
    ),
    TerminalWriter(
        writer_id='T03',
        module='horsies/core/worker/sql.py',
        statement='CANCEL_CANCELLED_WORKFLOW_TASKS_SQL',
        source_statuses=('CLAIMED',),
        target_status='CANCELLED',
        fence=Fence.WORKER_AND_GENERATION_PAIRWISE,
        guards=(),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SET_WISE,
        emits_notify=EmitsNotify.TRIGGERS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write='horsies_workflow_tasks -> SKIPPED',
        operation='CancelForWorkflow',
    ),
    TerminalWriter(
        writer_id='T04',
        module='horsies/core/worker/sql.py',
        statement='MARK_TASK_FAILED_WORKER_SQL',
        source_statuses=('RUNNING',),
        target_status='FAILED',
        fence=Fence.PRIOR_LOCKED_SELECT,
        guards=(),
        attempt=Attempt.CALLER_INSERTS,
        writes_result=True,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.TRIGGERS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='FailRunning',
    ),
    TerminalWriter(
        writer_id='T05',
        module='horsies/core/worker/sql.py',
        statement='MARK_TASK_FAILED_SQL',
        source_statuses=('RUNNING',),
        target_status='FAILED',
        fence=Fence.PRIOR_LOCKED_SELECT,
        guards=(),
        attempt=Attempt.CALLER_INSERTS,
        writes_result=True,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.TRIGGERS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='FailRunning',
    ),
    TerminalWriter(
        writer_id='T06',
        module='horsies/core/worker/sql.py',
        statement='MARK_TASK_COMPLETED_SQL',
        source_statuses=('RUNNING',),
        target_status='COMPLETED',
        fence=Fence.PRIOR_LOCKED_SELECT,
        guards=(),
        attempt=Attempt.CALLER_INSERTS,
        writes_result=True,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.TRIGGERS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='CompleteRunning',
    ),
    TerminalWriter(
        writer_id='T07',
        module='horsies/core/worker/sql.py',
        statement='FINALIZE_TASK_COMPLETED_SQL',
        source_statuses=('RUNNING',),
        target_status='COMPLETED',
        fence=Fence.WORKER_AND_GENERATION,
        guards=(),
        attempt=Attempt.FUSED_UPSERT,
        writes_result=True,
        shape=Shape.FUSED_CTE,
        emits_notify=EmitsNotify.FUSED_CAPACITY_WAKE,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='CompleteRunning fast path',
    ),
    TerminalWriter(
        writer_id='T08',
        module='horsies/core/worker/sql.py',
        statement='TERMINATE_ORPHANED_WORKFLOW_TASK_SQL',
        source_statuses=('CLAIMED',),
        target_status='CANCELLED',
        fence=Fence.WORKER_AND_GENERATION,
        guards=(Guard.WORKFLOW_LINK_ABSENT,),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.TRIGGERS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='CancelOrphan',
    ),
    TerminalWriter(
        writer_id='T09',
        module='horsies/core/workflows/sql.py',
        statement='CANCEL_CLAIMED_TASKS_FOR_PAUSED_WORKFLOWS_SQL',
        source_statuses=('CLAIMED',),
        target_status='CANCELLED',
        fence=Fence.NONE,
        guards=(Guard.WORKFLOW_STATUS, Guard.WORKFLOW_LINK_STATE),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SET_WISE,
        emits_notify=EmitsNotify.TRIGGERS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write='horsies_workflow_tasks -> READY',
        operation='AbandonForWorkflowPause',
    ),
    TerminalWriter(
        writer_id='T10',
        module='horsies/core/worker/child_runner.py',
        statement='_handle_workflow_stop_before_start',
        source_statuses=('CLAIMED',),
        target_status='CANCELLED',
        fence=Fence.WORKER_AND_GENERATION,
        guards=(),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.TRIGGERS_ONLY,
        driver=Driver.SYNC_PSYCOPG,
        coupled_write='horsies_workflow_tasks -> READY',
        operation='AbandonForWorkflowPause',
    ),
    TerminalWriter(
        writer_id='T11',
        module='horsies/core/worker/child_runner.py',
        statement='_handle_workflow_stop_before_start',
        source_statuses=('CLAIMED', 'PENDING'),
        target_status='CANCELLED',
        fence=Fence.WORKER_AND_GENERATION,
        guards=(),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.TRIGGERS_ONLY,
        driver=Driver.SYNC_PSYCOPG,
        coupled_write='horsies_workflow_tasks -> SKIPPED',
        operation='CancelForWorkflow',
    ),
    TerminalWriter(
        writer_id='T12',
        module='horsies/core/worker/child_runner.py',
        statement='_EXPIRE_CLAIMED_TASK_BEFORE_START_SQL',
        source_statuses=('CLAIMED',),
        target_status='EXPIRED',
        fence=Fence.WORKER,
        guards=(Guard.DEADLINE,),
        attempt=Attempt.NONE,
        writes_result=True,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.TRIGGERS_ONLY,
        driver=Driver.SYNC_PSYCOPG,
        coupled_write=None,
        operation='ExpireClaimed',
    ),
    TerminalWriter(
        writer_id='T13',
        module='horsies/core/brokers/postgres.py',
        statement='MARK_STALE_TASK_FAILED_SQL',
        source_statuses=('RUNNING',),
        target_status='FAILED',
        fence=Fence.NONE,
        guards=(Guard.STALENESS,),
        attempt=Attempt.CALLER_INSERTS,
        writes_result=True,
        shape=Shape.SINGLE,
        emits_notify=EmitsNotify.TRIGGERS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='FailStaleRunning',
    ),
    TerminalWriter(
        writer_id='T14',
        module='horsies/core/brokers/postgres.py',
        statement='EXPIRE_PENDING_TASKS_SQL',
        source_statuses=('PENDING',),
        target_status='EXPIRED',
        fence=Fence.NONE,
        guards=(Guard.DEADLINE,),
        attempt=Attempt.NONE,
        writes_result=True,
        shape=Shape.SET_WISE_SKIP_LOCKED,
        emits_notify=EmitsNotify.TRIGGERS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='ExpirePendingBatch',
    ),
    TerminalWriter(
        writer_id='T15',
        module='horsies/core/brokers/postgres.py',
        statement='TERMINATE_ORPHANED_CLAIMED_WORKFLOW_TASKS_SQL',
        source_statuses=('CLAIMED', 'PENDING'),
        target_status='CANCELLED',
        fence=Fence.NONE,
        guards=(Guard.WORKFLOW_LINK_ABSENT,),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SET_WISE_SKIP_LOCKED,
        emits_notify=EmitsNotify.TRIGGERS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write=None,
        operation='CancelOrphanBatch',
    ),
    TerminalWriter(
        writer_id='T16',
        module='horsies/core/models/workflow/handle.py',
        statement='MARK_ENQUEUED_NOT_STARTED_TASKS_CANCELLED_SQL',
        source_statuses=('PENDING', 'CLAIMED', 'RUNNING'),
        target_status='CANCELLED',
        fence=Fence.NONE,
        guards=(Guard.WORKFLOW_STATUS, Guard.WORKFLOW_LINK_STATE),
        attempt=Attempt.NONE,
        writes_result=False,
        shape=Shape.SET_WISE,
        emits_notify=EmitsNotify.TRIGGERS_ONLY,
        driver=Driver.ASYNC_SQLALCHEMY,
        coupled_write='horsies_workflow_tasks -> SKIPPED',
        operation='CancelForWorkflow',
    ),
)


BY_ID: dict[str, TerminalWriter] = {row.writer_id: row for row in MATRIX}

TERMINAL_STATUSES: frozenset[str] = frozenset(
    {'COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED'},
)
