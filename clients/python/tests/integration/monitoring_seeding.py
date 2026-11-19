"""Seed rows for the monitoring and web suites, on the baseline columns.

A fixture is a frozen record of the facts a test states. ``insert_rows``
writes each record where production holds it:

- a task with a live status: a ``horsies_tasks`` row;
- a task with a terminal status: a ``horsies_task_history`` row. The
  attempts of that task in the same call fold into its attempt snapshot,
  and a node in the same call that names the task gives its workflow;
- the attempts of a live task: ``horsies_task_attempts`` rows;
- workflows, nodes and schedule states: their own tables.

Every statement names every column, because the baseline has no column
defaults. The rows do not go through the ORM models, so their shape is the
baseline's, whatever columns the models map.

``task_row`` refuses a live task with a fact that only a history row holds
(an end time, an error code, a failed reason): the live table has no column
for it.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Final, assert_never

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.models.tasks import Execution
from horsies.core.types.status import TaskStatus
from tests.attempt_snapshot_support import AttemptRecord, encode_attempt_snapshot
from tests.integration.history_seeding import (
    HISTORY_SEED_CLASS_KEY,
    CoveredInstant,
    cover_history_instant,
    ensure_history_seedable,
)

FIXTURE_INPUT: Final = '{}'
"""The stored input of every fixture task and node: a send with no kwargs."""

FIXTURE_WORKER_PID: Final = 4242
FIXTURE_WORKER_PROCESS_NAME: Final = 'fixture-process'

FIXTURE_CLAIM_LEASE: Final = timedelta(minutes=5)
"""The lease of a CLAIMED fixture that states no expiry: it outlasts the test."""

_RESULT_ENVELOPE_VERSION: Final = 1
_HISTORY_SCHEMA_VERSION: Final = 1
_COMMAND_FINGERPRINT_VERSION: Final = 1

# The move that writes a history row of each terminal status. A fixture has no
# move of its own, so it takes the kind of the plain path to that status.
_FIXTURE_TERMINALIZATION_KIND: Final[dict[TaskStatus, TerminalizationKind]] = {
    TaskStatus.COMPLETED: TerminalizationKind.COMPLETE_FUSED,
    TaskStatus.FAILED: TerminalizationKind.FAIL_RUNNING,
    TaskStatus.CANCELLED: TerminalizationKind.CANCEL_ADMIN,
    TaskStatus.EXPIRED: TerminalizationKind.EXPIRE_PENDING,
}


# --------------------------------------------------------------------------- #
# Records
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class LiveTask:
    """The facts only a live row holds.

    ``claim_expires_at`` is set exactly when the status is CLAIMED
    (``horsies_tasks_status_live``).
    """

    claim_expires_at: datetime | None


@dataclass(frozen=True, slots=True)
class EndedTask:
    """The facts only a history row holds."""

    terminal_at: datetime
    error_code: str | None
    failed_reason: str | None


@dataclass(frozen=True, slots=True)
class TaskRow:
    """One task, live or ended. ``task_row`` builds it."""

    id: str
    task_name: str
    queue_name: str
    status: TaskStatus
    priority: int
    retry_count: int
    max_retries: int
    is_workflow_task: bool
    execution: Execution
    sent_at: datetime
    created_at: datetime
    enqueued_at: datetime
    claimed_at: datetime | None
    started_at: datetime | None
    good_until: datetime | None
    worker_id: str | None
    worker_hostname: str | None
    worker_pid: int | None
    worker_process_name: str | None
    lifecycle: LiveTask | EndedTask


@dataclass(frozen=True, slots=True)
class AttemptRow:
    """One finished attempt of a task."""

    task_id: str
    attempt: int
    outcome: str
    will_retry: bool
    started_at: datetime
    finished_at: datetime
    error_code: str | None
    error_message: str | None
    failed_reason: str | None
    worker_id: str
    worker_hostname: str
    worker_pid: int
    worker_process_name: str


@dataclass(frozen=True, slots=True)
class WorkflowParent:
    """The node of the parent run that started a child run."""

    workflow_id: str
    task_index: int
    root_workflow_id: str


@dataclass(frozen=True, slots=True)
class WorkflowRow:
    """One workflow run. A root run has no parent."""

    id: str
    name: str
    status: str
    on_error: str
    definition_key: str | None
    parent: WorkflowParent | None
    sent_at: datetime
    created_at: datetime
    completed_at: datetime | None


@dataclass(frozen=True, slots=True)
class TaskNode:
    """A node that runs a task. ``task_id`` is None until the task exists."""

    task_id: str | None


@dataclass(frozen=True, slots=True)
class SubworkflowNode:
    """A node that runs a child workflow."""

    sub_workflow_id: str | None


@dataclass(frozen=True, slots=True)
class NodeRow:
    """One workflow node."""

    id: str
    workflow_id: str
    task_index: int
    node_id: str | None
    task_name: str
    status: str
    dependencies: tuple[int, ...]
    allow_failed_deps: bool
    kind: TaskNode | SubworkflowNode
    result: str | None
    """The stored result text; a failed node's error is in it."""
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None


@dataclass(frozen=True, slots=True)
class LastRun:
    """The last fire of a schedule. A schedule that never fired has none."""

    at: datetime
    task_id: str
    run_count: int


@dataclass(frozen=True, slots=True)
class ScheduleRow:
    """One schedule state."""

    schedule_name: str
    next_run_at: datetime
    last_run: LastRun | None


type SeedRow = TaskRow | AttemptRow | WorkflowRow | NodeRow | ScheduleRow


# --------------------------------------------------------------------------- #
# Constructors
# --------------------------------------------------------------------------- #
def task_row(
    *,
    task_id: str,
    task_name: str,
    queue_name: str,
    status: TaskStatus,
    priority: int,
    retry_count: int,
    max_retries: int,
    is_workflow_task: bool,
    enqueued_at: datetime,
    claimed_at: datetime | None,
    started_at: datetime | None,
    completed_at: datetime | None,
    failed_at: datetime | None,
    good_until: datetime | None,
    error_code: str | None,
    failed_reason: str | None,
    worker_id: str | None,
    worker_hostname: str | None,
    worker_pid: int | None,
    worker_process_name: str | None,
    claim_expires_at: datetime | None,
) -> TaskRow:
    """Build one task from the facts a test states.

    A terminal status ends the task at ``completed_at`` or ``failed_at``,
    else now. A live status refuses an end time, an error code and a failed
    reason: the live table has no column for them. A CLAIMED task without a
    stated ``claim_expires_at`` holds a lease of ``FIXTURE_CLAIM_LEASE``.
    """
    now = datetime.now(timezone.utc)
    lifecycle: LiveTask | EndedTask
    if status.is_terminal:
        lifecycle = EndedTask(
            terminal_at=completed_at or failed_at or now,
            error_code=error_code,
            failed_reason=failed_reason,
        )
    else:
        refused = {
            'completed_at': completed_at,
            'failed_at': failed_at,
            'error_code': error_code,
            'failed_reason': failed_reason,
        }
        stated = sorted(name for name, value in refused.items() if value is not None)
        assert not stated, (
            f'a live {status.value} task has no {", ".join(stated)}: only a '
            'history row holds it'
        )
        match status:
            case TaskStatus.CLAIMED:
                lease_end = claim_expires_at or now + FIXTURE_CLAIM_LEASE
            case _:
                assert claim_expires_at is None, (
                    f'a {status.value} task holds no claim lease'
                )
                lease_end = None
        lifecycle = LiveTask(claim_expires_at=lease_end)
    return TaskRow(
        id=task_id,
        task_name=task_name,
        queue_name=queue_name,
        status=status,
        priority=priority,
        retry_count=retry_count,
        max_retries=max_retries,
        is_workflow_task=is_workflow_task,
        execution=Execution.ISOLATED,
        sent_at=now,
        created_at=now,
        enqueued_at=enqueued_at,
        claimed_at=claimed_at,
        started_at=started_at,
        good_until=good_until,
        worker_id=worker_id,
        worker_hostname=worker_hostname,
        worker_pid=worker_pid,
        worker_process_name=worker_process_name,
        lifecycle=lifecycle,
    )


# --------------------------------------------------------------------------- #
# Statements
# --------------------------------------------------------------------------- #
_INSERT_TASK_SQL = text(
    """
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution,
        retention_class_key, sent_at, created_at,
        command_fingerprint_version, command_fingerprint, idempotency_key_digest,
        status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
        started_at, finalizing_at, finalizing_by_worker_id,
        worker_pid, worker_hostname, worker_process_name,
        retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
        updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    ) VALUES (
        CAST(:id AS uuid), :task_name, :queue_name, :priority, :input, NULL,
        :execution, :retention_class_key, :sent_at, :created_at,
        :fingerprint_version, :fingerprint, NULL,
        :status, :enqueued_at, :claimed_at, :worker_id, :claim_expires_at,
        :started_at, NULL, NULL,
        :worker_pid, :worker_hostname, :worker_process_name,
        :retry_count, :max_retries, NULL, :good_until, :is_workflow_task,
        :created_at, FALSE, NULL, NULL
    )
    """
)

_INSERT_HISTORY_SQL = text(
    """
    INSERT INTO horsies_task_history (
        task_id, task_name, queue_name, priority,
        command_fingerprint_version, command_fingerprint, retention_class_key,
        is_workflow_task, workflow_id, rerun_of_task_id, rerun_root_task_id,
        sent_at, created_at, enqueued_at, claimed_at, started_at, good_until,
        terminal_at, status, terminalization_kind, retry_count, max_retries,
        last_claimed_worker_id, last_worker_hostname, last_worker_pid,
        last_worker_process_name, error_code, final_failed_reason,
        result_envelope_version, result_payload, result_digest,
        history_schema_version, attempt_archive_version, attempt_snapshot,
        attempt_snapshot_digest, rerun_input_disposition, rerun_input_version,
        rerun_input_digest, rerun_input_inline
    ) VALUES (
        CAST(:task_id AS uuid), :task_name, :queue_name, :priority,
        :fingerprint_version, :fingerprint, :retention_class_key,
        :is_workflow_task, CAST(:workflow_id AS uuid), NULL, NULL,
        :sent_at, :created_at, :enqueued_at, :claimed_at, :started_at,
        :good_until, :terminal_at, :status, :terminalization_kind,
        :retry_count, :max_retries,
        :worker_id, :worker_hostname, :worker_pid,
        :worker_process_name, :error_code, :failed_reason,
        :result_envelope_version, :result_payload, :result_digest,
        :history_schema_version, :attempt_archive_version, :attempt_snapshot,
        :attempt_snapshot_digest, 'NEVER_ELIGIBLE', NULL,
        NULL, NULL
    )
    """
)

_INSERT_ATTEMPT_SQL = text(
    """
    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry, started_at, finished_at,
        error_code, error_message, failed_reason,
        worker_id, worker_hostname, worker_pid, worker_process_name
    ) VALUES (
        CAST(:task_id AS uuid), :attempt, :outcome, :will_retry, :started_at,
        :finished_at, :error_code, :error_message, :failed_reason,
        :worker_id, :worker_hostname, :worker_pid, :worker_process_name
    )
    """
)

_INSERT_WORKFLOW_SQL = text(
    """
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index,
        success_policy, result, error, parent_workflow_id, parent_task_index,
        root_workflow_id, depth, sent_at, created_at, started_at, completed_at,
        updated_at
    ) VALUES (
        CAST(:id AS uuid), :name, :definition_key, :status, :on_error, NULL,
        NULL, NULL, NULL, CAST(:parent_workflow_id AS uuid), :parent_task_index,
        CAST(:root_workflow_id AS uuid), :depth, :sent_at, :created_at,
        NULL, :completed_at, :created_at
    )
    """
)

_INSERT_NODE_SQL = text(
    """
    INSERT INTO horsies_workflow_tasks (downstream_subworkflow,
        id, workflow_id, task_index, node_id, task_name, input, queue_name,
        priority, dependencies, args_from, workflow_ctx_from,
        allow_failed_deps, join_type, min_success, task_options, max_retries,
        execution, good_until, retention_class_key, status, task_id,
        is_subworkflow, sub_workflow_id, sub_workflow_name, sub_definition_key,
        sub_workflow_params, sub_workflow_summary, result, error, created_at,
        started_at, completed_at
    ) VALUES ('NONE',
        CAST(:id AS uuid), CAST(:workflow_id AS uuid), :task_index, :node_id,
        :task_name, :input, 'default', 100, :dependencies, NULL, NULL,
        :allow_failed_deps, 'all', NULL, NULL, :max_retries,
        :execution, NULL, :retention_class_key, :status,
        CAST(:task_id AS uuid), :is_subworkflow,
        CAST(:sub_workflow_id AS uuid), NULL, NULL,
        :sub_workflow_params, NULL, :result, NULL, :created_at, :started_at,
        :completed_at
    )
    """
)

_INSERT_SCHEDULE_SQL = text(
    """
    INSERT INTO horsies_schedule_state (
        schedule_name, next_run_at, last_run_at, last_task_id, run_count,
        config_hash, updated_at
    ) VALUES (
        :schedule_name, :next_run_at, :last_run_at,
        CAST(:last_task_id AS uuid), :run_count, :config_hash, now()
    )
    """
)


# --------------------------------------------------------------------------- #
# Parameters
# --------------------------------------------------------------------------- #
def _fingerprint(task: TaskRow) -> tuple[int | None, bytes | None]:
    """A workflow task carries no fingerprint; a sent task carries one."""
    if task.is_workflow_task:
        return None, None
    return _COMMAND_FINGERPRINT_VERSION, sha256(task.id.encode()).digest()


def _live_task_params(task: TaskRow, live: LiveTask) -> dict[str, object]:
    version, fingerprint = _fingerprint(task)
    return {
        'id': task.id,
        'task_name': task.task_name,
        'queue_name': task.queue_name,
        'priority': task.priority,
        'input': FIXTURE_INPUT,
        'execution': task.execution.value,
        'retention_class_key': HISTORY_SEED_CLASS_KEY,
        'sent_at': task.sent_at,
        'created_at': task.created_at,
        'fingerprint_version': version,
        'fingerprint': fingerprint,
        'status': task.status.value,
        'enqueued_at': task.enqueued_at,
        'claimed_at': task.claimed_at,
        'worker_id': task.worker_id,
        'claim_expires_at': live.claim_expires_at,
        'started_at': task.started_at,
        'worker_hostname': task.worker_hostname,
        'worker_pid': task.worker_pid,
        'worker_process_name': task.worker_process_name,
        'retry_count': task.retry_count,
        'max_retries': task.max_retries,
        'good_until': task.good_until,
        'is_workflow_task': task.is_workflow_task,
    }


def _attempt_record(row: AttemptRow) -> AttemptRecord:
    return AttemptRecord(
        attempt=row.attempt,
        outcome=row.outcome,
        will_retry=row.will_retry,
        started_at=row.started_at,
        finished_at=row.finished_at,
        error_code=row.error_code,
        error_message=row.error_message,
        failed_reason=row.failed_reason,
        worker_id=row.worker_id,
        worker_hostname=row.worker_hostname,
        worker_pid=row.worker_pid,
        worker_process_name=row.worker_process_name,
    )


def _history_params(
    task: TaskRow,
    ended: EndedTask,
    terminal_at: CoveredInstant,
    attempts: Sequence[AttemptRow],
    workflow_id: str | None,
) -> dict[str, object]:
    version, fingerprint = _fingerprint(task)
    snapshot = encode_attempt_snapshot(
        tuple(
            _attempt_record(row)
            for row in sorted(attempts, key=lambda row: row.attempt)
        ),
    )
    payload = b'{}' if task.status is TaskStatus.COMPLETED else None
    return {
        'task_id': task.id,
        'task_name': task.task_name,
        'queue_name': task.queue_name,
        'priority': task.priority,
        'fingerprint_version': version,
        'fingerprint': fingerprint,
        'retention_class_key': HISTORY_SEED_CLASS_KEY,
        'is_workflow_task': task.is_workflow_task,
        'workflow_id': workflow_id,
        'sent_at': task.sent_at,
        'created_at': task.created_at,
        'enqueued_at': task.enqueued_at,
        'claimed_at': task.claimed_at,
        'started_at': task.started_at,
        'good_until': task.good_until,
        'terminal_at': terminal_at,
        'status': task.status.value,
        'terminalization_kind': _FIXTURE_TERMINALIZATION_KIND[task.status].value,
        'retry_count': task.retry_count,
        'max_retries': task.max_retries,
        'worker_id': task.worker_id,
        'worker_hostname': task.worker_hostname,
        'worker_pid': task.worker_pid,
        'worker_process_name': task.worker_process_name,
        'error_code': ended.error_code,
        'failed_reason': ended.failed_reason,
        'result_envelope_version': _RESULT_ENVELOPE_VERSION,
        'result_payload': payload,
        'result_digest': sha256(payload).digest() if payload is not None else None,
        'history_schema_version': _HISTORY_SCHEMA_VERSION,
        'attempt_archive_version': snapshot.version,
        'attempt_snapshot': snapshot.payload,
        'attempt_snapshot_digest': snapshot.digest,
    }


def _attempt_params(row: AttemptRow) -> dict[str, object]:
    return {
        'task_id': row.task_id,
        'attempt': row.attempt,
        'outcome': row.outcome,
        'will_retry': row.will_retry,
        'started_at': row.started_at,
        'finished_at': row.finished_at,
        'error_code': row.error_code,
        'error_message': row.error_message,
        'failed_reason': row.failed_reason,
        'worker_id': row.worker_id,
        'worker_hostname': row.worker_hostname,
        'worker_pid': row.worker_pid,
        'worker_process_name': row.worker_process_name,
    }


def _workflow_params(row: WorkflowRow) -> dict[str, object]:
    match row.parent:
        case None:
            parent_workflow_id, parent_task_index = None, None
            root_workflow_id, depth = row.id, 0
        case WorkflowParent(
            workflow_id=parent_id, task_index=index, root_workflow_id=root_id,
        ):
            parent_workflow_id, parent_task_index = parent_id, index
            root_workflow_id, depth = root_id, 1
    return {
        'id': row.id,
        'name': row.name,
        'definition_key': row.definition_key,
        'status': row.status,
        'on_error': row.on_error,
        'parent_workflow_id': parent_workflow_id,
        'parent_task_index': parent_task_index,
        'root_workflow_id': root_workflow_id,
        'depth': depth,
        'sent_at': row.sent_at,
        'created_at': row.created_at,
        'completed_at': row.completed_at,
    }


def _node_params(row: NodeRow) -> dict[str, object]:
    # A task node carries the task facts; a sub-workflow node carries none
    # (horsies_workflow_tasks_task_facts_by_kind), and only a sub-workflow
    # node carries the params choice (horsies_workflow_tasks_sub_workflow_params_by_kind).
    match row.kind:
        case TaskNode(task_id=task_id):
            facts: dict[str, object] = {
                'is_subworkflow': False,
                'task_id': task_id,
                'sub_workflow_id': None,
                'max_retries': 0,
                'execution': Execution.ISOLATED.value,
                'retention_class_key': HISTORY_SEED_CLASS_KEY,
                'sub_workflow_params': None,
            }
        case SubworkflowNode(sub_workflow_id=sub_workflow_id):
            facts = {
                'is_subworkflow': True,
                'task_id': None,
                'sub_workflow_id': sub_workflow_id,
                'max_retries': None,
                'execution': None,
                'retention_class_key': None,
                'sub_workflow_params': 'NOT_GIVEN',
            }
        case _ as unreachable:
            assert_never(unreachable)
    return {
        'id': row.id,
        'workflow_id': row.workflow_id,
        'task_index': row.task_index,
        'node_id': row.node_id,
        'task_name': row.task_name,
        'input': FIXTURE_INPUT,
        'dependencies': list(row.dependencies),
        'allow_failed_deps': row.allow_failed_deps,
        'status': row.status,
        'result': row.result,
        'created_at': row.created_at,
        'started_at': row.started_at,
        'completed_at': row.completed_at,
        **facts,
    }


def _schedule_params(row: ScheduleRow) -> dict[str, object]:
    match row.last_run:
        case None:
            last_run_at, last_task_id, run_count = None, None, 0
        case LastRun(at=at, task_id=task_id, run_count=count):
            last_run_at, last_task_id, run_count = at, task_id, count
    return {
        'schedule_name': row.schedule_name,
        'next_run_at': row.next_run_at,
        'last_run_at': last_run_at,
        'last_task_id': last_task_id,
        'run_count': run_count,
        'config_hash': sha256(row.schedule_name.encode()).digest(),
    }


# --------------------------------------------------------------------------- #
# Insert
# --------------------------------------------------------------------------- #
async def insert_rows(session: AsyncSession, *rows: SeedRow) -> None:
    """Write the rows where production holds them, and commit.

    Order: workflows (roots first), tasks, the attempts of live tasks, nodes,
    schedule states, so every foreign key finds its row.
    """
    tasks = [row for row in rows if isinstance(row, TaskRow)]
    ended_ids = {
        task.id for task in tasks if isinstance(task.lifecycle, EndedTask)
    }
    attempts = [row for row in rows if isinstance(row, AttemptRow)]
    workflows = sorted(
        (row for row in rows if isinstance(row, WorkflowRow)),
        key=lambda row: row.parent is not None,
    )
    nodes = [row for row in rows if isinstance(row, NodeRow)]
    schedules = [row for row in rows if isinstance(row, ScheduleRow)]
    workflow_of_task = {
        node.kind.task_id: node.workflow_id
        for node in nodes
        if isinstance(node.kind, TaskNode) and node.kind.task_id is not None
    }

    connection = await session.connection()
    if tasks:
        await ensure_history_seedable(connection)
    for workflow in workflows:
        await connection.execute(_INSERT_WORKFLOW_SQL, _workflow_params(workflow))
    for task in tasks:
        match task.lifecycle:
            case LiveTask() as live:
                await connection.execute(
                    _INSERT_TASK_SQL, _live_task_params(task, live),
                )
            case EndedTask() as ended:
                # A task that ended before the coverage (a few seconds
                # ago, just after midnight UTC) needs the leaf of its day.
                terminal_at = await cover_history_instant(
                    connection, HISTORY_SEED_CLASS_KEY, ended.terminal_at,
                )
                await connection.execute(
                    _INSERT_HISTORY_SQL,
                    _history_params(
                        task,
                        ended,
                        terminal_at,
                        [row for row in attempts if row.task_id == task.id],
                        workflow_of_task.get(task.id),
                    ),
                )
            case _ as unreachable:
                assert_never(unreachable)
    for attempt in attempts:
        if attempt.task_id not in ended_ids:
            await connection.execute(_INSERT_ATTEMPT_SQL, _attempt_params(attempt))
    for node in nodes:
        await connection.execute(_INSERT_NODE_SQL, _node_params(node))
    for schedule in schedules:
        await connection.execute(_INSERT_SCHEDULE_SQL, _schedule_params(schedule))
    await session.commit()
