"""Every terminal writer pinned in both directions, against real PostgreSQL.

Sixteen statements can move a `horsies_tasks` row to a terminal status. Each is
exercised twice here: once where its guards are satisfied and the transition
must happen, once where they are not and the row must be left alone.

Coverage is driven by `tests/lifecycle_matrix.py` rather than by this file's
contents. Every row of that matrix must have a scenario registered below, so a
writer cannot be added to the matrix and silently go unexercised.

What a scenario supplies is the arrangement and the invocation, because those
differ per writer — a deadline for expiry, a stale heartbeat for the reaper, a
missing workflow linkage for the orphan paths. What the tests assert is uniform
and read off the matrix row: the eligible case reaches the declared target
status and records `terminal_at`; the ineligible case changes neither.

These are characterization tests. They pin behavior as it is, including where
it is asymmetric between writers, so that a later refactor is measured against
what the system does rather than what it was assumed to do.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

import psycopg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import (
    EXPIRE_PENDING_TASKS_SQL,
    MARK_STALE_TASK_FAILED_SQL,
    TERMINATE_ORPHANED_CLAIMED_WORKFLOW_TASKS_SQL,
)
from horsies.core.models.workflow.handle import (
    MARK_ENQUEUED_NOT_STARTED_TASKS_CANCELLED_SQL,
)
from horsies.core.worker.child_runner import (
    _expire_claimed_task_before_start,
    _handle_workflow_stop_before_start,
)
from horsies.core.worker.sql import (
    CANCEL_CANCELLED_WORKFLOW_TASKS_SQL,
    FINALIZE_TASK_COMPLETED_SQL,
    MARK_TASK_COMPLETED_SQL,
    MARK_TASK_FAILED_SQL,
    MARK_TASK_FAILED_WORKER_SQL,
    TERMINATE_ORPHANED_WORKFLOW_TASK_SQL,
    UNCLAIM_PAUSED_TASKS_SQL,
)
from horsies.core.workflows.sql import (
    CANCEL_CLAIMED_TASKS_FOR_PAUSED_WORKFLOWS_SQL,
)
from horsies.monitoring.task_actions import _CANCEL_TASK_SQL
from tests.integration.conftest import compute_test_enqueue_sha
from tests.lifecycle_matrix import MATRIX, TerminalWriter

pytestmark = [pytest.mark.integration]

WORKER_ID = 'characterization-worker'
OTHER_WORKER_ID = 'characterization-worker-other'


# ---------------------------------------------------------------------------
# Arrangement helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TaskRow:
    """A seeded task and the claim generation it was written with."""

    task_id: str
    claimed_at: datetime | None


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _seed_task(
    session: AsyncSession,
    *,
    status: str,
    worker_id: str | None = WORKER_ID,
    claimed_at: datetime | None = None,
    is_workflow_task: bool = False,
    good_until: datetime | None = None,
    started_at: datetime | None = None,
) -> TaskRow:
    """Insert one task row in a chosen lifecycle position."""
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='characterize')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, enqueued_at, created_at, updated_at,
                 claimed, retry_count, max_retries, claimed_at,
                 claimed_by_worker_id, enqueue_sha, is_workflow_task,
                 good_until, started_at, worker_hostname, worker_pid,
                 worker_process_name)
            VALUES
                (:id, 'characterize', 'default', 100, '[]', '{}',
                 :status, :sent_at, NOW(), NOW(), NOW(),
                 :claimed, 0, 0, :claimed_at,
                 :worker_id, :sha, :is_wf,
                 :good_until, :started_at, 'host', 1, 'proc')
        """),
        {
            'id': task_id,
            'status': status,
            'sent_at': sent_at,
            'claimed': worker_id is not None,
            'claimed_at': claimed_at,
            'worker_id': worker_id,
            'sha': sha,
            'is_wf': is_workflow_task,
            'good_until': good_until,
            'started_at': started_at,
        },
    )
    return TaskRow(task_id=task_id, claimed_at=claimed_at)


async def _seed_workflow(
    session: AsyncSession,
    task_id: str,
    *,
    wf_status: str,
    wt_status: str = 'ENQUEUED',
) -> str:
    """Link a task to a workflow in a chosen state."""
    wf_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES (:id, 'characterize_wf', :status, 'FAIL', 0, :id,
                    NOW(), NOW(), NOW(), NOW())
        """),
        {'id': wf_id, 'status': wf_status},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args,
                 task_kwargs, queue_name, priority, dependencies,
                 allow_failed_deps, join_type, is_subworkflow, status,
                 task_id, created_at)
            VALUES (:id, :wf_id, 0, 'node_0', 'characterize', '[]', '{}',
                    'default', 100, '{}', FALSE, 'all', FALSE, :wt_status,
                    :task_id, NOW())
        """),
        {
            'id': str(uuid.uuid4()),
            'wf_id': wf_id,
            'task_id': task_id,
            'wt_status': wt_status,
        },
    )
    return wf_id


async def _task_state(
    session: AsyncSession,
    task_id: str,
) -> tuple[str, datetime | None]:
    row = (
        await session.execute(
            text('SELECT status, terminal_at FROM horsies_tasks WHERE id = :id'),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None, f'task {task_id} vanished'
    return str(row[0]), row[1]


def _psycopg_dsn(db_url: str) -> str:
    return db_url.replace('postgresql+psycopg://', 'postgresql://')


# ---------------------------------------------------------------------------
# Scenario registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Context:
    """What a scenario needs to arrange and invoke its writer."""

    session: AsyncSession
    db_url: str


Scenario = Callable[[Context, bool], Awaitable[str]]
"""Arrange and invoke one writer; return the task id to assert on.

The bool is whether the writer's guards should be satisfied. What makes a
writer ineligible is the writer's own business — a replaced claim generation,
a wrong owner, an unmet deadline, a live workflow linkage — so each scenario
decides for itself.
"""


async def _t01_monitoring_cancel(ctx: Context, eligible: bool) -> str:
    task = await _seed_task(ctx.session, status='CLAIMED', claimed_at=_now())
    await ctx.session.commit()
    # The caller supplies the permitted source statuses; ineligible here means
    # the row's status is outside the set the caller allows.
    allowed = ['PENDING', 'CLAIMED'] if eligible else ['PENDING']
    await ctx.session.execute(
        _CANCEL_TASK_SQL, {'id': task.task_id, 'allowed': allowed},
    )
    await ctx.session.commit()
    return task.task_id


async def _paused_batch(ctx: Context, eligible: bool) -> str:
    generation = _now() - timedelta(minutes=5)
    task = await _seed_task(
        ctx.session, status='CLAIMED', claimed_at=generation, is_workflow_task=True,
    )
    await _seed_workflow(ctx.session, task.task_id, wf_status='PAUSED')
    await ctx.session.commit()
    dispatched = generation if eligible else generation - timedelta(minutes=1)
    await ctx.session.execute(
        UNCLAIM_PAUSED_TASKS_SQL,
        {
            'ids': [task.task_id],
            'claimed_ats': [dispatched],
            'wid': WORKER_ID,
        },
    )
    await ctx.session.commit()
    return task.task_id


async def _cancelled_batch(ctx: Context, eligible: bool) -> str:
    generation = _now() - timedelta(minutes=5)
    task = await _seed_task(
        ctx.session, status='CLAIMED', claimed_at=generation, is_workflow_task=True,
    )
    await _seed_workflow(ctx.session, task.task_id, wf_status='CANCELLED')
    await ctx.session.commit()
    dispatched = generation if eligible else generation - timedelta(minutes=1)
    await ctx.session.execute(
        CANCEL_CANCELLED_WORKFLOW_TASKS_SQL,
        {
            'ids': [task.task_id],
            'claimed_ats': [dispatched],
            'wid': WORKER_ID,
        },
    )
    await ctx.session.commit()
    return task.task_id


async def _fail_worker(ctx: Context, eligible: bool) -> str:
    task = await _seed_task(ctx.session, status='RUNNING', claimed_at=_now())
    await ctx.session.commit()
    await ctx.session.execute(
        MARK_TASK_FAILED_WORKER_SQL,
        {
            'id': task.task_id,
            'wid': WORKER_ID if eligible else OTHER_WORKER_ID,
            'reason': 'characterization',
            'result_json': '{"err": null}',
            'error_code': 'BROKER_ERROR',
        },
    )
    await ctx.session.commit()
    return task.task_id


async def _fail_running(ctx: Context, eligible: bool) -> str:
    task = await _seed_task(ctx.session, status='RUNNING', claimed_at=_now())
    await ctx.session.commit()
    await ctx.session.execute(
        MARK_TASK_FAILED_SQL,
        {
            'id': task.task_id,
            'wid': WORKER_ID if eligible else OTHER_WORKER_ID,
            'result_json': '{"err": null}',
            'error_code': 'WORKER_SERIALIZATION_ERROR',
        },
    )
    await ctx.session.commit()
    return task.task_id


async def _complete_running(ctx: Context, eligible: bool) -> str:
    task = await _seed_task(ctx.session, status='RUNNING', claimed_at=_now())
    await ctx.session.commit()
    await ctx.session.execute(
        MARK_TASK_COMPLETED_SQL,
        {
            'id': task.task_id,
            'wid': WORKER_ID if eligible else OTHER_WORKER_ID,
            'result_json': '{"ok": 1}',
        },
    )
    await ctx.session.commit()
    return task.task_id


async def _fused_complete(ctx: Context, eligible: bool) -> str:
    generation = _now() - timedelta(minutes=1)
    task = await _seed_task(
        ctx.session,
        status='RUNNING',
        claimed_at=generation,
        started_at=generation,
    )
    await ctx.session.commit()
    # Ineligible here is the claim-generation race the fused path exists to
    # reject: same worker, same status, a generation it no longer owns.
    dispatched = generation if eligible else generation - timedelta(minutes=1)
    await ctx.session.execute(
        FINALIZE_TASK_COMPLETED_SQL,
        {
            'id': task.task_id,
            'wid': WORKER_ID,
            'claimed_at': dispatched,
            'result_json': '{"ok": 1}',
            'notify_channel': 'task_queue_default',
            'notify_payload': f'capacity:{task.task_id}',
        },
    )
    await ctx.session.commit()
    return task.task_id


async def _orphan_single(ctx: Context, eligible: bool) -> str:
    generation = _now() - timedelta(minutes=1)
    task = await _seed_task(
        ctx.session, status='CLAIMED', claimed_at=generation, is_workflow_task=True,
    )
    if not eligible:
        # A live workflow-task linkage means it is not an orphan.
        await _seed_workflow(ctx.session, task.task_id, wf_status='RUNNING')
    await ctx.session.commit()
    await ctx.session.execute(
        TERMINATE_ORPHANED_WORKFLOW_TASK_SQL,
        {'id': task.task_id, 'wid': WORKER_ID, 'claimed_at': generation},
    )
    await ctx.session.commit()
    return task.task_id


async def _paused_workflow_api(ctx: Context, eligible: bool) -> str:
    task = await _seed_task(
        ctx.session, status='CLAIMED', claimed_at=_now(), is_workflow_task=True,
    )
    wf_id = await _seed_workflow(
        ctx.session,
        task.task_id,
        wf_status='PAUSED' if eligible else 'RUNNING',
    )
    await ctx.session.commit()
    await ctx.session.execute(
        CANCEL_CLAIMED_TASKS_FOR_PAUSED_WORKFLOWS_SQL, {'workflow_ids': [wf_id]},
    )
    await ctx.session.commit()
    return task.task_id


async def _child_pause(ctx: Context, eligible: bool) -> str:
    generation = _now() - timedelta(minutes=1)
    task = await _seed_task(
        ctx.session, status='CLAIMED', claimed_at=generation, is_workflow_task=True,
    )
    await _seed_workflow(ctx.session, task.task_id, wf_status='PAUSED')
    await ctx.session.commit()
    dispatched = generation if eligible else generation - timedelta(minutes=1)
    with psycopg.connect(_psycopg_dsn(ctx.db_url)) as conn:
        with conn.cursor() as cursor:
            _handle_workflow_stop_before_start(
                cursor, conn, task.task_id, 'PAUSED', WORKER_ID, dispatched,
            )
    await ctx.session.commit()
    return task.task_id


async def _child_cancel(ctx: Context, eligible: bool) -> str:
    generation = _now() - timedelta(minutes=1)
    task = await _seed_task(
        ctx.session, status='CLAIMED', claimed_at=generation, is_workflow_task=True,
    )
    await _seed_workflow(ctx.session, task.task_id, wf_status='CANCELLED')
    await ctx.session.commit()
    dispatched = generation if eligible else generation - timedelta(minutes=1)
    with psycopg.connect(_psycopg_dsn(ctx.db_url)) as conn:
        with conn.cursor() as cursor:
            _handle_workflow_stop_before_start(
                cursor, conn, task.task_id, 'CANCELLED', WORKER_ID, dispatched,
            )
    await ctx.session.commit()
    return task.task_id


async def _child_expire(ctx: Context, eligible: bool) -> str:
    # The deadline is the guard, not the claim generation: expiry is correct
    # for whichever generation holds a row whose good_until has passed.
    good_until = _now() - timedelta(minutes=1) if eligible else _now() + timedelta(hours=1)
    task = await _seed_task(
        ctx.session, status='CLAIMED', claimed_at=_now(), good_until=good_until,
    )
    await ctx.session.commit()
    with psycopg.connect(_psycopg_dsn(ctx.db_url)) as conn:
        with conn.cursor() as cursor:
            _expire_claimed_task_before_start(
                cursor, conn, task.task_id, WORKER_ID,
            )
    await ctx.session.commit()
    return task.task_id


async def _stale_running(ctx: Context, eligible: bool) -> str:
    started = _now() - timedelta(hours=1)
    task = await _seed_task(
        ctx.session, status='RUNNING', claimed_at=started, started_at=started,
    )
    await ctx.session.commit()
    # Ineligible raises the staleness bar past the row's own age rather than
    # changing the row, so the guard is what differs and nothing else.
    threshold = 60 if eligible else 86400
    await ctx.session.execute(
        MARK_STALE_TASK_FAILED_SQL,
        {
            'task_id': task.task_id,
            'failed_reason': 'characterization',
            'result': '{"err": null}',
            'error_code': 'WORKER_CRASHED',
            'stale_threshold': threshold,
            'finalizing_stale_threshold': threshold,
        },
    )
    await ctx.session.commit()
    return task.task_id


async def _expire_pending(ctx: Context, eligible: bool) -> str:
    good_until = _now() - timedelta(minutes=1) if eligible else _now() + timedelta(hours=1)
    task = await _seed_task(
        ctx.session, status='PENDING', worker_id=None, good_until=good_until,
    )
    await ctx.session.commit()
    await ctx.session.execute(
        EXPIRE_PENDING_TASKS_SQL,
        {
            'result': '{"err": null}',
            'error_code': 'TASK_EXPIRED',
            'batch_size': 100,
        },
    )
    await ctx.session.commit()
    return task.task_id


async def _orphan_batch(ctx: Context, eligible: bool) -> str:
    task = await _seed_task(
        ctx.session, status='CLAIMED', claimed_at=_now(), is_workflow_task=True,
    )
    if not eligible:
        await _seed_workflow(ctx.session, task.task_id, wf_status='RUNNING')
    await ctx.session.commit()
    await ctx.session.execute(TERMINATE_ORPHANED_CLAIMED_WORKFLOW_TASKS_SQL)
    await ctx.session.commit()
    return task.task_id


async def _workflow_cancel_batch(ctx: Context, eligible: bool) -> str:
    task = await _seed_task(
        ctx.session, status='CLAIMED', claimed_at=_now(), is_workflow_task=True,
    )
    wf_id = await _seed_workflow(
        ctx.session,
        task.task_id,
        wf_status='CANCELLED' if eligible else 'RUNNING',
    )
    await ctx.session.commit()
    await ctx.session.execute(
        MARK_ENQUEUED_NOT_STARTED_TASKS_CANCELLED_SQL, {'wf_id': wf_id},
    )
    await ctx.session.commit()
    return task.task_id


SCENARIOS: dict[str, Scenario] = {
    'T01': _t01_monitoring_cancel,
    'T02': _paused_batch,
    'T03': _cancelled_batch,
    'T04': _fail_worker,
    'T05': _fail_running,
    'T06': _complete_running,
    'T07': _fused_complete,
    'T08': _orphan_single,
    'T09': _paused_workflow_api,
    'T10': _child_pause,
    'T11': _child_cancel,
    'T12': _child_expire,
    'T13': _stale_running,
    'T14': _expire_pending,
    'T15': _orphan_batch,
    'T16': _workflow_cancel_batch,
}


def _ids(rows: tuple[TerminalWriter, ...]) -> list[str]:
    return [f'{row.writer_id}-{row.statement}' for row in rows]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_every_writer_has_a_scenario() -> None:
    """Coverage is driven by the matrix, not by this file's contents."""
    declared = {row.writer_id for row in MATRIX}
    registered = set(SCENARIOS)
    assert registered == declared, (
        f'unexercised: {sorted(declared - registered)}\n'
        f'unknown: {sorted(registered - declared)}'
    )


@pytest.mark.parametrize('row', MATRIX, ids=_ids(MATRIX))
@pytest.mark.asyncio(loop_scope='function')
async def test_transition_applies_when_guards_hold(
    row: TerminalWriter,
    session: AsyncSession,
    db_url: str,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The writer reaches its declared terminal status and dates the row."""
    task_id = await SCENARIOS[row.writer_id](Context(session, db_url), True)

    status, terminal_at = await _task_state(session, task_id)
    assert status == row.target_status, row.writer_id
    assert terminal_at is not None, (
        f'{row.writer_id} reached {status} without recording terminal_at'
    )


@pytest.mark.parametrize('row', MATRIX, ids=_ids(MATRIX))
@pytest.mark.asyncio(loop_scope='function')
async def test_transition_is_refused_when_guards_do_not_hold(
    row: TerminalWriter,
    session: AsyncSession,
    db_url: str,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A writer whose guard fails leaves the row exactly as it found it."""
    task_id = await SCENARIOS[row.writer_id](Context(session, db_url), False)

    status, terminal_at = await _task_state(session, task_id)
    assert status != row.target_status, (
        f'{row.writer_id} terminalized a row whose guards did not hold'
    )
    assert terminal_at is None, (
        f'{row.writer_id} recorded terminal_at on a live row'
    )
