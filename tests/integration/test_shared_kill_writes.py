# pyright: reportPrivateUsage=false
"""The rows of shared async runs that ended at a timeout or at a kill.

Each test enters at ``_finalize_shared_after`` with the outcome that the slot
gives a run, and reads the row and its attempt in a real database: the code,
the retry, and the class that the retry runs in.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.models.tasks import Execution, OperationalErrorCode, OutcomeCode
from horsies.core.worker.child_protocol import SeatTask
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.shared_stop import (
    BesideBlockedLoop,
    BlockedTheLoop,
    KilledRunVerdict,
    RetryPlacement,
    TimedOutAtKill,
)
from horsies.core.worker.slots import (
    KilledBySignal,
    SeatDeadlineEnded,
    SeatKilled,
    SharedHandle,
    SharedOutcome,
    SharedRunRequest,
)
from horsies.core.worker.worker import Worker
from tests.integration.conftest import compute_test_enqueue_sha

pytestmark = [pytest.mark.integration]

WORKER_ID = 'w-shared-kill-test'
FAKE_DEAD_PID = 2**22 - 1
TIMEOUT_MS = 5_000
WORK = SeatTask(timeout_ms=TIMEOUT_MS, cancel_grace_ms=1_000)


def _options(auto_retry_for: list[str]) -> str:
    return json.dumps(
        {
            'retry_policy': {
                'max_retries': 3,
                'intervals': [1],
                'backoff_strategy': 'fixed',
                'jitter': False,
                'auto_retry_for': auto_retry_for,
            },
            'timeout_ms': TIMEOUT_MS,
            'execution': Execution.SHARED_ASYNC.value,
            'cancel_grace_ms': 1_000,
        }
    )


def _worker(engine: AsyncEngine) -> Worker:
    cfg = WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
    )
    worker = Worker(
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
        listener=MagicMock(),
        cfg=cfg,
    )
    worker.worker_instance_id = WORKER_ID
    return worker


async def _insert_shared_task(
    session: AsyncSession,
    auto_retry_for: list[str],
    status: str = 'RUNNING',
) -> str:
    task_id = str(uuid.uuid4())
    options = _options(auto_retry_for)
    sent_at, sha = compute_test_enqueue_sha(
        task_name='shared_kill', task_options=options
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs, status,
                 sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, enqueue_sha, claimed_by_worker_id,
                 worker_pid, task_options, execution,
                 retention_class_key, command_fingerprint_version,
                 command_fingerprint, retain_rerun_input,
                 prepared_rerun_input_disposition)
            VALUES
                (:id, 'shared_kill', 'default', 100, '[]', '{}', :status,
                 :sent_at, NOW(), NOW(), TRUE, 0,
                 3, NOW(), :sha, :owner,
                 :worker_pid, :options, 'SHARED_ASYNC',
                 'standard_30d', 1,
                 sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                 FALSE, 'DECLINED_BY_POLICY')
        """),
        {
            'id': task_id,
            'status': status,
            'sent_at': sent_at,
            'sha': sha,
            'owner': WORKER_ID,
            'worker_pid': FAKE_DEAD_PID,
            'options': options,
        },
    )
    await session.commit()
    return task_id


async def _finalize(worker: Worker, task_id: str, outcome: SharedOutcome) -> None:
    request = SharedRunRequest(
        task_id=task_id,
        task_name='shared_kill',
        args_json=None,
        kwargs_json=None,
        worker_id=WORKER_ID,
        is_workflow_task=False,
        claimed_at=None,
        work=WORK,
    )
    handle = SharedHandle(request.frame_for(1), asyncio.get_running_loop())
    handle.finish(outcome)
    _ = await worker._finalize_shared_after(
        handle,
        task_id,
        'default',
        False,
        WORK,
        task_name='shared_kill',
        claimed_at=None,
    )


def _killed(verdict: KilledRunVerdict) -> SeatKilled:
    return SeatKilled(verdict=verdict, pid=FAKE_DEAD_PID, child_exit=KilledBySignal(9))


async def _row(session: AsyncSession, task_id: str) -> Row[Any]:
    session.expire_all()
    row = (
        await session.execute(
            text("""
                SELECT status, error_code, retry_count, execution
                FROM horsies_tasks WHERE id = CAST(:id AS uuid)
            """),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None
    return row


async def _terminal_row(session: AsyncSession, task_id: str) -> Row[Any]:
    session.expire_all()
    row = (
        await session.execute(
            text('SELECT status, error_code FROM itest_task_rows WHERE id = :id'),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None
    return row


async def _attempt(session: AsyncSession, task_id: str) -> Row[Any]:
    row = (
        await session.execute(
            text("""
                SELECT will_retry, error_code
                FROM horsies_task_attempts WHERE task_id = :id
            """),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None
    return row


@pytest.mark.asyncio(loop_scope='function')
@pytest.mark.parametrize(
    ('outcome', 'execution'),
    [
        (_killed(TimedOutAtKill(retry=RetryPlacement.ISOLATED)), 'ISOLATED'),
        (_killed(TimedOutAtKill(retry=RetryPlacement.SAME_CLASS)), 'SHARED_ASYNC'),
        (SeatDeadlineEnded(retry=RetryPlacement.ISOLATED), 'ISOLATED'),
        (SeatDeadlineEnded(retry=RetryPlacement.SAME_CLASS), 'SHARED_ASYNC'),
    ],
    ids=['stuck-kill', 'kill-past-deadline', 'stuck-then-ended', 'ended-at-timeout'],
)
async def test_a_timeout_retry_runs_where_its_verdict_says(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    outcome: SharedOutcome,
    execution: str,
) -> None:
    task_id = await _insert_shared_task(session, ['TASK_TIMEOUT'])

    await _finalize(_worker(engine), task_id, outcome)

    row = await _row(session, task_id)
    assert (row.status, row.retry_count, row.execution) == ('PENDING', 1, execution)
    attempt = await _attempt(session, task_id)
    assert (attempt.will_retry, attempt.error_code) == (
        True,
        OutcomeCode.TASK_TIMEOUT.value,
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_the_task_that_blocked_the_loop_retries_isolated(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    task_id = await _insert_shared_task(session, ['TASK_BLOCKED_LOOP'])

    await _finalize(
        _worker(engine),
        task_id,
        _killed(BlockedTheLoop(blocked_ms=5_100, where='app.py:3 in fetch')),
    )

    row = await _row(session, task_id)
    assert (row.status, row.retry_count, row.execution) == ('PENDING', 1, 'ISOLATED')
    attempt = await _attempt(session, task_id)
    assert attempt.error_code == OperationalErrorCode.TASK_BLOCKED_LOOP.value


@pytest.mark.asyncio(loop_scope='function')
async def test_a_neighbour_fails_with_its_code_when_its_policy_does_not_name_it(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """No free second run: a policy that retries timeouts only does not
    retry a neighbour of a blocked loop."""
    task_id = await _insert_shared_task(session, ['TASK_TIMEOUT'])

    await _finalize(
        _worker(engine),
        task_id,
        _killed(BesideBlockedLoop(holder_task_id='task-9', blocked_ms=5_100)),
    )

    row = await _terminal_row(session, task_id)
    assert (row.status, row.error_code) == (
        'FAILED',
        OperationalErrorCode.NEIGHBOUR_BLOCKED_LOOP.value,
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_a_neighbour_retries_in_its_class_when_its_policy_names_the_code(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    task_id = await _insert_shared_task(session, ['NEIGHBOUR_BLOCKED_LOOP'])

    await _finalize(
        _worker(engine),
        task_id,
        _killed(BesideBlockedLoop(holder_task_id='task-9', blocked_ms=5_100)),
    )

    row = await _row(session, task_id)
    assert (row.status, row.retry_count, row.execution) == (
        'PENDING',
        1,
        'SHARED_ASYNC',
    )
    attempt = await _attempt(session, task_id)
    assert attempt.error_code == OperationalErrorCode.NEIGHBOUR_BLOCKED_LOOP.value


@pytest.mark.asyncio(loop_scope='function')
async def test_a_neighbour_whose_code_had_not_started_returns_to_the_queue(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A row still CLAIMED ran no user code: it is requeued, with no attempt."""
    task_id = await _insert_shared_task(session, ['TASK_TIMEOUT'], status='CLAIMED')

    await _finalize(
        _worker(engine),
        task_id,
        _killed(BesideBlockedLoop(holder_task_id='task-9', blocked_ms=5_100)),
    )

    row = await _row(session, task_id)
    assert (row.status, row.retry_count, row.execution) == (
        'PENDING',
        0,
        'SHARED_ASYNC',
    )
