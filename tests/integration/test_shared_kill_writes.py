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
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.models.tasks import (
    Execution,
    OperationalErrorCode,
    OutcomeCode,
    TaskError,
    TaskResult,
)
from horsies.core.worker.child_protocol import SeatFinished, SeatTask
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.dispatch_claim import DispatchClaim
from horsies.core.worker.shared_stop import (
    BesideBlockedLoop,
    BlockedTheLoop,
    CancelEnd,
    KilledRunVerdict,
    RetryPlacement,
    ReturnedBeforeKill,
    TimedOutAtKill,
)
from horsies.core.worker.slots import (
    ChildKilledPastDeadlines,
    KilledBySignal,
    SeatDeadlineEnded,
    SeatKilled,
    SharedHandle,
    SharedOutcome,
    SharedRunRequest,
)
from horsies.core.worker.thread_slots import RunCulprit
from horsies.core.worker.worker import Worker
from tests.integration.conftest import compute_test_command_fingerprint
from tests.integration.history_seeding import read_attempts

pytestmark = [pytest.mark.integration]

WORKER_ID = 'w-shared-kill-test'
FAKE_DEAD_PID = 2**22 - 1
TIMEOUT_MS = 5_000
WORK = SeatTask(timeout_ms=TIMEOUT_MS, cancel_grace_ms=1_000)
CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)


def _options(auto_retry_for: list[str]) -> str:
    return json.dumps(
        {
            'retry_policy': {
                'max_retries': 3,
                'intervals': [1, 1, 1],
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
        dsn='postgresql://u:p@localhost/db',
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
    sent_at, fingerprint = compute_test_command_fingerprint(
        task_name='shared_kill', task_options=options
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    created_at, updated_at, retry_count, max_retries, started_at,
                    claimed_by_worker_id, claimed_at, worker_pid, worker_hostname,
                    worker_process_name, task_options, execution, retention_class_key,
                    command_fingerprint_version, command_fingerprint,
                    retain_rerun_input, enqueued_at, is_workflow_task, claim_expires_at
                ) VALUES (
                    :id, 'shared_kill', 'default', 100, '{}', :status, :sent_at, NOW(),
                    NOW(), 0, 3, NOW(), :owner, :claimed_at, :worker_pid, 'host-1',
                    'child-1', :options, 'SHARED_ASYNC', 'standard_30d', 1,
                    :command_fingerprint, FALSE, NOW(), FALSE,
                    CASE WHEN CAST(:status AS varchar) = 'CLAIMED' THEN NOW() + INTERVAL '5 minutes' END
                )
        """),
        {
            'id': task_id,
            'status': status,
            'sent_at': sent_at,
            'command_fingerprint': fingerprint,
            'owner': WORKER_ID,
            'claimed_at': CLAIMED_AT,
            'worker_pid': FAKE_DEAD_PID,
            'options': options,
        },
    )
    await session.commit()
    return task_id


async def _claim_of(worker: Worker, task_id: str) -> DispatchClaim:
    """The claim of the seeded row, with the retry facts that ``horsies_claim``
    returns for it."""
    async with worker.sf() as session:
        row = (
            await session.execute(
                text(
                    'SELECT claimed_at, retry_count, max_retries, task_options '
                    'FROM horsies_tasks WHERE id = CAST(:id AS uuid)'
                ),
                {'id': task_id},
            )
        ).one()
    return DispatchClaim(
        claimed_at=row.claimed_at,
        retry_count=row.retry_count,
        max_retries=row.max_retries,
        task_options=row.task_options,
    )


async def _finalize(worker: Worker, task_id: str, outcome: SharedOutcome) -> None:
    claim = await _claim_of(worker, task_id)
    request = SharedRunRequest(
        task_id=task_id,
        task_name='shared_kill',
        args_json=None,
        kwargs_json=None,
        worker_id=WORKER_ID,
        is_workflow_task=False,
        claim=claim,
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
        claim=claim,
    )


def _no_time_left(culprit_dispatch: int) -> ChildKilledPastDeadlines:
    """The child was killed when no run had time left. The run under test is
    run 1: it is the culprit when ``culprit_dispatch`` is 1."""
    return ChildKilledPastDeadlines(
        pid=FAKE_DEAD_PID,
        child_exit=KilledBySignal(9),
        culprit=RunCulprit(dispatch=culprit_dispatch, task_id='culprit'),
    )


def _killed(verdict: KilledRunVerdict) -> SeatKilled:
    return SeatKilled(verdict=verdict, pid=FAKE_DEAD_PID, child_exit=KilledBySignal(9))


async def _row(session: AsyncSession, task_id: str) -> Row[Any]:
    session.expire_all()
    row = (
        await session.execute(
            text("""
                SELECT status, retry_count, execution
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
        (_no_time_left(culprit_dispatch=1), 'ISOLATED'),
        (_no_time_left(culprit_dispatch=2), 'SHARED_ASYNC'),
        (_killed(TimedOutAtKill(retry=RetryPlacement.SAME_CLASS)), 'SHARED_ASYNC'),
        (
            SeatDeadlineEnded(
                retry=RetryPlacement.ISOLATED,
                cancel=CancelEnd.DROPPED_AFTER_GRACE,
            ),
            'ISOLATED',
        ),
        (
            SeatDeadlineEnded(
                retry=RetryPlacement.SAME_CLASS,
                cancel=CancelEnd.RETURNED_AFTER_SIGNAL,
            ),
            'SHARED_ASYNC',
        ),
    ],
    ids=[
        'killed-as-culprit',
        'killed-beside-the-culprit',
        'killed-past-deadline-no-named-cause',
        'stuck-then-ended',
        'ended-at-timeout',
    ],
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
@pytest.mark.parametrize(
    ('outcome', 'cancel_end'),
    [
        (
            SeatDeadlineEnded(
                retry=RetryPlacement.SAME_CLASS,
                cancel=CancelEnd.RETURNED_AFTER_SIGNAL,
            ),
            {'cancel_end': 'RETURNED_AFTER_SIGNAL'},
        ),
        (
            SeatDeadlineEnded(
                retry=RetryPlacement.ISOLATED,
                cancel=CancelEnd.DROPPED_AFTER_GRACE,
            ),
            {'cancel_end': 'DROPPED_AFTER_GRACE'},
        ),
        (_killed(TimedOutAtKill(retry=RetryPlacement.SAME_CLASS)), {}),
        (_no_time_left(culprit_dispatch=1), {}),
    ],
    ids=[
        'returned-after-signal',
        'dropped-after-grace',
        'killed-past-deadline',
        'killed-with-no-time-left',
    ],
)
async def test_the_timeout_data_names_how_the_run_ended(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    outcome: SharedOutcome,
    cancel_end: dict[str, str],
) -> None:
    """The data of the TASK_TIMEOUT error names the task and its
    ``timeout_ms``. A run that its child ended also names how it ended after
    the cancel at its deadline; a run that ended at a kill of its child does
    not."""
    task_id = await _insert_shared_task(session, [])

    await _finalize(_worker(engine), task_id, outcome)

    row = await _terminal_row(session, task_id)
    assert (row.status, row.error_code) == ('FAILED', OutcomeCode.TASK_TIMEOUT.value)
    stored = (
        await session.execute(
            text('SELECT result FROM itest_task_rows WHERE id = :id'),
            {'id': task_id},
        )
    ).scalar_one()
    assert json.loads(stored)['err']['data'] == {
        'task_id': task_id,
        'timeout_ms': TIMEOUT_MS,
        **cancel_end,
    }


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


def _returned_error(error_code: str) -> ReturnedBeforeKill:
    """The run returned this error before its deadline; the work that it
    left was stuck when the worker killed its child."""
    payload = serialize_error_payload(
        TaskResult(err=TaskError(error_code=error_code, message='upstream down')),
    )
    return ReturnedBeforeKill(
        returned=SeatFinished(ok=True, result_json=payload, failed_reason=None),
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_a_run_that_returned_before_the_kill_writes_what_it_returned(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The policy retries timeouts: the run is no timeout, so it fails with
    the code that it returned, and runs no more."""
    task_id = await _insert_shared_task(session, ['TASK_TIMEOUT'])

    await _finalize(_worker(engine), task_id, _killed(_returned_error('UPSTREAM_DOWN')))

    row = await _terminal_row(session, task_id)
    assert (row.status, row.error_code) == ('FAILED', 'UPSTREAM_DOWN')
    session.expire_all()
    attempts = await read_attempts(session, task_id)
    assert [(a.attempt, a.will_retry, a.error_code) for a in attempts] == [
        (1, False, 'UPSTREAM_DOWN'),
    ]


@pytest.mark.asyncio(loop_scope='function')
async def test_the_retry_of_a_run_that_returned_before_the_kill_stays_in_its_class(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The run is no culprit: its retry is not moved to an isolated child."""
    task_id = await _insert_shared_task(session, ['UPSTREAM_DOWN'])

    await _finalize(_worker(engine), task_id, _killed(_returned_error('UPSTREAM_DOWN')))

    row = await _row(session, task_id)
    assert (row.status, row.retry_count, row.execution) == (
        'PENDING',
        1,
        'SHARED_ASYNC',
    )
    attempt = await _attempt(session, task_id)
    assert (attempt.will_retry, attempt.error_code) == (True, 'UPSTREAM_DOWN')
