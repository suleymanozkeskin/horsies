"""``horsies_finish_failed_attempt`` against real PostgreSQL: each outcome.

The function is called through the worker's own client
(``finish_failed_attempt``) on an autocommit connection, as the failure
paths call it, so every test also runs the decoder. Each test asserts what
was written and what was not: the live row, the attempt rows, the history
row, the phase-2 pending row and the capacity wake.
"""

from __future__ import annotations

import math
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.archive.attempts import AttemptRecord
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    CapacityWoken,
    FailedAttemptOutcome,
    FailureApplied,
    LostClaim,
    ObservedForeignTerminalization,
    PhaseTwoOwed,
    RetryNotRequested,
    RetryPastDeadline,
    RetryScheduled,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.types.status import TaskStatus
from horsies.core.utils.db import autocommit_connection
from horsies.core.worker.failed_attempt import (
    AttemptFailure,
    AttemptFailureKind,
    FinishFailedAttempt,
    NoRetry,
    RetryAfter,
    RetryRequest,
    finish_failed_attempt,
)
from horsies.core.worker.shared_stop import RetryPlacement
from tests.integration.conftest import DB_URL, compute_test_enqueue_sha
from tests.integration.history_seeding import read_attempts

pytestmark = [pytest.mark.integration]

WORKER = 'w-failed-attempt'
QUEUE = 'failed_attempt_q'
TASK_NAME = 'failed_attempt_task'
ERROR_RESULT = '{"__h_task_result__":true,"ok":null,"err":{"error_code":"BOOM"}}'
NOTIFY_WAIT_S = 0.5
"""How long a test listens for a capacity wake that must not come."""


@dataclass(frozen=True, slots=True)
class _Seeded:
    task_id: str
    claimed_at: datetime


@pytest_asyncio.fixture
async def seed(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - schema and history leaves
) -> AsyncIterator[Any]:
    """A factory of RUNNING rows held by ``WORKER``."""

    async def make(
        *,
        retry_count: int = 0,
        max_retries: int = 0,
        good_until: datetime | None = None,
        is_workflow_task: bool = False,
        status: str = 'RUNNING',
    ) -> _Seeded:
        task_id = str(uuid.uuid4())
        claimed_at = datetime.now(timezone.utc) - timedelta(seconds=30)
        sent_at, sha = compute_test_enqueue_sha(
            task_name=TASK_NAME,
            queue_name=QUEUE,
            good_until=good_until,
        )
        async with AsyncSession(engine) as session:
            await session.execute(
                text("""
                    INSERT INTO horsies_tasks
                        (id, task_name, queue_name, priority, args, kwargs,
                         status, sent_at, created_at, updated_at, claimed,
                         retry_count, max_retries, started_at, good_until,
                         enqueue_sha, claimed_by_worker_id, claimed_at,
                         is_workflow_task, worker_hostname, worker_pid,
                         worker_process_name, retention_class_key,
                         command_fingerprint_version, command_fingerprint,
                         retain_rerun_input, prepared_rerun_input_disposition)
                    VALUES
                        (:id, :task_name, :queue, 100, '[]', '{}',
                         CAST(:status AS varchar), :sent_at, NOW(), NOW(), FALSE,
                         :retry_count, :max_retries,
                         CASE WHEN CAST(:status AS varchar) = 'RUNNING'
                              THEN NOW() END,
                         :good_until, :sha, :worker, :claimed_at,
                         :is_workflow_task, 'host-1', 4242, 'child-1',
                         'standard_30d', 1,
                         sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                         FALSE, 'DECLINED_BY_POLICY')
                """),
                {
                    'id': task_id,
                    'task_name': TASK_NAME,
                    'queue': QUEUE,
                    'status': status,
                    'sent_at': sent_at,
                    'retry_count': retry_count,
                    'max_retries': max_retries,
                    'good_until': good_until,
                    'sha': sha,
                    'worker': WORKER,
                    'claimed_at': claimed_at,
                    'is_workflow_task': is_workflow_task,
                },
            )
            if is_workflow_task:
                await _link_workflow_node(session, task_id)
            await session.commit()
        return _Seeded(task_id=task_id, claimed_at=claimed_at)

    yield make


async def _link_workflow_node(session: AsyncSession, task_id: str) -> None:
    workflow_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES (:wf, 'failed_attempt_wf', 'RUNNING', 'FAIL', 0, :wf,
                    NOW(), NOW(), NOW(), NOW())
        """),
        {'wf': workflow_id},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args,
                 task_kwargs, queue_name, priority, dependencies,
                 allow_failed_deps, join_type, is_subworkflow, status,
                 task_id, created_at)
            VALUES (:id, :wf, 0, 'node-0', :task_name, '[]', '{}', :queue,
                    100, '{}', FALSE, 'all', FALSE, 'RUNNING',
                    CAST(:task_id AS uuid), NOW())
        """),
        {
            'id': str(uuid.uuid4()),
            'wf': workflow_id,
            'task_name': TASK_NAME,
            'queue': QUEUE,
            'task_id': task_id,
        },
    )


def _own(seeded: _Seeded) -> OwnedClaim:
    """The fence of the claim that seeded the row."""
    return OwnedClaim(worker_id=WORKER, claimed_at=seeded.claimed_at)


def _command(
    seeded: _Seeded,
    *,
    retry: RetryRequest = NoRetry(),
    fence: OwnedClaim | None = None,
) -> FinishFailedAttempt:
    """The finish of ``seeded``, under its own claim unless ``fence`` is given."""
    return FinishFailedAttempt(
        task_id=seeded.task_id,
        fence=_own(seeded) if fence is None else fence,
        retry=retry,
        result_json=ERROR_RESULT,
        error_code='BOOM',
        failed_reason='final reason',
        attempt=AttemptFailure(
            kind=AttemptFailureKind.FAILED,
            error_message='boom message',
            failed_reason='attempt reason',
        ),
    )


def _retry(delay_s: float = 30.0, *, retry_count: int = 0) -> RetryAfter:
    return RetryAfter(
        retry_count=retry_count,
        delay_s=delay_s,
        placement=RetryPlacement.SAME_CLASS,
    )


async def _finish(
    engine: AsyncEngine, command: FinishFailedAttempt
) -> FailedAttemptOutcome:
    async with AsyncSession(engine) as session:
        connection = await autocommit_connection(session)
        return await finish_failed_attempt(connection, command)


@pytest_asyncio.fixture
async def wakes() -> AsyncIterator[psycopg.AsyncConnection[Any]]:
    """A connection that listens on the capacity channel of ``QUEUE``."""
    connection = await psycopg.AsyncConnection.connect(
        DB_URL.replace('postgresql+psycopg://', 'postgresql://'),
        autocommit=True,
    )
    await connection.execute(f'LISTEN task_queue_{QUEUE}')
    yield connection
    await connection.close()


async def _payloads(connection: psycopg.AsyncConnection[Any]) -> list[str]:
    return [
        notify.payload async for notify in connection.notifies(timeout=NOTIFY_WAIT_S)
    ]


async def _live_row(engine: AsyncEngine, task_id: str) -> Any:
    async with engine.connect() as connection:
        return (
            await connection.execute(
                text("""
                    SELECT status, retry_count, next_retry_at, enqueued_at,
                           claimed_by_worker_id, claimed_at, started_at,
                           execution, updated_at
                    FROM horsies_tasks WHERE id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        ).one_or_none()


async def _history_row(engine: AsyncEngine, task_id: str) -> Any:
    async with engine.connect() as connection:
        return (
            await connection.execute(
                text("""
                    SELECT status, terminalization_kind, error_code,
                           final_failed_reason,
                           convert_from(result_payload, 'UTF8') AS result
                    FROM horsies_task_history WHERE task_id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        ).one_or_none()


async def _attempts(engine: AsyncEngine, task_id: str) -> tuple[AttemptRecord, ...]:
    async with AsyncSession(engine) as session:
        return await read_attempts(session, task_id)


async def _database_clock(engine: AsyncEngine) -> datetime:
    """The database clock: the function computes the retry time from it."""
    async with engine.connect() as connection:
        return (await connection.execute(text('SELECT clock_timestamp()'))).scalar_one()


async def _pending_rows(engine: AsyncEngine, task_id: str) -> int:
    async with engine.connect() as connection:
        return (
            await connection.execute(
                text(
                    'SELECT count(*) FROM horsies_workflow_phase2_pending '
                    'WHERE task_id = CAST(:id AS uuid)'
                ),
                {'id': task_id},
            )
        ).scalar_one()


@pytest.mark.asyncio(loop_scope='function')
class TestTerminalFailure:
    async def test_a_plain_failure_moves_the_row_with_its_attempt_and_wakes_the_queue(
        self,
        engine: AsyncEngine,
        seed: Any,
        wakes: psycopg.AsyncConnection[Any],
    ) -> None:
        seeded = await seed()

        outcome = await _finish(engine, _command(seeded))

        match outcome:
            case FailureApplied(
                task_id=task_id,
                retry=RetryNotRequested(),
                after=CapacityWoken(),
                observed=observed,
            ):
                assert task_id == seeded.task_id
                assert observed.status is TaskStatus.RUNNING
                assert observed.worker_id == WORKER
                assert observed.claimed_at == seeded.claimed_at
            case other:
                raise AssertionError(other)
        assert await _live_row(engine, seeded.task_id) is None
        history = await _history_row(engine, seeded.task_id)
        assert tuple(history) == (
            'FAILED',
            'FAIL_RUNNING',
            'BOOM',
            'final reason',
            ERROR_RESULT,
        )
        (attempt,) = await _attempts(engine, seeded.task_id)
        assert (
            attempt.attempt,
            attempt.outcome,
            attempt.will_retry,
            attempt.error_code,
            attempt.error_message,
            attempt.failed_reason,
            attempt.worker_id,
            attempt.worker_hostname,
            attempt.worker_pid,
            attempt.worker_process_name,
        ) == (
            1,
            'FAILED',
            False,
            'BOOM',
            'boom message',
            'attempt reason',
            WORKER,
            'host-1',
            4242,
            'child-1',
        )
        assert await _payloads(wakes) == [f'capacity:{seeded.task_id}']

    async def test_a_workflow_task_failure_owes_phase_two_and_sends_no_wake(
        self,
        engine: AsyncEngine,
        seed: Any,
        wakes: psycopg.AsyncConnection[Any],
    ) -> None:
        seeded = await seed(is_workflow_task=True)

        outcome = await _finish(engine, _command(seeded))

        match outcome:
            case FailureApplied(after=PhaseTwoOwed(task_name=name, queue_name=queue)):
                assert (name, queue) == (TASK_NAME, QUEUE)
            case other:
                raise AssertionError(other)
        assert (await _history_row(engine, seeded.task_id)).status == 'FAILED'
        assert await _pending_rows(engine, seeded.task_id) == 1
        assert await _payloads(wakes) == []

    async def test_a_worker_failure_records_its_attempt_outcome(
        self,
        engine: AsyncEngine,
        seed: Any,
    ) -> None:
        seeded = await seed()
        command = _command(seeded)
        worker_failure = FinishFailedAttempt(
            task_id=command.task_id,
            fence=command.fence,
            retry=command.retry,
            result_json=command.result_json,
            error_code='BROKER_ERROR',
            failed_reason='worker lost it',
            attempt=AttemptFailure(
                kind=AttemptFailureKind.WORKER_FAILURE,
                error_message='worker lost it',
                failed_reason='worker lost it',
            ),
        )

        outcome = await _finish(engine, worker_failure)

        assert isinstance(outcome, FailureApplied)
        (attempt,) = await _attempts(engine, seeded.task_id)
        assert (attempt.outcome, attempt.error_code, attempt.failed_reason) == (
            'WORKER_FAILURE',
            'BROKER_ERROR',
            'worker lost it',
        )


@pytest.mark.asyncio(loop_scope='function')
class TestRetry:
    async def test_a_retry_writes_pending_and_an_attempt_that_will_retry(
        self,
        engine: AsyncEngine,
        seed: Any,
        wakes: psycopg.AsyncConnection[Any],
    ) -> None:
        seeded = await seed(retry_count=1, max_retries=3)
        before = await _database_clock(engine)

        outcome = await _finish(
            engine,
            _command(seeded, retry=_retry(30.0, retry_count=1)),
        )

        after = await _database_clock(engine)
        match outcome:
            case RetryScheduled(
                retry_count=retry_count,
                next_retry_at=next_retry_at,
                queue_name=queue,
            ):
                assert (retry_count, queue) == (2, QUEUE)
                assert before + timedelta(seconds=30) <= next_retry_at
                assert next_retry_at <= after + timedelta(seconds=30)
            case other:
                raise AssertionError(other)
        row = await _live_row(engine, seeded.task_id)
        assert row.status == 'PENDING'
        assert row.retry_count == 2
        assert row.next_retry_at == outcome.next_retry_at
        assert row.enqueued_at == outcome.next_retry_at
        assert (row.claimed_by_worker_id, row.claimed_at, row.started_at) == (
            None,
            None,
            None,
        )
        assert row.execution == 'ISOLATED'
        (attempt,) = await _attempts(engine, seeded.task_id)
        assert (attempt.attempt, attempt.outcome, attempt.will_retry) == (
            2,
            'FAILED',
            True,
        )
        assert await _history_row(engine, seeded.task_id) is None
        assert await _payloads(wakes) == []

    async def test_an_isolated_retry_moves_the_row_to_the_isolated_class(
        self,
        engine: AsyncEngine,
        seed: Any,
    ) -> None:
        seeded = await seed(max_retries=3)
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE horsies_tasks SET execution = 'SHARED_ASYNC' "
                    'WHERE id = CAST(:id AS uuid)'
                ),
                {'id': seeded.task_id},
            )
        isolated = RetryAfter(
            retry_count=0,
            delay_s=30.0,
            placement=RetryPlacement.ISOLATED,
        )

        outcome = await _finish(engine, _command(seeded, retry=isolated))

        assert isinstance(outcome, RetryScheduled)
        assert (await _live_row(engine, seeded.task_id)).execution == 'ISOLATED'

    async def test_a_same_class_retry_keeps_the_class(
        self,
        engine: AsyncEngine,
        seed: Any,
    ) -> None:
        seeded = await seed(max_retries=3)
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE horsies_tasks SET execution = 'SHARED_ASYNC' "
                    'WHERE id = CAST(:id AS uuid)'
                ),
                {'id': seeded.task_id},
            )

        outcome = await _finish(engine, _command(seeded, retry=_retry()))

        assert isinstance(outcome, RetryScheduled)
        assert (await _live_row(engine, seeded.task_id)).execution == 'SHARED_ASYNC'

    @pytest.mark.parametrize(
        'good_until_offset_s',
        [10.0, -10.0],
        ids=['next_attempt_after_good_until', 'good_until_already_past'],
    )
    async def test_a_retry_at_or_after_good_until_is_the_terminal_failure(
        self,
        engine: AsyncEngine,
        seed: Any,
        wakes: psycopg.AsyncConnection[Any],
        good_until_offset_s: float,
    ) -> None:
        good_until = datetime.now(timezone.utc) + timedelta(seconds=good_until_offset_s)
        seeded = await seed(max_retries=3, good_until=good_until)

        outcome = await _finish(engine, _command(seeded, retry=_retry(60.0)))

        match outcome:
            case FailureApplied(
                retry=RetryPastDeadline(next_retry_at=next_at, good_until=deadline),
                after=CapacityWoken(),
            ):
                assert deadline == good_until
                assert next_at >= deadline
            case other:
                raise AssertionError(other)
        assert (await _history_row(engine, seeded.task_id)).status == 'FAILED'
        (attempt,) = await _attempts(engine, seeded.task_id)
        assert attempt.will_retry is False
        assert await _payloads(wakes) == [f'capacity:{seeded.task_id}']

    async def test_a_retry_before_good_until_is_scheduled(
        self,
        engine: AsyncEngine,
        seed: Any,
    ) -> None:
        good_until = datetime.now(timezone.utc) + timedelta(hours=1)
        seeded = await seed(max_retries=3, good_until=good_until)

        outcome = await _finish(engine, _command(seeded, retry=_retry(60.0)))

        assert isinstance(outcome, RetryScheduled)


@pytest.mark.asyncio(loop_scope='function')
class TestNoWrite:
    async def test_a_replay_of_a_failure_is_already_applied_and_writes_nothing(
        self,
        engine: AsyncEngine,
        seed: Any,
        wakes: psycopg.AsyncConnection[Any],
    ) -> None:
        seeded = await seed()
        first = await _finish(engine, _command(seeded))
        assert isinstance(first, FailureApplied)
        assert await _payloads(wakes) == [f'capacity:{seeded.task_id}']

        replay = await _finish(engine, _command(seeded))

        match replay:
            case AlreadyApplied(terminal_at=terminal_at, kind=kind):
                assert terminal_at == first.terminal_at
                assert kind.value == 'FAIL_RUNNING'
            case other:
                raise AssertionError(other)
        assert len(await _attempts(engine, seeded.task_id)) == 1
        assert await _payloads(wakes) == []

    async def test_a_replay_of_a_scheduled_retry_is_a_lost_claim(
        self,
        engine: AsyncEngine,
        seed: Any,
    ) -> None:
        """The retry ended the claim generation, so a lost reply of a retry
        replays as a lost claim, with no second write."""
        seeded = await seed(max_retries=3)
        first = await _finish(engine, _command(seeded, retry=_retry()))
        assert isinstance(first, RetryScheduled)
        before = await _live_row(engine, seeded.task_id)

        replay = await _finish(engine, _command(seeded, retry=_retry()))

        assert isinstance(replay, LostClaim)
        assert replay.observed.status is TaskStatus.PENDING
        assert await _live_row(engine, seeded.task_id) == before
        assert len(await _attempts(engine, seeded.task_id)) == 1

    @pytest.mark.parametrize(
        ('worker', 'shift_s'),
        [(WORKER, 1.0), ('another-worker', 0.0)],
        ids=['stale_generation', 'another_worker'],
    )
    async def test_a_claim_this_call_does_not_hold_is_a_lost_claim(
        self,
        engine: AsyncEngine,
        seed: Any,
        worker: str,
        shift_s: float,
    ) -> None:
        seeded = await seed(max_retries=3)
        before = await _live_row(engine, seeded.task_id)

        outcome = await _finish(
            engine,
            _command(
                seeded,
                retry=_retry(),
                fence=OwnedClaim(
                    worker_id=worker,
                    claimed_at=seeded.claimed_at + timedelta(seconds=shift_s),
                ),
            ),
        )

        match outcome:
            case LostClaim(observed=observed):
                assert observed.status is TaskStatus.RUNNING
                assert observed.worker_id == WORKER
                assert observed.claimed_at == seeded.claimed_at
            case other:
                raise AssertionError(other)
        assert await _live_row(engine, seeded.task_id) == before
        assert await _attempts(engine, seeded.task_id) == ()

    async def test_a_changed_retry_count_is_part_of_the_fence(
        self,
        engine: AsyncEngine,
        seed: Any,
    ) -> None:
        """The delay belongs to the count that was read: a different count
        writes nothing."""
        seeded = await seed(retry_count=1, max_retries=3)
        before = await _live_row(engine, seeded.task_id)

        outcome = await _finish(
            engine,
            _command(seeded, retry=_retry(retry_count=0)),
        )

        assert isinstance(outcome, SourceStateConflict)
        assert await _live_row(engine, seeded.task_id) == before
        assert await _attempts(engine, seeded.task_id) == ()

    async def test_a_claimed_row_is_a_conflict_and_writes_nothing(
        self,
        engine: AsyncEngine,
        seed: Any,
    ) -> None:
        seeded = await seed(status='CLAIMED')
        before = await _live_row(engine, seeded.task_id)

        outcome = await _finish(engine, _command(seeded))

        match outcome:
            case SourceStateConflict(observed=observed):
                assert observed.status is TaskStatus.CLAIMED
            case other:
                raise AssertionError(other)
        assert await _live_row(engine, seeded.task_id) == before

    async def test_a_foreign_terminalization_is_a_conflict(
        self,
        engine: AsyncEngine,
        seed: Any,
    ) -> None:
        seeded = await seed()
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    'SELECT * FROM horsies_fail_stale_task(CAST(:id AS uuid), '
                    "0, 0, :result, 'WORKER_CRASHED', 'went silent')"
                ),
                {'id': seeded.task_id, 'result': ERROR_RESULT},
            )

        outcome = await _finish(engine, _command(seeded))

        match outcome:
            case SourceStateConflict(
                evidence=ObservedForeignTerminalization() as evidence
            ):
                assert evidence.committed_kind is not None
                assert evidence.committed_kind.value == 'FAIL_STALE'
            case other:
                raise AssertionError(other)

    async def test_an_unknown_task_is_absent(
        self, engine: AsyncEngine, seed: Any
    ) -> None:
        _ = seed
        missing = _Seeded(
            task_id=str(uuid.uuid4()), claimed_at=datetime.now(timezone.utc)
        )

        outcome = await _finish(engine, _command(missing))

        assert outcome == TaskAbsent(task_id=missing.task_id, ordinality=None)


_PARTIAL_RETRY_SQL = """
    SELECT * FROM horsies_finish_failed_attempt(
        CAST(:id AS uuid), :worker, NULL,
        CAST(:count AS integer), CAST(:delay AS double precision), :placement,
        :result, 'BOOM', NULL, :outcome, 'message', NULL
    )
"""


@pytest.mark.asyncio(loop_scope='function')
class TestArguments:
    """A wrong argument is refused before any lock or write."""

    @pytest.mark.parametrize(
        ('count', 'delay', 'placement', 'result', 'outcome', 'sqlstate'),
        [
            (0, None, 'same_class', ERROR_RESULT, 'FAILED', '22023'),
            (None, 1.0, None, ERROR_RESULT, 'FAILED', '22023'),
            (0, 1.0, 'elsewhere', ERROR_RESULT, 'FAILED', '22023'),
            (0, -1.0, 'same_class', ERROR_RESULT, 'FAILED', '22023'),
            (0, math.nan, 'same_class', ERROR_RESULT, 'FAILED', '22023'),
            (0, math.inf, 'same_class', ERROR_RESULT, 'FAILED', '22023'),
            (None, None, None, ERROR_RESULT, 'COMPLETED', '22023'),
            (None, None, None, None, 'FAILED', '23502'),
        ],
        ids=[
            'retry_without_delay',
            'delay_without_count',
            'unknown_placement',
            'negative_delay',
            'nan_delay',
            'infinite_delay',
            'completed_attempt',
            'no_result',
        ],
    )
    async def test_a_wrong_argument_raises_and_writes_nothing(
        self,
        engine: AsyncEngine,
        seed: Any,
        count: int | None,
        delay: float | None,
        placement: str | None,
        result: str | None,
        outcome: str,
        sqlstate: str,
    ) -> None:
        seeded = await seed(max_retries=3)
        before = await _live_row(engine, seeded.task_id)

        with pytest.raises(DBAPIError) as raised:
            async with engine.begin() as connection:
                await connection.execute(
                    text(_PARTIAL_RETRY_SQL),
                    {
                        'id': seeded.task_id,
                        'worker': WORKER,
                        'count': count,
                        'delay': delay,
                        'placement': placement,
                        'result': result,
                        'outcome': outcome,
                    },
                )

        assert getattr(raised.value.orig, 'sqlstate', None) == sqlstate
        assert await _live_row(engine, seeded.task_id) == before
        assert await _attempts(engine, seeded.task_id) == ()
