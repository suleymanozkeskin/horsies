"""``horsies_finish_workflow_success`` against real PostgreSQL: each outcome.

The function is called through the worker's own client
(``finish_workflow_success``), in autocommit, as phase 1 calls it, so every
test also runs the decoder. Each test asserts what was written and what was
not: the live row, the attempt rows, the history row, the phase-2 record of
the node and the capacity wake.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final

import psycopg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.archive.attempts import AttemptRecord
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    LostClaim,
    ObservedForeignTerminalization,
    PhaseTwoOwed,
    SourceStateConflict,
    SuccessApplied,
    TaskAbsent,
    WorkflowSuccessOutcome,
)
from horsies.core.types.status import TaskStatus
from horsies.core.worker.workflow_success import (
    FinishWorkflowSuccess,
    finish_workflow_success,
)
from tests.integration.conftest import DB_URL, compute_test_enqueue_sha
from tests.integration.history_seeding import read_attempts

pytestmark = [pytest.mark.integration]

WORKER: Final = 'w-workflow-success'
QUEUE: Final = 'workflow_success_q'
TASK_NAME: Final = 'workflow_success_task'
OK_RESULT: Final = '{"__h_task_result__":true,"ok":7,"err":null}'
NOTIFY_WAIT_S: Final = 0.5
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
    """A factory of rows held by ``WORKER``; a workflow task gets its node."""

    async def make(
        *,
        retry_count: int = 0,
        is_workflow_task: bool = True,
        status: str = 'RUNNING',
    ) -> _Seeded:
        task_id = str(uuid.uuid4())
        claimed_at = datetime.now(timezone.utc) - timedelta(seconds=30)
        sent_at, sha = compute_test_enqueue_sha(task_name=TASK_NAME, queue_name=QUEUE)
        async with AsyncSession(engine) as session:
            await session.execute(
                text("""
                    INSERT INTO horsies_tasks
                        (id, task_name, queue_name, priority, args, kwargs,
                         status, sent_at, created_at, updated_at, claimed,
                         retry_count, max_retries, started_at,
                         enqueue_sha, claimed_by_worker_id, claimed_at,
                         is_workflow_task, worker_hostname, worker_pid,
                         worker_process_name, retention_class_key,
                         command_fingerprint_version, command_fingerprint,
                         retain_rerun_input, prepared_rerun_input_disposition)
                    VALUES
                        (:id, :task_name, :queue, 100, '[]', '{}',
                         CAST(:status AS varchar), :sent_at, NOW(), NOW(),
                         CAST(:status AS varchar) = 'CLAIMED',
                         :retry_count, 3,
                         CASE WHEN CAST(:status AS varchar) = 'RUNNING'
                              THEN NOW() END,
                         :sha, :worker, :claimed_at,
                         :is_workflow_task, 'host-1', 4242, 'child-1',
                         'standard_30d', 1,
                         sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                         FALSE, 'NEVER_ELIGIBLE')
                """),
                {
                    'id': task_id,
                    'task_name': TASK_NAME,
                    'queue': QUEUE,
                    'status': status,
                    'sent_at': sent_at,
                    'retry_count': retry_count,
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
            VALUES (:wf, 'workflow_success_wf', 'RUNNING', 'FAIL', 0, :wf,
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


def _command(
    seeded: _Seeded, *, fence: OwnedClaim | None = None
) -> FinishWorkflowSuccess:
    """The success of ``seeded``, under its own claim unless ``fence`` is given."""
    return FinishWorkflowSuccess(
        task_id=seeded.task_id,
        fence=(
            OwnedClaim(worker_id=WORKER, claimed_at=seeded.claimed_at)
            if fence is None
            else fence
        ),
        result_json=OK_RESULT,
    )


async def _finish(
    engine: AsyncEngine, command: FinishWorkflowSuccess
) -> WorkflowSuccessOutcome:
    return await finish_workflow_success(
        async_sessionmaker(engine, expire_on_commit=False),
        command,
    )


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
                    SELECT status, retry_count, claimed_by_worker_id,
                           claimed_at, started_at, updated_at
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
                           convert_from(result_payload, 'UTF8') AS result
                    FROM horsies_task_history WHERE task_id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        ).one_or_none()


async def _attempts(engine: AsyncEngine, task_id: str) -> tuple[AttemptRecord, ...]:
    async with AsyncSession(engine) as session:
        return await read_attempts(session, task_id)


async def _pending_rows(engine: AsyncEngine, task_id: str) -> list[tuple[str, str]]:
    async with engine.connect() as connection:
        rows = (
            await connection.execute(
                text(
                    'SELECT terminal_status, terminalization_kind '
                    'FROM horsies_workflow_phase2_pending '
                    'WHERE task_id = CAST(:id AS uuid)'
                ),
                {'id': task_id},
            )
        ).all()
    return [(str(row[0]), str(row[1])) for row in rows]


@pytest.mark.asyncio(loop_scope='function')
class TestApplied:
    async def test_the_success_moves_the_row_with_its_attempt_and_owes_phase_two(
        self,
        engine: AsyncEngine,
        seed: Any,
        wakes: psycopg.AsyncConnection[Any],
    ) -> None:
        seeded = await seed()

        outcome = await _finish(engine, _command(seeded))

        match outcome:
            case SuccessApplied(
                task_id=task_id,
                observed=observed,
                phase_two=PhaseTwoOwed(task_name=name, queue_name=queue),
            ):
                assert task_id == seeded.task_id
                assert (name, queue) == (TASK_NAME, QUEUE)
                assert observed.status is TaskStatus.RUNNING
                assert observed.worker_id == WORKER
                assert observed.claimed_at == seeded.claimed_at
            case other:
                raise AssertionError(other)
        assert await _live_row(engine, seeded.task_id) is None
        assert tuple(await _history_row(engine, seeded.task_id)) == (
            'COMPLETED',
            TerminalizationKind.COMPLETE_LOCKED.value,
            None,
            OK_RESULT,
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
            'COMPLETED',
            False,
            None,
            None,
            None,
            WORKER,
            'host-1',
            4242,
            'child-1',
        )
        assert attempt.started_at <= attempt.finished_at
        assert await _pending_rows(engine, seeded.task_id) == [
            ('COMPLETED', TerminalizationKind.COMPLETE_LOCKED.value),
        ]
        assert await _payloads(wakes) == []

    async def test_the_attempt_number_follows_the_retry_count(
        self,
        engine: AsyncEngine,
        seed: Any,
    ) -> None:
        """Two failed attempts were retried; the success is the third."""
        seeded = await seed(retry_count=2)
        async with engine.begin() as connection:
            await connection.execute(
                text("""
                    INSERT INTO horsies_task_attempts
                        (task_id, attempt, outcome, will_retry,
                         started_at, finished_at, error_code)
                    SELECT CAST(:id AS uuid), n, 'FAILED', TRUE,
                           NOW() - interval '1 minute', NOW(), 'FLAKY'
                    FROM generate_series(1, 2) AS n
                """),
                {'id': seeded.task_id},
            )

        outcome = await _finish(engine, _command(seeded))

        assert isinstance(outcome, SuccessApplied)
        attempts = await _attempts(engine, seeded.task_id)
        assert [(a.attempt, a.outcome, a.will_retry) for a in attempts] == [
            (1, 'FAILED', True),
            (2, 'FAILED', True),
            (3, 'COMPLETED', False),
        ]

    async def test_a_null_generation_disables_the_generation_half_of_the_fence(
        self,
        engine: AsyncEngine,
        seed: Any,
    ) -> None:
        seeded = await seed()

        outcome = await _finish(
            engine,
            _command(seeded, fence=OwnedClaim(worker_id=WORKER, claimed_at=None)),
        )

        assert isinstance(outcome, SuccessApplied)


@pytest.mark.asyncio(loop_scope='function')
class TestNoWrite:
    async def test_a_replay_is_already_applied_and_writes_nothing(
        self,
        engine: AsyncEngine,
        seed: Any,
    ) -> None:
        seeded = await seed()
        first = await _finish(engine, _command(seeded))
        assert isinstance(first, SuccessApplied)

        replay = await _finish(engine, _command(seeded))

        match replay:
            case AlreadyApplied(terminal_at=terminal_at, kind=kind):
                assert terminal_at == first.terminal_at
                assert kind is TerminalizationKind.COMPLETE_LOCKED
            case other:
                raise AssertionError(other)
        assert len(await _attempts(engine, seeded.task_id)) == 1
        assert len(await _pending_rows(engine, seeded.task_id)) == 1

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
        seeded = await seed()
        before = await _live_row(engine, seeded.task_id)

        outcome = await _finish(
            engine,
            _command(
                seeded,
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
        assert await _pending_rows(engine, seeded.task_id) == []

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
        assert await _attempts(engine, seeded.task_id) == ()

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
                {'id': seeded.task_id, 'result': OK_RESULT},
            )

        outcome = await _finish(engine, _command(seeded))

        match outcome:
            case SourceStateConflict(
                evidence=ObservedForeignTerminalization() as evidence
            ):
                assert evidence.committed_kind is TerminalizationKind.FAIL_STALE
            case other:
                raise AssertionError(other)
        assert (await _history_row(engine, seeded.task_id)).status == 'FAILED'

    async def test_an_unknown_task_is_absent(
        self, engine: AsyncEngine, seed: Any
    ) -> None:
        _ = seed
        missing = _Seeded(
            task_id=str(uuid.uuid4()), claimed_at=datetime.now(timezone.utc)
        )

        outcome = await _finish(engine, _command(missing))

        assert outcome == TaskAbsent(task_id=missing.task_id, ordinality=None)


_DIRECT_SQL: Final = """
    SELECT * FROM horsies_finish_workflow_success(
        CAST(:id AS uuid), :worker, NULL, :result
    )
"""


@pytest.mark.asyncio(loop_scope='function')
class TestRefusedCall:
    """A call the function cannot serve raises and writes nothing."""

    @pytest.mark.parametrize(
        ('is_workflow_task', 'result', 'sqlstate', 'message'),
        [
            (False, OK_RESULT, '22023', 'is not a workflow task'),
            (True, None, '23502', 'a workflow task success needs a result payload'),
        ],
        ids=['plain_task', 'no_result'],
    )
    async def test_it_raises_and_writes_nothing(
        self,
        engine: AsyncEngine,
        seed: Any,
        is_workflow_task: bool,
        result: str | None,
        sqlstate: str,
        message: str,
    ) -> None:
        """The function's own refusal, by its SQLSTATE and its message: the
        move would refuse a NULL result too, after the locks and the
        attempt row."""
        seeded = await seed(is_workflow_task=is_workflow_task)
        before = await _live_row(engine, seeded.task_id)

        with pytest.raises(DBAPIError) as raised:
            async with engine.begin() as connection:
                await connection.execute(
                    text(_DIRECT_SQL),
                    {'id': seeded.task_id, 'worker': WORKER, 'result': result},
                )

        original = raised.value.orig
        assert isinstance(original, psycopg.Error)
        assert original.sqlstate == sqlstate
        assert original.diag.message_primary is not None
        assert message in original.diag.message_primary
        assert await _live_row(engine, seeded.task_id) == before
        assert await _attempts(engine, seeded.task_id) == ()
        assert await _history_row(engine, seeded.task_id) is None
