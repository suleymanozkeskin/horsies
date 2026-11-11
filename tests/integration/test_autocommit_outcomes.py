"""Outcomes of the one-statement writes that run in autocommit.

The fused completion runs as its own transaction (``apply_in_autocommit``);
the worker writes it through the batched statement of the finalize batcher
(``apply_fused_batch_in_autocommit``). These tests pin what the transaction
used to guarantee and what the caller relies on:

- ``Applied`` is committed with its attempt, its history row and its
  capacity NOTIFY.
- A replay is ``AlreadyApplied`` and writes nothing, also no NOTIFY.
- A lost claim generation is ``LostClaim`` and writes nothing.
- A lost reply is a retryable phase-1 error, and the replay classifies it.
- With the worker pre-ping off, a dead pooled connection fails the first
  statement with a retryable connection error, and the retry applies.
- ``SingleStatement`` carries one statement: Postgres refuses a second one.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator, Generator
from datetime import datetime, timedelta
from typing import Any, Final

import psycopg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.lifecycle.commands import CompleteTaskFused
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    TerminalizationOutcome,
)
from horsies.core.lifecycle.persistence import (
    BatchWriteError,
    FusedCompletionBatch,
    apply_fused_batch_in_autocommit,
    apply_in_autocommit,
)
from horsies.core.models.tasks import Execution, TaskResult
from horsies.core.types.result import Err, Ok, Result
from horsies.core.utils.db import (
    SingleStatement,
    is_retryable_connection_error,
    run_in_autocommit,
)
from horsies.core.worker import finalize as finalize_module
from horsies.core.worker.claiming import ClaimPass
from horsies.core.worker.runtime import FinalizeStage, _FinalizeError
from horsies.core.worker.worker import Worker
from tests.integration.autocommit_support import (
    QUEUE,
    TASK_NAME,
    WORKER_ID,
    ProxiedWorker,
    insert_task,
    ok_result_json,
    open_proxied_worker,
    proxied_url,
    psycopg_url,
    upstream_of,
)
from tests.integration.history_seeding import read_attempts
from tests.integration.round_trip_proxy import RoundTripProxy

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

CHANNEL: Final = f'task_queue_{QUEUE}'
NOTIFY_WAIT_S: Final = 5.0
REPLY_DROP_WAIT_S: Final = 5.0
BACKEND_EXIT_POLLS: Final = 50
BACKEND_EXIT_POLL_S: Final = 0.1


@pytest.fixture
def proxy(db_url: str) -> Generator[RoundTripProxy]:
    host, port = upstream_of(db_url)
    started = RoundTripProxy(host, port)
    started.start()
    yield started
    started.stop()


@pytest_asyncio.fixture(loop_scope='function')
async def direct(
    db_url: str,
    broker: PostgresBroker,  # noqa: ARG001 - the schema and history leaves
) -> AsyncGenerator[ProxiedWorker]:
    """A worker on the database URL itself, as the CLI builds it."""
    opened = await open_proxied_worker(db_url)
    yield opened
    _ = await opened.broker.close_async()


@pytest_asyncio.fixture(loop_scope='function')
async def listener(db_url: str) -> AsyncGenerator[psycopg.AsyncConnection[Any]]:
    connection = await psycopg.AsyncConnection.connect(
        psycopg_url(db_url),
        autocommit=True,
    )
    _ = await connection.execute(f'LISTEN {CHANNEL}')
    yield connection
    await connection.close()


def _complete(task_id: str, claimed_at: datetime) -> CompleteTaskFused:
    return CompleteTaskFused(
        task_id=task_id,
        fence=OwnedClaim(worker_id=WORKER_ID, claimed_at=claimed_at),
        result_json=ok_result_json(),
        notify_channel=CHANNEL,
        notify_payload=f'capacity:{task_id}',
    )


async def _payloads_until_sentinel(
    listener: psycopg.AsyncConnection[Any],
    session: AsyncSession,
) -> list[str]:
    """Every payload on ``CHANNEL`` committed before a sentinel NOTIFY that
    this call commits. Postgres delivers notifications in commit order."""
    sentinel = f'sentinel:{uuid.uuid4()}'
    _ = await session.execute(
        text('SELECT pg_notify(:channel, :payload)'),
        {'channel': CHANNEL, 'payload': sentinel},
    )
    await session.commit()
    payloads: list[str] = []
    async for notify in listener.notifies(timeout=NOTIFY_WAIT_S):
        if notify.payload == sentinel:
            return payloads
        payloads.append(notify.payload)
    raise AssertionError(f'sentinel NOTIFY not seen; payloads {payloads}')


async def _live_row(session: AsyncSession, task_id: str) -> Any:
    row = (
        await session.execute(
            text("""
                SELECT status, updated_at, finalizing_at FROM horsies_tasks
                WHERE id = CAST(:id AS uuid)
            """),
            {'id': task_id},
        )
    ).one_or_none()
    await session.commit()
    return row


async def _history_rows(session: AsyncSession, task_id: str) -> int:
    count = await session.scalar(
        text(
            'SELECT count(*) FROM horsies_task_history WHERE task_id = CAST(:id AS uuid)'
        ),
        {'id': task_id},
    )
    await session.commit()
    assert isinstance(count, int)
    return count


async def _attempt_count(session: AsyncSession, task_id: str) -> int:
    attempts = await read_attempts(session, task_id)
    await session.commit()
    return len(attempts)


class _RecordedOutcomes:
    """Calls the real batch write of finalize and keeps the outcome of each
    row that a write returned."""

    def __init__(self) -> None:
        self.outcomes: list[TerminalizationOutcome] = []

    async def __call__(
        self,
        session_factory: Any,
        batch: FusedCompletionBatch,
    ) -> Result[tuple[TerminalizationOutcome, ...], BatchWriteError]:
        written = await apply_fused_batch_in_autocommit(session_factory, batch)
        match written:
            case Ok(outcomes):
                self.outcomes.extend(outcomes)
            case Err():
                pass
        return written


@pytest.fixture
def recorded(monkeypatch: pytest.MonkeyPatch) -> _RecordedOutcomes:
    recording = _RecordedOutcomes()
    monkeypatch.setattr(finalize_module, 'apply_fused_batch_in_autocommit', recording)
    return recording


async def _finalize_success(
    worker: Worker,
    task_id: str,
    claimed_at: datetime,
) -> Result[None, _FinalizeError]:
    return await worker._finalize_task_result(
        task_id,
        True,
        ok_result_json(),
        None,
        queue_name=QUEUE,
        is_workflow_task=False,
        task_name=TASK_NAME,
        claimed_at=claimed_at,
    )


def _retryable_phase1_error(result: Result[None, _FinalizeError]) -> _FinalizeError:
    match result:
        case Err(error):
            assert error.stage is FinalizeStage.PHASE1
            assert error.retryable is True, error
            return error
        case Ok():
            raise AssertionError('finalize succeeded on a connection that failed')


async def _kill_the_pooled_backend(
    worker: Worker,
    session: AsyncSession,
) -> None:
    """Terminate the server side of the worker's one pooled connection and
    wait until it is gone. The client side stays in the pool."""
    async with worker.sf() as worker_session:
        pid = (
            await worker_session.execute(text('SELECT pg_backend_pid()'))
        ).scalar_one()
        await worker_session.commit()
    terminated = await session.scalar(
        text('SELECT pg_terminate_backend(:pid)'),
        {'pid': pid},
    )
    await session.commit()
    assert terminated is True
    for _ in range(BACKEND_EXIT_POLLS):
        alive = await session.scalar(
            text('SELECT count(*) FROM pg_stat_activity WHERE pid = :pid'),
            {'pid': pid},
        )
        await session.commit()
        if alive == 0:
            return
        await asyncio.sleep(BACKEND_EXIT_POLL_S)
    raise AssertionError(f'backend {pid} still runs after pg_terminate_backend')


async def _one_pooled_connection(opened: ProxiedWorker) -> None:
    """Use the worker's pool once, so it holds one open connection."""
    match await opened.broker.select_resources():
        case Ok(resources):
            async with resources.session_factory() as worker_session:
                _ = await worker_session.execute(text('SELECT 1'))
                await worker_session.commit()
            assert resources.engine.pool.checkedin() == 1  # type: ignore[attr-defined]
        case Err(error):
            raise AssertionError(error)


# ---------------------------------------------------------------------------
# The fused completion in autocommit
# ---------------------------------------------------------------------------


async def test_an_applied_completion_commits_its_rows_and_its_wake(
    direct: ProxiedWorker,
    listener: psycopg.AsyncConnection[Any],
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    task_id, claimed_at = await insert_task(session, status='RUNNING')

    outcome = await apply_in_autocommit(
        direct.worker.sf, _complete(task_id, claimed_at)
    )

    assert isinstance(outcome, Applied)
    assert await _live_row(session, task_id) is None
    assert await _history_rows(session, task_id) == 1
    assert await _attempt_count(session, task_id) == 1
    assert await _payloads_until_sentinel(listener, session) == [f'capacity:{task_id}']


async def test_a_replay_is_already_applied_and_writes_nothing(
    direct: ProxiedWorker,
    listener: psycopg.AsyncConnection[Any],
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    task_id, claimed_at = await insert_task(session, status='RUNNING')
    command = _complete(task_id, claimed_at)
    assert isinstance(await apply_in_autocommit(direct.worker.sf, command), Applied)
    assert await _payloads_until_sentinel(listener, session) == [f'capacity:{task_id}']

    replay = await apply_in_autocommit(direct.worker.sf, command)

    assert isinstance(replay, AlreadyApplied)
    assert await _history_rows(session, task_id) == 1
    assert await _attempt_count(session, task_id) == 1
    assert await _payloads_until_sentinel(listener, session) == []


async def test_a_lost_claim_generation_writes_nothing(
    direct: ProxiedWorker,
    listener: psycopg.AsyncConnection[Any],
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    task_id, claimed_at = await insert_task(session, status='RUNNING')
    before = await _live_row(session, task_id)

    outcome = await apply_in_autocommit(
        direct.worker.sf,
        _complete(task_id, claimed_at + timedelta(seconds=1)),
    )

    assert isinstance(outcome, LostClaim)
    assert await _live_row(session, task_id) == before
    assert await _history_rows(session, task_id) == 0
    assert await _attempt_count(session, task_id) == 0
    assert await _payloads_until_sentinel(listener, session) == []


# ---------------------------------------------------------------------------
# Lost reply and dead connection, through the worker's retry
# ---------------------------------------------------------------------------


async def test_a_lost_reply_is_retried_and_classified_on_the_replay(
    proxy: RoundTripProxy,
    db_url: str,
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    recorded: _RecordedOutcomes,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The completion commits, and its reply never reaches the worker."""
    opened = await open_proxied_worker(proxied_url(db_url, proxy))
    try:
        await _one_pooled_connection(opened)
        task_id, claimed_at = await insert_task(session, status='RUNNING')
        proxy.drop_next_execute_reply()

        first = await _finalize_success(opened.worker, task_id, claimed_at)

        assert proxy.reply_was_dropped(REPLY_DROP_WAIT_S)
        error = _retryable_phase1_error(first)
        # The premise of the replay: the statement committed.
        assert await _live_row(session, task_id) is None
        assert await _history_rows(session, task_id) == 1

        await opened.worker._retry_finalize_phase1(error, 0.0)
    finally:
        _ = await opened.broker.close_async()

    assert [type(outcome) for outcome in recorded.outcomes] == [AlreadyApplied]
    assert await _history_rows(session, task_id) == 1
    assert await _attempt_count(session, task_id) == 1


async def test_a_dead_connection_fails_the_completion_and_the_retry_applies(
    direct: ProxiedWorker,
    session: AsyncSession,
    recorded: _RecordedOutcomes,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Pre-ping off: the dead connection shows at the fused call, which never
    reached the server; the phase-1 retry runs on a new connection."""
    await _one_pooled_connection(direct)
    task_id, claimed_at = await insert_task(session, status='RUNNING')
    before = await _live_row(session, task_id)
    await _kill_the_pooled_backend(direct.worker, session)

    first = await _finalize_success(direct.worker, task_id, claimed_at)

    error = _retryable_phase1_error(first)
    assert await _live_row(session, task_id) == before
    assert recorded.outcomes == []

    await direct.worker._retry_finalize_phase1(error, 0.0)

    assert [type(outcome) for outcome in recorded.outcomes] == [Applied]
    assert await _live_row(session, task_id) is None
    assert await _history_rows(session, task_id) == 1


async def test_a_dead_connection_fails_the_plain_wake_as_retryable(
    direct: ProxiedWorker,
    session: AsyncSession,
) -> None:
    await _one_pooled_connection(direct)
    await _kill_the_pooled_backend(direct.worker, session)

    result = await direct.worker._finalize_workflow_phase(
        str(uuid.uuid4()),
        TaskResult(ok=None),
        queue_name=QUEUE,
        is_workflow_task=False,
        task_name=TASK_NAME,
    )

    match result:
        case Err(error):
            assert error.stage is FinalizeStage.PHASE2
            assert error.retryable is True
        case Ok():
            raise AssertionError('the wake succeeded on a dead connection')


async def test_a_dead_connection_fails_the_claim_pass_as_retryable(
    direct: ProxiedWorker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The claim loop retries a pass that raised a retryable connection
    error; the next pass claims the row."""
    worker = direct.worker
    worker._claim_slot_count = lambda: 1  # type: ignore[method-assign]
    worker._registered_classes = {TASK_NAME: Execution.ISOLATED}
    dispatched: list[str] = []

    async def _dispatch_one(task_id: str, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        dispatched.append(task_id)

    worker._dispatch_one = _dispatch_one  # type: ignore[method-assign]
    await _one_pooled_connection(direct)
    task_id, _ = await insert_task(session, status='PENDING')
    await _kill_the_pooled_backend(worker, session)

    with pytest.raises(Exception) as failed:
        await worker._claim_and_dispatch_all()

    assert is_retryable_connection_error(failed.value)
    assert dispatched == []
    assert await worker._claim_and_dispatch_all() == ClaimPass(
        claimed=1,
        dropped=0,
        short_of_plan=0,
    )
    assert dispatched == [task_id]


# ---------------------------------------------------------------------------
# One statement only
# ---------------------------------------------------------------------------


async def test_a_second_statement_is_refused_by_the_server(
    direct: ProxiedWorker,
) -> None:
    """Bound parameters send the text in the extended protocol, where
    Postgres refuses a text with two commands."""
    two_commands = SingleStatement(
        statement=text('SELECT CAST(:value AS integer); SELECT 1'),
        parameters={'value': 1},
    )

    with pytest.raises(DBAPIError) as refused:
        await run_in_autocommit(direct.worker.sf, two_commands)

    original = refused.value.orig
    assert isinstance(original, psycopg.Error)
    assert original.diag.message_primary == (
        'cannot insert multiple commands into a prepared statement'
    )
