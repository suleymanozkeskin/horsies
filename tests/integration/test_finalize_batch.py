"""The batched plain-success write, from the finalize entry points down.

Each test enters through the worker (``_finalize_task_result``,
``_finalize_after``, ``_retry_finalize_phase1``) or through the batch write
itself (``complete_fused_batch_in_core`` on the worker's core handle; the lock
order test sends the statement with ``apply_fused_batch``), against the real
schema. The finalizers of one test submit in one loop turn, so the first
flusher takes their rows as one batch.

Per row, the tests pin:

- the outcome of the single fused completion (fence, replay, absence);
- one statement and one round trip per batch;
- a refused batch: each row written alone, once;
- a lost reply: each row classified by its replay;
- the answer, the capacity wake and the finalizer end only after the commit;
- the write only after the attempt ended;
- rows that did not commit stay RUNNING for stale RUNNING recovery;
- sorted task-id lock order, and no deadlock with workflow-node finalizes.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator, Generator, Sequence
from dataclasses import replace
from datetime import datetime, timedelta, timezone
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
    TaskAbsent,
    TerminalizationOutcome,
)
from horsies.core.lifecycle.outcomes import decode_outcome_row
from horsies._core import CoreHandle
from horsies.core.lifecycle.persistence import (
    _COMPLETE_TASKS_FUSED_SQL,
    BatchWriteError,
    FusedCompletionBatch,
    apply_fused_batch,
    complete_fused_batch_in_core,
)
from horsies.core.models.child_sizing import parent_pool_flusher_bound
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker import finalize as finalize_module
from horsies.core.worker.child_protocol import NoTaskLoop, RssUnreadable
from horsies.core.worker.finalize_batch import (
    FinalizeBatcher,
    FlushersCancelled,
)
from horsies.core.worker.runtime import FinalizeStage, _FinalizeError
from horsies.core.worker.slots import Assigned, TaskFinished
from horsies.core.worker.worker import Worker
from horsies.core.utils.db import PooledStatements
from tests.dispatch_claim_support import claim_at
from tests.fixtures.slots.handles import unbound_handle
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
    steady_state_turns,
    upstream_of,
    warm_core_handle,
)
from tests.integration.history_seeding import read_attempts
from tests.integration.round_trip_proxy import RoundTripProxy

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

CHANNEL: Final = f'task_queue_{QUEUE}'
BATCH_ROWS: Final = 8
NOTIFY_WAIT_S: Final = 5.0
QUIET_WAIT_S: Final = 0.5
WAIT_POLLS: Final = 100
WAIT_POLL_S: Final = 0.05
REPLY_DROP_WAIT_S: Final = 5.0
OUTER_BOUND_S: Final = 30.0
TASK_LOCK_SEED: Final = 731
UNREGISTERED_CLASS: Final = 'standard_1777d'
"""A class of the baseline key form that no test registers: no history leaf
covers it, so the move of its row raises."""
QUEUE_ROOM: Final = 4 * BATCH_ROWS
"""The batch queue bound of the test workers. A worker's bound is twice its
claim budget, which is 2 for the one-slot test worker; these tests finalize
more rows at once than one slot ever holds."""

type Case = list[tuple[str, datetime]]


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def proxy(db_url: str) -> Generator[RoundTripProxy]:
    host, port = upstream_of(db_url)
    started = RoundTripProxy(host, port)
    started.start()
    yield started
    started.stop()


async def _open_worker(database_url: str) -> ProxiedWorker:
    """A worker as the CLI builds it, with ``QUEUE_ROOM`` in its batch queue."""
    opened = await open_proxied_worker(database_url)
    worker = opened.worker
    worker._finalize_batcher = FinalizeBatcher(
        worker._write_fused_batch,
        max_queued_rows=QUEUE_ROOM,
        max_flushers=parent_pool_flusher_bound(worker.cfg.parent_pool_size),
    )
    return opened


@pytest_asyncio.fixture(loop_scope='function')
async def direct(
    db_url: str,
    broker: PostgresBroker,  # noqa: ARG001 - the schema and history leaves
) -> AsyncGenerator[ProxiedWorker]:
    opened = await _open_worker(db_url)
    yield opened
    await opened.close()


@pytest_asyncio.fixture(loop_scope='function')
async def listener(db_url: str) -> AsyncGenerator[psycopg.AsyncConnection[Any]]:
    connection = await psycopg.AsyncConnection.connect(
        psycopg_url(db_url),
        autocommit=True,
    )
    _ = await connection.execute(f'LISTEN {CHANNEL}')
    yield connection
    await connection.close()


@pytest_asyncio.fixture(loop_scope='function')
async def blocker(db_url: str) -> AsyncGenerator[psycopg.AsyncConnection[Any]]:
    """A session that holds task advisory locks, to stop a batch mid-way."""
    connection = await psycopg.AsyncConnection.connect(
        psycopg_url(db_url),
        autocommit=True,
    )
    yield connection
    _ = await connection.execute('SELECT pg_advisory_unlock_all()')
    await connection.close()


class _RecordedBatches:
    """Calls the real batch write of finalize; keeps each batch's task ids
    and what the write returned."""

    def __init__(self) -> None:
        self.batches: list[tuple[str, ...]] = []
        self.results: list[
            Result[tuple[TerminalizationOutcome, ...], BatchWriteError]
        ] = []

    async def __call__(
        self,
        handle: CoreHandle,
        batch: FusedCompletionBatch,
    ) -> Result[tuple[TerminalizationOutcome, ...], BatchWriteError]:
        self.batches.append(tuple(command.task_id for command in batch.commands))
        written = await complete_fused_batch_in_core(handle, batch)
        self.results.append(written)
        return written

    def outcomes(self) -> dict[str, list[type[TerminalizationOutcome]]]:
        """The outcome types of each task, over every write, in order."""
        by_task: dict[str, list[type[TerminalizationOutcome]]] = {}
        for written in self.results:
            match written:
                case Ok(outcomes):
                    for outcome in outcomes:
                        by_task.setdefault(outcome.task_id, []).append(type(outcome))
                case Err():
                    pass
        return by_task


@pytest.fixture
def recorded(monkeypatch: pytest.MonkeyPatch) -> _RecordedBatches:
    recording = _RecordedBatches()
    monkeypatch.setattr(finalize_module, 'complete_fused_batch_in_core', recording)
    return recording


def _command(task_id: str, claimed_at: datetime) -> CompleteTaskFused:
    return CompleteTaskFused(
        task_id=task_id,
        fence=OwnedClaim(worker_id=WORKER_ID, claimed_at=claimed_at),
        result_json=ok_result_json(),
    )


async def _finalize(
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
        claim=claim_at(claimed_at),
    )


async def _finalize_together(
    worker: Worker,
    rows: Sequence[tuple[str, datetime]],
) -> list[Result[None, _FinalizeError]]:
    """Finalize ``rows`` from finalizers that all submit in one loop turn."""
    return list(
        await asyncio.wait_for(
            asyncio.gather(
                *(_finalize(worker, task_id, at) for task_id, at in rows),
            ),
            OUTER_BOUND_S,
        )
    )


async def _running_tasks(session: AsyncSession, count: int) -> Case:
    return [await insert_task(session, status='RUNNING') for _ in range(count)]


async def _live_statuses(session: AsyncSession, task_ids: list[str]) -> dict[str, str]:
    rows = (
        await session.execute(
            text(
                'SELECT id, status FROM horsies_tasks '
                'WHERE id = ANY(CAST(:ids AS uuid[]))'
            ),
            {'ids': task_ids},
        )
    ).all()
    await session.commit()
    return {str(row.id): str(row.status) for row in rows}


async def _history(
    session: AsyncSession, task_ids: list[str]
) -> dict[str, tuple[str, str | None]]:
    """Status and error code of each history row of ``task_ids``."""
    rows = (
        await session.execute(
            text(
                'SELECT task_id, status, error_code FROM horsies_task_history '
                'WHERE task_id = ANY(CAST(:ids AS uuid[]))'
            ),
            {'ids': task_ids},
        )
    ).all()
    await session.commit()
    return {str(row.task_id): (str(row.status), row.error_code) for row in rows}


async def _history_results(
    session: AsyncSession, task_ids: list[str]
) -> dict[str, str | None]:
    """The result text of each history row of ``task_ids``."""
    rows = (
        await session.execute(
            text(
                "SELECT task_id, convert_from(result_payload, 'UTF8') AS result "
                'FROM horsies_task_history WHERE task_id = ANY(CAST(:ids AS uuid[]))'
            ),
            {'ids': task_ids},
        )
    ).all()
    await session.commit()
    return {str(row.task_id): row.result for row in rows}


def _written(
    result: Result[tuple[TerminalizationOutcome, ...], BatchWriteError],
) -> list[type[TerminalizationOutcome]]:
    match result:
        case Ok(outcomes):
            return [type(outcome) for outcome in outcomes]
        case Err(error):
            raise AssertionError(f'the batch was not written: {error!r}')


async def _capacity_payloads(
    listener: psycopg.AsyncConnection[Any],
    session: AsyncSession,
) -> list[str]:
    """Every payload on ``CHANNEL`` committed before a sentinel NOTIFY."""
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


async def _hold_task_lock(blocker: psycopg.AsyncConnection[Any], task_id: str) -> None:
    """Take the advisory lock that every writer of ``task_id`` takes first."""
    _ = await blocker.execute(
        'SELECT pg_advisory_lock(hashtextextended(%s, %s))',
        (task_id, TASK_LOCK_SEED),
    )


async def _release_task_lock(
    blocker: psycopg.AsyncConnection[Any], task_id: str
) -> None:
    _ = await blocker.execute(
        'SELECT pg_advisory_unlock(hashtextextended(%s, %s))',
        (task_id, TASK_LOCK_SEED),
    )


async def _batch_backend_waiting(session: AsyncSession) -> int:
    """The pid of the backend whose batch statement waits on a lock."""
    for _ in range(WAIT_POLLS):
        pid = await session.scalar(
            text("""
                SELECT pid FROM pg_stat_activity
                WHERE query LIKE '%horsies_complete_tasks_fused(%'
                  AND wait_event_type = 'Lock'
                  AND pid <> pg_backend_pid()
            """)
        )
        await session.commit()
        if isinstance(pid, int):
            return pid
        await asyncio.sleep(WAIT_POLL_S)
    raise AssertionError('no batch statement waits on a lock')


async def _waiting_writer_query(session: AsyncSession) -> str:
    """The text of the fused completion statement that waits on a lock."""
    for _ in range(WAIT_POLLS):
        query = await session.scalar(
            text("""
                SELECT query FROM pg_stat_activity
                WHERE query LIKE '%horsies_complete_task%fused(%'
                  AND wait_event_type = 'Lock'
                  AND pid <> pg_backend_pid()
            """)
        )
        await session.commit()
        if isinstance(query, str):
            return query
        await asyncio.sleep(WAIT_POLL_S)
    raise AssertionError('no fused completion statement waits on a lock')


def _phase1_error(result: Result[None, _FinalizeError]) -> _FinalizeError:
    match result:
        case Err(error):
            assert error.stage is FinalizeStage.PHASE1
            return error
        case Ok():
            raise AssertionError('finalize succeeded')


# ---------------------------------------------------------------------------
# Outcomes per row
# ---------------------------------------------------------------------------


async def test_a_batch_applies_every_row_and_answers_each_finalizer(
    direct: ProxiedWorker,
    session: AsyncSession,
    listener: psycopg.AsyncConnection[Any],
    recorded: _RecordedBatches,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    rows = await _running_tasks(session, BATCH_ROWS)
    task_ids = [task_id for task_id, _ in rows]

    results = await _finalize_together(direct.worker, [(i, at) for i, at in rows])

    assert results == [Ok(None)] * BATCH_ROWS
    assert recorded.batches == [tuple(task_ids)]
    assert recorded.outcomes() == {task_id: [Applied] for task_id in task_ids}
    assert await _live_statuses(session, task_ids) == {}
    assert await _history(session, task_ids) == {
        task_id: ('COMPLETED', None) for task_id in task_ids
    }
    for task_id in task_ids:
        assert len(await read_attempts(session, task_id)) == 1
    # One statement wrote the batch: one capacity wake for the queue.
    assert await _capacity_payloads(listener, session) == ['capacity']


async def test_each_row_keeps_its_own_fence_in_one_batch(
    direct: ProxiedWorker,
    session: AsyncSession,
    recorded: _RecordedBatches,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Own row, foreign worker, stale claim generation, absent task and a
    committed row in one statement: each row answers on its own fence."""
    (
        (mine, mine_at),
        (foreign, foreign_at),
        (stale, stale_at),
        (done, done_at),
    ) = await _running_tasks(session, 4)
    _ = await session.execute(
        text(
            "UPDATE horsies_tasks SET claimed_by_worker_id = 'other-worker' "
            'WHERE id = CAST(:id AS uuid)'
        ),
        {'id': foreign},
    )
    await session.commit()
    assert await _finalize(direct.worker, done, done_at) == Ok(None)
    absent = str(uuid.uuid4())
    recorded.batches.clear()
    recorded.results.clear()

    results = await _finalize_together(
        direct.worker,
        [
            (mine, mine_at),
            (foreign, foreign_at),
            (stale, stale_at - timedelta(seconds=1)),
            (absent, mine_at),
            (done, done_at),
        ],
    )

    assert results == [Ok(None)] * 5
    assert recorded.batches == [(mine, foreign, stale, absent, done)]
    assert recorded.outcomes() == {
        mine: [Applied],
        foreign: [LostClaim],
        stale: [LostClaim],
        absent: [TaskAbsent],
        done: [AlreadyApplied],
    }
    assert await _live_statuses(session, [mine, foreign, stale]) == {
        foreign: 'RUNNING',
        stale: 'RUNNING',
    }
    assert set(await _history(session, [mine, foreign, stale, done])) == {mine, done}


async def test_a_batch_writes_each_result_text_as_given(
    direct: ProxiedWorker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Escapes, whitespace and characters outside ASCII reach the row
    unchanged."""
    (first, first_at), (second, second_at) = await _running_tasks(session, 2)
    texts = {
        first: '{\n\t"ok": "quote \\" backslash \\\\ escaped nul \\u0000"}',
        second: '{"ok":"é 中 𝄞"}',
    }
    batch = FusedCompletionBatch(
        commands=(
            replace(_command(first, first_at), result_json=texts[first]),
            replace(_command(second, second_at), result_json=texts[second]),
        ),
    )

    written = await complete_fused_batch_in_core(direct.worker.core_handle, batch)

    assert _written(written) == [Applied, Applied]
    assert await _history_results(session, [first, second]) == texts


async def test_a_claim_generation_keeps_its_microseconds_and_its_offset(
    direct: ProxiedWorker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The fence compares instants: the same instant at another UTC offset
    matches, one microsecond later does not."""
    (same, same_at), (later, later_at) = await _running_tasks(session, 2)
    batch = FusedCompletionBatch(
        commands=(
            _command(
                same, same_at.astimezone(timezone(timedelta(hours=5, minutes=30)))
            ),
            _command(later, later_at + timedelta(microseconds=1)),
        ),
    )

    written = await complete_fused_batch_in_core(direct.worker.core_handle, batch)

    assert _written(written) == [Applied, LostClaim]
    assert await _live_statuses(session, [same, later]) == {later: 'RUNNING'}


def _batch_row(task_id: str, claimed_at: str | None) -> list[str | None]:
    """One row of the batch parameter, as ``_fused_batch_parameters`` builds
    it, with ``claimed_at`` as given."""
    return [task_id, claimed_at, ok_result_json()]


async def test_the_stored_batch_function_refuses_a_null_generation(
    direct: ProxiedWorker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Every row needs its claim generation: a NULL claimed_at in the array
    refuses the whole statement (22023) before any row is written. The
    worker never sends one: each command carries a claim time, so this calls
    the statement with a hand-built parameter."""
    (first, _), (second, second_at) = await _running_tasks(session, 2)
    batch = json.dumps(
        [_batch_row(first, None), _batch_row(second, second_at.isoformat())],
    )

    with pytest.raises(DBAPIError) as raised:
        async with direct.worker.sf() as statement_session:
            await statement_session.execute(
                _COMPLETE_TASKS_FUSED_SQL,
                {'batch': batch, 'worker_id': WORKER_ID},
            )

    original = raised.value.orig
    assert isinstance(original, psycopg.Error)
    assert original.sqlstate == '22023'
    assert await _live_statuses(session, [first, second]) == {
        first: 'RUNNING',
        second: 'RUNNING',
    }


async def test_a_replay_of_a_whole_batch_is_already_applied_and_writes_nothing(
    direct: ProxiedWorker,
    session: AsyncSession,
    listener: psycopg.AsyncConnection[Any],
    recorded: _RecordedBatches,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    rows = await _running_tasks(session, 4)
    task_ids = [task_id for task_id, _ in rows]
    batch = [(task_id, at) for task_id, at in rows]
    assert await _finalize_together(direct.worker, batch) == [Ok(None)] * 4
    assert await _capacity_payloads(listener, session) == ['capacity']

    replay = await _finalize_together(direct.worker, batch)

    assert replay == [Ok(None)] * 4
    assert recorded.batches == [tuple(task_ids), tuple(task_ids)]
    assert recorded.outcomes() == {
        task_id: [Applied, AlreadyApplied] for task_id in task_ids
    }
    assert await _capacity_payloads(listener, session) == []
    for task_id in task_ids:
        assert len(await read_attempts(session, task_id)) == 1


async def test_a_lone_row_runs_the_batch_function_with_one_row(
    direct: ProxiedWorker,
    session: AsyncSession,
    blocker: psycopg.AsyncConnection[Any],
    recorded: _RecordedBatches,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The core sends ``horsies_complete_tasks_fused`` for a batch of one row
    too; it calls ``horsies_complete_task_fused`` once. The Python statement
    sent the single fused completion itself; the effects are the same."""
    task_id, claimed_at = await insert_task(session, status='RUNNING')
    await _hold_task_lock(blocker, task_id)
    finalizing = asyncio.create_task(_finalize(direct.worker, task_id, claimed_at))
    query = await _waiting_writer_query(session)
    await _release_task_lock(blocker, task_id)

    assert await asyncio.wait_for(finalizing, OUTER_BOUND_S) == Ok(None)
    assert 'horsies_complete_tasks_fused(' in query
    assert recorded.batches == [(task_id,)]
    assert recorded.outcomes() == {task_id: [Applied]}


# ---------------------------------------------------------------------------
# Round trips
# ---------------------------------------------------------------------------


async def test_a_batch_of_rows_is_one_round_trip(
    proxy: RoundTripProxy,
    db_url: str,
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    opened = await _open_worker(proxied_url(db_url, proxy))

    async def arrange() -> Case:
        return await _running_tasks(session, BATCH_ROWS)

    async def act(case: Case) -> None:
        results = await _finalize_together(
            opened.worker,
            [(task_id, at) for task_id, at in case],
        )
        assert results == [Ok(None)] * BATCH_ROWS

    try:
        case, turns = await steady_state_turns(proxy, arrange, act)
    finally:
        await opened.close()

    assert len(turns) == 1, turns
    assert set(await _history(session, [task_id for task_id, _ in case])) == {
        task_id for task_id, _ in case
    }


# ---------------------------------------------------------------------------
# Failures
# ---------------------------------------------------------------------------


async def test_a_bad_row_rolls_back_its_batch_and_each_row_is_written_alone_once(
    direct: ProxiedWorker,
    session: AsyncSession,
    recorded: _RecordedBatches,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """No history partition covers the bad row's class: its move raises, so
    the statement writes nothing. Each row is then written alone, once."""
    (good_a, good_a_at), (bad, bad_at), (good_b, good_b_at) = await _running_tasks(
        session, 3
    )
    _ = await session.execute(
        text(
            'UPDATE horsies_tasks SET retention_class_key = :class '
            'WHERE id = CAST(:id AS uuid)'
        ),
        {'class': UNREGISTERED_CLASS, 'id': bad},
    )
    await session.commit()

    results = await _finalize_together(
        direct.worker,
        [(good_a, good_a_at), (bad, bad_at), (good_b, good_b_at)],
    )

    assert recorded.batches == [(good_a, bad, good_b), (good_a,), (bad,), (good_b,)]
    assert isinstance(recorded.results[0], Err)
    assert results[0] == Ok(None) and results[2] == Ok(None)
    error = _phase1_error(results[1])
    assert error.retryable is False
    assert error.data is not None and error.data['batch_write'] == 'ROLLED_BACK'
    assert error.data['outcome']['result_json_str'] == ok_result_json()
    assert recorded.outcomes() == {good_a: [Applied], good_b: [Applied]}
    assert await _live_statuses(session, [good_a, bad, good_b]) == {bad: 'RUNNING'}


async def test_a_lost_reply_of_a_batch_is_classified_by_the_replay_of_each_row(
    proxy: RoundTripProxy,
    db_url: str,
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    recorded: _RecordedBatches,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The batch commits and its reply never reaches the worker: each row's
    finalizer gets a retryable COMMIT_UNKNOWN error, and the phase-1 retry of
    each row answers AlreadyApplied."""
    opened = await _open_worker(proxied_url(db_url, proxy))
    try:
        # Warm the core handle first: a drop armed before its connect and
        # its family read hits one of them, and the batch is then not sent.
        await warm_core_handle(opened)
        rows = await _running_tasks(session, 4)
        task_ids = [task_id for task_id, _ in rows]
        proxy.drop_next_execute_reply()

        first = await _finalize_together(opened.worker, [(i, at) for i, at in rows])

        assert proxy.reply_was_dropped(REPLY_DROP_WAIT_S)
        errors = [_phase1_error(result) for result in first]
        assert all(error.retryable for error in errors)
        assert {
            error.data['batch_write'] for error in errors if error.data is not None
        } == {'COMMIT_UNKNOWN'}
        # The premise of the replay: the statement committed.
        assert await _live_statuses(session, task_ids) == {}

        _ = await asyncio.wait_for(
            asyncio.gather(
                *(opened.worker._retry_finalize_phase1(error, 0.0) for error in errors),
            ),
            OUTER_BOUND_S,
        )
    finally:
        await opened.close()

    assert recorded.outcomes() == {task_id: [AlreadyApplied] for task_id in task_ids}
    assert len(recorded.batches) == 2
    for task_id in task_ids:
        assert len(await read_attempts(session, task_id)) == 1


# ---------------------------------------------------------------------------
# Promises: the commit comes first
# ---------------------------------------------------------------------------


async def test_the_finalizer_end_and_the_capacity_wake_follow_the_commit(
    direct: ProxiedWorker,
    session: AsyncSession,
    listener: psycopg.AsyncConnection[Any],
    blocker: psycopg.AsyncConnection[Any],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """While the batch statement waits, no finalizer of its rows has ended,
    the worker's capacity wake is not set, and no capacity NOTIFY is out."""
    worker = direct.worker
    rows = await _running_tasks(session, 3)
    task_ids = sorted(task_id for task_id, _ in rows)
    await _hold_task_lock(blocker, task_ids[-1])
    worker._capacity_released.clear()
    finalizers = [
        worker._spawn_background(
            _finalize(worker, task_id, at),
            name=f'finalize-{task_id}',
            finalizer=True,
        )
        for task_id, at in rows
    ]
    _ = await _batch_backend_waiting(session)

    assert not any(finalizer.done() for finalizer in finalizers)
    assert not worker._capacity_released.is_set()
    assert await _live_statuses(session, task_ids) == dict.fromkeys(task_ids, 'RUNNING')
    assert await _capacity_payloads(listener, session) == []

    await _release_task_lock(blocker, task_ids[-1])
    results = await asyncio.wait_for(asyncio.gather(*finalizers), OUTER_BOUND_S)

    assert results == [Ok(None)] * 3
    assert worker._capacity_released.is_set()
    assert await _live_statuses(session, task_ids) == {}
    # Each statement that wrote rows sends one constant capacity wake.
    payloads = await _capacity_payloads(listener, session)
    assert payloads and set(payloads) == {'capacity'}, payloads


async def test_the_row_is_written_only_after_its_attempt_ended(
    direct: ProxiedWorker,
    session: AsyncSession,
    recorded: _RecordedBatches,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Entered from the isolated finalize: the batcher gets the row only when
    the slot reported the end of the task."""
    task_id, claimed_at = await insert_task(session, status='RUNNING')
    handle = unbound_handle(task_id)
    handle.assigned.set_result(Assigned())
    finalizer = asyncio.create_task(
        direct.worker._finalize_after(
            handle,
            task_id,
            queue_name=QUEUE,
            is_workflow_task=False,
            task_name=TASK_NAME,
            claim=claim_at(claimed_at),
        )
    )
    for _ in range(10):
        await asyncio.sleep(0)

    assert recorded.batches == []
    assert await _live_statuses(session, [task_id]) == {task_id: 'RUNNING'}

    handle.finish(
        TaskFinished(True, ok_result_json(), None, RssUnreadable(), NoTaskLoop()),
    )
    assert await asyncio.wait_for(finalizer, OUTER_BOUND_S) == Ok(None)

    assert recorded.batches == [(task_id,)]
    assert await _history(session, [task_id]) == {task_id: ('COMPLETED', None)}


# ---------------------------------------------------------------------------
# Promises: rows that did not commit stay RUNNING
# ---------------------------------------------------------------------------


async def test_a_batch_killed_mid_statement_leaves_its_rows_for_stale_recovery(
    direct: ProxiedWorker,
    broker: PostgresBroker,
    session: AsyncSession,
    blocker: psycopg.AsyncConnection[Any],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The statement moved its first rows and waits on the last; its backend
    ends. Nothing of it commits: each row stays RUNNING, each finalizer gets
    a retryable COMMIT_UNKNOWN error, and stale RUNNING recovery fails the
    rows as crashed attempts."""
    rows = await _running_tasks(session, 3)
    task_ids = sorted(task_id for task_id, _ in rows)
    await _hold_task_lock(blocker, task_ids[-1])
    finalizing = asyncio.create_task(
        _finalize_together(direct.worker, [(i, at) for i, at in rows]),
    )
    pid = await _batch_backend_waiting(session)

    terminated = await session.scalar(
        text('SELECT pg_terminate_backend(:pid)'),
        {'pid': pid},
    )
    await session.commit()
    results = await asyncio.wait_for(finalizing, OUTER_BOUND_S)

    assert terminated is True
    errors = [_phase1_error(result) for result in results]
    assert all(error.retryable for error in errors)
    assert {
        error.data['batch_write'] for error in errors if error.data is not None
    } == {'COMMIT_UNKNOWN'}
    assert await _live_statuses(session, task_ids) == dict.fromkeys(task_ids, 'RUNNING')
    assert await _history(session, task_ids) == {}

    await _release_task_lock(blocker, task_ids[-1])
    await asyncio.sleep(0.05)
    recovered = await broker.mark_stale_tasks_as_failed(
        stale_threshold_ms=1,
        finalizing_stale_threshold_ms=1,
    )

    match recovered:
        case Ok(count):
            assert count == 3
        case Err(error):
            raise AssertionError(error)
    assert await _history(session, task_ids) == {
        task_id: ('FAILED', 'WORKER_CRASHED') for task_id in task_ids
    }


async def test_rows_that_a_stop_left_unsent_stay_running(
    direct: ProxiedWorker,
    session: AsyncSession,
    blocker: psycopg.AsyncConnection[Any],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A close that times out cancels the flushers. Its report names the rows
    that a write had received and the rows that no write received. The core
    runs a received write to its end, so each received row is RUNNING or
    committed; each row that no write received stays RUNNING."""
    worker = direct.worker
    first_batch = await _running_tasks(session, 2)
    second_batch = await _running_tasks(session, 2)
    unsent = await _running_tasks(session, 2)
    await _hold_task_lock(blocker, sorted(task_id for task_id, _ in first_batch)[-1])
    first = asyncio.create_task(_finalize_together(worker, first_batch))
    _ = await _batch_backend_waiting(session)
    # The second steady flusher hands its rows to the write too; hold them,
    # so neither write can end before the close timeout.
    for task_id, _ in second_batch:
        await _hold_task_lock(blocker, task_id)
    second = asyncio.create_task(_finalize_together(worker, second_batch))
    await _handed_to_the_write(worker, _ids(first_batch) | _ids(second_batch))
    # Both steady flushers write, and less than a full batch waits: no
    # flusher takes these rows.
    third = asyncio.create_task(_finalize_together(worker, unsent))
    await _queued(worker, _ids(unsent))

    report = await worker._finalize_batcher.close(0.2)
    _ = await asyncio.gather(first, second, third, return_exceptions=True)
    _ = await blocker.execute('SELECT pg_advisory_unlock_all()')

    assert report == FlushersCancelled(
        in_flight_task_ids=frozenset(_ids(first_batch) | _ids(second_batch)),
        unsent_task_ids=frozenset(_ids(unsent)),
    )
    await _batch_statements_ended(session)
    task_ids = sorted(_ids(first_batch) | _ids(second_batch) | _ids(unsent))
    live = await _live_statuses(session, task_ids)
    history = await _history(session, task_ids)
    for task_id in _ids(unsent):
        assert (live.get(task_id), history.get(task_id)) == ('RUNNING', None)
    for task_id in _ids(first_batch) | _ids(second_batch):
        assert (live.get(task_id), history.get(task_id)) in {
            ('RUNNING', None),
            (None, ('COMPLETED', None)),
        }


def _ids(rows: Case) -> set[str]:
    return {task_id for task_id, _ in rows}


async def _handed_to_the_write(worker: Worker, task_ids: set[str]) -> None:
    """Wait until the flushers have handed every row of ``task_ids`` to the
    write."""
    batcher = worker._finalize_batcher
    for _ in range(WAIT_POLLS):
        handed = {
            task_id for batch in batcher._in_flight.values() for task_id in batch
        }
        if task_ids <= handed:
            return
        await asyncio.sleep(WAIT_POLL_S)
    raise AssertionError(f'the flushers did not take the rows {sorted(task_ids)}')


async def _queued(worker: Worker, task_ids: set[str]) -> None:
    """Wait until every row of ``task_ids`` waits in the batch queue."""
    batcher = worker._finalize_batcher
    for _ in range(WAIT_POLLS):
        if task_ids <= {row.command.task_id for row in batcher._queue}:
            return
        await asyncio.sleep(WAIT_POLL_S)
    raise AssertionError(f'the rows {sorted(task_ids)} never waited in the queue')


async def _batch_statements_ended(session: AsyncSession) -> None:
    """Wait until no backend runs the batch statement."""
    for _ in range(WAIT_POLLS):
        running = await session.scalar(
            text("""
                SELECT count(*) FROM pg_stat_activity
                WHERE query LIKE '%horsies_complete_tasks_fused(%'
                  AND state = 'active'
                  AND pid <> pg_backend_pid()
            """)
        )
        await session.commit()
        if running == 0:
            return
        await asyncio.sleep(WAIT_POLL_S)
    raise AssertionError('a batch statement still runs')


# ---------------------------------------------------------------------------
# Lock order
# ---------------------------------------------------------------------------


async def test_the_batch_locks_its_rows_in_sorted_task_id_order(
    direct: ProxiedWorker,
    session: AsyncSession,
    blocker: psycopg.AsyncConnection[Any],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Input order high, middle, low. With the middle row's lock held
    elsewhere, the statement holds the low row's lock, waits on the middle
    one, and has not reached the high one."""
    rows = await _running_tasks(session, 3)
    by_id = dict(rows)
    low, middle, high = sorted(by_id)
    await _hold_task_lock(blocker, middle)
    batch = FusedCompletionBatch(
        commands=tuple(
            _command(task_id, by_id[task_id]) for task_id in (high, middle, low)
        ),
    )
    writing = asyncio.create_task(
        apply_fused_batch(PooledStatements(direct.worker.sf), batch),
    )
    pid = await _batch_backend_waiting(session)

    locks = (
        await session.execute(
            text("""
                SELECT t.task_id, l.granted
                FROM unnest(CAST(:ids AS text[])) AS t(task_id)
                JOIN pg_locks AS l
                  ON l.locktype = 'advisory'
                 AND l.pid = :pid
                 AND l.classid = (
                     (hashtextextended(t.task_id, :seed) >> 32) & 4294967295
                 )::oid
                 AND l.objid = (
                     hashtextextended(t.task_id, :seed) & 4294967295
                 )::oid
                 AND l.objsubid = 1
            """),
            {'ids': [low, middle, high], 'pid': pid, 'seed': TASK_LOCK_SEED},
        )
    ).all()
    await session.commit()
    await _release_task_lock(blocker, middle)
    written = await asyncio.wait_for(writing, OUTER_BOUND_S)

    assert {str(row.task_id): bool(row.granted) for row in locks} == {
        low: True,
        middle: False,
    }
    match written:
        case Ok(outcomes):
            assert [outcome.task_id for outcome in outcomes] == [high, middle, low]
            assert [outcome.ordinality for outcome in outcomes] == [1, 2, 3]
            assert {type(outcome) for outcome in outcomes} == {Applied}
        case Err(error):
            raise AssertionError(error)


async def test_two_flushers_and_workflow_node_finalizes_do_not_deadlock(
    direct: ProxiedWorker,
    session: AsyncSession,
    recorded: _RecordedBatches,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Plain rows in two concurrent batches, and the nodes of one workflow
    finalized through the locked path at the same time: every write ends,
    and no row is refused."""
    worker = direct.worker
    plain_first = await _running_tasks(session, BATCH_ROWS)
    plain_second = await _running_tasks(session, BATCH_ROWS)
    nodes = await _seed_running_workflow_nodes(session, 4)

    async def node_finalize(
        task_id: str, claimed_at: datetime
    ) -> Result[None, _FinalizeError]:
        return await worker._finalize_task_result(
            task_id,
            True,
            ok_result_json(),
            None,
            queue_name=QUEUE,
            is_workflow_task=True,
            task_name=TASK_NAME,
            claim=claim_at(claimed_at),
        )

    first = asyncio.create_task(_finalize_together(worker, plain_first))
    for _ in range(3):
        await asyncio.sleep(0)
    second = asyncio.create_task(_finalize_together(worker, plain_second))
    node_results = [
        asyncio.create_task(node_finalize(task_id, at)) for task_id, at in nodes
    ]

    assert await asyncio.wait_for(first, OUTER_BOUND_S) == [Ok(None)] * BATCH_ROWS
    assert await asyncio.wait_for(second, OUTER_BOUND_S) == [Ok(None)] * BATCH_ROWS
    for node_result in node_results:
        assert await asyncio.wait_for(node_result, OUTER_BOUND_S) == Ok(None)
    plain_ids = [task_id for task_id, _ in [*plain_first, *plain_second]]
    assert recorded.outcomes() == {task_id: [Applied] for task_id in plain_ids}
    assert len(recorded.batches) >= 2
    node_ids = [task_id for task_id, _ in nodes]
    assert await _live_statuses(session, [*plain_ids, *node_ids]) == {}


async def _seed_running_workflow_nodes(
    session: AsyncSession,
    count: int,
) -> list[tuple[str, datetime]]:
    """One RUNNING workflow with ``count`` independent RUNNING nodes, each
    linked to a RUNNING task that the worker owns."""
    workflow_id = str(uuid.uuid4())
    _ = await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES (:id, 'batch_wf', 'RUNNING', 'fail', 0, :id,
                    NOW(), NOW(), NOW(), NOW())
        """),
        {'id': workflow_id},
    )
    await session.commit()
    nodes: list[tuple[str, datetime]] = []
    for index in range(count):
        task_id, claimed_at = await insert_task(
            session,
            status='RUNNING',
            is_workflow_task=True,
        )
        _ = await session.execute(
            text("""
                INSERT INTO horsies_workflow_tasks
                    (downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                     queue_name, priority, dependencies,
                     allow_failed_deps, join_type, is_subworkflow, status,
                     task_id, started_at, created_at,
                     max_retries, execution, retention_class_key)
                VALUES ('NONE', :id, :workflow_id, :index, :node_id, :task_name, '{}',
                        :queue, 100, '{}', FALSE, 'all', FALSE,
                        'RUNNING', :task_id, NOW(), NOW(),
                        0, 'ISOLATED', 'standard_30d')
            """),
            {
                'id': str(uuid.uuid4()),
                'workflow_id': workflow_id,
                'index': index,
                'node_id': f'node_{index}',
                'task_name': TASK_NAME,
                'queue': QUEUE,
                'task_id': task_id,
            },
        )
        await session.commit()
        nodes.append((task_id, claimed_at))
    return nodes
