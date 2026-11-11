"""The finalize batcher, on a fake batch write.

Each test drives ``FinalizeBatcher`` with a write that records the batches it
gets and answers from a script. The real statement and its outcomes are
covered by ``tests/integration/test_finalize_batch.py``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Final
from unittest.mock import MagicMock

import psycopg
import pytest
from sqlalchemy.exc import OperationalError as SAOperationalError
from sqlalchemy.exc import IntegrityError as SAIntegrityError
from sqlalchemy.exc import TimeoutError as SATimeoutError

from horsies.core.lifecycle.commands import CompleteTaskFused
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    Applied,
    LostClaim,
    ObservedTaskState,
    TerminalizationOutcome,
)
from horsies.core.lifecycle.persistence import (
    FUSED_BATCH_MAX_ROWS,
    BatchCommitUnknown,
    BatchRolledBack,
    BatchWriteError,
    FusedCompletionBatch,
)
from horsies.core.types.result import Err, Ok, Result
from horsies.core.types.status import TaskStatus
from horsies.core.utils.db import NotSent, OutcomeUnknown, StatementRefused
from horsies.core.worker.finalize_batch import (
    FINALIZE_BATCH_FLUSHERS,
    BatcherState,
    BatchRowResult,
    FinalizeBatcher,
    FlushersCancelled,
    FlushersDrained,
)
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker

pytestmark = [pytest.mark.unit]

WORKER: Final = 'worker-1'
CLAIMED_AT: Final = datetime(2026, 11, 11, 9, 0, tzinfo=timezone.utc)
QUEUE_BOUND: Final = 1000
STEADY_ONLY: Final = FINALIZE_BATCH_FLUSHERS
"""A flusher bound with no room for a surge flusher."""
# A test of a wait fails, not hangs, when the wait never ends.
OUTER_BOUND_S: Final = 5.0

type WriteResult = Result[tuple[TerminalizationOutcome, ...], BatchWriteError]


def _command(task_id: str) -> CompleteTaskFused:
    return CompleteTaskFused(
        task_id=task_id,
        fence=OwnedClaim(worker_id=WORKER, claimed_at=CLAIMED_AT),
        result_json='{"ok":null}',
        notify_channel='task_queue_default',
        notify_payload=f'capacity:{task_id}',
    )


def _applied(task_id: str, ordinality: int) -> Applied:
    return Applied(
        task_id=task_id,
        ordinality=ordinality,
        terminal_at=CLAIMED_AT,
        kind=TerminalizationKind.COMPLETE_FUSED,
        observed=ObservedTaskState(
            status=TaskStatus.RUNNING,
            worker_id=WORKER,
            claimed_at=CLAIMED_AT,
        ),
    )


def _all_applied(batch: FusedCompletionBatch) -> WriteResult:
    return Ok(
        tuple(
            _applied(command.task_id, position)
            for position, command in enumerate(batch.commands, start=1)
        )
    )


def _refused() -> StatementRefused:
    return StatementRefused(
        error=SAIntegrityError(
            'SELECT',
            {},
            psycopg.IntegrityError('no partition for the row'),
        ),
    )


def _not_sent() -> NotSent:
    return NotSent(error=SATimeoutError('QueuePool limit reached'))


def _outcome_unknown() -> OutcomeUnknown:
    return OutcomeUnknown(
        error=SAOperationalError(
            'SELECT',
            {},
            psycopg.OperationalError('server closed the connection'),
            connection_invalidated=True,
        ),
    )


class _ScriptedWrite:
    """A batch write that records each batch and answers with ``answer``.

    With ``gated`` set, each write waits for ``release`` before it answers.
    """

    def __init__(
        self,
        answer: Callable[[FusedCompletionBatch], WriteResult] = _all_applied,
        *,
        gated: bool = False,
    ) -> None:
        self.batches: list[tuple[str, ...]] = []
        self.in_flight = 0
        self.most_in_flight = 0
        self._answer = answer
        self._gate = asyncio.Event()
        if not gated:
            self._gate.set()

    def release(self) -> None:
        self._gate.set()

    async def __call__(self, batch: FusedCompletionBatch) -> WriteResult:
        self.batches.append(tuple(command.task_id for command in batch.commands))
        self.in_flight += 1
        self.most_in_flight = max(self.most_in_flight, self.in_flight)
        try:
            await self._gate.wait()
        finally:
            self.in_flight -= 1
        return self._answer(batch)


async def _settle() -> None:
    """Run every task that is ready until none is: a few loop turns."""
    for _ in range(10):
        await asyncio.sleep(0)


async def _submit_alone(
    batcher: FinalizeBatcher,
    task_id: str,
) -> asyncio.Task[BatchRowResult]:
    """Submit one row and let the flushers take it before the next submit.

    Rows submitted in one loop turn all queue before a flusher runs, so the
    first flusher takes them together.
    """
    call = asyncio.create_task(batcher.complete(_command(task_id)))
    await _settle()
    return call


def _ok_task_id(result: BatchRowResult) -> str:
    match result:
        case Ok(outcome):
            return outcome.task_id
        case Err(error):
            raise AssertionError(f'expected an outcome, got {error!r}')


# ---------------------------------------------------------------------------
# Flush rule
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_lone_row_is_written_at_once() -> None:
    write = _ScriptedWrite()
    batcher = FinalizeBatcher(
        write, max_queued_rows=QUEUE_BOUND, max_flushers=STEADY_ONLY
    )

    result = await asyncio.wait_for(batcher.complete(_command('a')), OUTER_BOUND_S)

    assert _ok_task_id(result) == 'a'
    assert write.batches == [('a',)]


@pytest.mark.asyncio
async def test_rows_that_arrive_while_the_flushers_write_form_the_next_batch() -> None:
    write = _ScriptedWrite(gated=True)
    batcher = FinalizeBatcher(
        write, max_queued_rows=QUEUE_BOUND, max_flushers=STEADY_ONLY
    )
    first = [await _submit_alone(batcher, task_id) for task_id in ('a', 'b')]
    later = [
        asyncio.create_task(batcher.complete(_command(i))) for i in ('c', 'd', 'e')
    ]
    await _settle()

    assert write.batches == [('a',), ('b',)]
    write.release()
    results = await asyncio.wait_for(asyncio.gather(*first, *later), OUTER_BOUND_S)

    assert write.batches == [('a',), ('b',), ('c', 'd', 'e')]
    assert [_ok_task_id(result) for result in results] == ['a', 'b', 'c', 'd', 'e']


@pytest.mark.asyncio
async def test_a_batch_holds_at_most_the_max_rows() -> None:
    write = _ScriptedWrite()
    batcher = FinalizeBatcher(
        write, max_queued_rows=QUEUE_BOUND, max_flushers=STEADY_ONLY
    )
    task_ids = [f't{index}' for index in range(FUSED_BATCH_MAX_ROWS + 10)]

    _ = await asyncio.wait_for(
        asyncio.gather(*(batcher.complete(_command(i)) for i in task_ids)),
        OUTER_BOUND_S,
    )

    assert [len(batch) for batch in write.batches] == [FUSED_BATCH_MAX_ROWS, 10]
    assert [i for batch in write.batches for i in batch] == task_ids


@pytest.mark.asyncio
async def test_no_more_than_the_flusher_bound_write_at_once() -> None:
    write = _ScriptedWrite(gated=True)
    batcher = FinalizeBatcher(
        write, max_queued_rows=QUEUE_BOUND, max_flushers=STEADY_ONLY
    )
    calls = [
        asyncio.create_task(batcher.complete(_command(f't{index}')))
        for index in range(5)
    ]
    for index in range(5, 40):
        await _settle()
        calls.append(asyncio.create_task(batcher.complete(_command(f't{index}'))))
    await _settle()

    assert write.in_flight == FINALIZE_BATCH_FLUSHERS
    write.release()
    _ = await asyncio.wait_for(asyncio.gather(*calls), OUTER_BOUND_S)
    assert write.most_in_flight == FINALIZE_BATCH_FLUSHERS


@pytest.mark.asyncio
async def test_a_repeated_task_id_waits_for_the_next_batch() -> None:
    write = _ScriptedWrite()
    batcher = FinalizeBatcher(
        write, max_queued_rows=QUEUE_BOUND, max_flushers=STEADY_ONLY
    )
    first_flusher_rows = ('a', 'b', 'a', 'c')

    results = await asyncio.wait_for(
        asyncio.gather(*(batcher.complete(_command(i)) for i in first_flusher_rows)),
        OUTER_BOUND_S,
    )

    assert write.batches == [('a', 'b', 'c'), ('a',)]
    assert [_ok_task_id(result) for result in results] == list(first_flusher_rows)


# ---------------------------------------------------------------------------
# Answers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_each_row_gets_the_outcome_of_its_own_task() -> None:
    def mixed(batch: FusedCompletionBatch) -> WriteResult:
        outcomes: list[TerminalizationOutcome] = []
        for position, command in enumerate(batch.commands, start=1):
            if command.task_id == 'lost':
                outcomes.append(
                    LostClaim(
                        task_id='lost',
                        ordinality=position,
                        observed=ObservedTaskState(
                            status=TaskStatus.RUNNING,
                            worker_id='other',
                            claimed_at=CLAIMED_AT,
                        ),
                    )
                )
            else:
                outcomes.append(_applied(command.task_id, position))
        return Ok(tuple(outcomes))

    batcher = FinalizeBatcher(
        _ScriptedWrite(mixed), max_queued_rows=QUEUE_BOUND, max_flushers=STEADY_ONLY
    )

    applied, lost = await asyncio.wait_for(
        asyncio.gather(
            batcher.complete(_command('mine')),
            batcher.complete(_command('lost')),
        ),
        OUTER_BOUND_S,
    )

    assert isinstance(applied, Ok) and isinstance(applied.ok_value, Applied)
    assert applied.ok_value.task_id == 'mine'
    assert isinstance(lost, Ok) and isinstance(lost.ok_value, LostClaim)
    assert lost.ok_value.task_id == 'lost'


@pytest.mark.asyncio
async def test_the_answer_resolves_only_after_the_write_returned() -> None:
    write = _ScriptedWrite(gated=True)
    batcher = FinalizeBatcher(
        write, max_queued_rows=QUEUE_BOUND, max_flushers=STEADY_ONLY
    )

    call = asyncio.create_task(batcher.complete(_command('a')))
    await _settle()

    assert write.batches == [('a',)]
    assert not call.done()
    write.release()
    assert _ok_task_id(await asyncio.wait_for(call, OUTER_BOUND_S)) == 'a'


@pytest.mark.asyncio
async def test_a_refused_batch_writes_each_row_alone_once() -> None:
    def refuse_bad(batch: FusedCompletionBatch) -> WriteResult:
        if [command.task_id for command in batch.commands] != ['a'] and [
            command.task_id for command in batch.commands
        ] != ['b']:
            return Err(BatchRolledBack(cause=_refused()))
        return _all_applied(batch)

    write = _ScriptedWrite(refuse_bad)
    batcher = FinalizeBatcher(
        write, max_queued_rows=QUEUE_BOUND, max_flushers=STEADY_ONLY
    )

    good_a, bad, good_b = await asyncio.wait_for(
        asyncio.gather(
            batcher.complete(_command('a')),
            batcher.complete(_command('bad')),
            batcher.complete(_command('b')),
        ),
        OUTER_BOUND_S,
    )

    # The first flusher takes the three rows; the second finds none.
    assert write.batches == [('a', 'bad', 'b'), ('a',), ('bad',), ('b',)]
    assert _ok_task_id(good_a) == 'a'
    assert _ok_task_id(good_b) == 'b'
    match bad:
        case Err(BatchRolledBack(cause=StatementRefused()) as rolled_back):
            assert rolled_back.retryable is False
        case _:
            raise AssertionError(bad)


@pytest.mark.asyncio
async def test_a_refused_row_of_one_is_not_written_again() -> None:
    write = _ScriptedWrite(lambda _batch: Err(BatchRolledBack(cause=_refused())))
    batcher = FinalizeBatcher(
        write, max_queued_rows=QUEUE_BOUND, max_flushers=STEADY_ONLY
    )

    result = await asyncio.wait_for(batcher.complete(_command('a')), OUTER_BOUND_S)

    assert write.batches == [('a',)]
    assert isinstance(result, Err)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'error',
    [BatchRolledBack(cause=_not_sent()), BatchCommitUnknown(cause=_outcome_unknown())],
    ids=['not_sent', 'commit_unknown'],
)
async def test_a_failure_of_the_connection_answers_every_row_without_a_split(
    error: BatchWriteError,
) -> None:
    write = _ScriptedWrite(lambda _batch: Err(error))
    batcher = FinalizeBatcher(
        write, max_queued_rows=QUEUE_BOUND, max_flushers=STEADY_ONLY
    )

    results = await asyncio.wait_for(
        asyncio.gather(*(batcher.complete(_command(i)) for i in ('a', 'b', 'c'))),
        OUTER_BOUND_S,
    )

    assert write.batches == [('a', 'b', 'c')]
    assert results == [Err(error), Err(error), Err(error)]


def test_the_retryable_flag_follows_the_cause() -> None:
    assert BatchRolledBack(cause=_not_sent()).retryable is True
    assert BatchRolledBack(cause=_refused()).retryable is False
    deadlock = StatementRefused(
        error=SAOperationalError(
            'SELECT',
            {},
            psycopg.errors.DeadlockDetected('deadlock detected'),
        ),
    )
    assert BatchRolledBack(cause=deadlock).retryable is True


@pytest.mark.asyncio
async def test_a_defect_of_the_write_is_raised_to_every_row() -> None:
    writes: list[int] = []

    async def broken_once(batch: FusedCompletionBatch) -> WriteResult:
        writes.append(len(batch.commands))
        if len(writes) == 1:
            raise RuntimeError('FusedCompletionBatch returned a row without ordinality')
        return _all_applied(batch)

    batcher = FinalizeBatcher(
        broken_once, max_queued_rows=QUEUE_BOUND, max_flushers=STEADY_ONLY
    )

    results = await asyncio.wait_for(
        asyncio.gather(
            batcher.complete(_command('a')),
            batcher.complete(_command('b')),
            return_exceptions=True,
        ),
        OUTER_BOUND_S,
    )
    later = await asyncio.wait_for(batcher.complete(_command('c')), OUTER_BOUND_S)

    assert [type(result) for result in results] == [RuntimeError, RuntimeError]
    assert _ok_task_id(later) == 'c'
    assert writes == [2, 1]


@pytest.mark.asyncio
async def test_a_cancelled_caller_leaves_its_row_written() -> None:
    write = _ScriptedWrite(gated=True)
    batcher = FinalizeBatcher(
        write, max_queued_rows=QUEUE_BOUND, max_flushers=STEADY_ONLY
    )
    held = await _submit_alone(batcher, 'a')
    queued = await _submit_alone(batcher, 'b')
    queued_later = await _submit_alone(batcher, 'c')

    held.cancel()
    queued_later.cancel()
    write.release()
    assert _ok_task_id(await asyncio.wait_for(queued, OUTER_BOUND_S)) == 'b'
    await _settle()

    assert write.batches == [('a',), ('b',), ('c',)]
    assert held.cancelled() and queued_later.cancelled()


@pytest.mark.asyncio
async def test_a_full_queue_is_a_defect() -> None:
    batcher = FinalizeBatcher(
        _ScriptedWrite(), max_queued_rows=2, max_flushers=STEADY_ONLY
    )
    first = asyncio.create_task(batcher.complete(_command('a')))
    second = asyncio.create_task(batcher.complete(_command('b')))
    third = asyncio.create_task(batcher.complete(_command('c')))

    # The three submit in one loop turn, before a flusher takes a row.
    with pytest.raises(AssertionError, match='its bound'):
        await third
    assert [_ok_task_id(result) for result in await asyncio.gather(first, second)] == [
        'a',
        'b',
    ]


# ---------------------------------------------------------------------------
# Close
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_writes_the_queued_rows_first() -> None:
    write = _ScriptedWrite(gated=True)
    batcher = FinalizeBatcher(
        write, max_queued_rows=QUEUE_BOUND, max_flushers=STEADY_ONLY
    )
    calls = [asyncio.create_task(batcher.complete(_command(i))) for i in 'abc']
    await _settle()

    closing = asyncio.create_task(batcher.close(OUTER_BOUND_S))
    await _settle()
    assert not closing.done()
    write.release()

    assert await asyncio.wait_for(closing, OUTER_BOUND_S) == FlushersDrained()
    assert [_ok_task_id(call.result()) for call in calls] == ['a', 'b', 'c']
    assert write.batches == [('a', 'b', 'c')]


@pytest.mark.asyncio
async def test_close_cancels_the_flushers_at_its_timeout() -> None:
    write = _ScriptedWrite(gated=True)
    batcher = FinalizeBatcher(
        write, max_queued_rows=QUEUE_BOUND, max_flushers=STEADY_ONLY
    )
    calls = [await _submit_alone(batcher, task_id) for task_id in 'abcd']

    report = await asyncio.wait_for(batcher.close(0.05), OUTER_BOUND_S)

    assert report == FlushersCancelled(rows_in_flight=2, rows_unsent=2)
    _ = await asyncio.gather(*calls, return_exceptions=True)
    assert all(call.cancelled() for call in calls)


@pytest.mark.asyncio
async def test_a_closed_batcher_refuses_rows() -> None:
    batcher = FinalizeBatcher(
        _ScriptedWrite(), max_queued_rows=QUEUE_BOUND, max_flushers=STEADY_ONLY
    )

    assert await batcher.close(OUTER_BOUND_S) == FlushersDrained()
    assert batcher.state is BatcherState.CLOSED
    with pytest.raises(AssertionError, match='after the finalize batcher closed'):
        await batcher.complete(_command('a'))
    assert await batcher.close(OUTER_BOUND_S) == FlushersDrained()


# ---------------------------------------------------------------------------
# The batch type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize('size', [0, FUSED_BATCH_MAX_ROWS + 1], ids=['empty', 'over'])
def test_a_batch_refuses_a_size_out_of_its_bound(size: int) -> None:
    with pytest.raises(ValueError, match='holds 1 to'):
        _ = FusedCompletionBatch(
            commands=tuple(_command(f't{index}') for index in range(size)),
        )


def test_a_batch_refuses_a_task_id_twice() -> None:
    with pytest.raises(ValueError, match='twice'):
        _ = FusedCompletionBatch(commands=(_command('a'), _command('a')))


def test_a_batch_takes_one_to_the_max_rows() -> None:
    for size in (1, FUSED_BATCH_MAX_ROWS):
        batch = FusedCompletionBatch(
            commands=tuple(_command(f't{index}') for index in range(size)),
        )
        assert len(batch.commands) == size


# ---------------------------------------------------------------------------
# Surge flushers
# ---------------------------------------------------------------------------


async def _submit_together(
    batcher: FinalizeBatcher,
    task_ids: list[str],
) -> list[asyncio.Task[BatchRowResult]]:
    """Submit rows in one loop turn, then let the flushers take them."""
    calls = [asyncio.create_task(batcher.complete(_command(i))) for i in task_ids]
    await _settle()
    return calls


@pytest.mark.asyncio
async def test_a_surge_flusher_starts_only_when_a_full_batch_waits() -> None:
    write = _ScriptedWrite(gated=True)
    batcher = FinalizeBatcher(write, max_queued_rows=QUEUE_BOUND, max_flushers=4)
    held = [await _submit_alone(batcher, task_id) for task_id in ('a', 'b')]
    almost = await _submit_together(
        batcher,
        [f'w{index}' for index in range(FUSED_BATCH_MAX_ROWS - 1)],
    )

    assert write.in_flight == FINALIZE_BATCH_FLUSHERS

    full = await _submit_together(batcher, ['last'])

    assert write.in_flight == FINALIZE_BATCH_FLUSHERS + 1
    assert [len(batch) for batch in write.batches] == [1, 1, FUSED_BATCH_MAX_ROWS]
    write.release()
    _ = await asyncio.wait_for(asyncio.gather(*held, *almost, *full), OUTER_BOUND_S)


@pytest.mark.asyncio
async def test_a_surge_flusher_writes_full_batches_only() -> None:
    """With the steady flushers busy, 70 rows wait: the surge flusher writes
    64 of them and ends; a steady flusher writes the other 6."""
    write = _ScriptedWrite(gated=True)
    batcher = FinalizeBatcher(write, max_queued_rows=QUEUE_BOUND, max_flushers=3)
    held = [await _submit_alone(batcher, task_id) for task_id in ('a', 'b')]
    waiting = await _submit_together(batcher, [f'w{index}' for index in range(70)])

    assert [len(batch) for batch in write.batches] == [1, 1, FUSED_BATCH_MAX_ROWS]
    write.release()
    _ = await asyncio.wait_for(asyncio.gather(*held, *waiting), OUTER_BOUND_S)

    assert sorted(len(batch) for batch in write.batches) == [
        1,
        1,
        70 - FUSED_BATCH_MAX_ROWS,
        FUSED_BATCH_MAX_ROWS,
    ]
    assert write.most_in_flight == 3


@pytest.mark.asyncio
async def test_the_flusher_bound_holds_for_both_roles() -> None:
    write = _ScriptedWrite(gated=True)
    batcher = FinalizeBatcher(write, max_queued_rows=QUEUE_BOUND, max_flushers=3)
    held = [await _submit_alone(batcher, task_id) for task_id in ('a', 'b')]
    waiting = await _submit_together(
        batcher,
        [f'w{index}' for index in range(4 * FUSED_BATCH_MAX_ROWS)],
    )

    assert write.in_flight == 3
    write.release()
    _ = await asyncio.wait_for(asyncio.gather(*held, *waiting), OUTER_BOUND_S)
    assert write.most_in_flight == 3


@pytest.mark.asyncio
async def test_a_pool_of_one_connection_has_one_flusher() -> None:
    write = _ScriptedWrite(gated=True)
    batcher = FinalizeBatcher(write, max_queued_rows=QUEUE_BOUND, max_flushers=1)
    held = [await _submit_alone(batcher, task_id) for task_id in ('a', 'b')]
    waiting = await _submit_together(
        batcher,
        [f'w{index}' for index in range(2 * FUSED_BATCH_MAX_ROWS)],
    )

    assert write.in_flight == 1
    write.release()
    _ = await asyncio.wait_for(asyncio.gather(*held, *waiting), OUTER_BOUND_S)
    assert write.most_in_flight == 1


def test_the_flusher_bound_must_be_positive() -> None:
    with pytest.raises(ValueError, match='max_flushers'):
        _ = FinalizeBatcher(
            _ScriptedWrite(), max_queued_rows=QUEUE_BOUND, max_flushers=0
        )


@pytest.mark.parametrize('pool', [1, 5, 20])
def test_the_worker_bounds_its_flushers_by_its_pool(pool: int) -> None:
    worker = Worker(
        session_factory=MagicMock(),
        listener=MagicMock(),
        cfg=WorkerConfig(
            dsn='postgresql+psycopg://u:p@localhost/db',
            psycopg_dsn='postgresql://u:p@localhost/db',
            queues=['default'],
            parent_pool_size=pool,
        ),
    )
    batcher = worker._finalize_batcher  # pyright: ignore[reportPrivateUsage]

    assert batcher.max_flushers == pool
