"""Group commit of the plain-success writes of one worker.

A finalizer whose task returned a plain success hands its ``CompleteTaskFused``
to ``FinalizeBatcher.complete`` and waits for the outcome of its own row. The
batcher writes the queued rows with one statement per batch
(``apply_fused_batch_in_autocommit``): one round trip for up to
``FUSED_BATCH_MAX_ROWS`` rows.

Flush rule, with no timer:

- A submit starts a steady flusher when fewer than ``FINALIZE_BATCH_FLUSHERS``
  run. A steady flusher takes every queued row, up to ``FUSED_BATCH_MAX_ROWS``,
  writes them, and takes again. It ends when the queue is empty.
- A submit that leaves a full batch (``FUSED_BATCH_MAX_ROWS`` rows) waiting
  starts a surge flusher, while fewer than ``max_flushers`` flushers run in
  all. A surge flusher writes full batches only, and ends when less than a full
  batch waits.

Each steady flusher writes on one of the batcher's steady runners for as long
as it runs; the worker gives each a held connection (``HeldConnection``). A
surge flusher writes on the surge runner, a checkout per statement.

So a lone row is written at once, and the rows that arrive while the steady
flushers write form the next batch. A worker frees a seat only after the
commit of its row, so a timer would hold every seat longer. More steady
flushers would make each batch smaller and add commits; a surge flusher adds a
statement only when the batch cannot grow, so it never makes a batch smaller.

An answer resolves only after the statement of its batch returned: an
``Applied`` row is committed when its finalizer sees it, and the finalizer ends
(and frees its seat) only after that.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Final, assert_never

from horsies.core.lifecycle.commands import CompleteTaskFused
from horsies.core.lifecycle.outcomes import TerminalizationOutcome
from horsies.core.lifecycle.persistence import (
    FUSED_BATCH_MAX_ROWS,
    BatchCommitUnknown,
    BatchRolledBack,
    BatchWriteError,
    FusedCompletionBatch,
)
from horsies.core.logging import get_logger
from horsies.core.types.result import Err, Ok, Result
from horsies.core.utils.db import StatementRefused, StatementRunner

logger = get_logger('worker')

FINALIZE_BATCH_FLUSHERS: Final = 2
"""The steady flushers: they write whenever rows wait. Each one holds one
connection of the worker pool while it writes."""

FINALIZE_BATCH_CLOSE_TIMEOUT_S: Final = 10.0
"""How long ``FinalizeBatcher.close`` waits for the flushers to write the
queued rows before it cancels them."""

type BatchRowResult = Result[TerminalizationOutcome, BatchWriteError]
"""The answer for one row: its outcome, or the error of the write that held
it."""

type FusedBatchWrite = Callable[
    [FusedCompletionBatch, StatementRunner],
    Awaitable[Result[tuple[TerminalizationOutcome, ...], BatchWriteError]],
]
"""Writes one batch on a runner: ``apply_fused_batch``."""


class FlusherRole(Enum):
    """STEADY writes whenever rows wait. SURGE writes only while a full batch
    waits."""

    STEADY = 'STEADY'
    SURGE = 'SURGE'


class BatcherState(Enum):
    """OPEN takes rows. CLOSED refuses them: the worker stopped."""

    OPEN = 'OPEN'
    CLOSED = 'CLOSED'


@dataclass(frozen=True, slots=True)
class FlushersDrained:
    """``close`` saw every queued row written."""


@dataclass(frozen=True, slots=True)
class FlushersCancelled:
    """``close`` cancelled the flushers at its timeout.

    ``rows_in_flight`` were in a statement that the cancel ended: their commit
    is unknown. ``rows_unsent`` were never sent: they stay RUNNING, and stale
    RUNNING recovery fails them.
    """

    rows_in_flight: int
    rows_unsent: int


type CloseReport = FlushersDrained | FlushersCancelled


@dataclass(frozen=True, slots=True)
class _QueuedRow:
    command: CompleteTaskFused
    answer: asyncio.Future[BatchRowResult]


def _answer(row: _QueuedRow, result: BatchRowResult) -> None:
    """Resolve the answer of ``row``, unless its finalizer left (cancel)."""
    if not row.answer.done():
        row.answer.set_result(result)


def _fail(row: _QueuedRow, error: Exception) -> None:
    """Fail the answer of ``row`` with a defect, unless its finalizer left."""
    if not row.answer.done():
        row.answer.set_exception(error)


class FinalizeBatcher:
    """Group commit of plain-success writes; see the module docstring.

    Each row in flight submits once, and its finalizer waits for the answer
    before anything else, so the queue never holds more rows than the worker
    has in flight. ``max_queued_rows`` bounds it; a full queue is a defect.

    ``max_flushers`` bounds the flushers of both roles together: the size of
    the worker pool. The steady flushers are ``FINALIZE_BATCH_FLUSHERS``, or
    ``max_flushers`` when it is smaller. Each running steady flusher holds one
    of ``steady_runners``; every surge flusher writes on ``surge_runner``.

    Raises:
        ValueError: a bound is not positive, or ``steady_runners`` holds fewer
            runners than the steady flushers.
    """

    def __init__(
        self,
        write: FusedBatchWrite,
        *,
        steady_runners: Sequence[StatementRunner],
        surge_runner: StatementRunner,
        max_queued_rows: int,
        max_flushers: int,
    ) -> None:
        if max_queued_rows < 1:
            raise ValueError(f'max_queued_rows must be positive, got {max_queued_rows}')
        if max_flushers < 1:
            raise ValueError(f'max_flushers must be positive, got {max_flushers}')
        self._write = write
        self._max_queued_rows = max_queued_rows
        self._steady_bound = min(FINALIZE_BATCH_FLUSHERS, max_flushers)
        self._surge_bound = max_flushers - self._steady_bound
        if len(steady_runners) < self._steady_bound:
            raise ValueError(
                f'{self._steady_bound} steady flushers need as many runners, '
                f'got {len(steady_runners)}'
            )
        # A steady flusher takes a runner when it starts and gives it back in
        # its last synchronous step, with its count.
        self._free_steady_runners: list[StatementRunner] = list(
            steady_runners[: self._steady_bound],
        )
        self._surge_runner = surge_runner
        self._queue: deque[_QueuedRow] = deque()
        # The counts decide whether a submit starts a flusher. A flusher
        # lowers its count in its last synchronous step, so a submit never
        # sees a flusher that already ended. The set is for ``close``.
        self._running: dict[FlusherRole, int] = dict.fromkeys(FlusherRole, 0)
        self._flushers: set[asyncio.Task[None]] = set()
        self._rows_in_flight = 0
        self._state = BatcherState.OPEN

    @property
    def state(self) -> BatcherState:
        return self._state

    @property
    def max_flushers(self) -> int:
        """The most flushers of both roles that run at once."""
        return self._steady_bound + self._surge_bound

    async def complete(self, command: CompleteTaskFused) -> BatchRowResult:
        """Queue ``command`` and return the answer for its row.

        Outcomes:
            Ok(outcome): the statement of the row's batch returned this
                outcome; ``Applied`` is committed.
            Err(BatchRolledBack): the row did not change.
            Err(BatchCommitUnknown): the row's commit is unknown.

        A cancel of the caller leaves the row queued or in flight: its batch
        is still written, and nobody reads the answer.

        Raises:
            AssertionError: the batcher is closed, or the queue is full.
                Both are defects of the caller.
            Exception: a defect of the batch write (a row that breaks the
                contract of the statement), raised to every row of it.
        """
        answer: asyncio.Future[BatchRowResult] = (
            asyncio.get_running_loop().create_future()
        )
        self._enqueue(_QueuedRow(command=command, answer=answer))
        return await answer

    def _enqueue(self, row: _QueuedRow) -> None:
        """Queue ``row`` and start the flusher that the flush rule asks for."""
        match self._state:
            case BatcherState.OPEN:
                pass
            case BatcherState.CLOSED:
                raise AssertionError(
                    f'task {row.command.task_id}: a plain-success write after '
                    'the finalize batcher closed'
                )
            case _ as unreachable:
                assert_never(unreachable)
        if len(self._queue) >= self._max_queued_rows:
            raise AssertionError(
                f'task {row.command.task_id}: the finalize batch queue holds '
                f'{len(self._queue)} rows, its bound; each row in flight '
                'submits once'
            )
        self._queue.append(row)
        if self._running[FlusherRole.STEADY] < self._steady_bound:
            self._start_flusher(FlusherRole.STEADY)
        elif (
            self._running[FlusherRole.SURGE] < self._surge_bound
            and len(self._queue) >= FUSED_BATCH_MAX_ROWS
        ):
            self._start_flusher(FlusherRole.SURGE)

    def _start_flusher(self, role: FlusherRole) -> None:
        match role:
            case FlusherRole.STEADY:
                runner = self._free_steady_runners.pop()
            case FlusherRole.SURGE:
                runner = self._surge_runner
            case _ as unreachable:
                assert_never(unreachable)
        self._running[role] += 1
        flusher = asyncio.create_task(
            self._flush(role, runner),
            name=f'finalize-batch-flusher-{role.value.lower()}',
        )
        self._flushers.add(flusher)
        flusher.add_done_callback(self._flusher_ended)

    async def _flush(self, role: FlusherRole, runner: StatementRunner) -> None:
        """Write batches on ``runner`` while the flush rule of ``role`` finds
        one.

        Each round takes at most ``FUSED_BATCH_MAX_ROWS`` rows and answers
        every one of them; the queue holds at most ``max_queued_rows``.
        """
        try:
            while rows := self._take_batch(role):
                self._rows_in_flight += len(rows)
                try:
                    await self._write_and_answer(rows, runner)
                finally:
                    self._rows_in_flight -= len(rows)
        finally:
            self._running[role] -= 1
            match role:
                case FlusherRole.STEADY:
                    self._free_steady_runners.append(runner)
                case FlusherRole.SURGE:
                    pass
                case _ as unreachable:
                    assert_never(unreachable)

    def _take_batch(self, role: FlusherRole) -> tuple[_QueuedRow, ...]:
        """Take the queued rows, oldest first, up to ``FUSED_BATCH_MAX_ROWS``.

        A surge flusher takes nothing when less than a full batch waits. A row
        whose task id the batch already holds stays queued, first in line, for
        the next batch: the statement refuses a repeated id.
        """
        match role:
            case FlusherRole.STEADY:
                pass
            case FlusherRole.SURGE:
                if len(self._queue) < FUSED_BATCH_MAX_ROWS:
                    return ()
            case _ as unreachable:
                assert_never(unreachable)
        taken: list[_QueuedRow] = []
        task_ids: set[str] = set()
        skipped: list[_QueuedRow] = []
        while self._queue and len(taken) < FUSED_BATCH_MAX_ROWS:
            row = self._queue.popleft()
            if row.command.task_id in task_ids:
                skipped.append(row)
                continue
            task_ids.add(row.command.task_id)
            taken.append(row)
        self._queue.extendleft(reversed(skipped))
        return tuple(taken)

    async def _write_and_answer(
        self,
        rows: tuple[_QueuedRow, ...],
        runner: StatementRunner,
    ) -> None:
        """Write ``rows`` as one batch and answer each row.

        When the server or the driver refuses a batch of several rows, one
        row may be the cause: each row is then written alone, once, and gets
        the answer of its own write. A failed checkout or a broken connection
        is not about one row: every row gets that error.
        """
        try:
            written = await self._write(_batch_of(rows), runner)
            match written:
                case Ok(outcomes):
                    for row, outcome in zip(rows, outcomes, strict=True):
                        _answer(row, Ok(outcome))
                    logger.debug('Finalize batch wrote %d rows', len(rows))
                    return
                case Err(error):
                    pass
            match error:
                case BatchRolledBack(cause=StatementRefused() as refused) if (
                    len(rows) > 1
                ):
                    logger.warning(
                        'Finalize batch of %d rows refused (%s: %s); writing '
                        'each row alone',
                        len(rows),
                        type(refused.error).__name__,
                        str(refused.error)[:500],
                    )
                    for row in rows:
                        _answer(row, await self._write_alone(row, runner))
                case BatchRolledBack() | BatchCommitUnknown():
                    for row in rows:
                        _answer(row, Err(error))
                case _ as unreachable:
                    assert_never(unreachable)
        except asyncio.CancelledError:
            for row in rows:
                _ = row.answer.cancel()
            raise
        except Exception as exc:
            logger.error(
                'Finalize batch write of %d rows failed with a defect: %r',
                len(rows),
                exc,
            )
            for row in rows:
                _fail(row, exc)

    async def _write_alone(
        self,
        row: _QueuedRow,
        runner: StatementRunner,
    ) -> BatchRowResult:
        """Write ``row`` as a batch of one, on ``runner``."""
        match await self._write(_batch_of((row,)), runner):
            case Ok(outcomes):
                (outcome,) = outcomes
                return Ok(outcome)
            case Err(error):
                return Err(error)

    def _flusher_ended(self, flusher: asyncio.Task[None]) -> None:
        """Forget ``flusher``. A flusher that died of a defect while no other
        runs leaves rows that no flusher would take: they fail with it."""
        self._flushers.discard(flusher)
        if flusher.cancelled():
            return
        error = flusher.exception()
        if error is None or sum(self._running.values()) > 0:
            return
        logger.critical(
            'Finalize batch flusher died with %d rows queued: %r',
            len(self._queue),
            error,
        )
        while self._queue:
            row = self._queue.popleft()
            match error:
                case Exception():
                    _fail(row, error)
                case _:
                    _ = row.answer.cancel()

    async def close(self, timeout_s: float) -> CloseReport:
        """Refuse new rows, and wait up to ``timeout_s`` for the flushers.

        A flusher writes every row that is queued at the close. Past the
        timeout the flushers are cancelled, and the rows not yet answered
        stay with the database: committed, or RUNNING for stale recovery.
        Idempotent. Never raises.
        """
        self._state = BatcherState.CLOSED
        flushers = tuple(self._flushers)
        if not flushers:
            return FlushersDrained()
        _, pending = await asyncio.wait(flushers, timeout=max(0.0, timeout_s))
        if not pending:
            return FlushersDrained()
        rows_in_flight = self._rows_in_flight
        for flusher in pending:
            _ = flusher.cancel()
        _ = await asyncio.gather(*pending, return_exceptions=True)
        rows_unsent = len(self._queue)
        while self._queue:
            _ = self._queue.popleft().answer.cancel()
        return FlushersCancelled(
            rows_in_flight=rows_in_flight,
            rows_unsent=rows_unsent,
        )


def _batch_of(rows: tuple[_QueuedRow, ...]) -> FusedCompletionBatch:
    return FusedCompletionBatch(commands=tuple(row.command for row in rows))
