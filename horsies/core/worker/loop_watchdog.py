"""The watchdog of the event loop of a shared async child.

Every task of a shared async child runs on one event loop. A task that blocks
it, with ``time.sleep()``, a sync client or long CPU work, stops every task of
the child, and no deadline can act during the block. A thread that is not the
loop thread watches the loop: a timer on the loop records each turn, and the
thread checks that record. When the loop has not turned for
``LOOP_BLOCK_LIMIT_MS``, the thread names the run that holds the loop and
reports it to the parent through the frame writer, before anything else. The
parent then kills the child at once.

The run is the task that the loop thread runs (``asyncio.current_task``),
read by its mark: the task of a run or a task with its mark. Code that
belongs to no run gives ``CulpritUnknown``. A C call that holds the GIL stops
this thread as well: then no report comes, and the parent's backstop timer
ends the child.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import sys
import threading
import time
from collections.abc import Callable
from typing import Final, assert_never

from horsies.core.logging import get_logger
from horsies.core.models.boot_failure import ThreadStartRefused
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.child_protocol import (
    MAX_BLOCKED_WHERE_CHARS,
    BlockingRun,
    BootFailedFrame,
    CulpritUnknown,
    LoopBlockedFrame,
    LoopCulprit,
    boot_failed_frame,
)
from horsies.core.worker.run_mark import RunMark, Unmarked, mark_of

logger = get_logger('worker')

LOOP_BLOCK_LIMIT_MS: Final = 5_000
"""The longest time the loop of a shared async child may go without a turn.

The longest common CPU burst measured on one call was 674 ms (a backtracking
``re.findall`` over 10 MB of text); a full garbage collection of about one
million objects took 258 ms; stalls with no burst stayed under 149 ms. This
limit is 7.4 times the longest burst. During a block no timer of the loop
runs, so each deadline of the child can be late by up to this limit.
"""

LOOP_TURN_TICK_S: Final = 0.1
"""How often the loop records a turn. The timer also wakes an idle loop."""

WATCHDOG_POLL_S: Final = 0.1
"""How often the watchdog thread reads the record of the last turn."""

WATCHDOG_STOP_WAIT_S: Final = 1.0
"""The longest wait for the watchdog thread at the stop of the child."""


def describe_loop_thread(thread_id: int) -> str:
    """The innermost source line that thread ``thread_id`` runs, as
    ``file:line in function``, at most ``MAX_BLOCKED_WHERE_CHARS``
    characters. Reads the frames of another thread, so the line can be one
    step past the block when the block ends during the read."""
    frame = sys._current_frames().get(thread_id)
    match frame:
        case None:
            where = 'the loop thread has no frame'
        case _:
            code = frame.f_code
            where = f'{code.co_filename}:{frame.f_lineno} in {code.co_name}'
    safe = where.encode('utf-8', 'backslashreplace').decode('utf-8')
    return safe[:MAX_BLOCKED_WHERE_CHARS]


def loop_culprit(loop: asyncio.AbstractEventLoop) -> LoopCulprit:
    """The run that the loop runs now: the task that holds the loop, read by
    its mark. Safe from another thread."""
    task = asyncio.current_task(loop)
    if task is None:
        return CulpritUnknown()
    match mark_of(task):
        case RunMark(dispatch=dispatch, task_id=task_id):
            return BlockingRun(dispatch=dispatch, task_id=task_id)
        case Unmarked():
            return CulpritUnknown()
        case _ as unreachable:
            assert_never(unreachable)


class LoopWatchdog:
    """Watches one event loop from a thread of its own.

    ``start`` and ``stop`` run on the loop's thread. ``report`` gets the one
    ``LoopBlockedFrame`` of this child, on the watchdog thread; it must not
    block (the frame writer queues the frame). After a report the thread
    ends: the parent kills the child.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        report: Callable[[LoopBlockedFrame], None],
    ) -> None:
        self._loop = loop
        self._report = report
        self._last_turn = time.monotonic()
        self._loop_thread_id = threading.get_ident()
        self._stopped = threading.Event()
        self._tick: asyncio.TimerHandle = loop.call_later(LOOP_TURN_TICK_S, self._turn)
        self._thread = threading.Thread(
            target=self._watch,
            name='horsies-loop-watchdog',
            daemon=True,
        )

    def start(self) -> Result[None, BootFailedFrame]:
        """Start the thread. ``Err``: the OS refused it (``ThreadStartRefused``)."""
        self._last_turn = time.monotonic()
        try:
            self._thread.start()
        except RuntimeError as exc:
            self._tick.cancel()
            return Err(
                boot_failed_frame(ThreadStartRefused(), type(exc).__name__, str(exc)),
            )
        return Ok(None)

    def stop(self) -> None:
        """End the timer and the thread, bounded by ``WATCHDOG_STOP_WAIT_S``."""
        self._tick.cancel()
        self._stopped.set()
        if self._thread.is_alive():
            self._thread.join(WATCHDOG_STOP_WAIT_S)

    def _turn(self) -> None:
        self._last_turn = time.monotonic()
        self._tick = self._loop.call_later(LOOP_TURN_TICK_S, self._turn)

    def _watch(self) -> None:
        while not self._stopped.wait(WATCHDOG_POLL_S):
            blocked_ms = int((time.monotonic() - self._last_turn) * 1000)
            if blocked_ms >= LOOP_BLOCK_LIMIT_MS:
                self._report_block(blocked_ms)
                return

    def _report_block(self, blocked_ms: int) -> None:
        """Name the culprit, send the report, then log it."""
        frame = LoopBlockedFrame(
            culprit=loop_culprit(self._loop),
            blocked_ms=blocked_ms,
            where=describe_loop_thread(self._loop_thread_id),
        )
        self._report(frame)
        match frame.culprit:
            case BlockingRun(dispatch=dispatch, task_id=task_id):
                culprit = f'run {dispatch} (task {task_id})'
            case CulpritUnknown():
                culprit = 'code that belongs to no task'
            case _ as unreachable:
                assert_never(unreachable)
        logger.critical(
            'The event loop of this shared async child has not turned for %s ms: '
            '%s holds it at %s; the worker ends the child',
            blocked_ms,
            culprit,
            frame.where,
        )
