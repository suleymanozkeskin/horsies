"""The watchdog of the event loop of a shared async child, on a real loop.

The block limit is cut to 300 ms here, so a test blocks the loop for less
than one second.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import contextvars
import threading
import time

import pytest

from horsies.core.worker import loop_watchdog
from horsies.core.worker.child_protocol import (
    BlockingRun,
    CulpritUnknown,
    LoopBlockedFrame,
)
from horsies.core.worker.loop_watchdog import LoopWatchdog
from horsies.core.worker.run_mark import RUN_MARK, RunMark

pytestmark = [pytest.mark.unit]

LIMIT_MS = 300
WAIT_S = 5.0


@pytest.fixture(autouse=True)
def short_block_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(loop_watchdog, 'LOOP_BLOCK_LIMIT_MS', LIMIT_MS)


def _block_the_loop() -> None:
    time.sleep(LIMIT_MS / 1000 * 2.5)


def _watch(blocker_context: contextvars.Context | None) -> list[LoopBlockedFrame]:
    """Run a watchdog, then block the loop in a task with ``blocker_context``,
    or in a plain callback when it is None."""
    reports: list[LoopBlockedFrame] = []
    reported = threading.Event()

    def report(frame: LoopBlockedFrame) -> None:
        reports.append(frame)
        reported.set()

    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        watchdog = LoopWatchdog(loop, report)
        assert watchdog.start().is_ok()

        async def blocking_task() -> None:
            _block_the_loop()

        match blocker_context:
            case None:
                loop.call_soon(_block_the_loop)
                await asyncio.sleep(0.01)
            case context:
                await loop.create_task(blocking_task(), context=context)
        await asyncio.to_thread(reported.wait, WAIT_S)
        watchdog.stop()

    asyncio.run(scenario())
    return reports


def test_a_blocked_loop_is_reported_with_the_run_that_holds_it() -> None:
    context = contextvars.copy_context()
    _ = context.run(RUN_MARK.set, RunMark(dispatch=3, task_id='task-3'))

    reports = _watch(context)

    assert len(reports) == 1
    report = reports[0]
    assert report.culprit == BlockingRun(dispatch=3, task_id='task-3')
    assert report.blocked_ms >= LIMIT_MS
    assert 'test_loop_watchdog.py' in report.where
    assert '_block_the_loop' in report.where


def test_a_task_with_no_mark_gives_no_culprit() -> None:
    reports = _watch(contextvars.Context())

    assert [report.culprit for report in reports] == [CulpritUnknown()]


def test_a_callback_that_belongs_to_no_task_gives_no_culprit() -> None:
    reports = _watch(None)

    assert [report.culprit for report in reports] == [CulpritUnknown()]


def test_a_loop_that_turns_is_never_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    """Short waits and work far below the limit keep the loop turning. The
    limit is 1 s here, ten times each 0.1 s burst, so a loaded host cannot
    stretch a turn past it."""
    monkeypatch.setattr(loop_watchdog, 'LOOP_BLOCK_LIMIT_MS', 1_000)
    reports: list[LoopBlockedFrame] = []

    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        watchdog = LoopWatchdog(loop, reports.append)
        assert watchdog.start().is_ok()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            time.sleep(0.1)
            await asyncio.sleep(0.01)
        watchdog.stop()

    asyncio.run(scenario())
    assert reports == []
