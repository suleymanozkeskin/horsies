"""The watchdog of the event loop of a shared async child, on a real loop.

The block limit is cut to 300 ms here, so a test blocks the loop for less
than one second.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import contextvars
import ctypes
import threading
import time

import pytest

from horsies.core.worker import loop_watchdog
from horsies.core.worker.child_protocol import (
    BlockingRun,
    CulpritUnknown,
    LoopBlockedFrame,
    LoopCulprit,
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


def _busy(seconds: float) -> None:
    """Hold the loop with Python code, which lets other threads take the
    GIL between steps."""
    ends = time.perf_counter() + seconds
    while time.perf_counter() < ends:
        pass


def _marked(dispatch: int) -> contextvars.Context:
    context = contextvars.copy_context()
    _ = context.run(
        RUN_MARK.set, RunMark(dispatch=dispatch, task_id=f'task-{dispatch}')
    )
    return context


def test_many_short_steps_in_one_turn_name_no_culprit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """25 runs become ready in one turn, and each holds the loop for 60 ms:
    the turn lasts 1.5 s, past the limit of 1 s, while no run held the loop
    for more than 60 ms. The samples see different runs, so no run is named."""
    monkeypatch.setattr(loop_watchdog, 'LOOP_BLOCK_LIMIT_MS', 1_000)
    reports: list[LoopBlockedFrame] = []

    async def step(answer: asyncio.Event) -> None:
        await answer.wait()
        _busy(0.060)

    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        watchdog = LoopWatchdog(loop, reports.append)
        assert watchdog.start().is_ok()
        answer = asyncio.Event()
        runs = [
            loop.create_task(step(answer), context=_marked(dispatch))
            for dispatch in range(1, 26)
        ]
        await asyncio.sleep(0.3)
        loop.call_soon(answer.set)
        await asyncio.gather(*runs)
        watchdog.stop()

    asyncio.run(scenario())

    assert [report.culprit for report in reports] == [CulpritUnknown()]


def test_a_call_that_holds_the_gil_and_returns_names_its_run() -> None:
    """The C call holds the GIL for 1 s, past the limit, so the watching
    thread takes no sample during it. When the call returns, the same run
    still holds the loop: the watching thread gets the GIL, samples that
    run, and names it."""
    reports: list[LoopBlockedFrame] = []
    reported = threading.Event()

    def report(frame: LoopBlockedFrame) -> None:
        reports.append(frame)
        reported.set()

    async def holds_the_gil() -> None:
        ctypes.PyDLL(None).sleep(1)
        _busy(0.2)

    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        watchdog = LoopWatchdog(loop, report)
        assert watchdog.start().is_ok()
        await asyncio.sleep(0.15)
        await loop.create_task(holds_the_gil(), context=_marked(7))
        await asyncio.to_thread(reported.wait, WAIT_S)
        watchdog.stop()

    asyncio.run(scenario())

    assert [report.culprit for report in reports] == [
        BlockingRun(dispatch=7, task_id='task-7'),
    ]
    assert reports[0].blocked_ms >= 1_000


def test_samples_of_an_earlier_turn_do_not_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run 1 holds the loop for 0.35 s, below the limit of 0.6 s, and is
    sampled. The loop turns. Run 2 then holds it past the limit: only the
    samples since that turn count, so run 2 is named."""
    monkeypatch.setattr(loop_watchdog, 'LOOP_BLOCK_LIMIT_MS', 600)
    reports: list[LoopBlockedFrame] = []
    reported = threading.Event()

    def report(frame: LoopBlockedFrame) -> None:
        reports.append(frame)
        reported.set()

    async def hold(seconds: float) -> None:
        _busy(seconds)

    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        watchdog = LoopWatchdog(loop, report)
        assert watchdog.start().is_ok()
        await asyncio.sleep(0.15)
        await loop.create_task(hold(0.35), context=_marked(1))
        await asyncio.sleep(0.25)
        await loop.create_task(hold(1.0), context=_marked(2))
        await asyncio.to_thread(reported.wait, WAIT_S)
        watchdog.stop()

    asyncio.run(scenario())

    assert [report.culprit for report in reports] == [
        BlockingRun(dispatch=2, task_id='task-2'),
    ]


class TestTheSamplesOfABlock:
    """The pure rules: a run is named only when every sample saw its task."""

    def _task(
        self, loop: asyncio.AbstractEventLoop, dispatch: int
    ) -> asyncio.Task[None]:
        return loop.create_task(asyncio.sleep(0), context=_marked(dispatch))

    def test_one_task_in_every_sample_is_named(self) -> None:
        async def scenario() -> LoopCulprit:
            task = self._task(asyncio.get_running_loop(), 4)
            samples = loop_watchdog._with_sample(loop_watchdog._NoSample(), task)
            samples = loop_watchdog._with_sample(samples, task)
            await task
            return loop_watchdog.culprit_of(samples)

        assert asyncio.run(scenario()) == BlockingRun(dispatch=4, task_id='task-4')

    def test_two_tasks_name_no_culprit(self) -> None:
        async def scenario() -> LoopCulprit:
            loop = asyncio.get_running_loop()
            first, second = self._task(loop, 4), self._task(loop, 5)
            samples = loop_watchdog._with_sample(loop_watchdog._NoSample(), first)
            samples = loop_watchdog._with_sample(samples, second)
            samples = loop_watchdog._with_sample(samples, first)
            await asyncio.gather(first, second)
            return loop_watchdog.culprit_of(samples)

        assert asyncio.run(scenario()) == CulpritUnknown()

    def test_a_sample_with_no_task_names_no_culprit(self) -> None:
        async def scenario() -> LoopCulprit:
            task = self._task(asyncio.get_running_loop(), 4)
            samples = loop_watchdog._with_sample(loop_watchdog._NoSample(), task)
            samples = loop_watchdog._with_sample(samples, None)
            await task
            return loop_watchdog.culprit_of(samples)

        assert asyncio.run(scenario()) == CulpritUnknown()

    def test_no_sample_names_no_culprit(self) -> None:
        assert loop_watchdog.culprit_of(loop_watchdog._NoSample()) == CulpritUnknown()
