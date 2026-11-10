"""The mark of a run, and the live work that carries it, on a real event loop."""

from __future__ import annotations

import asyncio
import contextvars
import threading
from collections.abc import Coroutine
from typing import Any

import pytest

from horsies.core.worker.run_mark import (
    RUN_MARK,
    UNMARKED,
    MarkedWork,
    RunMark,
    ScanWhen,
    TrackedExecutor,
    mark_of,
)
from horsies.core.worker import run_mark

pytestmark = [pytest.mark.unit]

WAIT_S = 5.0
MARK = RunMark(dispatch=1, task_id='task-1')
OTHER = RunMark(dispatch=2, task_id='task-2')


def _marked(mark: RunMark) -> contextvars.Context:
    context = contextvars.copy_context()
    _ = context.run(RUN_MARK.set, mark)
    return context


def _run(scenario: Coroutine[Any, Any, None]) -> None:
    asyncio.run(asyncio.wait_for(scenario, WAIT_S))


class TestTheMark:
    def test_a_task_that_a_run_starts_carries_its_mark(self) -> None:
        async def scenario() -> None:
            loop = asyncio.get_running_loop()

            async def run_body() -> asyncio.Task[None]:
                return loop.create_task(asyncio.sleep(0))

            child = await loop.create_task(run_body(), context=_marked(MARK))
            assert mark_of(child) == MARK
            await child

        _run(scenario())

    def test_a_task_outside_every_run_carries_no_mark(self) -> None:
        async def scenario() -> None:
            task = asyncio.get_running_loop().create_task(asyncio.sleep(0))
            assert mark_of(task) == UNMARKED
            await task

        _run(scenario())


class TestMarkedWork:
    def test_the_factory_and_the_scan_find_every_live_task_of_a_run(self) -> None:
        async def scenario() -> None:
            loop = asyncio.get_running_loop()
            marked = MarkedWork(loop, lambda _mark: None)
            marked.install()
            marked.open(MARK)
            marked.open(OTHER)
            gate = asyncio.Event()

            async def run_body() -> tuple[asyncio.Task[Any], asyncio.Task[Any]]:
                through_factory = loop.create_task(gate.wait())
                built_directly = asyncio.Task(gate.wait(), loop=loop)
                return through_factory, built_directly

            own = loop.create_task(run_body(), context=_marked(MARK))
            through_factory, built_directly = await own
            neighbour = loop.create_task(gate.wait(), context=_marked(OTHER))

            alive = marked.alive(MARK, own, ScanWhen.ALWAYS)
            assert alive.tasks == frozenset({through_factory, built_directly})
            assert alive.executor_items == 0
            assert marked.alive(OTHER, neighbour, ScanWhen.ALWAYS).tasks == frozenset()
            gate.set()
            await asyncio.gather(through_factory, built_directly, neighbour)
            assert marked.alive(MARK, own, ScanWhen.ALWAYS).is_empty()

        _run(scenario())

    def test_executor_items_count_until_they_end_and_notify_the_loop(self) -> None:
        async def scenario() -> None:
            loop = asyncio.get_running_loop()
            ended: list[RunMark] = []
            marked = MarkedWork(loop, ended.append)
            marked.install()
            marked.open(MARK)
            release = threading.Event()
            started = threading.Event()

            def sync_call() -> None:
                started.set()
                release.wait(WAIT_S)

            async def run_body() -> asyncio.Future[None]:
                return loop.run_in_executor(None, sync_call)

            own = loop.create_task(run_body(), context=_marked(MARK))
            item = await own
            await asyncio.to_thread(started.wait, WAIT_S)
            assert marked.alive(MARK, own, ScanWhen.ALWAYS).executor_items == 1
            release.set()
            await item
            while not ended:
                await asyncio.sleep(0.001)
            assert ended == [MARK]
            assert marked.alive(MARK, own, ScanWhen.ALWAYS).executor_items == 0

        _run(scenario())

    def test_the_default_executor_is_tracked(self) -> None:
        async def scenario() -> None:
            loop = asyncio.get_running_loop()
            marked = MarkedWork(loop, lambda _mark: None)
            marked.install()
            assert isinstance(loop._default_executor, TrackedExecutor)  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType]

        _run(scenario())

    def test_work_of_a_closed_run_is_not_recorded(self) -> None:
        async def scenario() -> None:
            loop = asyncio.get_running_loop()
            marked = MarkedWork(loop, lambda _mark: None)
            marked.install()
            marked.open(MARK)
            marked.close(MARK)
            task = loop.create_task(asyncio.sleep(0), context=_marked(MARK))
            await task
            marked.open(MARK)
            assert marked.alive(MARK, task, ScanWhen.ALWAYS).is_empty()

        _run(scenario())

    def test_a_factory_set_before_keeps_building_tasks(self) -> None:
        built: list[asyncio.Task[Any]] = []

        async def scenario() -> None:
            loop = asyncio.get_running_loop()

            def factory(
                factory_loop: asyncio.AbstractEventLoop,
                coro: Coroutine[Any, Any, Any],
                **kwargs: Any,
            ) -> asyncio.Task[Any]:
                task = asyncio.Task(coro, loop=factory_loop, **kwargs)
                built.append(task)
                return task

            loop.set_task_factory(factory)
            marked = MarkedWork(loop, lambda _mark: None)
            marked.install()
            marked.open(MARK)
            gate = asyncio.Event()
            own = loop.create_task(gate.wait(), context=_marked(MARK))
            assert own in built
            # The task of the run itself is not its leftover work.
            assert marked.alive(MARK, own, ScanWhen.ALWAYS).is_empty()
            gate.set()
            await own

        _run(scenario())


class TestTheScan:
    """The record of the factory spares the scan of the end check."""

    def test_the_end_check_scans_only_when_no_recorded_task_is_alive(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        scans: list[int] = []
        all_tasks = asyncio.all_tasks

        def counted(
            loop: asyncio.AbstractEventLoop | None = None,
        ) -> set[asyncio.Task[Any]]:
            scans.append(1)
            return all_tasks(loop)

        monkeypatch.setattr(run_mark.asyncio, 'all_tasks', counted)

        async def scenario() -> None:
            loop = asyncio.get_running_loop()
            marked = MarkedWork(loop, lambda _mark: None)
            marked.install()
            marked.open(MARK)
            gate = asyncio.Event()
            own = loop.create_task(asyncio.sleep(0), context=_marked(MARK))
            left = loop.create_task(gate.wait(), context=_marked(MARK))
            await own
            scans.clear()

            alive = marked.alive(MARK, own, ScanWhen.RECORD_EMPTY)

            assert alive.tasks == frozenset({left})
            assert scans == []
            gate.set()
            await left
            assert marked.alive(MARK, own, ScanWhen.RECORD_EMPTY).is_empty()
            assert scans == [1]

        _run(scenario())

    def test_a_cancel_round_scans_each_time(self) -> None:
        """A recorded live task does not stop the scan that finds a task
        built directly."""

        async def scenario() -> None:
            loop = asyncio.get_running_loop()
            marked = MarkedWork(loop, lambda _mark: None)
            marked.install()
            marked.open(MARK)
            gate = asyncio.Event()

            async def run_body() -> tuple[asyncio.Task[Any], asyncio.Task[Any]]:
                return loop.create_task(gate.wait()), asyncio.Task(
                    gate.wait(), loop=loop
                )

            own = loop.create_task(run_body(), context=_marked(MARK))
            recorded, direct = await own

            assert marked.alive(MARK, own, ScanWhen.RECORD_EMPTY).tasks == frozenset(
                {recorded},
            )
            assert marked.alive(MARK, own, ScanWhen.ALWAYS).tasks == frozenset(
                {recorded, direct},
            )
            gate.set()
            await asyncio.gather(recorded, direct)

        _run(scenario())
