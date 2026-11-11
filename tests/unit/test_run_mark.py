"""The mark of a run, and the live work that carries it, on a real event loop."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import threading
from collections.abc import Coroutine
from typing import Any

import pytest

from horsies.core.worker.run_mark import (
    RUN_MARK,
    UNMARKED,
    ExecutorKept,
    ExecutorNotThreadPool,
    HookExecutorTracked,
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


class TestTheExecutorAfterHooks:
    """``track_executor_after_hooks``: a start hook that sets its own
    default executor keeps the work of each run tracked."""

    def test_the_executor_of_install_is_kept(self) -> None:
        async def scenario() -> None:
            loop = asyncio.get_running_loop()
            marked = MarkedWork(loop, lambda _mark: None)
            marked.install()

            assert marked.track_executor_after_hooks() == ExecutorKept()

        _run(scenario())

    def test_the_executor_of_a_hook_is_wrapped_and_runs_the_work(self) -> None:
        async def scenario() -> None:
            loop = asyncio.get_running_loop()
            marked = MarkedWork(loop, lambda _mark: None)
            marked.install()
            # An on_shared_async_child_start hook that wants more threads.
            hook_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=4,
                thread_name_prefix='hook-pool',
            )
            loop.set_default_executor(hook_pool)

            tracked = marked.track_executor_after_hooks()

            assert tracked == HookExecutorTracked(executor_type='ThreadPoolExecutor')
            marked.open(MARK)
            release = threading.Event()
            started = threading.Event()
            ran_on: list[str] = []

            def sync_call() -> None:
                ran_on.append(threading.current_thread().name)
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
            assert ran_on[0].startswith('hook-pool')
            while marked.alive(MARK, own, ScanWhen.ALWAYS).executor_items:
                await asyncio.sleep(0.001)

        _run(scenario())

    def test_a_default_executor_that_is_not_a_thread_pool_is_reported(self) -> None:
        async def scenario() -> None:
            loop = asyncio.get_running_loop()
            marked = MarkedWork(loop, lambda _mark: None)
            marked.install()
            loop._default_executor = None  # pyright: ignore[reportAttributeAccessIssue]

            assert marked.track_executor_after_hooks() == ExecutorNotThreadPool(
                executor_type='NoneType',
            )

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


class TestTheScanOfATurn:
    """The end checks of one loop turn share one scan of the live tasks."""

    def _counted_scans(self, monkeypatch: pytest.MonkeyPatch) -> list[int]:
        scans: list[int] = []
        all_tasks = asyncio.all_tasks

        def counted(
            loop: asyncio.AbstractEventLoop | None = None,
        ) -> set[asyncio.Task[Any]]:
            scans.append(1)
            return all_tasks(loop)

        monkeypatch.setattr(run_mark.asyncio, 'all_tasks', counted)
        return scans

    def test_the_end_checks_of_one_turn_scan_once(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """50 runs end in one turn, while 200 tasks of other runs live: the
        end checks of the 50 cost one scan, and each finds its own left
        task. The next turn scans again."""
        scans = self._counted_scans(monkeypatch)

        async def scenario() -> None:
            loop = asyncio.get_running_loop()
            marked = MarkedWork(loop, lambda _mark: None)
            marked.install()
            gate = asyncio.Event()
            others = [
                loop.create_task(gate.wait(), context=_marked(RunMark(1000 + n, 'x')))
                for n in range(200)
            ]
            runs: list[tuple[RunMark, asyncio.Task[Any], asyncio.Task[Any]]] = []
            for dispatch in range(1, 51):
                mark = RunMark(dispatch=dispatch, task_id=f'task-{dispatch}')
                marked.open(mark)

                async def run_body() -> asyncio.Task[Any]:
                    # Built directly: only a scan finds it.
                    return asyncio.Task(gate.wait(), loop=loop)

                own = loop.create_task(run_body(), context=_marked(mark))
                runs.append((mark, own, await own))
            scans.clear()

            for mark, own, left in runs:
                assert marked.alive(mark, own, ScanWhen.TURN).tasks == frozenset({left})
            for mark, own, _left in runs:
                assert not marked.alive(mark, own, ScanWhen.RECORD_EMPTY).is_empty()
            assert scans == [1]

            await asyncio.sleep(0)
            mark, own, left = runs[0]
            assert marked.alive(mark, own, ScanWhen.TURN).tasks == frozenset({left})
            assert scans == [1, 1]
            gate.set()
            await asyncio.gather(*others, *(left for _m, _o, left in runs))

        _run(scenario())

    def test_a_task_built_after_the_scan_is_found_at_the_next_turn(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        scans = self._counted_scans(monkeypatch)

        async def scenario() -> None:
            loop = asyncio.get_running_loop()
            marked = MarkedWork(loop, lambda _mark: None)
            marked.install()
            marked.open(MARK)
            gate = asyncio.Event()
            own = loop.create_task(asyncio.sleep(0), context=_marked(MARK))
            await own
            assert marked.alive(MARK, own, ScanWhen.RECORD_EMPTY).is_empty()
            direct = _marked(MARK).run(asyncio.Task, gate.wait(), loop=loop)

            await asyncio.sleep(0)

            assert marked.alive(MARK, own, ScanWhen.RECORD_EMPTY).tasks == frozenset(
                {direct},
            )
            assert len(scans) == 2
            gate.set()
            await direct

        _run(scenario())

    def test_a_cancel_round_scans_at_each_call(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        scans = self._counted_scans(monkeypatch)

        async def scenario() -> None:
            loop = asyncio.get_running_loop()
            marked = MarkedWork(loop, lambda _mark: None)
            marked.install()
            marked.open(MARK)
            own = loop.create_task(asyncio.sleep(0), context=_marked(MARK))
            await own
            scans.clear()

            for _ in range(3):
                assert marked.alive(MARK, own, ScanWhen.ALWAYS).is_empty()

            assert scans == [1, 1, 1]

        _run(scenario())
