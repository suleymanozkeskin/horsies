"""The mark of a run on a shared async child, and the live work that carries it.

Each run on a seat runs in a context where ``RUN_MARK`` holds its
``RunMark``. An asyncio task copies the context of the code that creates it,
so each task that a run starts, itself or through library code, carries the
mark of that run. Library work that a task causes counts as the work of that
task: for example the refill task that an asyncpg pool starts when a cancel
closed one of its connections. Work that the start hooks or the seat loop
start carries no mark.

``MarkedWork`` finds the work of a run that is still alive, in three ways:

- a task factory on the loop records each task created with a mark;
- a scan of ``asyncio.all_tasks()`` reads the mark in the context of each
  live task. It also finds a task that code built with ``asyncio.Task(...)``,
  which the factory does not see (aiohttp does this for its DNS lookups on
  Python 3.12 and 3.13);
- the default executor of the loop records each work item submitted with a
  mark: ``asyncio.to_thread``, ``loop.run_in_executor(None, ...)`` and
  ``loop.getaddrinfo``. A start hook that sets a default executor of its own
  keeps this: after the hooks, ``track_executor_after_hooks`` wraps that
  executor (``TrackedHookExecutor``).

Not seen: threads and processes that a task starts itself, work in an
executor that the task created, and ``anyio.to_thread.run_sync`` work, which
anyio runs on worker threads of its own, not in the default executor.

A scan costs one pass over every live task of the loop, so the end checks
share one: the first end check of a loop turn scans and keeps the live
tasks by mark, and every later end check of that turn reads that index. The
index is dropped at the next turn. The record of the factory spares even
that: the end check of a run reads the index only when no recorded task of
the run is alive (``ScanWhen.RECORD_EMPTY``), since a run with a live
recorded task has not ended; the return of a run reads it to name all its
left work (``ScanWhen.TURN``). The cancel rounds and the stuck check scan
each time (``ScanWhen.ALWAYS``): they must reach every live task of the run.

A task that code builds with ``asyncio.Task(...)`` after the scan of a turn,
in the same turn, is not in the index: an end check of its run in that turn
does not see it. The factory sees every task built with ``create_task``.

The records hold only live work: a task leaves its record when it is
garbage collected, and a work item when it ends. A run's records are dropped
when the run ends.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import weakref
from collections.abc import Callable, Coroutine
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final, ParamSpec, TypeVar, assert_never

T = TypeVar('T')
P = ParamSpec('P')


@dataclass(frozen=True, slots=True)
class RunMark:
    """The mark of run ``dispatch`` on this child. Dispatch numbers are
    unique in a child, so the mark names one run."""

    dispatch: int
    task_id: str


@dataclass(frozen=True, slots=True)
class Unmarked:
    """Work that belongs to no run: the start hooks, the seat loop itself."""


UNMARKED: Final = Unmarked()

RUN_MARK: Final[ContextVar[RunMark | Unmarked]] = ContextVar(
    'horsies_run_mark',
    default=UNMARKED,
)


def mark_of(task: asyncio.Task[Any]) -> RunMark | Unmarked:
    """The mark in the context of ``task``. Reads only, so any thread may
    call it."""
    match task.get_context().get(RUN_MARK, UNMARKED):
        case RunMark() as mark:
            return mark
        case _:
            return UNMARKED


class ScanWhen(Enum):
    """How ``MarkedWork.alive`` finds the live tasks that the record of the
    factory did not see."""

    RECORD_EMPTY = 'record_empty'
    """Reads the index of this loop turn, only when no recorded task of the
    run is alive: for the end check."""

    TURN = 'turn'
    """Reads the index of this loop turn at each call: for the return of a
    run, which names all the work that it left."""

    ALWAYS = 'always'
    """Scans every live task at each call, and keeps the result as the
    index of this turn: for the cancel rounds and the stuck check."""


@dataclass(frozen=True, slots=True)
class _TurnIndex:
    """The live tasks of the loop by mark, from one scan in this loop
    turn."""

    by_mark: dict[RunMark, list[asyncio.Task[Any]]]


@dataclass(frozen=True, slots=True)
class _NoTurnIndex:
    """No scan in this loop turn yet."""


@dataclass(frozen=True, slots=True)
class AliveWork:
    """The work of one run that is still alive.

    ``tasks`` holds its asyncio tasks that are not done, without the task of
    the run itself. ``executor_items`` counts its work items that still run
    in the default executor. A thread cannot be cancelled, so those items end
    only on their own.
    """

    tasks: frozenset[asyncio.Task[Any]]
    executor_items: int

    def is_empty(self) -> bool:
        return not self.tasks and self.executor_items == 0


class _MarkingTaskFactory:
    """The task factory of the loop: builds each task as before, then
    records it when it carries the mark of an open run."""

    def __init__(
        self,
        previous: asyncio.events._TaskFactory | None,  # pyright: ignore[reportPrivateUsage]
        record: Callable[[asyncio.Task[Any]], None],
    ) -> None:
        self._previous = previous
        self._record = record

    def __call__(
        self,
        loop: asyncio.AbstractEventLoop,
        coro: Coroutine[Any, Any, T],
        /,
        **kwargs: Any,
    ) -> asyncio.Future[T]:
        match self._previous:
            case None:
                built: asyncio.Future[T] = asyncio.Task(coro, loop=loop, **kwargs)
            case previous:
                built = previous(loop, coro, **kwargs)
        match built:
            case asyncio.Task():
                self._record(built)
            case _:
                pass
        return built


def _record_if_marked(
    marked: MarkedWork,
    future: concurrent.futures.Future[Any],
) -> None:
    """Record ``future`` under the mark in the context of the caller, if
    any."""
    match RUN_MARK.get():
        case RunMark() as mark:
            marked.record_item(mark, future)
        case Unmarked():
            pass
        case _ as unreachable:
            assert_never(unreachable)


class TrackedExecutor(concurrent.futures.ThreadPoolExecutor):
    """The default executor of a shared async child: a ``ThreadPoolExecutor``
    of ``max_workers`` threads that records each work item submitted with a
    mark, from the context of the caller. ``max_workers`` is 1 or more."""

    def __init__(self, marked: MarkedWork, max_workers: int) -> None:
        super().__init__(max_workers=max_workers, thread_name_prefix='asyncio')
        self._marked = marked

    def submit(
        self,
        fn: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> concurrent.futures.Future[T]:
        future = super().submit(fn, *args, **kwargs)
        _record_if_marked(self._marked, future)
        return future


class TrackedHookExecutor(concurrent.futures.ThreadPoolExecutor):
    """The default executor that a start hook set, wrapped: each work item
    runs in ``inner``, and an item submitted with a mark is recorded. The
    loop takes only a ``ThreadPoolExecutor`` as its default executor, so the
    wrapper is one; it starts no thread of its own."""

    def __init__(
        self,
        marked: MarkedWork,
        inner: concurrent.futures.ThreadPoolExecutor,
    ) -> None:
        super().__init__(max_workers=1, thread_name_prefix='asyncio-unused')
        self._marked = marked
        self._inner = inner

    def submit(
        self,
        fn: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> concurrent.futures.Future[T]:
        future = self._inner.submit(fn, *args, **kwargs)
        _record_if_marked(self._marked, future)
        return future

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        self._inner.shutdown(wait=wait, cancel_futures=cancel_futures)
        super().shutdown(wait=wait, cancel_futures=cancel_futures)


@dataclass(frozen=True, slots=True)
class ExecutorKept:
    """The default executor of the loop is still the one ``install`` set."""


@dataclass(frozen=True, slots=True)
class HookExecutorTracked:
    """A start hook set a default executor of its own, of type
    ``executor_type``. It is wrapped in ``TrackedHookExecutor`` and stays
    the executor that runs the work."""

    executor_type: str


@dataclass(frozen=True, slots=True)
class ExecutorNotThreadPool:
    """The default executor of the loop is not a ``ThreadPoolExecutor``
    (``executor_type``; ``NoneType`` when it was removed). The public API of
    the loop refuses such a value, so code set the attribute of the loop
    directly. Its work cannot be tracked."""

    executor_type: str


type ExecutorAfterHooks = ExecutorKept | HookExecutorTracked | ExecutorNotThreadPool
"""The default executor of the loop after the start hooks."""


class MarkedWork:
    """The live work of each open run of one shared async child, by mark.

    ``loop`` is the task loop of the child. ``on_item_done`` runs on that
    loop when a work item of a run ends. ``executor_threads`` is the size of
    the default executor that ``install`` sets. ``install`` sets the task
    factory and the default executor of the loop: call it once, before the
    start hooks and the first run. ``open`` and ``close`` bound the life of a
    run's records. Every method but ``record_item`` runs on the loop's
    thread; ``record_item`` and the end of a work item may run on any
    thread, and one lock guards the work items.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        on_item_done: Callable[[RunMark], None],
        executor_threads: int,
    ) -> None:
        self._loop = loop
        self._on_item_done = on_item_done
        # The default executor that records work items. It starts no thread
        # before its first submit.
        self._executor: concurrent.futures.ThreadPoolExecutor = TrackedExecutor(
            self,
            executor_threads,
        )
        self._tasks: dict[RunMark, weakref.WeakSet[asyncio.Task[Any]]] = {}
        self._items: dict[RunMark, set[concurrent.futures.Future[Any]]] = {}
        self._items_lock = threading.Lock()
        self._index: _TurnIndex | _NoTurnIndex = _NoTurnIndex()

    def install(self) -> None:
        """Set the task factory and the default executor of the loop. A
        factory that was set before keeps building the tasks."""
        self._loop.set_task_factory(
            _MarkingTaskFactory(self._loop.get_task_factory(), self._record_task),
        )
        self._loop.set_default_executor(self._executor)

    def track_executor_after_hooks(self) -> ExecutorAfterHooks:
        """Keep the work items of each run recorded after the start hooks.

        A hook that set a default executor of its own (for a size other
        than ``executor_threads``) replaced the one of ``install``: that
        executor is wrapped (``TrackedHookExecutor``) and set again, and it
        keeps its own size. Reads the default executor from the attribute
        of the loop that
        ``set_default_executor`` writes (asyncio's ``BaseEventLoop``). Call
        it once, after the hooks and before the first run.
        """
        current: object = getattr(self._loop, '_default_executor', None)
        match current:
            case _ if current is self._executor:
                return ExecutorKept()
            case concurrent.futures.ThreadPoolExecutor():
                self._executor = TrackedHookExecutor(self, current)
                self._loop.set_default_executor(self._executor)
                return HookExecutorTracked(executor_type=type(current).__qualname__)
            case _:
                return ExecutorNotThreadPool(executor_type=type(current).__qualname__)

    def open(self, mark: RunMark) -> None:
        """Start the records of a new run. A mark in use is a defect."""
        assert mark not in self._tasks, f'run {mark.dispatch} is open twice'
        self._tasks[mark] = weakref.WeakSet()
        with self._items_lock:
            self._items[mark] = set()

    def close(self, mark: RunMark) -> None:
        """Drop the records of a run that ended. Work that carries its mark
        later is not recorded."""
        del self._tasks[mark]
        with self._items_lock:
            del self._items[mark]

    def alive(
        self,
        mark: RunMark,
        own_task: asyncio.Task[Any],
        scan: ScanWhen,
    ) -> AliveWork:
        """The work of an open run that is still alive, without ``own_task``,
        the task of the run. ``scan`` decides whether the index of this
        turn, or a new scan, adds the marked tasks that the factory did not
        see; they are recorded then. At most one scan runs per loop turn
        for the end checks, and one per call for ``ScanWhen.ALWAYS``."""
        recorded = self._tasks[mark]
        live = {task for task in recorded if not task.done() and task is not own_task}
        found: list[asyncio.Task[Any]]
        match scan:
            case ScanWhen.RECORD_EMPTY if live:
                found = []
            case ScanWhen.RECORD_EMPTY | ScanWhen.TURN:
                found = self._index_of_turn().get(mark, [])
            case ScanWhen.ALWAYS:
                found = self._scan_into_index().get(mark, [])
            case _ as unreachable:
                assert_never(unreachable)
        for task in found:
            if task is not own_task and not task.done():
                recorded.add(task)
                live.add(task)
        tasks = frozenset(live)
        with self._items_lock:
            executor_items = len(self._items[mark])
        return AliveWork(tasks=tasks, executor_items=executor_items)

    def _index_of_turn(self) -> dict[RunMark, list[asyncio.Task[Any]]]:
        """The live tasks by mark from the scan of this loop turn; the first
        call of a turn scans."""
        match self._index:
            case _TurnIndex(by_mark=by_mark):
                return by_mark
            case _NoTurnIndex():
                return self._scan_into_index()
            case _ as unreachable:
                assert_never(unreachable)

    def _scan_into_index(self) -> dict[RunMark, list[asyncio.Task[Any]]]:
        """Scan every live task of the loop once, keep the marked ones by
        mark as the index of this turn, and drop that index at the next
        turn."""
        by_mark: dict[RunMark, list[asyncio.Task[Any]]] = {}
        for task in asyncio.all_tasks(self._loop):
            match mark_of(task):
                case RunMark() as mark:
                    by_mark.setdefault(mark, []).append(task)
                case Unmarked():
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
        match self._index:
            case _NoTurnIndex():
                _ = self._loop.call_soon(self._drop_index)
            case _TurnIndex():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        self._index = _TurnIndex(by_mark)
        return by_mark

    def _drop_index(self) -> None:
        """The loop turned: the next end check scans again."""
        self._index = _NoTurnIndex()

    def _record_task(self, task: asyncio.Task[Any]) -> None:
        match mark_of(task):
            case RunMark() as mark:
                recorded = self._tasks.get(mark)
                if recorded is not None and not task.done():
                    recorded.add(task)
            case Unmarked():
                pass
            case _ as unreachable:
                assert_never(unreachable)

    def record_item(
        self, mark: RunMark, future: concurrent.futures.Future[Any]
    ) -> None:
        """Record a work item of run ``mark``, until it ends. An item of a
        run that is not open is not recorded."""
        with self._items_lock:
            items = self._items.get(mark)
            if items is None:
                return
            items.add(future)
        future.add_done_callback(lambda done: self._item_done(mark, done))

    def _item_done(self, mark: RunMark, future: concurrent.futures.Future[Any]) -> None:
        """A work item ended, on any thread. Tell the loop."""
        with self._items_lock:
            items = self._items.get(mark)
            if items is not None:
                items.discard(future)
        try:
            self._loop.call_soon_threadsafe(self._on_item_done, mark)
        except RuntimeError:
            # The loop closed: the child is past its stop, and no run waits.
            return
