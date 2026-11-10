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
  ``loop.getaddrinfo``.

Not seen: threads and processes that a task starts itself, and work in an
executor that the task created.

The record of the factory spares the scan: the end check of a run scans
only when no recorded task of the run is alive (``ScanWhen.RECORD_EMPTY``),
since a run with a live recorded task has not ended. The cancel rounds scan
each time (``ScanWhen.ALWAYS``): they must reach every live task of the run.

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
    """When ``MarkedWork.alive`` scans every live task of the loop."""

    RECORD_EMPTY = 'record_empty'
    """Only when no recorded task of the run is alive: for the end check."""

    ALWAYS = 'always'
    """At each call: for the cancel rounds and the stuck check."""


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


class TrackedExecutor(concurrent.futures.ThreadPoolExecutor):
    """The default executor of a shared async child: a ``ThreadPoolExecutor``
    with the asyncio defaults that records each work item submitted with a
    mark, from the context of the caller."""

    def __init__(self, marked: MarkedWork) -> None:
        super().__init__(thread_name_prefix='asyncio')
        self._marked = marked

    def submit(
        self,
        fn: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> concurrent.futures.Future[T]:
        future = super().submit(fn, *args, **kwargs)
        match RUN_MARK.get():
            case RunMark() as mark:
                self._marked.record_item(mark, future)
            case Unmarked():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        return future


class MarkedWork:
    """The live work of each open run of one shared async child, by mark.

    ``loop`` is the task loop of the child. ``on_item_done`` runs on that
    loop when a work item of a run ends. ``install`` sets the task factory
    and the default executor of the loop: call it once, before the start
    hooks and the first run. ``open`` and ``close`` bound the life of a
    run's records. Every method but ``record_item`` runs on the loop's
    thread; ``record_item`` and the end of a work item may run on any
    thread, and one lock guards the work items.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        on_item_done: Callable[[RunMark], None],
    ) -> None:
        self._loop = loop
        self._on_item_done = on_item_done
        self._tasks: dict[RunMark, weakref.WeakSet[asyncio.Task[Any]]] = {}
        self._items: dict[RunMark, set[concurrent.futures.Future[Any]]] = {}
        self._items_lock = threading.Lock()

    def install(self) -> None:
        """Set the task factory and the default executor of the loop. A
        factory that was set before keeps building the tasks."""
        self._loop.set_task_factory(
            _MarkingTaskFactory(self._loop.get_task_factory(), self._record_task),
        )
        self._loop.set_default_executor(TrackedExecutor(self))

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
        the task of the run. ``scan`` decides whether one pass over every
        live task of the loop runs; that pass records the marked tasks that
        the factory did not see."""
        recorded = self._tasks[mark]
        live = {task for task in recorded if not task.done() and task is not own_task}
        match scan:
            case ScanWhen.RECORD_EMPTY if live:
                pass
            case ScanWhen.RECORD_EMPTY | ScanWhen.ALWAYS:
                for task in asyncio.all_tasks(self._loop):
                    if task is not own_task and mark_of(task) == mark:
                        recorded.add(task)
                        live.add(task)
            case _ as unreachable:
                assert_never(unreachable)
        tasks = frozenset(live)
        with self._items_lock:
            executor_items = len(self._items[mark])
        return AliveWork(tasks=tasks, executor_items=executor_items)

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
