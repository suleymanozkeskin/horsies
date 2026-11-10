"""Shared async slots of the slot pool, with real child processes.

Each child runs the real seat loop of ``shared_child`` through the scripted
child program, whose seats run scripted coroutines and need no database
(``tests/fixtures/slots/scripted_child.py``).
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from horsies.core.models.boot_failure import ChildPoolSetupFailed, ResourceShortage
from horsies.core.models.tasks import MIN_CANCEL_GRACE_MS, MIN_TASK_TIMEOUT_MS
from horsies.core.models.worker_profile import (
    MemoryLimit,
    NoTaskLimit,
    SharedAsyncChildren,
    SharedRecycle,
    TaskLimit,
)
from horsies.core.types.result import Err, Ok
from horsies.core.worker.child_protocol import (
    BlockingRun,
    DeadlineFiredFrame,
    LoopBlockedFrame,
    RunStuckFrame,
    SecondCancelFrame,
    InitFrame,
    IsolatedChild,
    RssMb,
    RssUnreadable,
    SeatRefusal,
    SeatTask,
    SeatTimedOut,
    SharedResultFrame,
)
from horsies.core.worker.live_dispatches import RegistryClosed
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.slots import (
    CHILD_KILL_WAIT_S,
    Assigned,
    BootAttemptsExhausted,
    ChildKilledPastDeadlines,
    ChildEnded,
    ChildHookFailed,
    ChildLost,
    CloseReason,
    Closing,
    DeadlineAcknowledged,
    DeadlineNotReported,
    DeadlineReport,
    DeadlineReported,
    SecondCancelReported,
    Down,
    KilledBySignal,
    NoSeatFree,
    NotDispatched,
    Open,
    OpenChildLoad,
    SeatDeadlineEnded,
    SeatKilled,
    SeatRuns,
    SeatTaskFinished,
    SharedAnswered,
    StuckReported,
    SharedHandle,
    SharedOutcome,
    SharedRunRequest,
    SlotPool,
    SlotPoolConfig,
    _SeatRun,
    place_on_shared_child,
    shared_recycle_reason,
)
from horsies.core.worker.thread_slots import ClosedAtDeadline, RunCulprit
from horsies.core.worker.shared_stop import (
    BesideBlockedLoop,
    BlockedLoopKill,
    BlockedTheLoop,
    LoopHolderUnknown,
    RetryPlacement,
    TimedOutAtKill,
)
from horsies.core.worker.stored_timeout import TimeoutValueInvalid

SCRIPTED_CHILD = str(
    Path(__file__).parent.parent / 'fixtures' / 'slots' / 'scripted_child.py'
)
SCRIPTED_PRELOAD = str(
    Path(__file__).parent.parent / 'fixtures' / 'slots' / 'scripted_preload.py'
)
OUTCOME_TIMEOUT_S = 30.0
LONG_TIMEOUT_MS = 60_000
SHORT_TIMEOUT_MS = MIN_TASK_TIMEOUT_MS
PAST_SHORT_DEADLINE_S = SHORT_TIMEOUT_MS / 1000 + 0.5
GRACE_MS = 1_000
PoolFactory = Callable[..., Awaitable[SlotPool]]


def _init(app_locator: str = 'ok', imports: tuple[str, ...] = ()) -> InitFrame:
    return InitFrame(
        app_locator=app_locator,
        imports=imports,
        sys_path_roots=(),
        parent_sys_path=tuple(sys.path),
        parent_argv=('pytest',),
        loglevel=30,
        database_url='postgresql://unused',
        connect_kwargs={},
        child_pool_min_size=0,
        child_pool_max_size=1,
        child_pool_check=False,
        process_name='test-child',
        child_kind=IsolatedChild(),
    )


def _config(
    count: int,
    seats: int,
    *,
    recycle: SharedRecycle | None = None,
    isolated: int = 0,
    init: InitFrame | None = None,
) -> SlotPoolConfig:
    return SlotPoolConfig(
        slots=isolated,
        init=init or _init(),
        max_tasks_per_child=0,
        max_memory_per_child_mb=None,
        child_entry=(SCRIPTED_CHILD,),
        child_preload=ChildPreload.NONE,
        preload_entry=(SCRIPTED_PRELOAD,),
        shared_async=SharedAsyncChildren(
            count=count,
            seats=seats,
            recycle=recycle or SharedRecycle(after_per_seat=NoTaskLimit()),
        ),
    )


_next_task = 0


def _request(
    task_name: str,
    argument: str | None = None,
    *,
    timeout_ms: int = LONG_TIMEOUT_MS,
    grace_ms: int = GRACE_MS,
) -> SharedRunRequest:
    global _next_task
    _next_task += 1
    return SharedRunRequest(
        task_id=f'shared-task-{_next_task}',
        task_name=task_name,
        args_json=None,
        kwargs_json=argument,
        worker_id='worker-1',
        is_workflow_task=False,
        claimed_at=None,
        work=SeatTask(timeout_ms=timeout_ms, cancel_grace_ms=grace_ms),
    )


async def _outcome(handle: SharedHandle) -> SharedOutcome:
    return await asyncio.wait_for(asyncio.shield(handle.outcome), OUTCOME_TIMEOUT_S)


async def _value(handle: SharedHandle) -> str:
    match await _outcome(handle):
        case SeatTaskFinished(ok=True, result_json=result_json):
            return result_json
        case other:
            raise AssertionError(other)


async def _until(predicate: Callable[[], bool], timeout_s: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        assert time.monotonic() < deadline, 'condition not reached in time'
        await asyncio.sleep(0.01)


@pytest_asyncio.fixture
async def pool_factory() -> AsyncIterator[PoolFactory]:
    pools: list[SlotPool] = []

    async def create(config: SlotPoolConfig) -> SlotPool:
        pool = SlotPool(config, pending_limit=10_000)
        match await pool.start():
            case Ok(_):
                pools.append(pool)
                return pool
            case Err(failure):
                await pool.stop(0)
                raise AssertionError(failure)

    yield create
    for pool in pools:
        await pool.stop(0)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


class TestPlacement:
    def test_the_child_with_the_fewest_tasks_takes_the_next(self) -> None:
        loads = (
            OpenChildLoad(index=2, runs=3, seats=5),
            OpenChildLoad(index=3, runs=1, seats=5),
            OpenChildLoad(index=4, runs=2, seats=5),
        )
        assert place_on_shared_child(loads) == 3

    def test_the_lowest_index_wins_among_equals(self) -> None:
        loads = (
            OpenChildLoad(index=5, runs=1, seats=2),
            OpenChildLoad(index=4, runs=1, seats=2),
        )
        assert place_on_shared_child(loads) == 4

    def test_a_full_child_takes_no_task(self) -> None:
        loads = (
            OpenChildLoad(index=0, runs=2, seats=2),
            OpenChildLoad(index=1, runs=3, seats=4),
        )
        assert place_on_shared_child(loads) == 1

    def test_no_free_seat(self) -> None:
        assert place_on_shared_child((OpenChildLoad(0, 1, 1),)) == NoSeatFree()
        assert place_on_shared_child(()) == NoSeatFree()


class TestRecycleReason:
    def _shared(self, recycle: SharedRecycle) -> SharedAsyncChildren:
        return SharedAsyncChildren(count=1, seats=4, recycle=recycle)

    def test_the_task_limit_counts_per_seat(self) -> None:
        shared = self._shared(SharedRecycle(after_per_seat=TaskLimit(tasks=3)))
        assert shared_recycle_reason(shared, 11, RssUnreadable()) is None
        assert (
            shared_recycle_reason(shared, 12, RssUnreadable()) is CloseReason.TASK_COUNT
        )

    def test_the_memory_limit_reads_the_rss_after_a_task(self) -> None:
        shared = self._shared(
            SharedRecycle(after_per_seat=NoTaskLimit(), above=MemoryLimit(mb=100)),
        )
        assert shared_recycle_reason(shared, 1, RssMb(99.9)) is None
        assert (
            shared_recycle_reason(shared, 1, RssMb(100.0)) is CloseReason.MEMORY_LIMIT
        )
        assert shared_recycle_reason(shared, 1, RssUnreadable()) is None

    def test_no_limit_never_closes(self) -> None:
        shared = self._shared(SharedRecycle(after_per_seat=NoTaskLimit()))
        assert shared_recycle_reason(shared, 10**9, RssMb(10**6)) is None


@pytest.mark.asyncio(loop_scope='function')
class TestSeats:
    async def test_one_child_runs_its_seats_at_once(
        self, pool_factory: PoolFactory
    ) -> None:
        pool = await pool_factory(_config(1, 4))
        handles = [pool.submit_shared(_request('stamp', '0.5')) for _ in range(4)]

        stamps = [(await _value(handle)).split(':') for handle in handles]

        assert len({pid for pid, _start, _end in stamps}) == 1
        assert max(float(start) for _pid, start, _end in stamps) < min(
            float(end) for _pid, _start, end in stamps
        )

    async def test_a_free_seat_takes_the_next_task_at_once(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
    ) -> None:
        pool = await pool_factory(_config(1, 2))
        first, second = tmp_path / 'first', tmp_path / 'second'
        held = [
            pool.submit_shared(_request('wait_for_file', str(first))),
            pool.submit_shared(_request('wait_for_file', str(second))),
        ]
        waiting = pool.submit_shared(_request('pid'))
        await asyncio.wait_for(asyncio.shield(held[1].assigned), OUTCOME_TIMEOUT_S)
        await asyncio.sleep(0.2)
        assert not waiting.assigned.done()

        first.touch()

        assert await _value(held[0]) == await _value(waiting)
        assert not held[1].outcome.done()
        second.touch()
        await _value(held[1])

    async def test_a_task_goes_to_the_child_with_the_fewest_tasks(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
    ) -> None:
        pool = await pool_factory(_config(2, 3))
        release = tmp_path / 'release'
        handles = [
            pool.submit_shared(_request('wait_for_file', str(release)))
            for _ in range(4)
        ]
        for handle in handles:
            await asyncio.wait_for(asyncio.shield(handle.assigned), OUTCOME_TIMEOUT_S)

        counts = [
            state.runs.count()
            for state in pool.shared_states()
            if isinstance(state, Open)
        ]
        release.touch()
        pids = [await _value(handle) for handle in handles]

        assert counts == [2, 2]
        assert pids[0] == pids[2] != pids[1] == pids[3]

    async def test_the_loop_runs_between_tasks(self, pool_factory: PoolFactory) -> None:
        """The loop of a shared child runs from its ready frame to its exit, not
        only while a task runs."""
        pool = await pool_factory(_config(1, 1))
        before = int(await _value(pool.submit_shared(_request('start_ticker'))))
        await asyncio.sleep(0.3)

        after = int(await _value(pool.submit_shared(_request('read_ticks'))))

        assert after - before >= 10

    async def test_every_task_of_a_child_is_in_the_heartbeat_registry(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
    ) -> None:
        pool = await pool_factory(_config(1, 3))
        release = tmp_path / 'release'
        handles = [
            pool.submit_shared(_request('wait_for_file', str(release)))
            for _ in range(3)
        ]
        for handle in handles:
            await asyncio.wait_for(asyncio.shield(handle.assigned), OUTCOME_TIMEOUT_S)

        held = pool.live_dispatches.snapshot()
        release.touch()
        for handle in handles:
            await _value(handle)

        assert sorted(item.task_id for item in held) == sorted(
            handle.frame.task_id for handle in handles
        )
        assert len({item.pid for item in held}) == 1
        assert pool.live_dispatches.snapshot() == ()


@pytest.mark.asyncio(loop_scope='function')
class TestRecycle:
    async def test_a_child_closes_after_its_tasks_per_seat(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(
            _config(1, 2, recycle=SharedRecycle(after_per_seat=TaskLimit(tasks=1))),
        )
        first = [await _value(pool.submit_shared(_request('pid'))) for _ in range(2)]
        later = [await _value(pool.submit_shared(_request('pid'))) for _ in range(2)]

        assert len(set(first)) == 1
        assert len(set(later)) == 1
        assert first[0] != later[0]
        await _until(lambda: not _alive(int(first[0])))

    async def test_a_closing_child_finishes_its_tasks_before_it_retires(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
    ) -> None:
        pool = await pool_factory(
            _config(1, 2, recycle=SharedRecycle(after_per_seat=TaskLimit(tasks=1))),
        )
        release = tmp_path / 'release'
        slow = pool.submit_shared(_request('wait_for_file', str(release)))
        await asyncio.wait_for(asyncio.shield(slow.assigned), OUTCOME_TIMEOUT_S)
        quick = [pool.submit_shared(_request('pid')) for _ in range(3)]
        first_pid = await _value(quick[0])
        assert await _value(quick[1]) == first_pid
        await _until(lambda: isinstance(pool.shared_states()[0], Closing))
        await asyncio.sleep(0.2)
        assert not quick[2].assigned.done()
        assert pool.shared_async_capacity() == 1

        release.touch()

        assert await _value(slow) == first_pid
        assert await _value(quick[2]) != first_pid

    async def test_a_child_above_its_memory_limit_is_replaced(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(
            _config(
                1,
                2,
                recycle=SharedRecycle(
                    after_per_seat=NoTaskLimit(), above=MemoryLimit(mb=1)
                ),
            ),
        )
        first = await _value(pool.submit_shared(_request('pid')))
        second = await _value(pool.submit_shared(_request('pid')))

        assert first != second


@pytest.mark.asyncio(loop_scope='function')
class TestDeadline:
    async def test_the_child_cancels_a_task_at_its_deadline(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 2))
        late = pool.submit_shared(_request('sleep', '30', timeout_ms=SHORT_TIMEOUT_MS))
        neighbour = pool.submit_shared(_request('sleep', '0.5'))

        assert await _outcome(late) == SeatDeadlineEnded(
            retry=RetryPlacement.SAME_CLASS,
        )
        assert isinstance(await _outcome(neighbour), SeatTaskFinished)

    async def test_a_value_after_the_deadline_is_the_timeout(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 1))
        handle = pool.submit_shared(
            _request('swallow_one_cancel', '0.2', timeout_ms=SHORT_TIMEOUT_MS)
        )

        assert await _outcome(handle) == SeatDeadlineEnded(
            retry=RetryPlacement.SAME_CLASS,
        )

    async def test_a_refusal_has_no_deadline_in_the_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """The refusal frame crosses to a real child; the run ends with its
        result, past the smallest deadline a task can have."""
        pool = await pool_factory(_config(1, 1))
        refused = replace(
            _request('sleep', str(PAST_SHORT_DEADLINE_S)),
            work=SeatRefusal(refusal=TimeoutValueInvalid(reason='y')),
        )

        assert isinstance(await _outcome(pool.submit_shared(refused)), SeatTaskFinished)

    async def test_the_backstop_kills_a_frozen_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """A loop that a task blocks cannot fire the deadline: the child sends
        no report, so the parent kills it, with every task in it."""
        pool = await pool_factory(_config(1, 2))
        frozen = pool.submit_shared(
            _request('block', '30', timeout_ms=SHORT_TIMEOUT_MS)
        )
        neighbour = pool.submit_shared(_request('sleep', '30'))
        await asyncio.wait_for(asyncio.shield(neighbour.assigned), OUTCOME_TIMEOUT_S)
        await asyncio.sleep(PAST_SHORT_DEADLINE_S)

        match await frozen.backstop_kill(CHILD_KILL_WAIT_S):
            case ChildEnded(child_exit=KilledBySignal(signal_number=number)):
                assert number == signal.SIGKILL
            case other:
                raise AssertionError(other)
        match await _outcome(neighbour):
            case ChildLost():
                pass
            case other:
                raise AssertionError(other)
        # The run past its deadline times out; no culprit is named, so its
        # retry stays in its class.
        match await _outcome(frozen):
            case SeatKilled(verdict=TimedOutAtKill(retry=RetryPlacement.SAME_CLASS)):
                pass
            case other:
                raise AssertionError(other)
        assert pool.live_dispatches.snapshot() == ()

    async def test_the_backstop_leaves_a_live_child_whose_task_is_stuck(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """The child reported the second cancel and the stuck state: its loop
        is alive. A neighbour with time left keeps the child from the kill."""
        pool = await pool_factory(_config(1, 2))
        stuck = pool.submit_shared(
            _request(
                'ignore_every_cancel',
                timeout_ms=SHORT_TIMEOUT_MS,
                grace_ms=MIN_CANCEL_GRACE_MS,
            )
        )
        neighbour = pool.submit_shared(_request('sleep', '30'))
        await asyncio.wait_for(asyncio.shield(neighbour.assigned), OUTCOME_TIMEOUT_S)
        await _until(lambda: _closed_by_a_stuck_task(pool))

        outcome = await stuck.backstop_kill(CHILD_KILL_WAIT_S)

        assert isinstance(outcome, DeadlineAcknowledged)
        assert _alive(outcome.pid)
        assert not stuck.outcome.done()

    async def test_the_backstop_kills_a_child_that_froze_after_the_deadline(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """The loop was alive at the deadline (the deadline report came) and
        froze before the end of the grace: C code holds the GIL, so neither
        the second cancel nor the watchdog can report."""
        pool = await pool_factory(_config(1, 2))
        frozen = pool.submit_shared(
            _request(
                'gil_hang_after_cancel',
                timeout_ms=SHORT_TIMEOUT_MS,
                grace_ms=MIN_CANCEL_GRACE_MS,
            )
        )
        neighbour = pool.submit_shared(_request('sleep', '30'))
        await asyncio.wait_for(asyncio.shield(neighbour.assigned), OUTCOME_TIMEOUT_S)
        await asyncio.sleep(PAST_SHORT_DEADLINE_S + 0.5)

        match await frozen.backstop_kill(CHILD_KILL_WAIT_S):
            case ChildEnded():
                pass
            case other:
                raise AssertionError(other)
        match await _outcome(frozen):
            case SeatKilled(
                verdict=TimedOutAtKill(retry=RetryPlacement.SAME_CLASS),
                child_exit=KilledBySignal(signal_number=number),
            ):
                assert number == signal.SIGKILL
            case other:
                raise AssertionError(other)
        assert isinstance(await _outcome(neighbour), ChildLost)

    async def test_the_backstop_after_the_end_kills_nothing(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 1))
        handle = pool.submit_shared(_request('pid'))
        pid = await _value(handle)

        outcome = await handle.backstop_kill(CHILD_KILL_WAIT_S)

        assert isinstance(outcome, SharedAnswered)
        assert _alive(int(pid))


def _closed_by_a_stuck_task(pool: SlotPool) -> bool:
    """Whether the one shared slot closed because a task is stuck: it
    closes at that deadline, and it is killed when no task has time left."""
    match pool.shared_states():
        case (Closing(reason=ClosedAtDeadline()),):
            return True
        case _:
            return False


@pytest.mark.asyncio(loop_scope='function')
class TestTheStopOfARun:
    async def test_a_stuck_task_closes_the_child_and_is_killed_when_only_stuck_tasks_are_left(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """The neighbour has more time and finishes; only then is the child
        killed. The stuck task times out, and its retry runs isolated."""
        pool = await pool_factory(_config(1, 2))
        neighbour = pool.submit_shared(_request('sleep', '2.5'))
        await asyncio.wait_for(asyncio.shield(neighbour.assigned), OUTCOME_TIMEOUT_S)
        stuck = pool.submit_shared(
            _request(
                'ignore_every_cancel',
                timeout_ms=SHORT_TIMEOUT_MS,
                grace_ms=MIN_CANCEL_GRACE_MS,
            )
        )
        await _until(lambda: _closed_by_a_stuck_task(pool))
        assert pool.shared_async_capacity() == 2
        assert not stuck.outcome.done()

        neighbour_pid = await _value(neighbour)
        stuck_outcome = await _outcome(stuck)

        # The stuck task is the culprit of its own stop: its retry runs
        # isolated (``culprit_retry``).
        assert stuck_outcome == ChildKilledPastDeadlines(
            pid=int(neighbour_pid),
            child_exit=KilledBySignal(signal.SIGKILL),
            culprit=RunCulprit(
                dispatch=stuck.frame.dispatch,
                task_id=stuck.frame.task_id,
            ),
        )
        replacement = await _value(pool.submit_shared(_request('pid')))
        assert int(replacement) != int(neighbour_pid)

    async def test_a_task_that_needed_the_second_cancel_closes_its_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """The second cancel ends the task: it times out with no kill, and the
        child is replaced after it."""
        pool = await pool_factory(_config(1, 1))
        first_pid = await _value(pool.submit_shared(_request('pid')))
        handle = pool.submit_shared(
            _request(
                'suppress_then_work',
                '30',
                timeout_ms=SHORT_TIMEOUT_MS,
                grace_ms=MIN_CANCEL_GRACE_MS,
            )
        )

        assert await _outcome(handle) == SeatDeadlineEnded(
            retry=RetryPlacement.SAME_CLASS,
        )
        replacement = await _value(pool.submit_shared(_request('pid')))
        assert replacement != first_pid
        assert not _alive(int(first_pid))

    async def test_an_executor_thread_that_runs_on_is_stuck(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """A cancel cannot stop a thread: the run is stuck, and as the only
        run of its child it is killed at once."""
        pool = await pool_factory(_config(1, 1))
        handle = pool.submit_shared(
            _request(
                'thread_outlives_cancel',
                '30',
                timeout_ms=SHORT_TIMEOUT_MS,
                grace_ms=MIN_CANCEL_GRACE_MS,
            )
        )

        match await _outcome(handle):
            case ChildKilledPastDeadlines(culprit=culprit):
                assert culprit == RunCulprit(
                    dispatch=handle.frame.dispatch,
                    task_id=handle.frame.task_id,
                )
            case other:
                raise AssertionError(other)

    @pytest.mark.parametrize('operation', ['leave_marked_task', 'leave_direct_task'])
    async def test_a_run_ends_when_the_work_it_left_ends(
        self,
        pool_factory: PoolFactory,
        operation: str,
    ) -> None:
        """``leave_direct_task`` builds its task with ``asyncio.Task(...)``,
        as aiohttp does: only the scan of the live tasks finds it."""
        pool = await pool_factory(_config(1, 1))
        started = time.monotonic()

        outcome = await _outcome(pool.submit_shared(_request(operation, '0.8')))

        assert isinstance(outcome, SeatTaskFinished)
        assert time.monotonic() - started >= 0.8


@pytest.mark.asyncio(loop_scope='function')
class TestBlockedLoop:
    async def test_the_watchdog_names_the_culprit_and_the_child_is_killed(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """The block limit is cut to 500 ms in this child. The task that
        blocks the loop gets its own verdict; its neighbour gets the
        neighbour verdict. Neither was past its deadline."""
        pool = await pool_factory(_config(1, 2, init=_init('block_limit:500')))
        neighbour = pool.submit_shared(_request('sleep', '30'))
        await asyncio.wait_for(asyncio.shield(neighbour.assigned), OUTCOME_TIMEOUT_S)
        culprit_request = _request('block', '30')
        culprit = pool.submit_shared(culprit_request)

        match await _outcome(culprit):
            case SeatKilled(verdict=BlockedTheLoop(blocked_ms=blocked_ms, where=where)):
                assert blocked_ms >= 500
                assert 'scripted_child.py' in where
            case other:
                raise AssertionError(other)
        match await _outcome(neighbour):
            case SeatKilled(verdict=BesideBlockedLoop(holder_task_id=holder)):
                assert holder == culprit_request.task_id
            case other:
                raise AssertionError(other)

    async def test_a_block_that_holds_the_gil_names_no_culprit(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """C code that holds the GIL stops the watchdog too: no report comes.
        The backstop ends the child; no task is named, so the neighbour with
        time left takes the crash path."""
        pool = await pool_factory(_config(1, 2, init=_init('block_limit:500')))
        neighbour = pool.submit_shared(_request('sleep', '30'))
        await asyncio.wait_for(asyncio.shield(neighbour.assigned), OUTCOME_TIMEOUT_S)
        frozen = pool.submit_shared(_request('gil_hang', timeout_ms=SHORT_TIMEOUT_MS))
        await asyncio.wait_for(asyncio.shield(frozen.assigned), OUTCOME_TIMEOUT_S)
        await asyncio.sleep(1.5)
        assert not neighbour.outcome.done()

        match await frozen.backstop_kill(CHILD_KILL_WAIT_S):
            case ChildEnded():
                pass
            case other:
                raise AssertionError(other)
        assert isinstance(await _outcome(neighbour), ChildLost)
        match await _outcome(frozen):
            case SeatKilled(verdict=TimedOutAtKill(retry=RetryPlacement.SAME_CLASS)):
                pass
            case other:
                raise AssertionError(other)

    async def test_a_frozen_child_that_holds_only_a_refusal_is_ended(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """A refusal has no deadline; its own backstop ends the frozen child,
        and the refusal takes the crash path."""
        pool = await pool_factory(_config(1, 1))
        refusal = pool.submit_shared(
            replace(
                _request('gil_hang'),
                work=SeatRefusal(refusal=TimeoutValueInvalid(reason='y')),
            )
        )
        await asyncio.wait_for(asyncio.shield(refusal.assigned), OUTCOME_TIMEOUT_S)
        await asyncio.sleep(0.5)

        match await refusal.backstop_kill(CHILD_KILL_WAIT_S):
            case ChildEnded():
                pass
            case other:
                raise AssertionError(other)
        assert isinstance(await _outcome(refusal), ChildLost)


@pytest.mark.asyncio(loop_scope='function')
class TestLoss:
    async def test_a_crash_ends_every_task_of_the_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 4))
        neighbours = [pool.submit_shared(_request('sleep', '30')) for _ in range(3)]
        crasher = pool.submit_shared(_request('kill_self', '0.2'))

        outcomes = [await _outcome(handle) for handle in (*neighbours, crasher)]

        for outcome in outcomes:
            match outcome:
                case ChildLost(child_exit=KilledBySignal(signal_number=number)):
                    assert number == signal.SIGKILL
                case other:
                    raise AssertionError(other)
        assert (
            len({outcome.pid for outcome in outcomes if isinstance(outcome, ChildLost)})
            == 1
        )
        replacement = await _value(pool.submit_shared(_request('pid')))
        assert int(replacement) != outcomes[0].pid  # type: ignore[union-attr]

    async def test_a_task_that_escapes_the_seat_runner_ends_the_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 2))
        neighbour = pool.submit_shared(_request('sleep', '30'))
        await asyncio.wait_for(asyncio.shield(neighbour.assigned), OUTCOME_TIMEOUT_S)
        defect = pool.submit_shared(_request('raises'))

        for handle in (defect, neighbour):
            assert isinstance(await _outcome(handle), ChildLost)


@pytest.mark.asyncio(loop_scope='function')
class TestCapacity:
    async def test_open_children_count_every_seat(
        self, pool_factory: PoolFactory
    ) -> None:
        pool = await pool_factory(_config(2, 5))
        assert pool.shared_async_capacity() == 10

    async def test_a_down_child_counts_nothing(self, pool_factory: PoolFactory) -> None:
        """The shared slots come after the isolated ones: index 1 here."""
        pool = await pool_factory(
            _config(2, 5, isolated=1, init=_init('short_on_slot:1')),
        )
        await _until(lambda: isinstance(pool.shared_states()[0], Down))

        state = pool.shared_states()[0]
        assert isinstance(state, Down)
        assert isinstance(state.cause, ResourceShortage)
        assert pool.shared_async_capacity() == 5
        assert pool.slots_in_service() == 1


@pytest.mark.asyncio(loop_scope='function')
class TestStartAndStop:
    async def test_a_seat_start_that_fails_is_a_boot_failure(self) -> None:
        pool = SlotPool(_config(1, 2, init=_init('seat_start_fails')), pending_limit=10)
        try:
            outcome = await asyncio.wait_for(pool.start(), OUTCOME_TIMEOUT_S)
        finally:
            await pool.stop(0)
        match outcome:
            case Err(BootAttemptsExhausted(cause=cause)):
                assert cause == ChildPoolSetupFailed()
            case other:
                raise AssertionError(other)

    async def test_a_failing_async_start_hook_stops_the_pool(self) -> None:
        pool = SlotPool(_config(1, 2, init=_init('seat_hook_fails')), pending_limit=10)
        try:
            outcome = await asyncio.wait_for(pool.start(), OUTCOME_TIMEOUT_S)
        finally:
            await pool.stop(0)
        assert outcome == Err(ChildHookFailed())

    async def test_stop_lets_shared_tasks_finish(
        self, pool_factory: PoolFactory
    ) -> None:
        pool = await pool_factory(_config(1, 2))
        handle = pool.submit_shared(_request('sleep', '0.3'))
        await asyncio.wait_for(asyncio.shield(handle.assigned), OUTCOME_TIMEOUT_S)

        report = await pool.stop(10.0)

        assert report.killed_task_ids == ()
        assert isinstance(await _outcome(handle), SeatTaskFinished)

    async def test_stop_kills_a_shared_child_past_the_drain(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 2))
        handles = [pool.submit_shared(_request('sleep', '30')) for _ in range(2)]
        for handle in handles:
            await asyncio.wait_for(asyncio.shield(handle.assigned), OUTCOME_TIMEOUT_S)

        report = await pool.stop(0.3)

        assert sorted(report.killed_task_ids) == sorted(
            handle.frame.task_id for handle in handles
        )
        for handle in handles:
            assert isinstance(await _outcome(handle), ChildLost)

    async def test_a_task_after_stop_is_not_dispatched(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 1))
        await pool.stop(1.0)

        handle = pool.submit_shared(_request('pid'))

        match await _outcome(handle):
            case NotDispatched():
                pass
            case other:
                raise AssertionError(other)
        assert isinstance(handle.assigned.result(), NotDispatched)

    async def test_isolated_and_shared_slots_serve_side_by_side(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        from horsies.core.worker.child_protocol import RunFrame
        from horsies.core.worker.slots import TaskFinished
        from horsies.core.worker.stored_timeout import NoTimeout

        pool = await pool_factory(_config(1, 2, isolated=1))
        isolated = pool.submit(
            RunFrame(
                task_id='isolated-1',
                task_name='pid',
                args_json=None,
                kwargs_json=None,
                worker_id='worker-1',
                is_workflow_task=False,
                timeout=NoTimeout(),
                claimed_at=None,
            ),
        )
        shared = pool.submit_shared(_request('pid'))

        isolated_outcome = await asyncio.wait_for(
            asyncio.shield(isolated.outcome),
            OUTCOME_TIMEOUT_S,
        )
        assert isinstance(isolated_outcome, TaskFinished)
        assert isolated_outcome.result_json != await _value(shared)
        assert isinstance(isolated.assigned.result(), Assigned)


class TestRefusalReports:
    """A run without a deadline cannot report one: such a frame is a protocol
    violation, and the run stays in flight for the loss of the child."""

    def _refused_run(self) -> tuple[SlotPool, SeatRuns]:
        pool = SlotPool(_config(1, 1), pending_limit=1)
        request = replace(
            _request('echo'),
            work=SeatRefusal(refusal=TimeoutValueInvalid(reason='y')),
        )
        runs = SeatRuns(1)
        handle = SharedHandle(request.frame_for(1), asyncio.get_running_loop())
        runs.add(
            1,
            _SeatRun(handle, RegistryClosed(), DeadlineNotReported(), time.monotonic()),
        )
        return pool, runs

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_timed_out_end_of_a_refusal_is_refused(self) -> None:
        pool, runs = self._refused_run()
        frame = SharedResultFrame(
            dispatch=1,
            task_id=runs.task_ids()[0],
            end=SeatTimedOut(),
            rss=RssMb(1.0),
        )

        assert pool._end_run(runs, frame) is False
        assert runs.count() == 1

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_deadline_report_of_a_refusal_is_refused(self) -> None:
        pool, runs = self._refused_run()
        frame = DeadlineFiredFrame(dispatch=1, task_id=runs.task_ids()[0])

        assert pool._note_deadline(MagicMock(), runs, frame) is False
        assert runs.get(1) is not None
        assert runs.get(1).deadline == DeadlineNotReported()  # type: ignore[union-attr]


class TestStopReportOrder:
    """The child reports the stop of a run in order: the deadline, the second
    cancel, the stuck state. A report out of that order, or about a run that
    is not in flight, is a protocol violation."""

    def _run_at(self, deadline: DeadlineReport) -> tuple[SlotPool, SeatRuns, MagicMock]:
        pool = SlotPool(_config(1, 2), pending_limit=1)
        runs = SeatRuns(2)
        handle = SharedHandle(_request('echo').frame_for(1), asyncio.get_running_loop())
        runs.add(1, _SeatRun(handle, RegistryClosed(), deadline, time.monotonic()))
        slot = pool._shared_slots[0]
        slot.state = Open(runs)
        child = MagicMock()
        child.killed_by_parent = False
        return pool, runs, child

    @pytest.mark.asyncio(loop_scope='function')
    @pytest.mark.parametrize(
        ('deadline', 'report'),
        [
            (DeadlineNotReported(), SecondCancelFrame(1, '')),
            (DeadlineReported(), RunStuckFrame(1, '', 1, 0, ('x',))),
            (StuckReported(), SecondCancelFrame(1, '')),
        ],
    )
    async def test_a_report_out_of_order_is_refused(
        self,
        deadline: DeadlineReport,
        report: SecondCancelFrame | RunStuckFrame,
    ) -> None:
        pool, runs, child = self._run_at(deadline)
        frame = replace(report, task_id=runs.task_ids()[0])

        slot = pool._shared_slots[0]
        assert pool._note_stop_report(slot, child, runs, frame) is False
        assert runs.get(1).deadline == deadline  # type: ignore[union-attr]
        child.kill_for_caller.assert_not_called()

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_report_of_another_task_is_refused(self) -> None:
        pool, runs, child = self._run_at(DeadlineReported())

        refused = pool._note_stop_report(
            pool._shared_slots[0],
            child,
            runs,
            SecondCancelFrame(dispatch=1, task_id='another task'),
        )

        assert refused is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_second_cancel_closes_the_child_and_kills_nothing(self) -> None:
        pool, runs, child = self._run_at(DeadlineReported())
        frame = SecondCancelFrame(dispatch=1, task_id=runs.task_ids()[0])

        assert pool._note_stop_report(pool._shared_slots[0], child, runs, frame)
        assert pool.shared_states() == (Closing(CloseReason.SECOND_CANCEL, runs),)
        child.kill_for_caller.assert_not_called()

    @pytest.mark.asyncio(loop_scope='function')
    @pytest.mark.parametrize(
        ('deadline', 'retry'),
        [
            (DeadlineReported(), RetryPlacement.SAME_CLASS),
            (SecondCancelReported(), RetryPlacement.SAME_CLASS),
            (StuckReported(), RetryPlacement.ISOLATED),
        ],
    )
    async def test_a_run_that_ends_after_its_deadline_retries_by_its_stop(
        self,
        deadline: DeadlineReport,
        retry: RetryPlacement,
    ) -> None:
        """A stuck run that ends with no kill was still named stuck: its
        retry runs isolated."""
        pool, runs, _child = self._run_at(deadline)
        handle = runs.get(1).handle  # type: ignore[union-attr]
        frame = SharedResultFrame(
            dispatch=1,
            task_id=runs.task_ids()[0],
            end=SeatTimedOut(),
            rss=RssMb(1.0),
        )

        assert pool._end_run(runs, frame) is True
        assert handle.outcome.result() == SeatDeadlineEnded(retry=retry)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_blocked_loop_report_of_a_finished_run_names_no_culprit(
        self,
    ) -> None:
        """The run that held the loop already ended: the kill names no task."""
        pool, runs, child = self._run_at(DeadlineNotReported())
        frame = LoopBlockedFrame(
            culprit=BlockingRun(dispatch=9, task_id='gone'),
            blocked_ms=5000,
            where='x',
        )

        assert pool._kill_for_blocked_loop(pool._shared_slots[0], child, runs, frame)
        assert runs.kill == BlockedLoopKill(LoopHolderUnknown(), 5000, 'x')
        child.kill_for_caller.assert_called_once()


IDLE_CPU_PROBE_S = 1.0
IDLE_CPU_LIMIT_S = 0.3


@pytest.mark.asyncio(loop_scope='function')
async def test_a_production_shared_child_waits_for_frames_without_spinning() -> None:
    """The real child program runs as ``__main__``. A frame type that it
    builds there must match the one that the seat loop checks, or the loop
    spins and never takes a frame. The pool opens without a connection, so
    no database is needed."""
    import psutil

    from horsies.core.worker.slots import CHILD_ENTRY

    init = replace(
        _init('tests.e2e.tasks.instance:app'),
        parent_sys_path=(str(Path(__file__).parent.parent.parent), *sys.path),
        database_url='postgresql://unused@127.0.0.1:1/unused',
    )
    config = replace(_config(1, 2, init=init), child_entry=CHILD_ENTRY)
    pool = SlotPool(config, pending_limit=10)
    try:
        match await asyncio.wait_for(pool.start(), OUTCOME_TIMEOUT_S):
            case Ok(_):
                pass
            case Err(failure):
                raise AssertionError(failure)
        child = pool._shared_slots[0].child
        assert child is not None
        process = psutil.Process(child.pid)
        before = sum(process.cpu_times()[:2])
        await asyncio.sleep(IDLE_CPU_PROBE_S)
        used = sum(process.cpu_times()[:2]) - before
    finally:
        await pool.stop(1.0)

    assert used < IDLE_CPU_LIMIT_S
