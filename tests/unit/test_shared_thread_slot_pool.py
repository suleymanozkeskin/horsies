"""Shared thread slots of the slot pool, with real child processes.

Each child runs the real seats and run table of ``thread_child`` through the
scripted child program, whose seats run scripted ``def`` calls and need no
database (``tests/fixtures/slots/scripted_child.py``).
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

import pytest
import pytest_asyncio

from horsies.core.models.boot_failure import ResourceShortage
from horsies.core.models.tasks import MIN_TASK_TIMEOUT_MS
from horsies.core.models.worker_profile import (
    ResolvedPool,
    MemoryLimit,
    NoTaskLimit,
    SharedAsyncChildren,
    SharedRecycle,
    SharedThreadChildren,
    TaskLimit,
)
from horsies.core.types.result import Err, Ok
from horsies.core.worker.child_main import BAD_PARENT_FRAME_EXIT_CODE
from horsies.core.worker.child_protocol import (
    InitFrame,
    IsolatedChild,
    SeatRefusal,
    SeatTask,
    SharedRunFrame,
    SharedThreadChild,
    ThreadSeatTask,
)
from horsies.core.worker import slots as slots_module
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.slots import (
    CHILD_KILL_WAIT_S,
    ChildEnded,
    ChildKilledPastDeadlines,
    ChildLost,
    Closing,
    DeadlineAcknowledged,
    Down,
    ExitedWith,
    Idle,
    KilledBySignal,
    NotDispatched,
    Open,
    SeatDeadlineEnded,
    SeatKilled,
    SeatTaskFinished,
    SharedHandle,
    SharedOutcome,
    SharedRunRequest,
    SlotPool,
    SlotPoolConfig,
)
from horsies.core.worker.stored_timeout import TimeoutValueInvalid
from horsies.core.worker.shared_stop import RetryPlacement, TimedOutAtKill
from horsies.core.worker.thread_slots import (
    ClosedAtDeadline,
    RunCulprit,
    ThreadRunRequest,
)

pytestmark = [pytest.mark.unit]

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
PoolFactory = Callable[..., Awaitable[SlotPool]]


def _init(app_locator: str = 'ok') -> InitFrame:
    return InitFrame(
        app_locator=app_locator,
        imports=(),
        sys_path_roots=(),
        parent_sys_path=tuple(sys.path),
        parent_argv=('pytest',),
        loglevel=30,
        database_url='postgresql://unused',
        connect_kwargs={},
        process_name='test-child',
        child_kind=IsolatedChild(
            pool=ResolvedPool(min_size=0, max_size=1, check=False),
        ),
    )


def _config(
    count: int,
    seats: int,
    *,
    recycle: SharedRecycle | None = None,
    isolated: int = 0,
    shared_async: SharedAsyncChildren | None = None,
    init: InitFrame | None = None,
) -> SlotPoolConfig:
    config = SlotPoolConfig(
        slots=isolated,
        init=init or _init(),
        max_tasks_per_child=0,
        max_memory_per_child_mb=None,
        child_entry=(SCRIPTED_CHILD,),
        child_preload=ChildPreload.NONE,
        preload_entry=(SCRIPTED_PRELOAD,),
        shared_thread=SharedThreadChildren(
            count=count,
            seats=seats,
            recycle=recycle or SharedRecycle(after_per_seat=NoTaskLimit()),
        ),
    )
    if shared_async is None:
        return config
    return replace(config, shared_async=shared_async)


_next_task = 0


def _request(
    task_name: str,
    argument: str | None = None,
    *,
    timeout_ms: int = LONG_TIMEOUT_MS,
) -> ThreadRunRequest:
    global _next_task
    _next_task += 1
    return ThreadRunRequest(
        task_id=f'thread-task-{_next_task}',
        task_name=task_name,
        args_json=None,
        kwargs_json=argument,
        worker_id='worker-1',
        is_workflow_task=False,
        claimed_at=None,
        work=ThreadSeatTask(timeout_ms=timeout_ms),
    )


async def _outcome(handle: SharedHandle) -> SharedOutcome:
    return await asyncio.wait_for(asyncio.shield(handle.outcome), OUTCOME_TIMEOUT_S)


async def _value(handle: SharedHandle) -> str:
    match await _outcome(handle):
        case SeatTaskFinished(ok=True, result_json=result_json):
            return result_json
        case other:
            raise AssertionError(other)


async def _assigned(handle: SharedHandle) -> None:
    await asyncio.wait_for(asyncio.shield(handle.assigned), OUTCOME_TIMEOUT_S)


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


def _killed_by_sigkill(outcome: SharedOutcome) -> bool:
    match outcome:
        case ChildKilledPastDeadlines(child_exit=KilledBySignal(signal_number=number)):
            return number == signal.SIGKILL
        case _:
            return False


@pytest.mark.asyncio(loop_scope='function')
class TestSeats:
    async def test_one_child_runs_its_seats_at_once(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 4))
        handles = [pool.submit_shared(_request('stamp', '0.5')) for _ in range(4)]

        stamps = [(await _value(handle)).split(':') for handle in handles]

        assert len({pid for pid, _start, _end in stamps}) == 1
        assert max(float(start) for _pid, start, _end in stamps) < min(
            float(end) for _pid, _start, end in stamps
        )

    async def test_each_task_runs_on_a_seat_thread(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
    ) -> None:
        pool = await pool_factory(_config(1, 3))
        release = tmp_path / 'release'
        held = [
            pool.submit_shared(_request('wait_for_file', str(release)))
            for _ in range(2)
        ]
        for handle in held:
            await _assigned(handle)

        name = await _value(pool.submit_shared(_request('thread_name')))
        release.touch()
        for handle in held:
            await _value(handle)

        assert name.startswith('horsies-seat-')

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
        await _assigned(held[1])
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
            await _assigned(handle)

        counts = [
            state.runs.count()
            for state in pool.thread_states()
            if isinstance(state, Open)
        ]
        release.touch()
        pids = [await _value(handle) for handle in handles]

        assert counts == [2, 2]
        assert pids[0] == pids[2] != pids[1] == pids[3]

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
            await _assigned(handle)

        held = pool.live_dispatches.snapshot()
        release.touch()
        for handle in handles:
            await _value(handle)

        assert sorted(item.task_id for item in held) == sorted(
            handle.frame.task_id for handle in handles
        )
        assert len({item.pid for item in held}) == 1
        assert pool.live_dispatches.snapshot() == ()

    async def test_each_kind_of_shared_child_takes_only_its_own_tasks(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(
            _config(1, 2, shared_async=SharedAsyncChildren(count=1, seats=2)),
        )
        async_request = SharedRunRequest(
            task_id='async-task',
            task_name='pid',
            args_json=None,
            kwargs_json=None,
            worker_id='worker-1',
            is_workflow_task=False,
            claimed_at=None,
            work=SeatTask(timeout_ms=LONG_TIMEOUT_MS, cancel_grace_ms=1_000),
        )

        async_pid = await _value(pool.submit_shared(async_request))
        thread_pid = await _value(pool.submit_shared(_request('pid')))
        thread_name = await _value(pool.submit_shared(_request('thread_name')))

        assert async_pid != thread_pid
        assert thread_name.startswith('horsies-seat-')
        assert pool.shared_async_capacity() == 2
        assert pool.shared_thread_capacity() == 2


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
    async def test_a_deadline_closes_the_child_and_a_neighbour_with_time_left_finishes(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """At the deadline the child closes; the kill waits for the neighbour,
        which still has time left and ends by itself."""
        pool = await pool_factory(_config(1, 3))
        stuck = pool.submit_shared(_request('sleep', '30', timeout_ms=SHORT_TIMEOUT_MS))
        neighbour = pool.submit_shared(_request('stamp', '2.5'))
        await _assigned(neighbour)
        await _until(lambda: isinstance(pool.thread_states()[0], Closing))
        later = pool.submit_shared(_request('pid'))

        match pool.thread_states()[0]:
            case Closing(reason=reason):
                assert reason == ClosedAtDeadline(
                    RunCulprit(stuck.frame.dispatch, stuck.frame.task_id),
                )
            case other:
                raise AssertionError(other)
        assert pool.shared_thread_capacity() == 2
        await asyncio.sleep(0.3)
        assert not later.assigned.done()
        neighbour_pid, _started, neighbour_end = (await _value(neighbour)).split(':')
        stuck_outcome = await _outcome(stuck)
        killed_at = time.time()

        assert _killed_by_sigkill(stuck_outcome)
        assert stuck_outcome == ChildKilledPastDeadlines(
            pid=int(neighbour_pid),
            child_exit=KilledBySignal(signal.SIGKILL),
            culprit=RunCulprit(stuck.frame.dispatch, stuck.frame.task_id),
        )
        assert killed_at >= float(neighbour_end)
        assert await _value(later) != neighbour_pid
        assert not _alive(int(neighbour_pid))

    async def test_the_kill_waits_for_the_last_deadline_in_the_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 2))
        submitted = time.monotonic()
        first = pool.submit_shared(_request('sleep', '30', timeout_ms=SHORT_TIMEOUT_MS))
        second = pool.submit_shared(_request('sleep', '30', timeout_ms=2_500))

        outcomes = [await _outcome(first), await _outcome(second)]
        elapsed_s = time.monotonic() - submitted

        culprit = RunCulprit(first.frame.dispatch, first.frame.task_id)
        for outcome in outcomes:
            match outcome:
                case ChildKilledPastDeadlines(culprit=named) if _killed_by_sigkill(
                    outcome
                ):
                    assert named == culprit
                case other:
                    raise AssertionError(other)
        assert elapsed_s >= 2.5

    async def test_a_task_that_ends_after_its_deadline_is_the_timeout(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """A neighbour with time left keeps the child alive, so the late task
        ends by itself; its value is dropped. No task is killed, and the
        closed child retires by its stop frame."""
        pool = await pool_factory(_config(1, 2))
        late = pool.submit_shared(
            _request(
                'late_value', str(PAST_SHORT_DEADLINE_S), timeout_ms=SHORT_TIMEOUT_MS
            ),
        )
        neighbour = pool.submit_shared(
            _request('sleep', str(PAST_SHORT_DEADLINE_S + 1))
        )
        await _assigned(neighbour)
        old_child = pool._thread_slots[0].child
        assert old_child is not None

        assert await _outcome(late) == SeatDeadlineEnded(
            retry=RetryPlacement.SAME_CLASS,
        )
        assert await _value(neighbour) == str(old_child.pid)
        next_pid = await _value(pool.submit_shared(_request('pid')))

        assert int(next_pid) != old_child.pid
        assert await old_child.wait_exit(CHILD_KILL_WAIT_S) == ExitedWith(0)

    async def test_a_task_alone_in_its_child_is_killed_at_its_deadline(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """No other task has time left, so the kill comes at the deadline."""
        pool = await pool_factory(_config(1, 2))
        submitted = time.monotonic()
        alone = pool.submit_shared(_request('sleep', '30', timeout_ms=SHORT_TIMEOUT_MS))

        outcome = await _outcome(alone)
        elapsed_s = time.monotonic() - submitted

        assert _killed_by_sigkill(outcome)
        assert SHORT_TIMEOUT_MS / 1000 <= elapsed_s < SHORT_TIMEOUT_MS / 1000 + 2.0

    async def test_a_refusal_has_no_deadline_in_the_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 1))
        refused = replace(
            _request('sleep', str(PAST_SHORT_DEADLINE_S)),
            work=SeatRefusal(refusal=TimeoutValueInvalid(reason='y')),
        )

        assert isinstance(await _outcome(pool.submit_shared(refused)), SeatTaskFinished)
        assert isinstance(pool.thread_states()[0], Open)

    async def test_the_backstop_kills_a_child_frozen_by_the_gil(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """C code that holds the GIL stops the deadline thread too: the child
        sends no report, so the parent kills it, with every task in it."""
        pool = await pool_factory(_config(1, 2))
        neighbour = pool.submit_shared(_request('sleep', '30'))
        await _assigned(neighbour)
        frozen = pool.submit_shared(_request('gil_hang', timeout_ms=SHORT_TIMEOUT_MS))
        await _assigned(frozen)
        await asyncio.sleep(PAST_SHORT_DEADLINE_S)

        match await frozen.backstop_kill(CHILD_KILL_WAIT_S):
            case ChildEnded(child_exit=KilledBySignal(signal_number=number)):
                assert number == signal.SIGKILL
            case other:
                raise AssertionError(other)
        assert isinstance(await _outcome(neighbour), ChildLost)
        assert pool.live_dispatches.snapshot() == ()

    async def test_the_backstop_leaves_a_child_that_reported_the_deadline(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 2))
        stuck = pool.submit_shared(_request('sleep', '30', timeout_ms=SHORT_TIMEOUT_MS))
        neighbour = pool.submit_shared(_request('sleep', '30'))
        await _assigned(neighbour)
        await _until(lambda: isinstance(pool.thread_states()[0], Closing))

        outcome = await stuck.backstop_kill(CHILD_KILL_WAIT_S)

        assert isinstance(outcome, DeadlineAcknowledged)
        assert _alive(outcome.pid)
        assert not stuck.outcome.done()


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

        pids: set[int] = set()
        for outcome in outcomes:
            match outcome:
                case ChildLost(
                    child_exit=KilledBySignal(signal_number=number), pid=pid
                ):
                    assert number == signal.SIGKILL
                    pids.add(pid)
                case other:
                    raise AssertionError(other)
        assert len(pids) == 1
        replacement = await _value(pool.submit_shared(_request('pid')))
        assert int(replacement) not in pids

    async def test_a_task_that_escapes_the_seat_runner_ends_the_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 2))
        neighbour = pool.submit_shared(_request('sleep', '30'))
        await _assigned(neighbour)
        defect = pool.submit_shared(_request('raises'))

        for handle in (defect, neighbour):
            match await _outcome(handle):
                case ChildLost(child_exit=ExitedWith(code=1)):
                    pass
                case other:
                    raise AssertionError(other)

    async def test_the_run_of_a_shared_async_child_ends_the_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """A run with the task work of the other kind is a parent defect."""
        pool = await pool_factory(_config(1, 2))
        neighbour = pool.submit_shared(_request('sleep', '30'))
        await _assigned(neighbour)
        child = pool._thread_slots[0].child
        assert child is not None

        child.send(
            SharedRunFrame(
                dispatch=999,
                task_id='wrong-kind',
                task_name='pid',
                args_json=None,
                kwargs_json=None,
                worker_id='worker-1',
                is_workflow_task=False,
                claimed_at=None,
                work=SeatTask(timeout_ms=LONG_TIMEOUT_MS, cancel_grace_ms=1_000),
            ),
        )

        match await _outcome(neighbour):
            case ChildLost(child_exit=ExitedWith(code=code)):
                assert code == BAD_PARENT_FRAME_EXIT_CODE
            case other:
                raise AssertionError(other)


@pytest.mark.asyncio(loop_scope='function')
class TestCapacity:
    async def test_open_children_count_every_seat(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(2, 5))
        assert pool.shared_thread_capacity() == 10
        assert pool.shared_async_capacity() == 0

    async def test_a_down_child_counts_nothing(self, pool_factory: PoolFactory) -> None:
        """The thread slots come after the isolated and the async slots."""
        pool = await pool_factory(
            _config(
                2,
                5,
                isolated=1,
                shared_async=SharedAsyncChildren(count=1, seats=3),
                init=_init('short_on_slot:2'),
            ),
        )
        await _until(lambda: isinstance(pool.thread_states()[0], Down))

        state = pool.thread_states()[0]
        assert isinstance(state, Down)
        assert isinstance(state.cause, ResourceShortage)
        assert pool.shared_thread_capacity() == 5
        assert pool.shared_async_capacity() == 3
        assert pool.slots_in_service() == 1


@pytest.mark.asyncio(loop_scope='function')
class TestNewKindBaseline:
    async def test_the_first_ready_child_of_a_kind_down_at_the_first_start_reports_its_baseline(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The thread slot is short of descriptors at the first start, so
        the worker starts on the isolated child, and the baseline guard of
        the thread kind has nothing to check. The first thread child that
        becomes ready later reports its baseline once; a replacement of the
        same kind reports nothing, and neither does a replacement of the
        isolated kind, whose baseline the first start returned."""
        monkeypatch.setattr(slots_module, 'SLOT_DOWN_FIRST_DELAY_S', 0.05)
        init = replace(
            _init(f'short_first_on_slot:{tmp_path / "marks"}'),
            imports=('1', '2'),
        )
        pool = SlotPool(_config(1, 1, isolated=1, init=init), pending_limit=10)
        try:
            match await asyncio.wait_for(pool.start(), OUTCOME_TIMEOUT_S):
                case Ok(baselines):
                    assert [baseline.child_kind for baseline in baselines] == [
                        init.child_kind,
                    ]
                case Err(failure):
                    raise AssertionError(failure)

            late = await asyncio.wait_for(
                pool.wait_new_kind_baseline(),
                OUTCOME_TIMEOUT_S,
            )

            assert late.child_kind == SharedThreadChild(
                seats=1,
                pool=ResolvedPool(min_size=0, max_size=2, check=False),
            )
            first = pool._thread_slots[0].child
            isolated = pool._slots[0].child
            assert first is not None and late.pid == first.pid
            assert isolated is not None
            os.kill(first.pid, signal.SIGKILL)
            os.kill(isolated.pid, signal.SIGKILL)
            await _until(
                lambda: pool._thread_slots[0].child not in (None, first)
                and isinstance(pool.thread_states()[0], Open)
                and pool._slots[0].child not in (None, isolated)
                and isinstance(pool.states()[0], Idle),
            )
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(pool.wait_new_kind_baseline(), 0.5)
        finally:
            await pool.stop(0)


@pytest.mark.asyncio(loop_scope='function')
class TestStop:
    async def test_stop_lets_thread_tasks_finish(
        self, pool_factory: PoolFactory
    ) -> None:
        pool = await pool_factory(_config(1, 2))
        handle = pool.submit_shared(_request('sleep', '0.3'))
        await _assigned(handle)

        report = await pool.stop(10.0)

        assert report.killed_task_ids == ()
        assert isinstance(await _outcome(handle), SeatTaskFinished)

    async def test_stop_kills_a_thread_child_past_the_drain(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 2))
        handles = [pool.submit_shared(_request('sleep', '30')) for _ in range(2)]
        for handle in handles:
            await _assigned(handle)

        report = await pool.stop(0.3)

        assert sorted(report.killed_task_ids) == sorted(
            handle.frame.task_id for handle in handles
        )
        for handle in handles:
            assert isinstance(await _outcome(handle), ChildLost)

    async def test_the_task_that_closed_the_child_retries_isolated_at_the_drain(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """A neighbour with time left keeps the closed child alive. The drain
        deadline of the stop kills it: the task whose deadline closed the
        child keeps its isolated retry, and the neighbour takes the crash
        path."""
        pool = await pool_factory(_config(1, 2))
        neighbour = pool.submit_shared(_request('sleep', '30'))
        await _assigned(neighbour)
        closer = pool.submit_shared(
            _request('sleep', '30', timeout_ms=SHORT_TIMEOUT_MS),
        )
        await _until(lambda: isinstance(pool.thread_states()[0], Closing))

        await pool.stop(0.3)

        match await _outcome(closer):
            case SeatKilled(verdict=verdict):
                assert verdict == TimedOutAtKill(RetryPlacement.ISOLATED)
            case other:
                raise AssertionError(other)
        assert isinstance(await _outcome(neighbour), ChildLost)

    async def test_a_task_after_stop_is_not_dispatched(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 1))
        await pool.stop(1.0)

        handle = pool.submit_shared(_request('pid'))

        assert isinstance(await _outcome(handle), NotDispatched)
