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

import pytest
import pytest_asyncio

from horsies.core.models.boot_failure import ChildPoolSetupFailed, ResourceShortage
from horsies.core.models.tasks import MIN_TASK_TIMEOUT_MS
from horsies.core.models.worker_profile import (
    MemoryLimit,
    NoTaskLimit,
    SharedAsyncChildren,
    SharedRecycle,
    TaskLimit,
)
from horsies.core.types.result import Err, Ok
from horsies.core.worker.child_protocol import (
    InitFrame,
    IsolatedChild,
    RssMb,
    RssUnreadable,
)
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.slots import (
    CHILD_KILL_WAIT_S,
    Assigned,
    BootAttemptsExhausted,
    ChildEnded,
    ChildHookFailed,
    ChildLost,
    CloseReason,
    Closing,
    DeadlineAcknowledged,
    Down,
    KilledBySignal,
    NoSeatFree,
    NotDispatched,
    Open,
    OpenChildLoad,
    SeatDeadlineEnded,
    SeatTaskFinished,
    SharedAnswered,
    SharedHandle,
    SharedOutcome,
    SharedRunRequest,
    SlotPool,
    SlotPoolConfig,
    place_on_shared_child,
    shared_recycle_reason,
)

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
        timeout_ms=timeout_ms,
        cancel_grace_ms=GRACE_MS,
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

        assert await _outcome(late) == SeatDeadlineEnded()
        assert isinstance(await _outcome(neighbour), SeatTaskFinished)

    async def test_a_value_after_the_deadline_is_the_timeout(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 1))
        handle = pool.submit_shared(
            _request('swallow_one_cancel', '0.2', timeout_ms=SHORT_TIMEOUT_MS)
        )

        assert await _outcome(handle) == SeatDeadlineEnded()

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
        assert pool.live_dispatches.snapshot() == ()

    async def test_the_backstop_leaves_a_live_child_whose_task_ignores_the_cancel(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(1, 2))
        stuck = pool.submit_shared(
            _request('ignore_every_cancel', timeout_ms=SHORT_TIMEOUT_MS)
        )
        await asyncio.wait_for(asyncio.shield(stuck.assigned), OUTCOME_TIMEOUT_S)
        await asyncio.sleep(PAST_SHORT_DEADLINE_S)

        outcome = await stuck.backstop_kill(CHILD_KILL_WAIT_S)

        assert isinstance(outcome, DeadlineAcknowledged)
        assert _alive(outcome.pid)
        assert not stuck.outcome.done()
        assert isinstance(
            await _outcome(pool.submit_shared(_request('pid'))), SeatTaskFinished
        )

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
