"""Runner heartbeats written by the worker process, and their fail-closed stop.

The beat thread and the watchdog run for real, with short intervals. The
writer is scripted, so no database is needed. Children are real processes:
a plain ``sleep`` process, or a slot pool child from the scripted child
program. The statement itself runs against Postgres in
``tests/integration/test_runner_heartbeat_writer.py``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import logging
import signal
import subprocess
import sys
import threading
import time
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import psutil
import pytest
import pytest_asyncio

from horsies.core.models.recovery import (
    RUNNER_BEAT_LANDING_ALLOWANCE_MS,
    RecoveryConfig,
    fail_closed_threshold_floor_ms,
)
from horsies.core.types.result import Err, Ok
from horsies.core.worker import runner_heartbeat
from horsies.core.models.worker_profile import ResolvedPool
from horsies.core.worker.child_protocol import InitFrame, IsolatedChild, RunFrame
from horsies.core.worker.config import ChildPreload, WorkerConfig
from horsies.core.worker.live_dispatches import (
    Held,
    LiveDispatch,
    LiveDispatches,
    RegistryClosed,
)
from horsies.core.worker.runner_heartbeat import (
    INSERT_RUNNER_HEARTBEATS_SQL,
    RUNNER_HEARTBEAT_LOST_EXIT_CODE,
    BeatsOverdue,
    BeatsWritten,
    BeatThreadDied,
    BeatTiming,
    BeatWriteFailed,
    BeatWriteOutcome,
    ExitPolicy,
    FailClosedCause,
    KillRefused,
    NoBeatFailure,
    PostgresBeatWriter,
    ProcessEnded,
    ProcessRuns,
    RunnerHeartbeat,
    RunnerHeartbeatLostError,
    SessionGone,
    SessionKilled,
    WatchFailed,
    _connect_settings,
    _dispatches_json,
    _PrepareThreshold,
    _PsycopgDefaultPrepare,
    StaleRunningRecovery,
    StaleRunningRecoveryOff,
    StaleRunningRecoveryOn,
    beat_timing,
    child_process_state,
    describe_fail_closed,
    kill_child_session,
    retry_delay_s,
    stale_running_recovery,
)
from horsies.core.worker import slots as slots_module
from horsies.core.worker.slots import (
    Busy,
    ChildLost,
    Down,
    KillSent,
    LossCause,
    SlotHandle,
    SlotPool,
    SlotPoolConfig,
    StopReport,
    TaskFinished,
)
from horsies.core.worker.stored_timeout import NoTimeout
from horsies.core.worker.worker import Worker

pytestmark = [pytest.mark.unit]

SCRIPTED_CHILD = str(
    Path(__file__).parent.parent / 'fixtures' / 'slots' / 'scripted_child.py',
)
INTERVAL_S = 0.05
# Long enough that a test about beats never fails closed by accident.
SAFE_WINDOW_S = 30.0
SHORT_WINDOW_S = 0.5
WAIT_S = 10.0
CLAIMED_AT = datetime(2026, 11, 10, 9, 30, 0, 125, tzinfo=timezone.utc)

_next_id = 0


def _frame(task_name: str = 'beat_test', argument: str | None = None) -> RunFrame:
    global _next_id
    _next_id += 1
    return RunFrame(
        task_id=f'task-{_next_id}',
        task_name=task_name,
        args_json=None,
        kwargs_json=argument,
        worker_id='worker-1',
        is_workflow_task=False,
        timeout=NoTimeout(),
        claimed_at=CLAIMED_AT,
    )


def _hold(live: LiveDispatches, frame: RunFrame, pid: int) -> int:
    match live.hold(frame, pid, seats=1):
        case Held(ticket=ticket):
            return ticket
        case RegistryClosed():
            raise AssertionError('the registry is closed')


class _ScriptedWriter:
    """Records each write; ``respond`` gives the outcome of call ``n`` (from 1)."""

    def __init__(
        self,
        respond: Callable[[int], BeatWriteOutcome] = lambda _: BeatsWritten(rows=0),
    ) -> None:
        self._respond = respond
        self._lock = threading.Lock()
        self.writes: list[tuple[str, ...]] = []
        self.outcomes: list[BeatWriteOutcome] = []
        self.closed = threading.Event()

    def write(self, dispatches: Sequence[LiveDispatch]) -> BeatWriteOutcome:
        with self._lock:
            self.writes.append(tuple(d.task_id for d in dispatches))
            call = len(self.writes)
        outcome = self._respond(call)
        with self._lock:
            self.outcomes.append(outcome)
        return outcome

    def close(self) -> None:
        self.closed.set()

    def count_with(self, task_id: str) -> int:
        with self._lock:
            return sum(task_id in write for write in self.writes)

    def count(self) -> int:
        with self._lock:
            return len(self.writes)


def _always_failing(_: int) -> BeatWriteOutcome:
    return BeatWriteFailed(detail='OperationalError: connection refused')


class _FailClosedRecorder:
    """Stands in for the worker's stop. Runs on the loop, like the real one."""

    def __init__(self) -> None:
        self.causes: list[FailClosedCause] = []
        self.called = asyncio.Event()

    def __call__(self, cause: FailClosedCause) -> None:
        self.causes.append(cause)
        self.called.set()


class _ExitRecorder:
    def __init__(self) -> None:
        self.codes: list[int] = []
        self.called = threading.Event()

    def __call__(self, code: int) -> None:
        self.codes.append(code)
        self.called.set()


HeartbeatFactory = Callable[..., RunnerHeartbeat]


def _recovery_on(window_s: float) -> StaleRunningRecoveryOn:
    """Stale running recovery whose fail-closed window is ``window_s``."""
    return StaleRunningRecoveryOn(
        threshold_ms=round(
            window_s * 1000 * runner_heartbeat.FAIL_CLOSED_WINDOW_DIVISOR
        ),
    )


@pytest_asyncio.fixture
async def heartbeat_factory() -> AsyncIterator[HeartbeatFactory]:
    """Launch heartbeats on the test's loop; stop each at teardown."""
    launched: list[RunnerHeartbeat] = []
    loop = asyncio.get_running_loop()

    def launch(
        live: LiveDispatches,
        writer: Any,
        *,
        window_s: float = SAFE_WINDOW_S,
        interval_s: float = INTERVAL_S,
        stale_running: StaleRunningRecovery | None = None,
        on_fail_closed: Callable[[FailClosedCause], None] | None = None,
        exit_policy: ExitPolicy | None = None,
        loop_override: asyncio.AbstractEventLoop | None = None,
    ) -> RunnerHeartbeat:
        """``window_s`` is the fail-closed window of ``StaleRunningRecoveryOn``,
        used unless ``stale_running`` is given."""
        heartbeat = RunnerHeartbeat.launch(
            live=live,
            writer=writer,
            timing=BeatTiming(interval_s=interval_s),
            stale_running=stale_running or _recovery_on(window_s),
            loop=loop_override or loop,
            on_fail_closed=on_fail_closed or _FailClosedRecorder(),
            exit_policy=exit_policy or ExitPolicy(exit_process=_ExitRecorder()),
        )
        launched.append(heartbeat)
        return heartbeat

    yield launch
    for heartbeat in launched:
        await asyncio.to_thread(heartbeat.stop)


@pytest.fixture
def processes() -> Iterator[Callable[..., subprocess.Popen[bytes]]]:
    """Start processes that each lead a session; kill and reap them at teardown."""
    started: list[subprocess.Popen[bytes]] = []

    def start(code: str = 'import time; time.sleep(60)') -> subprocess.Popen[bytes]:
        process = subprocess.Popen([sys.executable, '-c', code], start_new_session=True)
        started.append(process)
        return process

    yield start
    for process in started:
        if process.poll() is None:
            process.kill()
        process.wait(5)


def _until(predicate: Callable[[], bool], timeout_s: float = WAIT_S) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        assert time.monotonic() < deadline, 'condition not reached in time'
        time.sleep(0.01)


async def _until_async(
    predicate: Callable[[], bool], timeout_s: float = WAIT_S
) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        assert time.monotonic() < deadline, 'condition not reached in time'
        await asyncio.sleep(0.01)


def _zombie(start: Callable[..., subprocess.Popen[bytes]]) -> subprocess.Popen[bytes]:
    """A child that exited and is not reaped yet."""
    process = start('pass')
    _until(lambda: psutil.Process(process.pid).status() == psutil.STATUS_ZOMBIE)
    return process


# ---------------------------------------------------------------------------
# Which tasks get a beat
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_beats_only_held_tasks_whose_child_runs(
    heartbeat_factory: HeartbeatFactory,
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    live = LiveDispatches(capacity=4)
    running = _frame()
    zombie = _frame()
    reaped = _frame()
    released = _frame()
    _hold(live, running, processes().pid)
    _hold(live, zombie, _zombie(processes).pid)
    gone = processes('pass')
    gone.wait(5)
    _hold(live, reaped, gone.pid)
    released_ticket = _hold(live, released, processes().pid)
    _ = live.release(released_ticket)
    writer = _ScriptedWriter()

    heartbeat_factory(live, writer)
    await _until_async(lambda: writer.count() >= 3)

    assert set(writer.writes) == {(running.task_id,)}


@pytest.mark.asyncio
async def test_no_beat_after_the_child_is_released(
    heartbeat_factory: HeartbeatFactory,
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    """A kill releases the child first; later writes leave its task out."""
    live = LiveDispatches(capacity=2)
    kept = _frame()
    killed = _frame()
    _hold(live, kept, processes().pid)
    victim = processes()
    _hold(live, killed, victim.pid)
    writer = _ScriptedWriter()
    heartbeat_factory(live, writer)
    await _until_async(lambda: writer.count_with(killed.task_id) >= 1)

    _ = live.release_child(victim.pid)
    # A write that read the registry before the release may still land.
    await asyncio.sleep(3 * INTERVAL_S)
    settled = writer.count()
    await _until_async(lambda: writer.count() >= settled + 3)

    assert all(killed.task_id not in write for write in writer.writes[settled:])
    assert all(kept.task_id in write for write in writer.writes[settled:])


@pytest.mark.asyncio
async def test_an_idle_worker_writes_nothing_and_never_fails_closed(
    heartbeat_factory: HeartbeatFactory,
) -> None:
    """With no held task the database is not touched, so it cannot fail."""
    writer = _ScriptedWriter(_always_failing)
    stopped = _FailClosedRecorder()

    heartbeat_factory(
        LiveDispatches(capacity=1), writer, window_s=0.2, on_fail_closed=stopped
    )
    await asyncio.sleep(1.0)

    assert writer.count() == 0
    assert stopped.causes == []


# ---------------------------------------------------------------------------
# The slot pool keeps the registry
# ---------------------------------------------------------------------------


def _init() -> InitFrame:
    return InitFrame(
        app_locator='ok',
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
            task_send_pool=2,
        ),
    )


@pytest_asyncio.fixture
async def pool() -> AsyncIterator[SlotPool]:
    started = SlotPool(
        SlotPoolConfig(
            slots=1,
            init=_init(),
            max_tasks_per_child=0,
            max_memory_per_child_mb=None,
            child_entry=(SCRIPTED_CHILD,),
            child_preload=ChildPreload.NONE,
        ),
        pending_limit=10,
    )
    match await started.start():
        case Ok(_):
            pass
        case Err(failure):
            await started.stop(0)
            raise AssertionError(failure)
    yield started
    await started.stop(0)


async def _outcome(handle: SlotHandle) -> object:
    return await asyncio.wait_for(asyncio.shield(handle.outcome), WAIT_S)


@pytest.mark.asyncio
async def test_the_pool_holds_a_task_from_the_send_to_the_answer(
    pool: SlotPool,
) -> None:
    handle = pool.submit(_frame('sleep', '0.3'))
    await asyncio.wait_for(handle.assigned, WAIT_S)

    held = pool.live_dispatches.snapshot()
    assert [(d.task_id, d.claimed_at) for d in held] == [
        (handle.frame.task_id, CLAIMED_AT),
    ]
    assert isinstance(await _outcome(handle), TaskFinished)
    assert pool.live_dispatches.snapshot() == ()


@pytest.mark.asyncio
async def test_a_kill_releases_the_task_before_the_child_exits(pool: SlotPool) -> None:
    handle = pool.submit(_frame('hang'))
    await asyncio.wait_for(handle.assigned, WAIT_S)
    [held] = pool.live_dispatches.snapshot()

    assert handle.kill() == KillSent()

    # No await since the kill: the exit cannot have been seen yet.
    assert pool.live_dispatches.snapshot() == ()
    assert psutil.pid_exists(held.pid)
    assert isinstance(await _outcome(handle), ChildLost)


@pytest.mark.asyncio
async def test_a_closed_registry_kills_the_child_that_takes_a_task(
    pool: SlotPool,
) -> None:
    """After a fail-closed stop, a task that reaches a child cannot run unbeaten."""
    _ = pool.live_dispatches.close()

    handle = pool.submit(_frame('hang'))
    outcome = await _outcome(handle)

    assert isinstance(outcome, ChildLost)
    assert outcome.cause is LossCause.KILLED_BY_PARENT


@pytest.mark.asyncio
async def test_a_fail_closed_kill_reads_as_a_kill_by_the_worker(
    pool: SlotPool,
    heartbeat_factory: HeartbeatFactory,
) -> None:
    """Enter at the watchdog, read the loss at the pool: the kill is the
    worker's, as a kill by the pool is, not a death of the child."""
    handle = pool.submit(_frame('hang'))
    await asyncio.wait_for(handle.assigned, WAIT_S)
    stopped = _FailClosedRecorder()

    heartbeat_factory(
        pool.live_dispatches,
        _ScriptedWriter(_always_failing),
        window_s=SHORT_WINDOW_S,
        on_fail_closed=stopped,
    )
    outcome = await _outcome(handle)

    assert isinstance(outcome, ChildLost)
    assert outcome.cause is LossCause.KILLED_BY_PARENT
    await asyncio.wait_for(stopped.called.wait(), WAIT_S)
    assert isinstance(stopped.causes[0], BeatsOverdue)


@pytest.mark.asyncio
async def test_a_task_that_holds_the_gil_keeps_its_beats(
    pool: SlotPool,
    heartbeat_factory: HeartbeatFactory,
) -> None:
    """C code holding the child's GIL cannot stop a beat written by the parent."""
    writer = _ScriptedWriter()
    heartbeat_factory(pool.live_dispatches, writer)
    handle = pool.submit(_frame('gil_hang'))
    await asyncio.wait_for(handle.assigned, WAIT_S)
    task_id = handle.frame.task_id

    await _until_async(lambda: writer.count_with(task_id) >= 10)
    handle.kill()
    await _outcome(handle)
    settled = writer.count_with(task_id)
    await asyncio.sleep(5 * INTERVAL_S)

    assert writer.count_with(task_id) <= settled + 1


@pytest.mark.asyncio
async def test_a_slot_that_goes_down_holds_no_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The child of slot 0 dies in its task, and its replacement cannot boot
    for a cause that can clear. The slot is Down, and only the task of the
    serving slot is held."""
    monkeypatch.setattr(slots_module, 'SLOT_DOWN_FIRST_DELAY_S', 3600.0)
    init = replace(
        _init(),
        app_locator=f'short_after_first:{tmp_path / "marks"}',
        imports=('1000', '2'),
    )
    down_pool = SlotPool(
        SlotPoolConfig(
            slots=2,
            init=init,
            max_tasks_per_child=0,
            max_memory_per_child_mb=None,
            child_entry=(SCRIPTED_CHILD,),
            child_preload=ChildPreload.NONE,
        ),
        pending_limit=10,
    )
    assert isinstance(await asyncio.wait_for(down_pool.start(), WAIT_S), Ok)
    try:
        lost = down_pool.submit(_frame('kill_self'))
        assert isinstance(await _outcome(lost), ChildLost)
        await _until_async(lambda: any(isinstance(s, Down) for s in down_pool.states()))
        running = down_pool.submit(_frame('hang'))
        await asyncio.wait_for(running.assigned, WAIT_S)

        [held] = down_pool.live_dispatches.snapshot()
        serving = [
            slot.child for slot in down_pool._slots if isinstance(slot.state, Busy)
        ]
        assert held.task_id == running.frame.task_id
        assert [child.pid for child in serving if child is not None] == [held.pid]
    finally:
        await down_pool.stop(0)
    assert down_pool.live_dispatches.snapshot() == ()


# ---------------------------------------------------------------------------
# Fail closed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_failed_write_inside_the_window_recovers(
    heartbeat_factory: HeartbeatFactory,
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    """Two failures take 0.75 s of retries, well inside the 2 s window. The
    beat that lands then restarts the window: the worker runs on past it."""
    window_s = 2.0
    failed = BeatWriteFailed(detail='OperationalError: server closed the connection')

    def respond(call: int) -> BeatWriteOutcome:
        if call <= 2:
            return failed
        return BeatsWritten(rows=1)

    live = LiveDispatches(capacity=1)
    child = processes()
    _hold(live, _frame(), child.pid)
    writer = _ScriptedWriter(respond)
    stopped = _FailClosedRecorder()
    exits = _ExitRecorder()

    heartbeat_factory(
        live,
        writer,
        window_s=window_s,
        on_fail_closed=stopped,
        exit_policy=ExitPolicy(exit_process=exits),
    )
    await _until_async(lambda: writer.count() >= 3)
    await asyncio.sleep(window_s * 1.5)

    assert writer.outcomes[:2] == [failed, failed]
    assert all(isinstance(outcome, BeatsWritten) for outcome in writer.outcomes[2:])
    assert stopped.causes == []
    assert exits.codes == []
    assert child.poll() is None


@pytest.mark.asyncio
async def test_beats_that_land_within_the_window_never_stop_the_worker(
    heartbeat_factory: HeartbeatFactory,
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    """Each write takes 0.9 s, longer than the 0.1 s interval, so the next
    write starts when one lands. The 1.5 s window counts from each landing:
    0.9 s pass between two. Counted from the start of a write, 1.8 s would
    pass, and the worker would stop while every beat lands."""
    write_s = 0.9

    def slow(_: int) -> BeatWriteOutcome:
        time.sleep(write_s)
        return BeatsWritten(rows=1)

    live = LiveDispatches(capacity=1)
    child = processes()
    _hold(live, _frame(), child.pid)
    writer = _ScriptedWriter(slow)
    stopped = _FailClosedRecorder()

    heartbeat_factory(
        live,
        writer,
        window_s=1.5,
        interval_s=0.1,
        on_fail_closed=stopped,
    )
    await _until_async(lambda: writer.count() >= 5)

    assert stopped.causes == []
    assert child.poll() is None


@pytest.mark.asyncio
async def test_writes_failing_past_the_window_stop_the_worker(
    heartbeat_factory: HeartbeatFactory,
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    live = LiveDispatches(capacity=1)
    child = processes()
    frame = _frame()
    _hold(live, frame, child.pid)
    stopped = _FailClosedRecorder()
    exits = _ExitRecorder()
    heartbeat = heartbeat_factory(
        live,
        _ScriptedWriter(_always_failing),
        window_s=SHORT_WINDOW_S,
        on_fail_closed=stopped,
        exit_policy=ExitPolicy(exit_process=exits),
    )

    await asyncio.wait_for(stopped.called.wait(), WAIT_S)

    [cause] = stopped.causes
    assert isinstance(cause, BeatsOverdue)
    assert cause.uncovered_s > SHORT_WINDOW_S
    assert cause.last_failure == BeatWriteFailed(
        detail='OperationalError: connection refused'
    )
    assert await asyncio.to_thread(child.wait, WAIT_S) == -signal.SIGKILL
    assert live.hold(_frame(), pid=child.pid, seats=1) == RegistryClosed()
    await asyncio.to_thread(heartbeat.stop)
    assert exits.codes == []


@pytest.mark.asyncio
async def test_a_write_that_never_returns_stops_the_worker(
    heartbeat_factory: HeartbeatFactory,
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    """The watchdog does not wait for the beat thread, which is stuck in I/O."""
    release = threading.Event()

    def hang(_: int) -> BeatWriteOutcome:
        release.wait(WAIT_S)
        return BeatsWritten(rows=1)

    live = LiveDispatches(capacity=1)
    child = processes()
    _hold(live, _frame(), child.pid)
    stopped = _FailClosedRecorder()
    try:
        heartbeat_factory(
            live,
            _ScriptedWriter(hang),
            window_s=SHORT_WINDOW_S,
            on_fail_closed=stopped,
        )
        await asyncio.wait_for(stopped.called.wait(), WAIT_S)
    finally:
        release.set()

    [cause] = stopped.causes
    assert isinstance(cause, BeatsOverdue)
    assert cause.last_failure == NoBeatFailure()
    assert await asyncio.to_thread(child.wait, WAIT_S) == -signal.SIGKILL


@pytest.mark.asyncio
async def test_a_dead_beat_thread_stops_the_worker(
    heartbeat_factory: HeartbeatFactory,
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    def defect(_: int) -> BeatWriteOutcome:
        raise RuntimeError('scripted defect in the beat thread')

    live = LiveDispatches(capacity=1)
    child = processes()
    _hold(live, _frame(), child.pid)
    writer = _ScriptedWriter(defect)
    stopped = _FailClosedRecorder()

    heartbeat_factory(live, writer, on_fail_closed=stopped)
    await asyncio.wait_for(stopped.called.wait(), WAIT_S)

    [cause] = stopped.causes
    assert cause == BeatThreadDied('RuntimeError: scripted defect in the beat thread')
    assert await asyncio.to_thread(child.wait, WAIT_S) == -signal.SIGKILL
    assert writer.closed.is_set()


@pytest.mark.asyncio
async def test_a_worker_that_does_not_stop_is_exited(
    heartbeat_factory: HeartbeatFactory,
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    live = LiveDispatches(capacity=1)
    _hold(live, _frame(), processes().pid)
    exits = _ExitRecorder()

    heartbeat_factory(
        live,
        _ScriptedWriter(_always_failing),
        window_s=SHORT_WINDOW_S,
        exit_policy=ExitPolicy(grace_s=0.2, exit_process=exits),
    )
    await asyncio.to_thread(exits.called.wait, WAIT_S)

    assert exits.codes == [RUNNER_HEARTBEAT_LOST_EXIT_CODE]


@pytest.mark.asyncio
async def test_a_closed_loop_exits_without_the_grace(
    heartbeat_factory: HeartbeatFactory,
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    closed_loop = asyncio.new_event_loop()
    closed_loop.close()
    live = LiveDispatches(capacity=1)
    _hold(live, _frame(), processes().pid)
    exits = _ExitRecorder()

    heartbeat_factory(
        live,
        _ScriptedWriter(_always_failing),
        window_s=SHORT_WINDOW_S,
        exit_policy=ExitPolicy(grace_s=WAIT_S * 10, exit_process=exits),
        loop_override=closed_loop,
    )

    assert await asyncio.to_thread(exits.called.wait, WAIT_S)
    assert exits.codes == [RUNNER_HEARTBEAT_LOST_EXIT_CODE]


@pytest.mark.asyncio
async def test_stop_ends_both_threads(heartbeat_factory: HeartbeatFactory) -> None:
    writer = _ScriptedWriter()
    heartbeat = heartbeat_factory(LiveDispatches(capacity=1), writer)

    await asyncio.to_thread(heartbeat.stop)

    names = {thread.name for thread in threading.enumerate()}
    assert runner_heartbeat.BEAT_THREAD_NAME not in names
    assert runner_heartbeat.WATCH_THREAD_NAME not in names
    assert writer.closed.is_set()


# ---------------------------------------------------------------------------
# Stale running recovery off: log, never stop
# ---------------------------------------------------------------------------


def _beat_records(
    caplog: pytest.LogCaptureFixture, text: str
) -> list[logging.LogRecord]:
    return [record for record in caplog.records if text in record.getMessage()]


@pytest.mark.asyncio
async def test_with_recovery_off_failing_writes_never_stop_the_worker(
    heartbeat_factory: HeartbeatFactory,
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    """No reaper fails the row, so stopping would protect nothing."""
    live = LiveDispatches(capacity=2)
    child = processes()
    frame = _frame()
    _hold(live, frame, child.pid)
    writer = _ScriptedWriter(_always_failing)
    stopped = _FailClosedRecorder()
    exits = _ExitRecorder()

    heartbeat_factory(
        live,
        writer,
        stale_running=StaleRunningRecoveryOff(),
        on_fail_closed=stopped,
        exit_policy=ExitPolicy(grace_s=0.1, exit_process=exits),
    )
    await asyncio.sleep(1.5)

    assert stopped.causes == []
    assert exits.codes == []
    assert child.poll() is None
    assert writer.count_with(frame.task_id) >= 5
    assert isinstance(live.hold(_frame(), pid=child.pid + 1, seats=1), Held)


@pytest.mark.asyncio
async def test_with_recovery_off_a_dead_beat_thread_is_logged_and_the_worker_runs(
    heartbeat_factory: HeartbeatFactory,
    processes: Callable[..., subprocess.Popen[bytes]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    def defect(_: int) -> BeatWriteOutcome:
        raise RuntimeError('scripted defect in the beat thread')

    live = LiveDispatches(capacity=1)
    child = processes()
    _hold(live, _frame(), child.pid)
    stopped = _FailClosedRecorder()

    with caplog.at_level(logging.CRITICAL, logger='horsies.worker'):
        heartbeat_factory(
            live,
            _ScriptedWriter(defect),
            stale_running=StaleRunningRecoveryOff(),
            on_fail_closed=stopped,
        )
        await _until_async(
            lambda: bool(_beat_records(caplog, 'runs on without runner heartbeats')),
        )

    assert stopped.causes == []
    assert child.poll() is None


@pytest.mark.asyncio
async def test_with_recovery_off_a_late_beat_is_logged(
    heartbeat_factory: HeartbeatFactory,
    processes: Callable[..., subprocess.Popen[bytes]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A write that never returns is no failure yet; the watchdog logs it."""
    release = threading.Event()

    def hang(_: int) -> BeatWriteOutcome:
        release.wait(WAIT_S)
        return BeatsWritten(rows=1)

    live = LiveDispatches(capacity=1)
    child = processes()
    _hold(live, _frame(), child.pid)
    stopped = _FailClosedRecorder()
    try:
        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            heartbeat_factory(
                live,
                _ScriptedWriter(hang),
                stale_running=StaleRunningRecoveryOff(),
                on_fail_closed=stopped,
            )
            await _until_async(
                lambda: bool(_beat_records(caplog, 'No runner heartbeat landed')),
            )
    finally:
        release.set()

    [late] = _beat_records(caplog, 'No runner heartbeat landed')
    assert 'the last write has not returned' in late.getMessage()
    assert stopped.causes == []
    assert child.poll() is None


@pytest.mark.parametrize(
    'stale_running',
    [StaleRunningRecoveryOff(), _recovery_on(SAFE_WINDOW_S)],
    ids=['recovery off', 'recovery on'],
)
@pytest.mark.asyncio
async def test_failed_writes_are_logged_at_most_once_per_log_interval(
    heartbeat_factory: HeartbeatFactory,
    processes: Callable[..., subprocess.Popen[bytes]],
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    stale_running: StaleRunningRecovery,
) -> None:
    """The writes fail about 20 times a second; the log has one line per 0.5 s."""
    monkeypatch.setattr(runner_heartbeat, 'BEAT_FAILURE_LOG_INTERVAL_S', 0.5)
    live = LiveDispatches(capacity=1)
    _hold(live, _frame(), processes().pid)
    writer = _ScriptedWriter(_always_failing)

    with caplog.at_level(logging.ERROR, logger='horsies.worker'):
        heartbeat_factory(live, writer, stale_running=stale_running)
        await asyncio.sleep(1.2)
        written = writer.count()
        logged = len(_beat_records(caplog, 'Runner heartbeat write failed'))

    assert written >= 15
    assert 2 <= logged <= 4


# ---------------------------------------------------------------------------
# The worker stops through its real callback
# ---------------------------------------------------------------------------


def _worker(recovery: RecoveryConfig | None = None) -> Worker:
    cfg = WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
        recovery_config=recovery,
    )
    listener = MagicMock()
    listener.close = AsyncMock()
    return Worker(session_factory=MagicMock(), listener=listener, cfg=cfg)


@pytest.mark.asyncio
async def test_start_slots_launches_the_heartbeat_on_the_pool_registry() -> None:
    recovery = RecoveryConfig(
        runner_heartbeat_interval_ms=2_000,
        running_stale_threshold_ms=10_000,
    )
    worker = _worker(recovery)
    pool = MagicMock()
    pool.start = AsyncMock(return_value=Ok(()))
    pool.live_dispatches = LiveDispatches(capacity=1)
    launched = MagicMock()

    with (
        patch('horsies.core.worker.worker.SlotPool', return_value=pool),
        patch.object(RunnerHeartbeat, 'launch', launched),
    ):
        await worker._start_slots()

    kwargs = launched.call_args.kwargs
    assert kwargs['live'] is pool.live_dispatches
    assert kwargs['timing'] == BeatTiming(interval_s=2.0)
    assert kwargs['stale_running'] == StaleRunningRecoveryOn(threshold_ms=10_000)
    assert kwargs['on_fail_closed'] == worker._stop_for_lost_runner_heartbeat
    assert worker._runner_heartbeat is launched.return_value


@pytest.mark.asyncio
async def test_a_heartbeat_that_fails_closed_stops_the_worker_non_zero(
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    """Enter at the worker's launch, so the real callback is wired."""
    worker = _worker(
        RecoveryConfig(
            runner_heartbeat_interval_ms=1_000,
            running_stale_threshold_ms=fail_closed_threshold_floor_ms(1_000),
        ),
    )
    pool = MagicMock()
    pool.live_dispatches = LiveDispatches(capacity=1)
    child = processes()
    _hold(pool.live_dispatches, _frame(), child.pid)

    with (
        patch(
            'horsies.core.worker.worker.beat_timing',
            return_value=BeatTiming(interval_s=INTERVAL_S),
        ),
        patch(
            'horsies.core.worker.worker.PostgresBeatWriter',
            return_value=_ScriptedWriter(_always_failing),
        ),
    ):
        worker._runner_heartbeat = worker._launch_runner_heartbeat(pool)
    try:
        await asyncio.wait_for(worker._stop.wait(), WAIT_S)
    finally:
        await worker._stop_runner_heartbeat()

    assert isinstance(worker._fatal_background_error, RunnerHeartbeatLostError)
    assert 'no runner heartbeat landed' in str(worker._fatal_background_error)
    assert await asyncio.to_thread(child.wait, WAIT_S) == -signal.SIGKILL
    with pytest.raises(RunnerHeartbeatLostError):
        worker._raise_captured_fatal_error()


@pytest.mark.parametrize(
    'recovery',
    [None, RecoveryConfig(auto_fail_stale_running=False)],
    ids=['no recovery config', 'auto fail off'],
)
@pytest.mark.asyncio
async def test_a_worker_whose_reaper_fails_no_running_row_runs_on(
    processes: Callable[..., subprocess.Popen[bytes]],
    recovery: RecoveryConfig | None,
) -> None:
    """Enter at the worker's launch: the recovery config decides the mode."""
    worker = _worker(recovery)
    pool = MagicMock()
    pool.live_dispatches = LiveDispatches(capacity=1)
    child = processes()
    _hold(pool.live_dispatches, _frame(), child.pid)

    with (
        patch(
            'horsies.core.worker.worker.beat_timing',
            return_value=BeatTiming(interval_s=INTERVAL_S),
        ),
        patch(
            'horsies.core.worker.worker.PostgresBeatWriter',
            return_value=_ScriptedWriter(_always_failing),
        ),
    ):
        worker._runner_heartbeat = worker._launch_runner_heartbeat(pool)
    try:
        await asyncio.sleep(1.5)
    finally:
        await worker._stop_runner_heartbeat()

    assert not worker._stop.is_set()
    assert worker._fatal_background_error is None
    assert child.poll() is None


@pytest.mark.asyncio
async def test_stop_ends_the_heartbeat_after_the_children() -> None:
    worker = _worker()
    order: list[str] = []

    async def stop_pool(_: float) -> StopReport:
        order.append('children')
        return StopReport(())

    pool = MagicMock()
    pool.stop = AsyncMock(side_effect=stop_pool)
    heartbeat = MagicMock()
    heartbeat.stop = MagicMock(side_effect=lambda: order.append('heartbeat'))
    worker._slots = pool
    worker._runner_heartbeat = heartbeat

    await worker.stop()

    assert order == ['children', 'heartbeat']
    assert worker._runner_heartbeat is None


@pytest.mark.asyncio
async def test_a_failed_start_stops_the_heartbeat() -> None:
    worker = _worker()
    pool = MagicMock()
    pool.stop = AsyncMock(return_value=StopReport(()))
    heartbeat = MagicMock()
    worker._slots = pool
    worker._runner_heartbeat = heartbeat

    await worker._cleanup_after_failed_start()

    heartbeat.stop.assert_called_once_with()
    assert worker._runner_heartbeat is None


# ---------------------------------------------------------------------------
# Parts
# ---------------------------------------------------------------------------


def test_the_window_is_half_the_stale_threshold() -> None:
    recovery = RecoveryConfig(
        runner_heartbeat_interval_ms=30_000,
        running_stale_threshold_ms=300_000,
    )

    assert beat_timing(recovery) == BeatTiming(interval_s=30.0)
    assert stale_running_recovery(recovery) == StaleRunningRecoveryOn(300_000)
    assert StaleRunningRecoveryOn(300_000).fail_closed_window_s() == 150.0


def test_the_smallest_window_holds_one_interval_and_a_landing() -> None:
    interval_ms = 1_000
    recovery = RecoveryConfig(
        runner_heartbeat_interval_ms=interval_ms,
        running_stale_threshold_ms=fail_closed_threshold_floor_ms(interval_ms),
    )
    on = stale_running_recovery(recovery)

    assert isinstance(on, StaleRunningRecoveryOn)
    assert on.fail_closed_window_s() == (
        beat_timing(recovery).interval_s + RUNNER_BEAT_LANDING_ALLOWANCE_MS / 1000
    )


def test_the_kill_margin_is_above_one_watch_and_below_one_interval() -> None:
    """Above: the watchdog sees the end of the window within one watch
    interval. Below the smallest beat interval (1 s): a beat that lands
    within the allowance never starts its window early."""
    assert (
        runner_heartbeat.WATCH_INTERVAL_S
        < runner_heartbeat.FAIL_CLOSED_KILL_MARGIN_S
        < 1.0
    )


class _Clock:
    """Stands in for ``time`` in the heartbeat module: scripted readings."""

    def __init__(self, *readings: float) -> None:
        self._readings = list(readings)

    def monotonic(self) -> float:
        return self._readings.pop(0)


def _beat_once_at(
    monkeypatch: pytest.MonkeyPatch,
    child_pid: int,
    stale_running: StaleRunningRecovery,
    *,
    started_at_s: float,
    landed_at_s: float,
) -> tuple[float, float]:
    """Run one beat whose write starts and lands at the given clock readings.

    Returns the time from which the watchdog counts, and the wait to the
    next write. The interval is 1 s.
    """
    live = LiveDispatches(capacity=1)
    _hold(live, _frame(), child_pid)
    loop = asyncio.new_event_loop()
    try:
        heartbeat = RunnerHeartbeat(
            live=live,
            writer=_ScriptedWriter(lambda _: BeatsWritten(rows=1)),
            timing=BeatTiming(interval_s=1.0),
            stale_running=stale_running,
            loop=loop,
            on_fail_closed=_FailClosedRecorder(),
            exit_policy=ExitPolicy(exit_process=_ExitRecorder()),
        )
        monkeypatch.setattr(runner_heartbeat, 'time', _Clock(started_at_s, landed_at_s))
        delay_s = heartbeat._beat_once()
    finally:
        loop.close()
    return heartbeat._read_coverage().covered_at_s, delay_s


@pytest.mark.parametrize(
    'stale_running',
    [StaleRunningRecoveryOn(threshold_ms=6_000), StaleRunningRecoveryOff()],
    ids=['recovery on', 'recovery off'],
)
def test_a_landed_beat_counts_from_its_landing(
    monkeypatch: pytest.MonkeyPatch,
    processes: Callable[..., subprocess.Popen[bytes]],
    stale_running: StaleRunningRecovery,
) -> None:
    covered_at_s, delay_s = _beat_once_at(
        monkeypatch,
        processes().pid,
        stale_running,
        started_at_s=100.0,
        landed_at_s=101.5,
    )

    assert covered_at_s == 101.5
    assert delay_s == 0.0


def test_a_beat_that_took_most_of_the_window_ends_it_before_the_reaper(
    monkeypatch: pytest.MonkeyPatch,
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    """The reaper may count the beat from the start of its write. Its 3 s
    window then ends the kill margin before the 6 s threshold from that
    start, not 3 s after the landing."""
    covered_at_s, _ = _beat_once_at(
        monkeypatch,
        processes().pid,
        StaleRunningRecoveryOn(threshold_ms=6_000),
        started_at_s=100.0,
        landed_at_s=102.9,
    )

    window_end_s = covered_at_s + 3.0
    assert window_end_s == 106.0 - runner_heartbeat.FAIL_CLOSED_KILL_MARGIN_S


@pytest.mark.parametrize(
    ('recovery', 'expected'),
    [
        (None, StaleRunningRecoveryOff()),
        (RecoveryConfig(auto_fail_stale_running=False), StaleRunningRecoveryOff()),
        (RecoveryConfig(), StaleRunningRecoveryOn(threshold_ms=300_000)),
    ],
    ids=['no recovery config', 'auto fail off', 'auto fail on'],
)
def test_only_a_reaper_that_fails_stale_running_rows_makes_beats_fail_closed(
    recovery: RecoveryConfig | None,
    expected: StaleRunningRecovery,
) -> None:
    assert stale_running_recovery(recovery) == expected


@pytest.mark.parametrize(
    ('failures', 'interval_s', 'delay_s'),
    [
        (1, 30.0, 0.25),
        (2, 30.0, 0.5),
        (4, 30.0, 2.0),
        (6, 30.0, 5.0),
        (1_000, 30.0, 5.0),
        (3, 0.4, 0.4),
    ],
)
def test_retry_delays_double_and_stay_bounded(
    failures: int,
    interval_s: float,
    delay_s: float,
) -> None:
    assert retry_delay_s(failures, interval_s) == delay_s


def test_a_running_child_runs(
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    assert child_process_state(processes().pid) == ProcessRuns()


def test_a_zombie_child_has_ended(
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    assert child_process_state(_zombie(processes).pid) == ProcessEnded()


def test_a_reaped_child_has_ended(
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    gone = processes('pass')
    gone.wait(5)

    assert child_process_state(gone.pid) == ProcessEnded()


def test_kill_child_session_kills_the_session(
    processes: Callable[..., subprocess.Popen[bytes]],
) -> None:
    child = processes()

    assert kill_child_session(child.pid) == SessionKilled()
    assert child.wait(WAIT_S) == -signal.SIGKILL
    assert kill_child_session(child.pid) == SessionGone()


def test_kill_child_session_reports_a_refused_kill() -> None:
    with patch('os.killpg', side_effect=PermissionError('not permitted')):
        assert kill_child_session(4242) == KillRefused('not permitted')


def test_the_statement_fences_each_row() -> None:
    """Revert-proofing anchor: each predicate of the beat fence is in the statement."""
    statement = ' '.join(INSERT_RUNNER_HEARTBEATS_SQL.split())

    assert "WHERE t.status = 'RUNNING'" in statement
    assert 'AND t.claimed_by_worker_id = CAST(%(worker_id)s AS VARCHAR)' in statement
    assert 'AND (d.claimed_at IS NULL OR t.claimed_at = d.claimed_at)' in statement
    assert 'AND t.finalizing_at IS NULL' in statement
    assert "SELECT t.id, CAST(%(worker_id)s AS VARCHAR), 'runner', NOW()" in statement


def test_dispatch_rows_carry_the_claim_generation() -> None:
    held = (
        LiveDispatch(ticket=1, task_id='a', claimed_at=CLAIMED_AT, pid=1),
        LiveDispatch(ticket=2, task_id='b', claimed_at=None, pid=2),
    )

    assert _dispatches_json(held) == (
        '[{"task_id":"a","claimed_at":"2026-11-10T09:30:00.000125+00:00"},'
        '{"task_id":"b","claimed_at":null}]'
    )


def test_connect_settings_keep_the_prepare_threshold_apart() -> None:
    settings = _connect_settings(
        'postgresql://u:p@localhost:5432/db',
        {'keepalives': 1, 'tcp_user_timeout': 30_000, 'prepare_threshold': None},
    )

    assert settings.prepare == _PrepareThreshold(None)
    assert 'keepalives=1' in settings.conninfo
    assert 'tcp_user_timeout=30000' in settings.conninfo
    assert (
        f'connect_timeout={runner_heartbeat.BEAT_CONNECT_TIMEOUT_S}'
        in settings.conninfo
    )
    assert 'p@' not in repr(settings) and 'password' not in repr(settings)


def test_connect_settings_leave_psycopgs_default_prepare() -> None:
    settings = _connect_settings('postgresql://u:p@localhost/db', {})

    assert settings.prepare == _PsycopgDefaultPrepare()


def test_a_connect_parameter_of_another_type_is_a_defect() -> None:
    with pytest.raises(AssertionError, match="'keepalives' has type list"):
        _connect_settings('postgresql://u:p@localhost/db', {'keepalives': [1]})


def test_an_unreachable_database_is_a_failed_write() -> None:
    writer = PostgresBeatWriter(
        database_url='postgresql://u:p@127.0.0.1:1/db',
        connect_kwargs={},
        worker_id='worker-1',
    )
    held = (LiveDispatch(ticket=1, task_id='a', claimed_at=None, pid=1),)

    first = writer.write(held)
    second = writer.write(held)

    assert isinstance(first, BeatWriteFailed)
    assert first.detail.startswith('OperationalError')
    assert isinstance(second, BeatWriteFailed)
    writer.close()


def test_each_cause_names_what_failed() -> None:
    assert describe_fail_closed(BeatThreadDied('RuntimeError: x')) == (
        'the runner heartbeat thread died: RuntimeError: x'
    )
    assert describe_fail_closed(WatchFailed('ValueError: y')) == (
        'the runner heartbeat watchdog failed: ValueError: y'
    )
    overdue = describe_fail_closed(
        BeatsOverdue(
            uncovered_s=151.0,
            window_s=150.0,
            last_failure=BeatWriteFailed('OperationalError: refused'),
        ),
    )
    assert overdue == (
        'no runner heartbeat landed for 151.0s, past 150.0s (half of '
        'running_stale_threshold_ms); last error: OperationalError: refused'
    )
    assert describe_fail_closed(
        BeatsOverdue(uncovered_s=2.0, window_s=1.0, last_failure=NoBeatFailure()),
    ).endswith('the last write has not returned')
