"""Slot pool tests with real child processes and no database.

Crash operations that open a macOS crash dialog (SIGSEGV, abort) run only on
Linux. On macOS the same verdicts are covered through SIGKILL and ``os._exit``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import os
import random
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import ClassVar, cast

import pytest
import pytest_asyncio

from horsies.core.types.result import Err, Ok
from horsies.core.worker.child_protocol import (
    InitFrame,
    ResultFrame,
    RssUnreadable,
    RunFrame,
    encode_child_frame,
)
from horsies.core.worker import slots
from horsies.core.worker.slots import (
    CHILD_BOOT_ATTEMPTS,
    CHILD_BOOT_KILL_DELAY_MAX_S,
    SLOT_DEFECT_LIMIT,
    Assigned,
    BootAttemptsExhausted,
    Busy,
    Child,
    ChildBaseline,
    ChildHookFailed,
    ChildLost,
    ExitedWith,
    ExitUnknown,
    FrameProtocol,
    Idle,
    KilledBySignal,
    KillSent,
    LossCause,
    NotDispatched,
    NothingToKill,
    NotRunCause,
    PoolStopped,
    SlotHandle,
    SlotPool,
    SlotPoolConfig,
    SpawnFailed,
    TaskFinished,
    child_exit_from_returncode,
    describe_exit,
    interpreter_flags,
    outside_kill_delay_s,
)

SCRIPTED_CHILD = str(Path(__file__).parent.parent / 'fixtures' / 'slots' / 'scripted_child.py')
linux_only = pytest.mark.skipif(
    sys.platform != 'linux',
    reason='opens a crash dialog on macOS',
)
OUTCOME_TIMEOUT_S = 30.0
NO_SUCH_PID = 2**30
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
    )


def _config(
    slots: int = 1,
    *,
    init: InitFrame | None = None,
    max_tasks_per_child: int = 0,
    max_memory_per_child_mb: int | None = None,
    child_entry: tuple[str, ...] = (SCRIPTED_CHILD,),
) -> SlotPoolConfig:
    return SlotPoolConfig(
        slots=slots,
        init=init or _init(),
        max_tasks_per_child=max_tasks_per_child,
        max_memory_per_child_mb=max_memory_per_child_mb,
        child_entry=child_entry,
    )


_next_id = 0


def _frame(task_name: str, argument: str | None = None) -> RunFrame:
    global _next_id
    _next_id += 1
    return RunFrame(
        task_id=f'task-{_next_id}',
        task_name=task_name,
        args_json=None,
        kwargs_json=argument,
        worker_id='worker-1',
        runner_heartbeat_interval_ms=30_000,
        is_workflow_task=False,
        timeout_ms=None,
        claimed_at=None,
    )


async def _outcome(handle: SlotHandle) -> TaskFinished | ChildLost | NotDispatched:
    return await asyncio.wait_for(asyncio.shield(handle.outcome), OUTCOME_TIMEOUT_S)


async def _result_json(handle: SlotHandle) -> str:
    match await _outcome(handle):
        case TaskFinished(result_json=result_json):
            return result_json
        case other:
            raise AssertionError(other)


async def _started_pool(config: SlotPoolConfig) -> SlotPool:
    pool = SlotPool(config, pending_limit=100_000)
    match await pool.start():
        case Ok(_):
            return pool
        case Err(failure):
            await pool.stop(0)
            raise AssertionError(failure)


@pytest_asyncio.fixture
async def pool_factory() -> AsyncIterator[PoolFactory]:
    pools: list[SlotPool] = []

    async def create(config: SlotPoolConfig | None = None) -> SlotPool:
        pool = await _started_pool(config or _config())
        pools.append(pool)
        return pool

    yield create
    for pool in pools:
        await pool.stop(0)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    state = subprocess.run(
        ['ps', '-o', 'stat=', '-p', str(pid)],
        capture_output=True,
        text=True,
        check=False,
    )
    return bool(state.stdout.strip()) and not state.stdout.strip().startswith('Z')


async def _until(predicate: Callable[[], bool], timeout_s: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        assert time.monotonic() < deadline, 'condition not reached in time'
        await asyncio.sleep(0.01)


class TestExitDescription:
    @pytest.mark.parametrize(
        ('returncode', 'expected'),
        [(0, ExitedWith(0)), (3, ExitedWith(3)), (-9, KilledBySignal(9))],
    )
    def test_returncode(self, returncode: int, expected: object) -> None:
        assert child_exit_from_returncode(returncode) == expected

    def test_text_names_the_signal(self) -> None:
        assert describe_exit(KilledBySignal(11)) == 'signal 11 (SIGSEGV)'

    def test_text_for_an_unknown_signal_number(self) -> None:
        assert describe_exit(KilledBySignal(250)) == 'signal 250'

    def test_text_for_an_exit_code(self) -> None:
        assert describe_exit(ExitedWith(2)) == 'exit code 2'

    def test_text_for_an_unknown_exit(self) -> None:
        assert 'no exit' in describe_exit(ExitUnknown())


class TestInterpreterFlags:
    def _child_flags(self, *python_args: str) -> list[str]:
        code = 'from horsies.core.worker.slots import interpreter_flags; print(interpreter_flags())'
        result = subprocess.run(
            [sys.executable, *python_args, '-c', code],
            capture_output=True,
            text=True,
            check=True,
        )
        return list(eval(result.stdout.strip()))  # noqa: S307

    def test_no_flags_by_default(self) -> None:
        assert '-O' not in interpreter_flags()

    def test_optimize_is_forwarded(self) -> None:
        assert '-OO' in self._child_flags('-OO')

    def test_warning_options_are_forwarded(self) -> None:
        flags = self._child_flags('-W', 'error::ResourceWarning')
        assert flags[flags.index('-W') + 1] == 'error::ResourceWarning'

    def test_dev_mode_is_forwarded(self) -> None:
        flags = self._child_flags('-X', 'dev')
        assert flags[flags.index('-X') + 1] == 'dev'


class TestFrameProtocol:
    """Byte-level behavior, with no process."""

    def _protocol(self) -> tuple[FrameProtocol, list[object], list[bool], list[bool]]:
        frames: list[object] = []
        closed: list[bool] = []
        aborted: list[bool] = []

        class FakeTransport(asyncio.Transport):
            def abort(self) -> None:
                aborted.append(True)

        protocol = FrameProtocol(frames.append, lambda: closed.append(True))
        protocol.connection_made(FakeTransport())
        return protocol, frames, closed, aborted

    def _feed(self, protocol: FrameProtocol, data: bytes, chunk: int) -> None:
        offset = 0
        while offset < len(data):
            target = protocol.get_buffer(-1)
            size = min(chunk, len(target), len(data) - offset)
            target[:size] = data[offset : offset + size]
            protocol.buffer_updated(size)
            offset += size

    def _result_bytes(self, body: str) -> bytes:
        encoded = encode_child_frame(ResultFrame('t', True, body, None, RssUnreadable()))
        return encoded.head + encoded.body

    @pytest.mark.parametrize('chunk', [1, 2, 7, 1 << 20])
    def test_any_read_size_gives_the_same_frames(self, chunk: int) -> None:
        protocol, frames, _, _ = self._protocol()
        self._feed(protocol, self._result_bytes('abc') + self._result_bytes(''), chunk)
        assert len(frames) == 2

    def test_partial_frame_is_reported(self) -> None:
        protocol, frames, _, _ = self._protocol()
        data = self._result_bytes('abc')
        self._feed(protocol, data[:-1], 1 << 20)
        assert frames == []
        assert protocol.has_partial_frame()

    def test_no_partial_frame_between_frames(self) -> None:
        protocol, _, _, _ = self._protocol()
        self._feed(protocol, self._result_bytes('abc'), 1 << 20)
        assert not protocol.has_partial_frame()

    def test_over_bound_header_allocates_nothing_and_aborts(self) -> None:
        protocol, frames, _, aborted = self._protocol()
        self._feed(protocol, b'\xff' * 12, 1 << 20)
        assert frames == [] and aborted == [True]
        assert protocol.violation is not None

    def test_connection_lost_reports_closed(self) -> None:
        protocol, _, closed, _ = self._protocol()
        protocol.connection_lost(None)
        assert closed == [True]


@pytest.mark.asyncio(loop_scope='function')
class TestBoot:
    async def test_start_reports_one_baseline_per_child(self) -> None:
        pool = SlotPool(_config(2), pending_limit=10)
        match await pool.start():
            case Ok(baselines):
                assert len(baselines) == 2
                assert all(isinstance(item, ChildBaseline) for item in baselines)
                assert len({item.pid for item in baselines}) == 2
            case Err(failure):
                raise AssertionError(failure)
        await pool.stop(0)

    async def test_hook_failure_stops_at_once(self) -> None:
        pool = SlotPool(_config(init=_init('hook_fails')), pending_limit=10)
        started = time.monotonic()
        assert await pool.start() == Err(ChildHookFailed())
        assert time.monotonic() - started < 5
        await pool.stop(0)

    async def test_boot_failure_is_bounded_and_named(self) -> None:
        pool = SlotPool(_config(init=_init('raises')), pending_limit=10)
        match await pool.start():
            case Err(BootAttemptsExhausted(attempts=3, last_detail=detail)):
                assert 'exit code 1' in detail
            case other:
                raise AssertionError(other)
        await pool.stop(0)

    async def test_transient_boot_failure_is_retried(self, tmp_path: Path) -> None:
        marks = tmp_path / 'marks'
        pool = SlotPool(
            _config(init=_init(f'fails_first:{marks}', ('2',))),
            pending_limit=10,
        )
        assert isinstance(await pool.start(), Ok)
        await pool.stop(0)
        assert marks.read_text() == 'xx'

    async def test_spawn_failure_is_typed(self) -> None:
        config = _config()
        pool = SlotPool(config, pending_limit=10)
        real_executable = sys.executable
        sys.executable = '/nonexistent/python'
        try:
            match await pool.start():
                case Err(SpawnFailed(detail=detail)):
                    assert 'FileNotFoundError' in detail
                case other:
                    raise AssertionError(other)
        finally:
            sys.executable = real_executable
        await pool.stop(0)

    async def test_child_program_receives_socket_and_parent_pid(self) -> None:
        child_entry = ('-c', 'import sys; sys.exit(0 if len(sys.argv) == 3 else 9)')
        pool = SlotPool(_config(child_entry=child_entry), pending_limit=10)
        match await pool.start():
            case Err(BootAttemptsExhausted(last_detail=detail)):
                assert 'exit code 0' in detail
            case other:
                raise AssertionError(other)
        await pool.stop(0)


@pytest.mark.asyncio(loop_scope='function')
class TestTaskRuns:
    async def test_result_is_returned(self, pool_factory: PoolFactory) -> None:
        pool = await pool_factory()
        handle = pool.submit(_frame('echo', 'hello'))
        match await _outcome(handle):
            case TaskFinished(ok=True, result_json='hello', failed_reason=None):
                pass
            case other:
                raise AssertionError(other)

    async def test_assigned_resolves_before_the_result(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        handle = pool.submit(_frame('sleep', '0.3'))
        assert await asyncio.wait_for(handle.assigned, 5) == Assigned()
        assert not handle.outcome.done()
        await _outcome(handle)

    async def test_queued_task_is_not_assigned_while_the_slot_is_busy(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        first = pool.submit(_frame('sleep', '0.3'))
        second = pool.submit(_frame('echo', 'x'))
        await asyncio.wait_for(first.assigned, 5)
        assert not second.assigned.done()
        await _outcome(first)
        await _outcome(second)

    async def test_tasks_run_in_submit_order_on_one_slot(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        handles = [pool.submit(_frame('echo', str(index))) for index in range(20)]
        results = [await _result_json(handle) for handle in handles]
        assert results == [str(index) for index in range(20)]

    async def test_large_body_round_trip(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        body = 'é' * (8 << 20)
        match await _outcome(pool.submit(_frame('echo', body))):
            case TaskFinished(result_json=result_json):
                assert result_json == body
            case other:
                raise AssertionError(other)

    async def test_parent_loop_stays_free_during_a_large_result(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        handle = pool.submit(_frame('echo', 'x' * (64 << 20)))
        worst = 0.0
        last = time.perf_counter()
        while not handle.outcome.done():
            await asyncio.sleep(0.001)
            now = time.perf_counter()
            worst = max(worst, now - last)
            last = now
        assert worst < 2.0

    async def test_child_program_restores_parent_argv_and_keeps_asserts(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        match await _outcome(pool.submit(_frame('state'))):
            case TaskFinished(result_json=result_json):
                assert result_json.endswith('|True')
            case other:
                raise AssertionError(other)

    async def test_terminate_works_on_a_task_subprocess(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        match await _outcome(pool.submit(_frame('sub_terminate'))):
            case TaskFinished(result_json=result_json):
                assert result_json == str(-signal.SIGTERM)
            case other:
                raise AssertionError(other)


@pytest.mark.asyncio(loop_scope='function')
class TestVerdicts:
    """Each verdict rule, with healthy siblings on other slots."""

    async def _assert_lost_with_siblings(
        self,
        pool_factory: PoolFactory,
        task_name: str,
        argument: str | None = None,
    ) -> ChildLost:
        pool = await pool_factory(_config(4))
        siblings = [pool.submit(_frame('sleep', '1.0')) for _ in range(3)]
        await asyncio.gather(*(item.assigned for item in siblings))
        lost = await _outcome(pool.submit(_frame(task_name, argument)))
        assert isinstance(lost, ChildLost), lost
        finished = [await _outcome(item) for item in siblings]
        assert all(isinstance(item, TaskFinished) for item in finished), finished
        return lost

    async def test_clean_exit_with_no_result_is_lost(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        lost = await self._assert_lost_with_siblings(pool_factory, 'exit_zero')
        assert lost.child_exit == ExitedWith(0)
        assert lost.cause is LossCause.CHILD_DIED

    async def test_sigkill_is_lost_with_the_signal(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        lost = await self._assert_lost_with_siblings(pool_factory, 'kill_self')
        assert lost.child_exit == KilledBySignal(signal.SIGKILL)

    @linux_only
    async def test_real_segfault_is_lost_with_the_signal(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        lost = await self._assert_lost_with_siblings(pool_factory, 'segv')
        assert lost.child_exit == KilledBySignal(signal.SIGSEGV)

    @linux_only
    async def test_abort_is_lost_with_the_signal(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        lost = await self._assert_lost_with_siblings(pool_factory, 'abort')
        assert lost.child_exit == KilledBySignal(signal.SIGABRT)

    async def test_exception_in_the_task_runner_is_lost(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        lost = await self._assert_lost_with_siblings(pool_factory, 'raises')
        assert lost.child_exit == ExitedWith(1)

    async def test_death_mid_frame_reports_a_partial_frame(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        lost = await self._assert_lost_with_siblings(pool_factory, 'die_mid_frame')
        assert lost.partial_frame is True

    async def test_garbage_kills_only_that_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        lost = await self._assert_lost_with_siblings(pool_factory, 'garbage')
        assert lost.cause is LossCause.PROTOCOL_VIOLATION

    async def test_malformed_metadata_kills_only_that_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        lost = await self._assert_lost_with_siblings(pool_factory, 'bad_json_frame')
        assert lost.cause is LossCause.PROTOCOL_VIOLATION

    async def test_result_for_another_task_kills_only_that_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        lost = await self._assert_lost_with_siblings(pool_factory, 'wrong_task_id')
        assert lost.cause is LossCause.PROTOCOL_VIOLATION

    async def test_complete_result_wins_over_the_death_after_it(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        match await _outcome(pool.submit(_frame('exit_after_result'))):
            case TaskFinished(result_json='complete'):
                pass
            case other:
                raise AssertionError(other)

    async def test_forked_helper_does_not_hide_a_death(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        handle = pool.submit(_frame('fork_helper_hang', '20'))
        await asyncio.wait_for(handle.assigned, 5)
        await asyncio.sleep(0.3)
        child = pool._slots[0].child
        assert child is not None
        started = time.monotonic()
        os.kill(child.pid, signal.SIGKILL)
        lost = await _outcome(handle)
        assert isinstance(lost, ChildLost)
        assert time.monotonic() - started < 2.0

    async def test_forked_helper_does_not_lose_a_finished_result(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        match await _outcome(pool.submit(_frame('fork_helper_result_then_die', '10'))):
            case TaskFinished(result_json='complete'):
                pass
            case other:
                raise AssertionError(other)

    async def _dead_child_with(self, unread: bytes) -> Child:
        """A child that is already gone, with ``unread`` waiting in its socket."""

        class ExitedProcess:
            pid = NO_SUCH_PID
            returncode = -signal.SIGKILL

            async def wait(self) -> int:
                return self.returncode

        parent_sock, child_sock = socket.socketpair()
        child = await Child._attach(
            cast('asyncio.subprocess.Process', ExitedProcess()),
            parent_sock,
            _init(),
        )
        await asyncio.sleep(0)
        assert child.has_exited
        child_sock.sendall(unread)
        self._peers.append(child_sock)
        return child

    _peers: ClassVar[list[socket.socket]] = []

    async def test_complete_frame_still_unread_wins_over_the_death(self) -> None:
        """Only the drain can find this frame: the transport has not read it yet."""
        encoded = encode_child_frame(ResultFrame('t', True, 'kept', None, RssUnreadable()))
        child = await self._dead_child_with(encoded.head + encoded.body)
        match await child.await_frame(None):
            case ResultFrame(result_json='kept'):
                pass
            case other:
                raise AssertionError(other)
        await child.dispose()

    async def test_half_a_frame_still_unread_is_reported_partial(self) -> None:
        encoded = encode_child_frame(ResultFrame('t', True, 'lost', None, RssUnreadable()))
        child = await self._dead_child_with(encoded.head[:-1])
        match await child.await_frame(None):
            case ChildLost(partial_frame=True, child_exit=KilledBySignal(9)):
                pass
            case other:
                raise AssertionError(other)
        await child.dispose()

    async def test_task_dispatched_to_a_dying_child_is_reported_lost(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        first = pool.submit(_frame('exit_after_result'))
        second = pool.submit(_frame('echo', 'x'))
        assert isinstance(await _outcome(first), TaskFinished)
        # Either the slot saw the exit first and replaced the child, or the
        # frame reached the dead child. The row decides what happens next.
        assert isinstance(await _outcome(second), (TaskFinished, ChildLost))


@pytest.mark.asyncio(loop_scope='function')
class TestKill:
    async def test_kill_reports_killed_by_parent(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        handle = pool.submit(_frame('hang'))
        await asyncio.wait_for(handle.assigned, 5)
        assert handle.kill() == KillSent()
        lost = await _outcome(handle)
        assert isinstance(lost, ChildLost)
        assert lost.cause is LossCause.KILLED_BY_PARENT

    async def test_kill_stops_code_that_holds_the_gil(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        handle = pool.submit(_frame('gil_hang'))
        await asyncio.wait_for(handle.assigned, 5)
        await asyncio.sleep(0.2)
        handle.kill()
        assert isinstance(await _outcome(handle), ChildLost)

    async def test_kill_after_the_result_touches_nothing(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """A late timer cannot reach the next task on the same slot."""
        pool = await pool_factory()
        finished = pool.submit(_frame('echo', 'x'))
        await _outcome(finished)
        running = pool.submit(_frame('sleep', '0.3'))
        await asyncio.wait_for(running.assigned, 5)
        assert finished.kill() == NothingToKill()
        assert isinstance(await _outcome(running), TaskFinished)

    async def test_kill_before_assignment_touches_nothing(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        running = pool.submit(_frame('sleep', '0.3'))
        queued = pool.submit(_frame('echo', 'x'))
        assert queued.kill() == NothingToKill()
        await _outcome(running)
        assert isinstance(await _outcome(queued), TaskFinished)

    async def test_kill_reaches_task_subprocesses(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
    ) -> None:
        pool = await pool_factory()
        marker = tmp_path / 'sub_pid'
        handle = pool.submit(_frame('spawn_sub_hang', str(marker)))
        await _until(lambda: marker.exists() and bool(marker.read_text()))
        sub_pid = int(marker.read_text())
        assert _alive(sub_pid)
        handle.kill()
        await _outcome(handle)
        await _until(lambda: not _alive(sub_pid))

    async def test_crash_cleans_up_task_subprocesses(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
    ) -> None:
        pool = await pool_factory()
        marker = tmp_path / 'sub_pid'
        handle = pool.submit(_frame('spawn_sub_hang', str(marker)))
        await _until(lambda: marker.exists() and bool(marker.read_text()))
        sub_pid = int(marker.read_text())
        child = pool._slots[0].child
        assert child is not None
        os.kill(child.pid, signal.SIGKILL)
        await _outcome(handle)
        await _until(lambda: not _alive(sub_pid))

    async def test_kill_works_on_a_stopped_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        handle = pool.submit(_frame('stop_mid_frame'))
        await asyncio.wait_for(handle.assigned, 5)
        await asyncio.sleep(0.2)
        handle.kill()
        lost = await _outcome(handle)
        assert isinstance(lost, ChildLost) and lost.partial_frame

    async def test_a_lost_channel_needs_a_bound_like_any_hang(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """A task that closes its own socket looks like a hang, on Linux."""
        pool = await pool_factory()
        handle = pool.submit(_frame('close_socket'))
        await asyncio.wait_for(handle.assigned, 5)
        await asyncio.sleep(0.3)
        if not handle.outcome.done():
            handle.kill()
        assert isinstance(await _outcome(handle), ChildLost)


@pytest.mark.asyncio(loop_scope='function')
class TestRecycle:
    async def test_task_count_replaces_the_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(max_tasks_per_child=2))
        handles = [pool.submit(_frame('pid')) for _ in range(5)]
        pids = [await _result_json(handle) for handle in handles]
        assert pids[0] == pids[1] != pids[2]
        assert pids[2] == pids[3] != pids[4]

    async def test_a_limit_of_one_runs_every_task_on_a_new_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(max_tasks_per_child=1))
        handles = [pool.submit(_frame('pid')) for _ in range(3)]
        assert len({await _result_json(handle) for handle in handles}) == 3

    async def test_memory_limit_replaces_the_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(max_memory_per_child_mb=1))
        handles = [pool.submit(_frame('pid')) for _ in range(2)]
        first, second = [await _result_json(handle) for handle in handles]
        assert first != second

    async def test_zero_limit_keeps_the_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(max_tasks_per_child=0))
        handles = [pool.submit(_frame('pid')) for _ in range(5)]
        assert len({await _result_json(handle) for handle in handles}) == 1

    async def test_recycle_does_not_kill_a_background_process_of_a_task(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory(_config(max_tasks_per_child=1))
        match await _outcome(pool.submit(_frame('spawn_sub'))):
            case TaskFinished(result_json=result_json):
                sub_pid = int(result_json)
            case other:
                raise AssertionError(other)
        await _outcome(pool.submit(_frame('pid')))
        assert _alive(sub_pid)
        os.kill(sub_pid, signal.SIGKILL)

    async def test_lost_idle_child_is_replaced(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        child = pool._slots[0].child
        assert child is not None
        old_pid = child.pid
        os.kill(old_pid, signal.SIGKILL)
        await _until(lambda: child.has_exited)
        match await _outcome(pool.submit(_frame('pid'))):
            case TaskFinished(result_json=result_json):
                assert result_json != str(old_pid)
            case ChildLost():
                # The frame beat the exit. Row status decides, and the next
                # task runs on a new child.
                match await _outcome(pool.submit(_frame('pid'))):
                    case TaskFinished(result_json=result_json):
                        assert result_json != str(old_pid)
                    case other:
                        raise AssertionError(other)
            case other:
                raise AssertionError(other)


@pytest.mark.asyncio(loop_scope='function')
class TestChildFaults:
    async def test_zero_length_header_kills_the_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        match await _outcome(pool.submit(_frame('zero_header'))):
            case ChildLost(cause=LossCause.PROTOCOL_VIOLATION):
                pass
            case other:
                raise AssertionError(other)

    async def test_long_failure_reason_keeps_the_result(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        match await _outcome(pool.submit(_frame('long_reason'))):
            case TaskFinished(result_json='kept', failed_reason=str() as reason):
                assert len(reason) < 70_000
                assert 'cut from' in reason
            case other:
                raise AssertionError(other)

    async def test_escaped_exception_ends_a_child_with_a_live_thread(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """A normal exit would wait for that thread, with no bound."""
        pool = await pool_factory()
        handle = pool.submit(_frame('raise_with_thread'))
        match await asyncio.wait_for(asyncio.shield(handle.outcome), 5):
            case ChildLost(child_exit=ExitedWith(1)):
                pass
            case other:
                raise AssertionError(other)


@pytest.mark.asyncio(loop_scope='function')
class TestSlotDefects:
    """A frame that cannot be encoded is a defect in the slot's path."""

    @staticmethod
    def _unsendable() -> RunFrame:
        return _frame('echo', 'bad \ud800 text')

    async def test_a_defect_costs_only_its_task(self, pool_factory: PoolFactory) -> None:
        pool = await pool_factory()
        first = pool._slots[0].child
        assert first is not None
        match await _outcome(pool.submit(self._unsendable())):
            case ChildLost(cause=LossCause.SLOT_DEFECT):
                pass
            case other:
                raise AssertionError(other)
        assert not _alive(first.pid)
        assert await _result_json(pool.submit(_frame('echo', 'next'))) == 'next'

    async def test_repeated_defects_fail_the_pool(self, pool_factory: PoolFactory) -> None:
        pool = await pool_factory()
        for _ in range(SLOT_DEFECT_LIMIT):
            await _outcome(pool.submit(self._unsendable()))
        failure = await asyncio.wait_for(pool.wait_failed(), OUTCOME_TIMEOUT_S)
        assert isinstance(failure, BootAttemptsExhausted)

    async def test_a_finished_task_clears_the_count(self, pool_factory: PoolFactory) -> None:
        pool = await pool_factory()
        for _ in range(3):
            for _ in range(SLOT_DEFECT_LIMIT - 1):
                await _outcome(pool.submit(self._unsendable()))
            assert await _result_json(pool.submit(_frame('echo', 'ok'))) == 'ok'


@pytest.mark.asyncio(loop_scope='function')
class TestBootKills:
    """An outside SIGKILL on a replacement child says nothing about the code."""

    async def test_kills_of_a_replacement_do_not_stop_the_pool(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(slots, 'CHILD_BOOT_RETRY_DELAY_S', 0.01)
        kills = CHILD_BOOT_ATTEMPTS + 2
        marks = tmp_path / 'marks'
        init = _init(f'killed_after_first:{marks}', (str(kills),))
        pool = await pool_factory(_config(init=init, max_tasks_per_child=1))

        assert await _result_json(pool.submit(_frame('echo', 'first'))) == 'first'
        assert await _result_json(pool.submit(_frame('echo', 'next'))) == 'next'

        assert len(marks.read_text()) == kills + 2
        assert pool._failure is None

    async def test_a_kill_during_the_first_start_still_counts(self) -> None:
        pool = SlotPool(_config(init=_init('killed_always')), pending_limit=10)
        match await asyncio.wait_for(pool.start(), OUTCOME_TIMEOUT_S):
            case Err(BootAttemptsExhausted(attempts=attempts)):
                assert attempts == CHILD_BOOT_ATTEMPTS
            case other:
                raise AssertionError(other)
        await pool.stop(0)

    async def test_a_replacement_that_cannot_boot_still_counts(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(slots, 'CHILD_BOOT_RETRY_DELAY_S', 0.01)
        init = _init(f'fails_after_first:{tmp_path / "marks"}', ('100',))
        pool = await pool_factory(_config(init=init, max_tasks_per_child=1))

        await _outcome(pool.submit(_frame('echo', 'first')))

        failure = await asyncio.wait_for(pool.wait_failed(), OUTCOME_TIMEOUT_S)
        assert isinstance(failure, BootAttemptsExhausted)

    async def test_the_wait_grows_and_stays_bounded(self) -> None:
        assert outside_kill_delay_s(1) < outside_kill_delay_s(2)
        assert outside_kill_delay_s(10_000) == CHILD_BOOT_KILL_DELAY_MAX_S


@pytest.mark.asyncio(loop_scope='function')
class TestStopDuringBoot:
    @staticmethod
    async def _booting_child(pool: SlotPool) -> int:
        for _ in range(1000):
            child = pool._slots[0].child
            if child is not None:
                return child.pid
            await asyncio.sleep(0.01)
        raise AssertionError('no child started')

    async def test_stop_ends_the_child_and_the_start(self) -> None:
        pool = SlotPool(_config(init=_init('hangs')), pending_limit=10)
        starting = asyncio.ensure_future(pool.start())
        pid = await self._booting_child(pool)

        await pool.stop(0)

        assert await asyncio.wait_for(starting, 5) == Err(PoolStopped())
        assert not _alive(pid)
        assert pool._slots[0].child is None

    async def test_stop_starts_no_further_child(self) -> None:
        """Each new child would otherwise leak on a start retry."""
        pool = SlotPool(_config(init=_init('hangs')), pending_limit=10)
        starting = asyncio.ensure_future(pool.start())
        first = await self._booting_child(pool)

        await pool.stop(30)

        await asyncio.wait_for(starting, 5)
        assert not _alive(first)
        assert all(task.done() for task in pool._tasks)
        assert pool._slots[0].child is None

    async def test_a_replaced_child_does_not_count_as_another_slot(self) -> None:
        pool = SlotPool(_config(2), pending_limit=10)
        baseline = ChildBaseline(pid=1, rss=RssUnreadable())
        pool._note_ready(pool._slots[0], baseline)
        pool._note_ready(pool._slots[0], baseline)
        assert not pool._first_generation.done()
        pool._note_ready(pool._slots[1], baseline)
        assert pool._first_generation.done()


@pytest.mark.asyncio(loop_scope='function')
class TestStates:
    async def test_states_follow_the_task(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        assert pool.states() == (Idle(),)
        frame = _frame('sleep', '0.3')
        handle = pool.submit(frame)
        await asyncio.wait_for(handle.assigned, 5)
        assert pool.states() == (Busy(frame.task_id),)
        await _outcome(handle)


@pytest.mark.asyncio(loop_scope='function')
class TestStop:
    async def test_queued_tasks_are_released_at_once(self) -> None:
        pool = await _started_pool(_config())
        running = pool.submit(_frame('sleep', '0.5'))
        queued = pool.submit(_frame('echo', 'x'))
        await asyncio.wait_for(running.assigned, 5)
        stopping = asyncio.ensure_future(pool.stop(10))
        await asyncio.wait_for(queued.outcome, 1)
        assert queued.outcome.result() == NotDispatched(NotRunCause.POOL_STOPPING)
        assert queued.assigned.result() == NotDispatched(NotRunCause.POOL_STOPPING)
        assert isinstance(await _outcome(running), TaskFinished)
        report = await stopping
        assert report.killed_task_ids == ()

    async def test_drain_deadline_kills_and_reports(self) -> None:
        pool = await _started_pool(_config())
        frame = _frame('hang')
        handle = pool.submit(frame)
        await asyncio.wait_for(handle.assigned, 5)
        report = await pool.stop(0.2)
        assert report.killed_task_ids == (frame.task_id,)
        lost = await _outcome(handle)
        assert isinstance(lost, ChildLost)
        assert lost.cause is LossCause.KILLED_BY_PARENT

    async def test_queue_past_its_limit_refuses_the_task(self) -> None:
        pool = SlotPool(_config(), pending_limit=1)
        assert isinstance(await pool.start(), Ok)
        running = pool.submit(_frame('sleep', '0.3'))
        await asyncio.wait_for(running.assigned, 5)
        queued = pool.submit(_frame('echo', 'x'))
        refused = pool.submit(_frame('echo', 'y'))
        assert refused.outcome.result() == NotDispatched(NotRunCause.QUEUE_FULL)
        assert isinstance(await _outcome(queued), TaskFinished)
        await pool.stop(5)

    async def test_submit_after_stop_is_not_dispatched(self) -> None:
        pool = await _started_pool(_config())
        await pool.stop(5)
        handle = pool.submit(_frame('echo', 'x'))
        assert handle.outcome.result() == NotDispatched(NotRunCause.POOL_STOPPING)

    async def test_stop_leaves_no_child_running(self) -> None:
        pool = await _started_pool(_config(3))
        pids = [slot.child.pid for slot in pool._slots if slot.child]
        assert len(pids) == 3
        await pool.stop(5)
        await _until(lambda: not any(_alive(pid) for pid in pids))

    async def test_sigterm_to_the_child_does_not_stop_a_task(self) -> None:
        pool = await _started_pool(_config())
        handle = pool.submit(_frame('ignore_term_sleep', '0.6'))
        await asyncio.wait_for(handle.assigned, 5)
        await asyncio.sleep(0.2)
        child = pool._slots[0].child
        assert child is not None
        os.kill(child.pid, signal.SIGTERM)
        match await _outcome(handle):
            case TaskFinished(result_json='survived'):
                pass
            case other:
                raise AssertionError(other)
        await pool.stop(5)


@pytest.mark.asyncio(loop_scope='function')
class TestPoolFailure:
    async def test_replacement_failure_is_loud_and_releases_queued_tasks(
        self,
        tmp_path: Path,
    ) -> None:
        pool = await _started_pool(_config(max_tasks_per_child=1))
        real_executable = sys.executable
        finished = pool.submit(_frame('echo', 'x'))
        queued = pool.submit(_frame('echo', 'y'))
        assert isinstance(await _outcome(finished), TaskFinished)
        sys.executable = '/nonexistent/python'
        try:
            failure = await asyncio.wait_for(pool.wait_failed(), 15)
        finally:
            sys.executable = real_executable
        assert isinstance(failure, SpawnFailed)
        outcome = await _outcome(queued)
        assert outcome == NotDispatched(NotRunCause.POOL_FAILED) or isinstance(
            outcome,
            TaskFinished,
        )
        assert pool.submit(_frame('echo', 'z')).outcome.result() == NotDispatched(
            NotRunCause.POOL_FAILED,
        )
        await pool.stop(0)


PARENT_PROGRAM = """
import asyncio, os, sys, time
sys.path[:0] = {paths!r}
from horsies.core.worker.child_protocol import InitFrame, RunFrame
from horsies.core.worker.slots import SlotPool, SlotPoolConfig

async def main():
    init = InitFrame('ok', (), (), tuple(sys.path), ('p',), 30, 'x', {{}}, 0, 1, False, 'c')
    pool = SlotPool(SlotPoolConfig(1, init, 0, None, ({child!r},)), 10)
    await pool.start()
    pool.submit(RunFrame('t', {task!r}, None, None, 'w', 30000, False, None, None))
    await asyncio.sleep(0.3)
    print(pool._slots[0].child.pid, flush=True)
    await asyncio.sleep(60)

asyncio.run(main())
"""


class TestParentDeath:
    def _kill_parent(self, task_name: str) -> float:
        code = PARENT_PROGRAM.format(
            paths=sys.path,
            child=SCRIPTED_CHILD,
            task=task_name,
        )
        parent = subprocess.Popen(
            [sys.executable, '-c', code],
            stdout=subprocess.PIPE,
            text=True,
        )
        assert parent.stdout is not None
        child_pid = int(parent.stdout.readline())
        assert _alive(child_pid)
        started = time.monotonic()
        parent.kill()
        parent.wait()
        while _alive(child_pid) and time.monotonic() - started < 10:
            time.sleep(0.01)
        assert not _alive(child_pid)
        return time.monotonic() - started

    def test_child_exits_at_once_while_the_task_runs(self) -> None:
        assert self._kill_parent('hang') < 3.0

    @pytest.mark.skipif(sys.platform != 'linux', reason='needs PR_SET_PDEATHSIG')
    def test_child_exits_while_c_code_holds_the_gil(self) -> None:
        assert self._kill_parent('gil_hang') < 3.0


@pytest.mark.asyncio(loop_scope='function')
class TestChaos:
    """A seeded mix. Every task must get the outcome its operation predicts."""

    OPERATIONS = ('exit_zero', 'kill_self', 'die_mid_frame', 'garbage', 'exit_after_result')

    @staticmethod
    def _predicted(name: str, outcome: object) -> bool:
        match name, outcome:
            case 'exit_zero', ChildLost(child_exit=ExitedWith(0)):
                return True
            case 'kill_self', ChildLost(child_exit=KilledBySignal(9)):
                return True
            case 'die_mid_frame', ChildLost(partial_frame=True):
                return True
            case 'garbage', ChildLost(cause=LossCause.PROTOCOL_VIOLATION):
                return True
            case 'exit_after_result', TaskFinished(result_json='complete'):
                return True
            case _:
                return False

    @pytest.mark.parametrize('seed', [1, 2])
    async def test_every_task_gets_its_predicted_outcome(self, seed: int) -> None:
        rng = random.Random(seed)
        pool = await _started_pool(_config(4, max_tasks_per_child=25))
        fds_before = len(os.listdir('/dev/fd'))
        threads_before = threading.active_count()
        submitted: list[tuple[str, SlotHandle, str]] = []
        for index in range(400):
            if rng.random() < 0.25:
                name = rng.choice(self.OPERATIONS)
                submitted.append((name, pool.submit(_frame(name)), ''))
            else:
                body = str(index) * rng.choice([1, 100, 20_000])
                submitted.append(('echo', pool.submit(_frame('echo', body)), body))
        counts: Counter[str] = Counter()
        wrong: list[str] = []
        dead_child_retries = 0
        for name, handle, body in submitted:
            outcome = await asyncio.wait_for(asyncio.shield(handle.outcome), 60)
            if name == 'echo':
                if isinstance(outcome, ChildLost):
                    # Dispatched to a child that had just exited. Its row is
                    # still CLAIMED, so the caller requeues it.
                    dead_child_retries += 1
                elif not (isinstance(outcome, TaskFinished) and outcome.result_json == body):
                    wrong.append(f'echo: {outcome!r}'[:120])
            else:
                if isinstance(outcome, ChildLost) and outcome.child_exit == ExitedWith(1):
                    dead_child_retries += 1
                elif not self._predicted(name, outcome):
                    wrong.append(f'{name}: {outcome!r}'[:120])
            counts[name] += 1
        assert wrong == []
        assert sum(counts.values()) == 400
        assert dead_child_retries <= counts['exit_after_result']
        await _until(lambda: all(isinstance(state, Idle) for state in pool.states()))
        assert threading.active_count() <= threads_before + 1
        assert len(os.listdir('/dev/fd')) <= fds_before + 2
        await pool.stop(5)

