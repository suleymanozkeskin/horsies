"""Slot pool tests with real child processes and no database.

Crash operations that open a macOS crash dialog (SIGSEGV, abort) run only on
Linux. On macOS the same verdicts are covered through SIGKILL and ``os._exit``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import errno
import logging
import os
import random
import resource
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import Counter, deque
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, ClassVar, cast
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from horsies.core.types.result import Err, Ok
from horsies.core.models.boot_failure import (
    AppLoadFailed,
    ChildPoolSetupFailed,
    ResourceShortage,
    ShortageErrno,
    SpawnFailed,
    TaskLoopCreateFailed,
    ThreadStartRefused,
)
from horsies.core.worker.child_main import (
    BAD_PARENT_FRAME_EXIT_CODE,
    BOOT_FAILED_EXIT_CODE,
    STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE,
)
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.preload_main import REPARENT_FAILED_EXIT_CODE
from horsies.core.models.worker_profile import ResolvedPool
from horsies.core.worker.child_protocol import (
    FRAME_HEADER,
    BodyReturnedFrame,
    BootFailedFrame,
    IsolatedChild,
    ChildFrame,
    InitFrame,
    LeftWorkEnd,
    LeftWorkEndedFrame,
    LeftWorkStuckFrame,
    LoopHoldsNoTask,
    LoopHoldsTasks,
    MalformedFrame,
    NoTaskLoop,
    ReadyFrame,
    ResultFrame,
    RssUnreadable,
    RunFrame,
    SeatFinished,
    StopFrame,
    encode_child_frame,
    encode_parent_frame,
)
from horsies.core.worker.stored_timeout import NoTimeout, TaskTimeout
from horsies.core.worker.task_loop import ISOLATED_LEFTOVER_GRACE_S
from horsies.core.worker.worker import Worker, WorkerConfig
from horsies.core.worker import slots
from horsies.core.worker.slots import (
    CHILD_BOOT_ATTEMPTS,
    DIRECT_BODY_BYTES,
    FRAME_READ_BUFFER_BYTES,
    JOINED_WRITE_BODY_BYTES,
    CHILD_BOOT_KILL_DELAY_MAX_S,
    SLOT_DEFECT_LIMIT,
    Assigned,
    BootAttemptsExhausted,
    BootFailure,
    Busy,
    Child,
    ChildBaseline,
    ChildEnded,
    ChildEndUnknown,
    ChildHookFailed,
    ChildLost,
    Down,
    ExitedWith,
    ExitUnknown,
    FrameProtocol,
    Idle,
    KilledBySignal,
    KillSent,
    LossCause,
    LostAfterReturn,
    LostBeforeReady,
    NotDispatched,
    NothingToKill,
    NotRunCause,
    PoolStopped,
    ReturnSeen,
    SlotAnswered,
    SlotDefectsRepeated,
    SlotHandle,
    SlotOutcome,
    SlotPool,
    SlotPoolConfig,
    Starting,
    TaskFinished,
    child_exit_from_returncode,
    describe_exit,
    interpreter_flags,
    outside_kill_delay_s,
)

SCRIPTED_CHILD = str(Path(__file__).parent.parent / 'fixtures' / 'slots' / 'scripted_child.py')
SCRIPTED_PRELOAD = str(Path(__file__).parent.parent / 'fixtures' / 'slots' / 'scripted_preload.py')
linux_only = pytest.mark.skipif(
    sys.platform != 'linux',
    reason='opens a crash dialog on macOS',
)
OUTCOME_TIMEOUT_S = 30.0
NO_SUCH_PID = 2**30
PoolFactory = Callable[..., Awaitable[SlotPool]]


@dataclass(slots=True)
class _StartPath:
    mode: ChildPreload = ChildPreload.NONE


_start_path = _StartPath()


@pytest.fixture(
    autouse=True,
    params=[ChildPreload.NONE, ChildPreload.LIBRARY],
    ids=['exec', 'preload'],
)
def start_path(request: pytest.FixtureRequest) -> Iterator[None]:
    """Run every test here on both start paths. ``preload`` needs Linux."""
    if request.param is ChildPreload.LIBRARY and sys.platform != 'linux':
        pytest.skip('preload is Linux only')
    _start_path.mode = request.param
    yield
    _start_path.mode = ChildPreload.NONE


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
        process_name='test-child',
        child_kind=IsolatedChild(
            pool=ResolvedPool(min_size=0, max_size=1, check=False),
            task_send_pool=2,
        ),
    )


def _config(
    slots: int = 1,
    *,
    init: InitFrame | None = None,
    max_tasks_per_child: int = 0,
    max_memory_per_child_mb: int | None = None,
    child_entry: tuple[str, ...] | None = None,
) -> SlotPoolConfig:
    """Build a config. An explicit ``child_entry`` tests that program, so ``exec``."""
    return SlotPoolConfig(
        slots=slots,
        init=init or _init(),
        max_tasks_per_child=max_tasks_per_child,
        max_memory_per_child_mb=max_memory_per_child_mb,
        child_entry=child_entry or (SCRIPTED_CHILD,),
        child_preload=ChildPreload.NONE if child_entry else _start_path.mode,
        preload_entry=(SCRIPTED_PRELOAD,),
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
        is_workflow_task=False,
        timeout=NoTimeout(),
        claimed_at=None,
    )


async def _outcome(handle: SlotHandle) -> SlotOutcome:
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


EMFILE_DETAIL = 'OSError: [Errno 24] Too many open files'
EMFILE_DOWN_TEXT = f'the OS ran short of a resource (EMFILE): {EMFILE_DETAIL}'


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

            def is_closing(self) -> bool:
                return bool(aborted)

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
        encoded = encode_child_frame(ResultFrame('t', True, body, None, RssUnreadable(), NoTaskLoop()))
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

    def _results(self, frames: list[object]) -> list[str]:
        return [cast(ResultFrame, frame).result_json for frame in frames]

    def test_every_complete_frame_of_one_read_is_delivered_in_that_read(self) -> None:
        protocol, frames, _, _ = self._protocol()
        data = b''.join(self._result_bytes(f'r{index}') for index in range(5))
        assert len(data) <= FRAME_READ_BUFFER_BYTES

        target = protocol.get_buffer(-1)
        target[: len(data)] = data
        protocol.buffer_updated(len(data))

        assert self._results(frames) == [f'r{index}' for index in range(5)]
        assert not protocol.has_partial_frame()

    @pytest.mark.parametrize('chunk', [1, 7, 4096, 1 << 20])
    def test_a_large_body_arrives_whole_at_any_read_size(self, chunk: int) -> None:
        protocol, frames, _, aborted = self._protocol()
        large = 'x' * (DIRECT_BODY_BYTES * 3 + 17)

        self._feed(protocol, self._result_bytes(large), chunk)

        assert self._results(frames) == [large]
        assert aborted == []
        assert not protocol.has_partial_frame()

    def test_small_and_large_frames_keep_their_order(self) -> None:
        protocol, frames, _, _ = self._protocol()
        large = 'y' * (DIRECT_BODY_BYTES + 1)
        data = (
            self._result_bytes('first')
            + self._result_bytes(large)
            + self._result_bytes('last')
        )

        self._feed(protocol, data, 4096)

        assert self._results(frames) == ['first', large, 'last']

    def test_a_large_body_does_not_grow_the_read_buffer(self) -> None:
        protocol, frames, _, _ = self._protocol()
        self._feed(protocol, self._result_bytes('z' * (DIRECT_BODY_BYTES * 4)), 4096)
        assert len(frames) == 1

        free = protocol.get_buffer(-1)

        assert len(free) <= FRAME_READ_BUFFER_BYTES

    def test_a_partial_large_body_is_reported(self) -> None:
        protocol, frames, _, _ = self._protocol()
        data = self._result_bytes('w' * (DIRECT_BODY_BYTES * 2))

        self._feed(protocol, data[:-1], 4096)

        assert frames == []
        assert protocol.has_partial_frame()

    def test_a_frame_that_closes_the_channel_ends_delivery_of_that_read(self) -> None:
        frames: list[object] = []
        aborted: list[bool] = []

        class FakeTransport(asyncio.Transport):
            def abort(self) -> None:
                aborted.append(True)

            def is_closing(self) -> bool:
                return bool(aborted)

        transport = FakeTransport()

        def on_frame(frame: object) -> None:
            frames.append(frame)
            transport.abort()

        protocol = FrameProtocol(on_frame, lambda: None)
        protocol.connection_made(transport)
        data = b''.join(self._result_bytes(f'r{index}') for index in range(3))

        self._feed(protocol, data, 1 << 20)

        assert self._results(frames) == ['r0']

    def test_a_malformed_frame_after_a_good_one_aborts_after_the_good_one(self) -> None:
        protocol, frames, _, aborted = self._protocol()
        bad_meta = b'not json'
        data = self._result_bytes('good') + FRAME_HEADER.pack(len(bad_meta), 0) + bad_meta

        self._feed(protocol, data, 1 << 20)

        assert self._results(frames) == ['good']
        assert aborted == [True]
        assert isinstance(protocol.violation, MalformedFrame)


@pytest.mark.asyncio(loop_scope='function')
class TestChildSend:
    """How a parent frame reaches the transport of a child channel."""

    class _RecordingTransport(asyncio.Transport):
        def __init__(self) -> None:
            super().__init__()
            self.writes: list[bytes] = []

        def write(self, data: bytes | bytearray | memoryview) -> None:
            self.writes.append(bytes(data))

        def is_closing(self) -> bool:
            return False

    class _NeverExits:
        pid = 4242

        async def wait(self) -> int:
            await asyncio.Event().wait()
            return 0

    def _child(self, transport: asyncio.Transport) -> Child:
        protocol = FrameProtocol(lambda _frame: None, lambda: None)
        return Child(
            cast(Any, self._NeverExits()),
            protocol,
            transport,
            deque(),
            asyncio.Event(),
        )

    def _run_frame(self, args_bytes: int) -> RunFrame:
        return RunFrame(
            task_id='task-a',
            task_name='send_test',
            args_json='[' + '1,' * (args_bytes // 2) + '1]',
            kwargs_json=None,
            worker_id='worker-1',
            is_workflow_task=False,
            timeout=NoTimeout(),
            claimed_at=None,
        )

    async def test_a_frame_with_a_small_body_is_one_write(self) -> None:
        transport = self._RecordingTransport()
        child = self._child(transport)
        frame = self._run_frame(64)

        child.send(frame)

        encoded = encode_parent_frame(frame)
        assert transport.writes == [encoded.head + encoded.body]

    async def test_a_large_body_goes_in_a_write_of_its_own(self) -> None:
        transport = self._RecordingTransport()
        child = self._child(transport)
        frame = self._run_frame(JOINED_WRITE_BODY_BYTES + 10)

        child.send(frame)

        encoded = encode_parent_frame(frame)
        assert transport.writes == [encoded.head, encoded.body]


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
            case Err(
                BootAttemptsExhausted(
                    attempts=3,
                    cause=LostBeforeReady(child_exit=ExitedWith(1)),
                    detail=detail,
                ),
            ):
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

    async def test_a_child_that_cannot_create_its_task_loop_reports_it_and_exits(
        self,
    ) -> None:
        """The frame comes in place of the ready frame, so the child never
        takes a task. This drives one child directly, to read the frame and
        the exit code that the pool sees as one failed boot attempt."""
        config = _config(init=_init('loop_create_fails'))
        match await Child.start(config, config.init):
            case Ok(child):
                pass
            case Err(failed):
                raise AssertionError(failed)
        try:
            frame = await child.await_frame(OUTCOME_TIMEOUT_S)
            assert frame == BootFailedFrame(
                cause=ResourceShortage(ShortageErrno.EMFILE),
                error_name='OSError',
                detail='[Errno 24] Too many open files',
            )
            await _until(lambda: child.has_exited)
            exited = ExitedWith(BOOT_FAILED_EXIT_CODE)
            assert child._current_exit() == exited
        finally:
            await child.dispose_lost()

    async def test_a_task_loop_failure_on_the_only_slot_uses_every_attempt(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """EMFILE can clear, but no other slot can serve, so each failed
        boot counts, and the log line of each names the slot, the cause, the
        count and the next wait."""
        monkeypatch.setattr(slots, 'SLOT_DOWN_FIRST_DELAY_S', 0.01)
        pool = SlotPool(_config(init=_init('loop_create_fails')), pending_limit=10)
        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            started = await pool.start()
        await pool.stop(0)

        assert started == Err(
            BootAttemptsExhausted(
                attempts=CHILD_BOOT_ATTEMPTS,
                cause=ResourceShortage(ShortageErrno.EMFILE),
                detail=EMFILE_DETAIL,
            ),
        )
        down_lines = [
            record.getMessage()
            for record in caplog.records
            if record.getMessage().startswith('Slot 0 is down')
        ]
        assert down_lines == [
            f'Slot 0 is down: {EMFILE_DOWN_TEXT}. Failed boots: {number}, '
            f'counted attempts: {number}/{CHILD_BOOT_ATTEMPTS}. '
            f'Next boot in {slots.slot_down_delay_s(number):.1f}s'
            for number in range(1, CHILD_BOOT_ATTEMPTS + 1)
        ]

    async def test_no_task_reaches_a_child_that_has_no_task_loop(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A task that waits for the slot is released unrun when the pool fails."""
        monkeypatch.setattr(slots, 'SLOT_DOWN_FIRST_DELAY_S', 0.01)
        pool = SlotPool(_config(init=_init('loop_create_fails')), pending_limit=10)
        handle = pool.submit(_frame('echo', 'x'))
        started = await pool.start()
        await pool.stop(0)

        assert isinstance(started, Err)
        assert handle.assigned.result() == NotDispatched(NotRunCause.POOL_FAILED)
        assert await _outcome(handle) == NotDispatched(NotRunCause.POOL_FAILED)

    async def test_a_transient_task_loop_failure_at_boot_is_retried(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The first boot cannot create the loop. The second boot can, and
        its child runs on that loop."""
        monkeypatch.setattr(slots, 'SLOT_DOWN_FIRST_DELAY_S', 0.01)
        marks = tmp_path / 'marks'
        pool = SlotPool(
            _config(init=_init(f'loop_fails_first:{marks}', ('1',))),
            pending_limit=10,
        )
        try:
            assert isinstance(await pool.start(), Ok)
            assert await _result_json(pool.submit(_frame('loop_state'))) == 'LoopOpen'
        finally:
            await pool.stop(0)
        assert marks.read_text() == 'xx'

    async def test_a_replacement_on_the_only_slot_that_cannot_create_its_loop_fails_the_pool(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The first child serves a task and retires. Its replacement cannot
        create the loop. No other slot can serve, so the failed boots count,
        and the next task never reaches a child."""
        monkeypatch.setattr(slots, 'SLOT_DOWN_FIRST_DELAY_S', 0.01)
        init = _init(f'loop_fails_after_first:{tmp_path / "marks"}', ('100', '1'))
        pool = await _started_pool(_config(init=init, max_tasks_per_child=1))
        try:
            assert await _result_json(pool.submit(_frame('echo', 'first'))) == 'first'
            waiting = pool.submit(_frame('echo', 'next'))

            failure = await asyncio.wait_for(pool.wait_failed(), OUTCOME_TIMEOUT_S)

            assert failure == BootAttemptsExhausted(
                attempts=CHILD_BOOT_ATTEMPTS,
                cause=ResourceShortage(ShortageErrno.EMFILE),
                detail=EMFILE_DETAIL,
            )
            assert await _outcome(waiting) == NotDispatched(NotRunCause.POOL_FAILED)
        finally:
            await pool.stop(0)

    async def test_a_spawn_of_a_missing_interpreter_counts(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """ENOENT is not in the errno table, so the cause repeats."""
        monkeypatch.setattr(slots, 'SLOT_DOWN_FIRST_DELAY_S', 0.01)
        pool = SlotPool(_config(), pending_limit=10)
        real_executable = sys.executable
        sys.executable = '/nonexistent/python'
        try:
            match await pool.start():
                case Err(
                    BootAttemptsExhausted(
                        attempts=slots.CHILD_BOOT_ATTEMPTS,
                        cause=SpawnFailed(),
                        detail=detail,
                    ),
                ):
                    assert 'FileNotFoundError' in detail
                case other:
                    raise AssertionError(other)
        finally:
            sys.executable = real_executable
        await pool.stop(0)

    async def test_a_missing_interpreter_stops_the_worker_while_another_slot_serves(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Slot 1 replaces its child after the interpreter path went away.
        ENOENT fails every spawn the same way, so the failed spawns count
        although slot 0 serves, and the pool fails instead of keeping slot 1
        down for the life of the worker."""
        monkeypatch.setattr(slots, 'CHILD_BOOT_RETRY_DELAY_S', 0.01)
        pool = await _started_pool(_config(2, child_entry=(SCRIPTED_CHILD,)))
        try:
            replaced = pool._slots[1].child
            assert replaced is not None
            monkeypatch.setattr(sys, 'executable', '/nonexistent/python')
            os.kill(replaced.pid, signal.SIGKILL)

            failure = await asyncio.wait_for(pool.wait_failed(), OUTCOME_TIMEOUT_S)
            monkeypatch.undo()

            match failure:
                case BootAttemptsExhausted(
                    attempts=slots.CHILD_BOOT_ATTEMPTS,
                    cause=SpawnFailed(),
                    detail=detail,
                ):
                    assert detail.startswith('FileNotFoundError:')
                case other:
                    raise AssertionError(other)
        finally:
            await pool.stop(0)

    async def test_a_loop_policy_that_raises_stops_the_worker_while_another_slot_serves(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The event loop policy of slot 1 raises on every boot. That cause
        repeats, so its failed boots count although slot 0 serves."""
        monkeypatch.setattr(slots, 'CHILD_BOOT_RETRY_DELAY_S', 0.01)
        pool = SlotPool(
            _config(2, init=_init('loop_policy_refuses_on_slot:1')),
            pending_limit=10,
        )
        try:
            started = await asyncio.wait_for(pool.start(), OUTCOME_TIMEOUT_S)

            assert started == Err(
                BootAttemptsExhausted(
                    attempts=CHILD_BOOT_ATTEMPTS,
                    cause=TaskLoopCreateFailed(),
                    detail='NotImplementedError: this policy cannot build a loop here',
                ),
            )
        finally:
            await pool.stop(0)

    async def test_child_program_receives_socket_and_parent_pid(self) -> None:
        child_entry = ('-c', 'import sys; sys.exit(0 if len(sys.argv) == 3 else 9)')
        pool = SlotPool(_config(child_entry=child_entry), pending_limit=10)
        match await pool.start():
            case Err(BootAttemptsExhausted(detail=detail)):
                assert 'exit code 0' in detail
            case other:
                raise AssertionError(other)
        await pool.stop(0)


SHORT_OF_DESCRIPTORS = ResourceShortage(ShortageErrno.EMFILE)


def _down_lines(caplog: pytest.LogCaptureFixture, slot: int) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith(f'Slot {slot} is down')
    ]


@pytest.mark.asyncio(loop_scope='function')
class TestBootCauses:
    """The cause of a failed boot decides whether the worker stops."""

    async def test_a_reported_app_load_failure_repeats_and_stops_the_pool(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(slots, 'CHILD_BOOT_RETRY_DELAY_S', 0.01)
        pool = SlotPool(_config(2, init=_init('report_app_load')), pending_limit=10)
        started = await asyncio.wait_for(pool.start(), OUTCOME_TIMEOUT_S)
        await pool.stop(0)

        assert started == Err(
            BootAttemptsExhausted(
                CHILD_BOOT_ATTEMPTS,
                AppLoadFailed(),
                'ImportError: scripted',
            ),
        )

    async def test_a_reported_pool_setup_failure_repeats_and_stops_the_pool(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(slots, 'CHILD_BOOT_RETRY_DELAY_S', 0.01)
        pool = SlotPool(_config(init=_init('report_pool_setup')), pending_limit=10)
        started = await asyncio.wait_for(pool.start(), OUTCOME_TIMEOUT_S)
        await pool.stop(0)

        assert started == Err(
            BootAttemptsExhausted(
                CHILD_BOOT_ATTEMPTS,
                ChildPoolSetupFailed(),
                'ValueError: scripted',
            ),
        )

    async def test_a_death_without_a_frame_counts_while_another_child_serves(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A replacement that dies with no boot failure frame names no cause,
        so its failed boots count as a cause that repeats, also while the
        other slot serves."""
        monkeypatch.setattr(slots, 'CHILD_BOOT_RETRY_DELAY_S', 0.01)
        init = _init(f'fails_after_first:{tmp_path / "marks"}', ('100', '2'))
        pool = await _started_pool(_config(2, init=init, max_tasks_per_child=1))
        try:
            await _outcome(pool.submit(_frame('echo', 'first')))
            failure = await asyncio.wait_for(pool.wait_failed(), OUTCOME_TIMEOUT_S)
        finally:
            await pool.stop(0)

        match failure:
            case BootAttemptsExhausted(
                attempts=slots.CHILD_BOOT_ATTEMPTS,
                cause=LostBeforeReady(
                    loss=LossCause.CHILD_DIED,
                    child_exit=ExitedWith(1),
                ),
            ):
                pass
            case other:
                raise AssertionError(other)

    async def test_a_refused_control_thread_is_reported(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The child cannot start the thread that reads parent frames. It
        reports the cause, and the only slot uses every attempt."""
        monkeypatch.setattr(slots, 'SLOT_DOWN_FIRST_DELAY_S', 0.01)
        monkeypatch.setenv('HORSIES_TEST_REFUSE_THREADS', '1')
        pool = SlotPool(_config(child_entry=(SCRIPTED_CHILD,)), pending_limit=10)
        started = await asyncio.wait_for(pool.start(), OUTCOME_TIMEOUT_S)
        await pool.stop(0)

        match started:
            case Err(BootAttemptsExhausted(cause=ThreadStartRefused(), detail=detail)):
                assert detail == "RuntimeError: can't start new thread"
            case other:
                raise AssertionError(other)


@pytest.mark.asyncio(loop_scope='function')
class TestDownSlots:
    """A slot whose boot fails for a cause that can clear stays down alone."""

    async def test_the_first_start_is_degraded_when_one_slot_boots(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setattr(slots, 'SLOT_DOWN_FIRST_DELAY_S', 0.01)
        pool = SlotPool(_config(2, init=_init('short_on_slot:1')), pending_limit=10)
        try:
            with caplog.at_level(logging.WARNING, logger='horsies.worker'):
                started = await asyncio.wait_for(pool.start(), OUTCOME_TIMEOUT_S)

            match started:
                case Ok(baselines):
                    assert len(baselines) == 1
                case Err(failure):
                    raise AssertionError(failure)
            assert isinstance(pool.states()[0], Idle)
            match pool.states()[1]:
                case Down(cause=cause, detail=detail, failed_boots=failed_boots):
                    assert cause == SHORT_OF_DESCRIPTORS
                    assert detail == EMFILE_DETAIL
                    assert failed_boots >= 1
                case other:
                    raise AssertionError(other)
            assert pool.slots_in_service() == 1
            assert [down.slot for down in pool.down_slots()] == [1]
            assert 'Worker starts degraded: 1 of 2 slots serve' in caplog.text
            assert await _result_json(pool.submit(_frame('echo', 'served'))) == 'served'
            assert pool._failure is None
        finally:
            await pool.stop(0)

    async def test_the_first_start_stops_when_no_slot_boots(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Every slot is down, so failed boots count, and the start fails."""
        monkeypatch.setattr(slots, 'SLOT_DOWN_FIRST_DELAY_S', 0.01)
        pool = SlotPool(_config(2, init=_init('short_always')), pending_limit=10)
        started = await asyncio.wait_for(pool.start(), OUTCOME_TIMEOUT_S)
        await pool.stop(0)

        assert started == Err(
            BootAttemptsExhausted(
                CHILD_BOOT_ATTEMPTS,
                SHORT_OF_DESCRIPTORS,
                EMFILE_DETAIL,
            ),
        )

    async def test_a_down_slot_returns_to_service_and_its_boots_do_not_count(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The first two boots pass. The retired child's replacement is short
        of descriptors twice, while the other slot serves, then boots."""
        monkeypatch.setattr(slots, 'SLOT_DOWN_FIRST_DELAY_S', 0.01)
        marks = tmp_path / 'marks'
        init = _init(f'short_after_first:{marks}', ('2', '2'))
        pool = await _started_pool(_config(2, init=init, max_tasks_per_child=1))
        try:
            with caplog.at_level(logging.WARNING, logger='horsies.worker'):
                await _outcome(pool.submit(_frame('echo', 'first')))
                await _until(
                    lambda: len(marks.read_text()) == 5
                    and all(isinstance(state, Idle) for state in pool.states()),
                )
            assert pool._failure is None
            assert pool.slots_in_service() == 2
            assert pool.down_slots() == ()
        finally:
            await pool.stop(0)

        down = [line for slot in (0, 1) for line in _down_lines(caplog, slot)]
        assert len(down) == 2
        assert 'Failed boots: 1, counted attempts: 0/3' in down[0]
        assert 'Failed boots: 2, counted attempts: 0/3' in down[1]
        assert 'back in service after 2 failed boots' in caplog.text

    async def test_a_pool_where_every_slot_is_down_fails(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Both children retire, and no replacement can boot. With no slot
        that can serve, the failed boots count."""
        monkeypatch.setattr(slots, 'SLOT_DOWN_FIRST_DELAY_S', 0.01)
        init = _init(f'short_after_first:{tmp_path / "marks"}', ('1000', '2'))
        pool = await _started_pool(_config(2, init=init, max_tasks_per_child=1))
        try:
            first = pool.submit(_frame('sleep', '0.2'))
            second = pool.submit(_frame('sleep', '0.2'))
            await _outcome(first)
            await _outcome(second)
            failure = await asyncio.wait_for(pool.wait_failed(), OUTCOME_TIMEOUT_S)
        finally:
            await pool.stop(0)

        assert failure == BootAttemptsExhausted(
            CHILD_BOOT_ATTEMPTS,
            SHORT_OF_DESCRIPTORS,
            EMFILE_DETAIL,
        )

    async def test_stop_ends_the_booting_child_of_a_down_slot(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A slot that is down keeps that state while its next child boots.
        A stop kills that child, as it kills any child that boots."""
        monkeypatch.setattr(slots, 'SLOT_DOWN_FIRST_DELAY_S', 0.01)
        init = _init(f'short_then_hang:{tmp_path / "marks"}', ('1',))
        pool = SlotPool(_config(2, init=init), pending_limit=10)
        assert isinstance(await asyncio.wait_for(pool.start(), OUTCOME_TIMEOUT_S), Ok)
        await _until(lambda: pool._slots[1].child is not None)
        assert isinstance(pool.states()[1], Down)
        booting = pool._slots[1].child
        assert booting is not None

        await asyncio.wait_for(pool.stop(30), 10)

        assert not _alive(booting.pid)

    async def test_stop_ends_a_down_slot_at_once(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A slot that waits a long time before its next boot does not hold
        up the stop."""
        monkeypatch.setattr(slots, 'SLOT_DOWN_FIRST_DELAY_S', 3600.0)
        pool = SlotPool(_config(2, init=_init('short_on_slot:1')), pending_limit=10)
        assert isinstance(await asyncio.wait_for(pool.start(), OUTCOME_TIMEOUT_S), Ok)

        await asyncio.wait_for(pool.stop(30), 10)

        assert all(task.done() for task in pool._tasks)

    async def test_a_parent_out_of_descriptors_takes_only_the_replacing_slot_down(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The worker parent is at its descriptor limit when slot 1 replaces
        a child, so the socket pair of the replacement fails with EMFILE.
        The cause can clear: the slot is down, no defect is counted, and the
        slot returns to service when descriptors are free again."""
        monkeypatch.setattr(slots, 'SLOT_DOWN_FIRST_DELAY_S', 0.05)
        pool = await _started_pool(_config(2))
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        try:
            replaced = pool._slots[1].child
            assert replaced is not None
            resource.setrlimit(resource.RLIMIT_NOFILE, (NO_NEW_DESCRIPTOR_LIMIT, hard))
            try:
                os.kill(replaced.pid, signal.SIGKILL)
                await _until(lambda: isinstance(pool.states()[1], Down))
            finally:
                resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))

            match pool.states()[1]:
                case Down(cause=cause, detail=detail):
                    assert cause == SHORT_OF_DESCRIPTORS
                    assert detail == EMFILE_DETAIL
                case other:
                    raise AssertionError(other)
            assert pool._failure is None
            assert pool._slots[1].defects == 0
            await _until(lambda: isinstance(pool.states()[1], Idle))
            assert pool.slots_in_service() == 2
            assert pool._failure is None
        finally:
            await pool.stop(0)

    async def test_only_consecutive_counted_attempts_reach_the_bound(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Slot 1 is short of descriptors on every boot. Slot 0 is down for
        two of those boots, serves for five, then is down again. The five
        spared boots start the count again, so the pool fails at the third
        counted boot in a row, not at the first boot of the second window."""
        slot_0_down = Down(
            SHORT_OF_DESCRIPTORS,
            EMFILE_DETAIL,
            datetime.now(timezone.utc),
            1,
        )
        slot_0_at_boot = ['down'] * 2 + ['idle'] * 5 + ['down'] * 3
        pool = SlotPool(_config(2), pending_limit=10)
        slot_0, slot_1 = pool._slots
        pool._first_generation.set_result(None)
        boots: list[int] = []

        async def short_of_descriptors(
            _slot: object,
            _init: object,
        ) -> Err[BootFailure]:
            match slot_0_at_boot[len(boots)]:
                case 'down':
                    slot_0.state = slot_0_down
                case _:
                    slot_0.state = Idle()
            boots.append(len(boots) + 1)
            return Err(BootFailure(SHORT_OF_DESCRIPTORS, EMFILE_DETAIL))

        async def no_wait(_delay_s: float) -> None:
            return None

        monkeypatch.setattr(pool, '_boot_attempt', short_of_descriptors)
        monkeypatch.setattr(pool, '_pause', no_wait)

        result = await pool._boot_child(slot_1)

        assert result == Err(
            BootAttemptsExhausted(
                CHILD_BOOT_ATTEMPTS,
                SHORT_OF_DESCRIPTORS,
                EMFILE_DETAIL,
            ),
        )
        assert boots == list(range(1, len(slot_0_at_boot) + 1))


# Descriptors 0, 1 and 2 stay open. Each new descriptor gets a number at or
# above this soft limit, so the OS refuses it with EMFILE.
NO_NEW_DESCRIPTOR_LIMIT = 3


class _RefusedBuffers:
    """A socket whose buffer size the OS refuses."""

    def __init__(self) -> None:
        self.closed = False

    def setsockopt(self, *_args: object) -> None:
        raise OSError(errno.ENOBUFS, 'No buffer space available')

    def close(self) -> None:
        self.closed = True


class TestSocketPair:
    """The socket pair of a new child follows the errno rule of a boot step."""

    @pytest.mark.parametrize(
        ('number', 'cause'),
        [
            (errno.EMFILE, ResourceShortage(ShortageErrno.EMFILE)),
            (errno.ENFILE, ResourceShortage(ShortageErrno.ENFILE)),
            (errno.EOPNOTSUPP, SpawnFailed()),
        ],
        ids=['emfile', 'enfile', 'eopnotsupp'],
    )
    def test_a_refused_pair_names_its_cause(
        self,
        monkeypatch: pytest.MonkeyPatch,
        number: int,
        cause: ResourceShortage | SpawnFailed,
    ) -> None:
        def refuse() -> tuple[socket.socket, socket.socket]:
            raise OSError(number, os.strerror(number))

        monkeypatch.setattr(socket, 'socketpair', refuse)

        assert slots._socket_pair() == Err(
            BootFailure(cause, f'OSError: [Errno {number}] {os.strerror(number)}'),
        )

    def test_a_refused_buffer_size_closes_both_sockets(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        pair = (_RefusedBuffers(), _RefusedBuffers())
        monkeypatch.setattr(socket, 'socketpair', lambda: pair)

        match slots._socket_pair():
            case Err(BootFailure(cause=SpawnFailed(), detail=detail)):
                assert detail.startswith(f'OSError: [Errno {errno.ENOBUFS}]')
            case other:
                raise AssertionError(other)
        assert [sock.closed for sock in pair] == [True, True]


VALID_APP = """
from pydantic import SecretStr
from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig

app = Horsies(
    config=AppConfig(
        broker=PostgresConfig(
            database_url=SecretStr('postgresql+psycopg://user:pass@localhost/db'),
        ),
    ),
)
"""


@pytest.mark.asyncio(loop_scope='function')
class TestRealChildBootCauses:
    """The real child program names the cause of each boot step that fails."""

    @staticmethod
    async def _start(app_file: Path, init: InitFrame | None = None) -> object:
        boot_init = init or _init(f'{app_file}:app')
        pool = SlotPool(
            _config(init=boot_init, child_entry=slots.CHILD_ENTRY),
            pending_limit=10,
        )
        started = await asyncio.wait_for(pool.start(), 3 * OUTCOME_TIMEOUT_S)
        await pool.stop(0)
        return started

    @pytest.fixture(autouse=True)
    def _short_waits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(slots, 'CHILD_BOOT_RETRY_DELAY_S', 0.01)
        monkeypatch.setattr(slots, 'SLOT_DOWN_FIRST_DELAY_S', 0.01)

    async def test_an_app_that_does_not_import_is_an_app_load_failure(
        self,
        tmp_path: Path,
    ) -> None:
        app_file = tmp_path / 'broken_app.py'
        app_file.write_text("raise ImportError('scripted import failure')\n")

        assert await self._start(app_file) == Err(
            BootAttemptsExhausted(
                CHILD_BOOT_ATTEMPTS,
                AppLoadFailed(),
                'ImportError: scripted import failure',
            ),
        )

    async def test_an_import_short_of_descriptors_is_a_resource_shortage(
        self,
        tmp_path: Path,
    ) -> None:
        """The cause can clear. The only slot cannot serve, so it still counts."""
        app_file = tmp_path / 'short_app.py'
        app_file.write_text(
            "import errno\nraise OSError(errno.EMFILE, 'Too many open files')\n",
        )

        assert await self._start(app_file) == Err(
            BootAttemptsExhausted(
                CHILD_BOOT_ATTEMPTS,
                SHORT_OF_DESCRIPTORS,
                EMFILE_DETAIL,
            ),
        )

    async def test_a_pool_that_cannot_be_set_up_is_a_pool_setup_failure(
        self,
        tmp_path: Path,
    ) -> None:
        app_file = tmp_path / 'valid_app.py'
        app_file.write_text(VALID_APP)
        init = replace(
            _init(f'{app_file}:app'),
            child_kind=IsolatedChild(
                pool=ResolvedPool(min_size=2, max_size=1, check=False),
                task_send_pool=2,
            ),
        )

        assert await self._start(app_file, init) == Err(
            BootAttemptsExhausted(
                CHILD_BOOT_ATTEMPTS,
                ChildPoolSetupFailed(),
                'ValueError: min_size must be <= max_size',
            ),
        )

    async def test_a_valid_app_boots(self, tmp_path: Path) -> None:
        app_file = tmp_path / 'valid_app.py'
        app_file.write_text(VALID_APP)

        assert isinstance(await self._start(app_file), Ok)

    @pytest.mark.parametrize(
        ('hooks', 'thread'),
        [
            ('', 'a thread of the connection pool'),
            (
                'app.on_child_process_start(lambda: None)\n',
                "the watchdog of start hook '<lambda>'",
            ),
        ],
        ids=['pool', 'hook-watchdog'],
    )
    async def test_a_slot_whose_threads_are_refused_is_down_while_another_serves(
        self,
        tmp_path: Path,
        hooks: str,
        thread: str,
    ) -> None:
        """The child of slot 1 starts its control thread, then the OS refuses
        each next thread, as at a thread limit: the watchdog of a start hook,
        or the threads of the connection pool. The cause can clear, so only
        slot 1 is down, and the worker starts on slot 0."""
        app_file = tmp_path / 'refusing_app.py'
        app_file.write_text(VALID_APP + REFUSE_THREADS_IN_SLOT_1 + hooks)
        pool = SlotPool(
            _config(2, init=_init(f'{app_file}:app'), child_entry=slots.CHILD_ENTRY),
            pending_limit=10,
        )
        try:
            started = await asyncio.wait_for(pool.start(), 3 * OUTCOME_TIMEOUT_S)

            match started:
                case Ok(baselines):
                    assert len(baselines) == 1
                case Err(failure):
                    raise AssertionError(failure)
            assert isinstance(pool.states()[0], Idle)
            match pool.states()[1]:
                case Down(cause=ThreadStartRefused(), detail=detail):
                    assert detail == f"RuntimeError: {thread}: can't start new thread"
                case other:
                    raise AssertionError(other)
            assert pool._failure is None
        finally:
            await pool.stop(0)


# Appended to an app module: in the child of slot 1, each thread that starts
# after the import is refused, as at a thread limit of the process.
REFUSE_THREADS_IN_SLOT_1 = """
import multiprocessing
import threading


def _refuse(_thread: threading.Thread) -> None:
    raise RuntimeError("can't start new thread")


if multiprocessing.current_process().name.endswith('-1'):
    threading.Thread.start = _refuse
"""


class TestDownSlotRules:
    """Pure rules of a slot that is down."""

    def test_a_boot_failure_frame_carries_its_cause(self) -> None:
        frame = BootFailedFrame(TaskLoopCreateFailed(), 'OSError', '[Errno 24] x')
        assert slots.boot_failure_of(frame) == BootFailure(
            TaskLoopCreateFailed(),
            'OSError: [Errno 24] x',
        )

    def test_the_hook_exit_code_is_a_hook_failure(self) -> None:
        lost = ChildLost(
            LossCause.CHILD_DIED,
            ExitedWith(slots.CHILD_HOOK_FAILURE_EXIT_CODE),
            partial_frame=False,
            pid=1,
        )
        assert slots.boot_failure_of(lost) == ChildHookFailed()

    def test_the_reparent_exit_code_is_a_spawn_failure(self) -> None:
        lost = ChildLost(
            LossCause.CHILD_DIED,
            ExitedWith(REPARENT_FAILED_EXIT_CODE),
            partial_frame=False,
            pid=1,
        )
        match slots.boot_failure_of(lost):
            case BootFailure(cause=SpawnFailed(), detail=detail):
                assert 'did not reach the worker' in detail
            case other:
                raise AssertionError(other)

    @pytest.mark.parametrize(
        'child_exit',
        [
            ExitedWith(1),
            ExitedWith(STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE),
            KilledBySignal(11),
        ],
        ids=['exit-1', 'exit-74', 'sigsegv'],
    )
    def test_any_other_end_with_no_frame_is_lost_before_ready(
        self,
        child_exit: ExitedWith | KilledBySignal,
    ) -> None:
        lost = ChildLost(LossCause.CHILD_DIED, child_exit, partial_frame=False, pid=1)
        match slots.boot_failure_of(lost):
            case BootFailure(cause=LostBeforeReady(loss=LossCause.CHILD_DIED) as cause):
                assert cause.child_exit == child_exit
            case other:
                raise AssertionError(other)

    def test_no_reply_in_time_is_not_ready_in_time(self) -> None:
        match slots.boot_failure_of(slots.WaitExpired()):
            case BootFailure(cause=slots.NotReadyInTime()):
                pass
            case other:
                raise AssertionError(other)

    def test_the_wait_doubles_and_stays_bounded(self) -> None:
        assert slots.slot_down_delay_s(1) == slots.SLOT_DOWN_FIRST_DELAY_S
        assert slots.slot_down_delay_s(2) == 2 * slots.SLOT_DOWN_FIRST_DELAY_S
        assert slots.slot_down_delay_s(3) == 4 * slots.SLOT_DOWN_FIRST_DELAY_S
        assert slots.slot_down_delay_s(10**9) == slots.SLOT_DOWN_MAX_DELAY_S
        waits = [slots.slot_down_delay_s(boots) for boots in range(1, 20)]
        assert waits == sorted(waits)
        assert max(waits) == slots.SLOT_DOWN_MAX_DELAY_S

    def test_only_a_slot_that_is_down_leaves_the_budget(self) -> None:
        down = Down(SHORT_OF_DESCRIPTORS, EMFILE_DETAIL, datetime.now(timezone.utc), 1)
        states: list[slots.SlotState] = [
            Starting(),
            Idle(),
            Busy('t'),
            slots.Retiring(),
            down,
        ]
        assert [slots._in_service(state) for state in states] == [True] * 4 + [False]


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

    async def test_async_runs_of_one_child_share_one_loop(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """``serve`` gives each task of a child the same task loop."""
        pool = await pool_factory()
        handles = [pool.submit(_frame('loop_count')) for _ in range(2)]
        assert [await _result_json(handle) for handle in handles] == ['1', '1']

    async def test_no_loop_runs_between_async_runs(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """A run after an async run sees no running loop and can run its own."""
        pool = await pool_factory()
        handles = [
            pool.submit(_frame('loop_count')),
            pool.submit(_frame('no_running_loop')),
            pool.submit(_frame('loop_count')),
        ]
        results = [await _result_json(handle) for handle in handles]
        assert results == ['1', 'False|ran', '1']

    async def test_a_task_that_leaves_loop_tasks_retires_its_child(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
    ) -> None:
        """The leftover is cancelled before the outcome, and the next task
        runs in a new child."""
        marker = tmp_path / 'cancelled'
        pool = await pool_factory()
        match await _outcome(pool.submit(_frame('leave_pending', str(marker)))):
            case TaskFinished(result_json=first_pid, task_loop=task_loop):
                assert task_loop == LoopHoldsTasks(
                    count=1,
                    names=('_mark_when_cancelled',),
                )
            case other:
                raise AssertionError(other)
        assert marker.read_text(encoding='ascii') == 'cancelled'
        assert await _result_json(pool.submit(_frame('pid'))) != first_pid

    async def test_a_task_that_leaves_no_loop_task_keeps_its_child(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        match await _outcome(pool.submit(_frame('async_pid'))):
            case TaskFinished(result_json=first_pid, task_loop=task_loop):
                assert task_loop == LoopHoldsNoTask()
            case other:
                raise AssertionError(other)
        assert await _result_json(pool.submit(_frame('pid'))) == first_pid

    async def test_the_loop_is_open_before_the_first_async_task(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """The child of an app with async tasks creates its loop at boot."""
        pool = await pool_factory()
        assert await _result_json(pool.submit(_frame('loop_state'))) == 'LoopOpen'

    async def test_an_app_with_no_async_task_creates_no_loop(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """Loop creation would fail in this child, and its boot still passes."""
        pool = await pool_factory(_config(init=_init('no_async_no_descriptors')))
        handles = [pool.submit(_frame('loop_state')), pool.submit(_frame('echo', 'x'))]
        assert [await _result_json(handle) for handle in handles] == [
            'LoopNotCreated',
            'x',
        ]

    async def test_a_run_frame_after_a_run_that_left_work_ends_the_child(
        self,
        tmp_path: Path,
    ) -> None:
        """The child refuses to run a task after a task that left work on its
        loop, also when that work ended: cancelled code can leave broken
        state.

        The pool never sends that frame, so this drives one child directly.
        """
        config = _config()
        match await Child.start(config, config.init):
            case Ok(child):
                pass
            case Err(failed):
                raise AssertionError(failed)
        try:
            assert isinstance(await child.await_frame(OUTCOME_TIMEOUT_S), ReadyFrame)
            child.send(_frame('leave_pending', str(tmp_path / 'cancelled')))
            match await child.await_frame(OUTCOME_TIMEOUT_S):
                case BodyReturnedFrame(left=LoopHoldsTasks(count=1)):
                    pass
                case other:
                    raise AssertionError(other)
            match await child.await_frame(OUTCOME_TIMEOUT_S):
                case LeftWorkEndedFrame(end=LeftWorkEnd.CANCELLED):
                    pass
                case other:
                    raise AssertionError(other)
            child.send(_frame('echo', 'x'))
            match await child.await_frame(OUTCOME_TIMEOUT_S):
                case ChildLost(child_exit=child_exit):
                    assert child_exit == ExitedWith(BAD_PARENT_FRAME_EXIT_CODE)
                case other:
                    raise AssertionError(other)
        finally:
            await child.dispose_lost()

    async def test_a_stop_with_executor_threads_left_ends_the_process(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """An executor thread that outlives the close does not hold the exit.

        Without the bound the child waits for the thread at exit, and the
        parent kills it only after ``CHILD_STOP_TIMEOUT_S``.
        """
        pool = await pool_factory(
            _config(max_tasks_per_child=1, init=_init('close_timeout:0.2')),
        )
        pid = int(await _result_json(pool.submit(_frame('thread_outlives_wait', '60'))))
        child = pool._slots[0].child
        assert child is not None and child.pid == pid
        await _until(lambda: child.has_exited, timeout_s=slots.CHILD_STOP_TIMEOUT_S / 2)
        assert child._current_exit() == ExitedWith(STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE)

    async def test_an_idle_executor_after_a_stubborn_leftover_exits_normally(
        self,
    ) -> None:
        """The leftover ignores every cancel, so the child reports it stuck.
        A stop frame then makes the close use the whole close deadline. The
        idle executor still shuts down, so the child exits 0, not with
        ``STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE``.

        The pool kills a child that reports stuck work, so this drives one
        child directly.
        """
        config = _config(init=_init('close_timeout:0.2'))
        match await Child.start(config, config.init):
            case Ok(child):
                pass
            case Err(failed):
                raise AssertionError(failed)
        try:
            assert isinstance(await child.await_frame(OUTCOME_TIMEOUT_S), ReadyFrame)
            child.send(_frame('idle_executor_beside_a_stubborn_task'))
            assert isinstance(
                await child.await_frame(OUTCOME_TIMEOUT_S), BodyReturnedFrame
            )
            assert isinstance(
                await child.await_frame(OUTCOME_TIMEOUT_S), LeftWorkStuckFrame
            )
            child.send(StopFrame())
            assert await child.wait_exit(slots.CHILD_STOP_TIMEOUT_S / 2) == ExitedWith(
                0
            )
        finally:
            await child.dispose_lost()

    async def test_a_stop_with_no_executor_threads_exits_normally(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """With nothing left to wait for, the child exits 0 and runs ``atexit``."""
        pool = await pool_factory(_config(max_tasks_per_child=1))
        assert await _result_json(pool.submit(_frame('loop_count'))) == '1'
        child = pool._slots[0].child
        assert child is not None
        await _until(lambda: child.has_exited, timeout_s=slots.CHILD_STOP_TIMEOUT_S / 2)
        assert child._current_exit() == ExitedWith(0)

    async def test_a_stopped_child_ends_without_the_interpreter_teardown(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
    ) -> None:
        """A child stopped after its last task waits for the non-daemon
        thread of the task and runs its ``atexit`` handler, as ``sys.exit``
        does, then ends with ``os._exit``: the teardown that would free the
        module-level object, and write to the pages shared with the preload
        parent, does not run."""
        pool = await pool_factory(_config(max_tasks_per_child=1))
        pid = int(await _result_json(pool.submit(_frame('exit_marks', str(tmp_path)))))
        child = pool._slots[0].child
        assert child is not None and child.pid == pid
        await _until(lambda: child.has_exited, timeout_s=slots.CHILD_STOP_TIMEOUT_S / 2)

        assert child._current_exit() == ExitedWith(0)
        assert (tmp_path / 'atexit').exists()
        assert (tmp_path / 'thread').exists()
        assert not (tmp_path / 'teardown').exists()

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

    async def test_optimize_reaches_a_real_child(
        self,
        pool_factory: PoolFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(slots, 'interpreter_flags', lambda: ('-O',))
        pool = await pool_factory()
        state = await _result_json(pool.submit(_frame('state')))
        assert state.endswith('|False')

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


def _left_work_lines(caplog: pytest.LogCaptureFixture, task_id: str) -> list[str]:
    """The log lines of the run of ``task_id`` that left work on its loop."""
    return [
        record.getMessage()
        for record in caplog.records
        if f'({task_id}) returned and left' in record.getMessage()
    ]


def _worker_without_database() -> Worker:
    """A worker whose writes fail: a path that writes would show here."""
    return Worker(
        session_factory=MagicMock(),
        listener=MagicMock(),
        cfg=WorkerConfig(
            dsn='postgresql+psycopg://u:p@localhost/db',
            psycopg_dsn='postgresql://u:p@localhost/db',
            queues=['default'],
        ),
    )


@pytest.mark.asyncio(loop_scope='function')
class TestLeftWork:
    """An async task whose body returns with asyncio tasks on the task loop:
    the outcome is what it returned, written only after that work ended."""

    async def test_work_that_ends_within_the_grace_ends_before_the_outcome(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        marker = tmp_path / 'ended'
        pool = await pool_factory()
        frame = _frame('leave_ending', str(marker))
        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            outcome = await _outcome(pool.submit(frame))
            ended_at_outcome = marker.read_text(encoding='ascii')
        match outcome:
            case TaskFinished(
                ok=True, result_json=pid, failed_reason=None, task_loop=left
            ):
                assert left == LoopHoldsTasks(count=1, names=('_write_after',))
            case other:
                raise AssertionError(other)
        assert ended_at_outcome == 'ended'
        lines = _left_work_lines(caplog, frame.task_id)
        assert len(lines) == 1
        assert lines[0].startswith(
            f'Task leave_ending ({frame.task_id}) returned and left 1'
        )
        assert (
            f'child pid={pid} (_write_after); that work ended with no cancel'
            in lines[0]
        )
        assert await _result_json(pool.submit(_frame('pid'))) != pid

    async def test_work_alive_after_the_grace_is_cancelled_before_the_outcome(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        marker = tmp_path / 'cancelled'
        pool = await pool_factory()
        frame = _frame('leave_pending', str(marker))
        handle = pool.submit(frame)
        await handle.assigned
        started = time.monotonic()
        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            outcome = await _outcome(handle)
            cancelled_at_outcome = marker.read_text(encoding='ascii')
        elapsed = time.monotonic() - started
        match outcome:
            case TaskFinished(ok=True, result_json=pid, task_loop=left):
                assert left == LoopHoldsTasks(count=1, names=('_mark_when_cancelled',))
            case other:
                raise AssertionError(other)
        assert cancelled_at_outcome == 'cancelled'
        assert elapsed >= ISOLATED_LEFTOVER_GRACE_S
        lines = _left_work_lines(caplog, frame.task_id)
        assert len(lines) == 1
        assert 'ended after a cancel' in lines[0]
        assert 'cancelled code can leave broken state' in lines[0]
        assert await _result_json(pool.submit(_frame('pid'))) != pid

    async def test_work_that_swallows_every_cancel_gets_its_child_killed(
        self,
        pool_factory: PoolFactory,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The outcome is what the body returned, after the exit of the
        killed child: no timeout, no crash."""
        pool = await pool_factory()
        frame = _frame('leave_stubborn')
        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            outcome = await _outcome(pool.submit(frame))
        match outcome:
            case LostAfterReturn(returned=returned, lost=lost):
                pid = returned.result_json
                assert returned == SeatFinished(
                    ok=True, result_json=pid, failed_reason=None
                )
                assert lost == ChildLost(
                    cause=LossCause.KILLED_BY_PARENT,
                    child_exit=KilledBySignal(signal.SIGKILL),
                    partial_frame=False,
                    pid=int(pid),
                )
            case other:
                raise AssertionError(other)
        assert not _alive(int(pid))
        lines = _left_work_lines(caplog, frame.task_id)
        assert len(lines) == 1
        assert (
            'did not stop after the cancel rounds (1 task(s) alive: _ignore_cancel)'
            in lines[0]
        )
        assert await _result_json(pool.submit(_frame('pid'))) != pid

    async def test_a_child_that_reports_nothing_after_the_return_is_killed(
        self,
        pool_factory: PoolFactory,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The work holds the loop thread, so no cancel round runs."""
        backstop_s = 2.0
        monkeypatch.setattr(slots, 'ISOLATED_LEFTOVER_BACKSTOP_S', backstop_s)
        pool = await pool_factory()
        frame = _frame('leave_blocking')
        handle = pool.submit(frame)
        await handle.assigned
        started = time.monotonic()
        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            outcome = await _outcome(handle)
        assert time.monotonic() - started >= backstop_s
        match outcome:
            case LostAfterReturn(returned=returned, lost=lost):
                assert returned.ok
                assert lost.cause is LossCause.KILLED_BY_PARENT
                assert lost.child_exit == KilledBySignal(signal.SIGKILL)
            case other:
                raise AssertionError(other)
        lines = _left_work_lines(caplog, frame.task_id)
        assert len(lines) == 1
        assert f'was not reported ended or stuck within {backstop_s}s' in lines[0]

    async def test_a_child_that_dies_after_the_return_keeps_what_the_body_returned(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        match await _outcome(pool.submit(_frame('leave_exiting', '3'))):
            case LostAfterReturn(returned=returned, lost=lost):
                assert returned.ok
                assert lost.cause is LossCause.CHILD_DIED
                assert lost.child_exit == ExitedWith(3)
                assert lost.pid == int(returned.result_json)
            case other:
                raise AssertionError(other)

    async def test_a_system_exit_of_the_left_work_ends_the_child_at_once(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """A normal exit would wait for the thread that the work started: the
        child exits with the code of the ``SystemExit`` instead."""
        pool = await pool_factory()
        handle = pool.submit(_frame('leave_sys_exit', '5'))
        await handle.assigned
        started = time.monotonic()
        match await _outcome(handle):
            case LostAfterReturn(returned=returned, lost=lost):
                assert returned.ok
                assert lost.cause is LossCause.CHILD_DIED
                assert lost.child_exit == ExitedWith(5)
            case other:
                raise AssertionError(other)
        assert time.monotonic() - started < ISOLATED_LEFTOVER_GRACE_S * 2

    async def test_a_deadline_kill_after_the_return_kills_nothing(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
    ) -> None:
        marker = tmp_path / 'cancelled'
        pool = await pool_factory()
        handle = pool.submit(_frame('leave_pending', str(marker)))
        await _until(lambda: isinstance(handle._return, ReturnSeen))

        match await handle.kill_and_wait(OUTCOME_TIMEOUT_S):
            case ReturnSeen(returned=returned):
                pass
            case other:
                raise AssertionError(other)
        match await _outcome(handle):
            case TaskFinished(result_json=pid):
                assert pid == returned.result_json
            case other:
                raise AssertionError(other)
        assert marker.read_text(encoding='ascii') == 'cancelled'
        assert await handle.wait_child_exit() == ExitedWith(0)

    async def test_the_deadline_does_not_end_a_task_whose_body_returned(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
    ) -> None:
        """The deadline fires while the child waits for the left work. The
        body returned before it, so the outcome is what it returned, and the
        child exits by its stop frame, not by a kill."""
        marker = tmp_path / 'cancelled'
        pool = await pool_factory()
        handle = pool.submit(_frame('leave_pending', str(marker)))
        deadline = TaskTimeout(ms=int(ISOLATED_LEFTOVER_GRACE_S * 1000 / 4))

        outcome = await asyncio.wait_for(
            _worker_without_database()._await_slot_outcome(
                handle,
                handle.frame.task_id,
                deadline,
                None,
            ),
            OUTCOME_TIMEOUT_S,
        )

        match outcome:
            case TaskFinished(ok=True, task_loop=LoopHoldsTasks(count=1)):
                pass
            case other:
                raise AssertionError(other)
        assert marker.read_text(encoding='ascii') == 'cancelled'
        assert await handle.wait_child_exit() == ExitedWith(0)

    async def test_an_async_task_that_leaves_no_work_reports_at_once(self) -> None:
        """No added wait: the result frame is the first frame after the run.

        This drives one child directly, to see each frame.
        """
        config = _config()
        match await Child.start(config, config.init):
            case Ok(child):
                pass
            case Err(failed):
                raise AssertionError(failed)
        try:
            assert isinstance(await child.await_frame(OUTCOME_TIMEOUT_S), ReadyFrame)
            started = time.monotonic()
            child.send(_frame('async_pid'))
            match await child.await_frame(OUTCOME_TIMEOUT_S):
                case ResultFrame(task_loop=LoopHoldsNoTask()):
                    pass
                case other:
                    raise AssertionError(other)
            assert time.monotonic() - started < ISOLATED_LEFTOVER_GRACE_S / 2
        finally:
            await child.dispose_lost()

    async def test_the_marker_follows_the_end_of_work_left_within_the_grace(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
    ) -> None:
        """The marker is the last write of the run: written after the work
        that the task left ended, so the runner heartbeats cover that work."""
        work = tmp_path / 'ended'
        pool = await pool_factory()
        match await _outcome(pool.submit(_frame('leave_ending', str(work)))):
            case TaskFinished(task_loop=LoopHoldsTasks(count=1)):
                pass
            case other:
                raise AssertionError(other)
        marks = Path(f'{work}.marks').read_text(encoding='ascii')
        assert marks == 'MarkerAfterLeftWork ended\n'

    async def test_the_marker_follows_the_cancel_of_work_left_past_the_grace(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
    ) -> None:
        work = tmp_path / 'cancelled'
        pool = await pool_factory()
        match await _outcome(pool.submit(_frame('leave_pending', str(work)))):
            case TaskFinished(task_loop=LoopHoldsTasks(count=1)):
                pass
            case other:
                raise AssertionError(other)
        marks = Path(f'{work}.marks').read_text(encoding='ascii')
        assert marks == 'MarkerAfterLeftWork cancelled\n'

    async def test_work_that_does_not_stop_gets_no_marker(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
    ) -> None:
        """The parent kills the child; its runner heartbeats cover the row
        until the exit."""
        work = tmp_path / 'stubborn'
        pool = await pool_factory()
        match await _outcome(pool.submit(_frame('leave_stubborn', str(work)))):
            case LostAfterReturn(lost=lost):
                assert lost.cause is LossCause.KILLED_BY_PARENT
            case other:
                raise AssertionError(other)
        assert not Path(f'{work}.marks').exists()

    @pytest.mark.parametrize('task_name', ['marker_probe', 'marker_probe_async'])
    async def test_a_run_that_leaves_no_work_writes_its_marker_before_its_result(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
        task_name: str,
    ) -> None:
        """As before the change: the marker at the return, then the result
        frame, so the marker is there when the outcome is read."""
        probe = tmp_path / 'probe'
        pool = await pool_factory()
        match await _outcome(pool.submit(_frame(task_name, str(probe)))):
            case TaskFinished(task_loop=LoopHoldsNoTask()):
                marks_at_outcome = Path(f'{probe}.marks').read_text(encoding='ascii')
            case other:
                raise AssertionError(other)
        assert marks_at_outcome == 'MarkerAtReturn absent\n'

    async def test_a_run_that_stopped_before_running_writes_no_marker(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
    ) -> None:
        probe = tmp_path / 'stopped'
        pool = await pool_factory()
        match await _outcome(pool.submit(_frame('stopped_run', str(probe)))):
            case TaskFinished(
                ok=True, result_json='stopped', failed_reason='CLAIM_LOST'
            ):
                pass
            case other:
                raise AssertionError(other)
        assert not Path(f'{probe}.marks').exists()

    async def test_a_def_task_keeps_its_child_and_reports_an_empty_loop(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        match await _outcome(pool.submit(_frame('pid'))):
            case TaskFinished(result_json=first_pid, task_loop=task_loop):
                assert task_loop == LoopHoldsNoTask()
            case other:
                raise AssertionError(other)
        assert await _result_json(pool.submit(_frame('pid'))) == first_pid


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
        encoded = encode_child_frame(ResultFrame('t', True, 'kept', None, RssUnreadable(), NoTaskLoop()))
        child = await self._dead_child_with(encoded.head + encoded.body)
        match await child.await_frame(None):
            case ResultFrame(result_json='kept'):
                pass
            case other:
                raise AssertionError(other)
        await child.dispose()

    async def test_half_a_frame_still_unread_is_reported_partial(self) -> None:
        encoded = encode_child_frame(ResultFrame('t', True, 'lost', None, RssUnreadable(), NoTaskLoop()))
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

    async def test_a_child_killed_after_its_result_takes_no_next_task(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """The kill lands after the result frame arrived, before the slot read it."""
        pool = await pool_factory()
        first = pool.submit(_frame('sleep', '0.2'))
        await asyncio.wait_for(first.assigned, 5)
        child = pool._slots[0].child
        assert child is not None
        deliver = child._protocol._on_frame

        def deliver_then_kill(frame: ChildFrame) -> None:
            deliver(frame)
            assert first.kill() == KillSent()

        child._protocol._on_frame = deliver_then_kill
        second = pool.submit(_frame('echo', 'x'))

        assert isinstance(await _outcome(first), TaskFinished)
        assert child.killed_by_parent
        assert await _result_json(second) == 'x'

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
class TestKillAndWait:
    """The timeout path waits for the exit before it writes the row."""

    async def test_the_exit_of_the_killed_child_is_seen(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        handle = pool.submit(_frame('hang'))
        await asyncio.wait_for(handle.assigned, 5)
        child = pool._slots[0].child
        assert child is not None

        ended = await handle.kill_and_wait(OUTCOME_TIMEOUT_S)

        assert ended == ChildEnded(pid=child.pid, child_exit=KilledBySignal(9))
        assert not _alive(child.pid)
        lost = await _outcome(handle)
        assert isinstance(lost, ChildLost)
        assert lost.cause is LossCause.KILLED_BY_PARENT

    async def test_an_exit_after_the_bounded_wait_is_still_seen(
        self,
        pool_factory: PoolFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The slot gives up its bounded wait before the exit, as for a child
        stuck in the kernel. The handle still sees the exit later, so a
        worker with stale RUNNING recovery off can write the row then."""
        pool = await pool_factory()
        handle = pool.submit(_frame('hang'))
        await asyncio.wait_for(handle.assigned, 5)
        child = pool._slots[0].child
        assert child is not None
        monkeypatch.setattr(slots, 'CHILD_KILL_WAIT_S', 0.0)

        ended = await handle.kill_and_wait(0.0)
        await child.dispose()

        assert ended == ChildEndUnknown(pid=child.pid)
        late = await asyncio.wait_for(handle.wait_child_exit(), OUTCOME_TIMEOUT_S)
        assert late == KilledBySignal(signal.SIGKILL)

    async def test_a_finished_task_reports_the_slot_answer(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        """A late timer cannot reach the next task on the same slot."""
        pool = await pool_factory()
        finished = pool.submit(_frame('echo', 'x'))
        answer = await _outcome(finished)
        running = pool.submit(_frame('sleep', '0.3'))
        await asyncio.wait_for(running.assigned, 5)

        assert await finished.kill_and_wait(OUTCOME_TIMEOUT_S) == SlotAnswered(answer)
        assert isinstance(await _outcome(running), TaskFinished)

    async def test_a_task_no_child_took_has_nothing_to_kill(
        self,
        pool_factory: PoolFactory,
    ) -> None:
        pool = await pool_factory()
        running = pool.submit(_frame('sleep', '0.3'))
        queued = pool.submit(_frame('echo', 'x'))

        assert await queued.kill_and_wait(OUTCOME_TIMEOUT_S) == NothingToKill()
        await _outcome(running)
        assert isinstance(await _outcome(queued), TaskFinished)

    async def test_a_child_that_does_not_end_in_the_wait_is_reported(self) -> None:
        """A process stuck in the kernel ignores SIGKILL until it leaves it."""

        class StuckProcess:
            pid = NO_SUCH_PID
            returncode = None

            async def wait(self) -> int:
                await asyncio.Event().wait()
                raise AssertionError('a stuck process does not end')

        parent_sock, child_sock = socket.socketpair()
        child = await Child._attach(
            cast('asyncio.subprocess.Process', StuckProcess()),
            parent_sock,
            _init(),
        )
        handle = SlotHandle(_frame('hang'), asyncio.get_running_loop())
        handle.bind_child(child)
        try:
            ended = await handle.kill_and_wait(0.05)
        finally:
            child._exit.cancel()
            child._transport.abort()
            child_sock.close()

        assert ended == ChildEndUnknown(pid=NO_SUCH_PID)
        assert child.killed_by_parent
        assert not handle.outcome.done()


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
        assert isinstance(failure, SlotDefectsRepeated)
        assert failure.slot == 0

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
        """The other child keeps serving, so this slot only waits and retries."""
        monkeypatch.setattr(slots, 'CHILD_BOOT_RETRY_DELAY_S', 0.01)
        kills = CHILD_BOOT_ATTEMPTS + 2
        marks = tmp_path / 'marks'
        init = _init(f'killed_after_first:{marks}', (str(kills), '2'))
        pool = await pool_factory(_config(2, init=init, max_tasks_per_child=1))

        assert await _result_json(pool.submit(_frame('echo', 'first'))) == 'first'
        for _ in range(1000):
            if marks.exists() and len(marks.read_text()) == kills + 3:
                break
            await asyncio.sleep(0.01)

        assert len(marks.read_text()) == kills + 3
        assert await _result_json(pool.submit(_frame('echo', 'next'))) == 'next'
        assert pool._failure is None

    async def test_a_kill_counts_when_no_other_child_serves(
        self,
        pool_factory: PoolFactory,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No healthy task is at risk, and a worker that does no work must fail."""
        monkeypatch.setattr(slots, 'CHILD_BOOT_RETRY_DELAY_S', 0.01)
        init = _init(f'killed_after_first:{tmp_path / "marks"}', ('100', '1'))
        pool = await pool_factory(_config(init=init, max_tasks_per_child=1))

        await _outcome(pool.submit(_frame('echo', 'first')))

        failure = await asyncio.wait_for(pool.wait_failed(), OUTCOME_TIMEOUT_S)
        assert isinstance(failure, BootAttemptsExhausted)

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
        init = _init(f'fails_after_first:{tmp_path / "marks"}', ('100', '2'))
        pool = await pool_factory(_config(2, init=init, max_tasks_per_child=1))

        await _outcome(pool.submit(_frame('echo', 'first')))

        # The other child is serving, so only the kind of death decides here.
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

        await asyncio.wait_for(pool.stop(30), 5)

        await asyncio.wait_for(starting, 5)
        assert not _alive(first)
        assert all(task.done() for task in pool._tasks)
        assert pool._slots[0].child is None

    async def test_a_child_that_starts_during_the_stop_is_ended(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``stop`` cannot see a child that is still on its way up."""
        pool = SlotPool(_config(init=_init('hangs')), pending_limit=10)
        real_start = Child.start
        started: list[Child] = []

        async def start_then_stop(*args: Any) -> Any:
            result = await real_start(*args)
            pool._stopping = True
            match result:
                case Ok(child):
                    started.append(child)
                case Err(_):
                    pass
            return result

        monkeypatch.setattr(Child, 'start', start_then_stop)

        reply = await pool._boot_child(pool._slots[0])

        assert reply == Err(PoolStopped())
        assert pool._slots[0].child is None
        assert not _alive(started[0].pid)

    async def test_a_replaced_child_does_not_count_as_another_slot(self) -> None:
        pool = SlotPool(_config(2), pending_limit=10)
        baseline = ChildBaseline(
            pid=1,
            rss=RssUnreadable(),
            child_kind=IsolatedChild(pool=ResolvedPool(0, 2, True), task_send_pool=2),
        )
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
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The only slot cannot spawn its replacement, so the failed spawns
        count and the pool fails."""
        monkeypatch.setattr(slots, 'SLOT_DOWN_FIRST_DELAY_S', 0.01)
        pool = await _started_pool(
            _config(max_tasks_per_child=1, child_entry=(SCRIPTED_CHILD,)),
        )
        real_executable = sys.executable
        finished = pool.submit(_frame('echo', 'x'))
        queued = pool.submit(_frame('echo', 'y'))
        assert isinstance(await _outcome(finished), TaskFinished)
        sys.executable = '/nonexistent/python'
        try:
            failure = await asyncio.wait_for(pool.wait_failed(), 15)
        finally:
            sys.executable = real_executable
        match failure:
            case BootAttemptsExhausted(cause=SpawnFailed()):
                pass
            case other:
                raise AssertionError(other)
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
from horsies.core.models.worker_profile import ResolvedPool
from horsies.core.worker.child_protocol import InitFrame, IsolatedChild, RunFrame
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.slots import SlotPool, SlotPoolConfig

async def main():
    init = InitFrame(
        'ok', (), (), tuple(sys.path), ('p',), 30, 'x', {{}}, 'c',
        IsolatedChild(ResolvedPool(0, 1, False), task_send_pool=2),
    )
    pool = SlotPool(
        SlotPoolConfig(1, init, 0, None, ({child!r},), ChildPreload({mode!r}), ({preload!r},)),
        10,
    )
    await pool.start()
    pool.submit(RunFrame('t', {task!r}, None, None, 'w', False, None, None))
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
            preload=SCRIPTED_PRELOAD,
            mode=_start_path.mode.value,
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

