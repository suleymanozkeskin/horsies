"""Worker-owned child processes: one process and one private socket per slot.

Each slot runs one task at a time. A crash, a timeout kill, or a bad frame
reaches that slot only, because slots share no queue, lock, or pipe.

Verdict rules. Every outcome comes from one of these, in this order:

- A result is a complete frame, and nothing else.
- A death is ``process.wait()``, and nothing else.
- Socket EOF is a hint: a helper forked by task code can hold the socket open
  after the child is gone.
- After a death, read what the child already wrote. A complete frame wins.
- EOF with a live child means no result can arrive: bounded grace, then kill.
- A bad frame kills that child. The task is lost, the worker is not.

Whether a lost task had started is not decided here. The task row holds that
fact, and the caller reads it.
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import sys
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Final, TypeAlias, assert_never, cast

from horsies.core.logging import get_logger
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.child_runner import CHILD_HOOK_FAILURE_EXIT_CODE
from horsies.core.worker.child_protocol import (
    FRAME_HEADER,
    ChildFrame,
    FrameOverBound,
    InitFrame,
    MalformedFrame,
    ParentFrame,
    ReadyFrame,
    ResultFrame,
    RssMb,
    RssReading,
    RssUnreadable,
    RunFrame,
    StopFrame,
    decode_child_frame,
    encode_parent_frame,
    parse_header,
)

logger = get_logger('worker')

CHILD_ENTRY: Final[tuple[str, ...]] = ('-m', 'horsies.core.worker.child_main')
SOCKET_BUFFER_BYTES: Final = 4 << 20
CHILD_BOOT_ATTEMPTS: Final = 3
CHILD_BOOT_RETRY_DELAY_S: Final = 0.5
CHILD_READY_TIMEOUT_S: Final = 120.0
CHILD_STOP_TIMEOUT_S: Final = 10.0
CHILD_KILL_WAIT_S: Final = 30.0
CHANNEL_LOST_GRACE_S: Final = 0.2
DRAIN_QUIET_PASSES: Final = 3
DRAIN_DEADLINE_S: Final = 5.0


@dataclass(frozen=True, slots=True)
class ExitedWith:
    """The child exited with this code."""

    code: int


@dataclass(frozen=True, slots=True)
class KilledBySignal:
    """A signal ended the child."""

    signal_number: int


@dataclass(frozen=True, slots=True)
class ExitUnknown:
    """The child did not end within ``CHILD_KILL_WAIT_S`` after SIGKILL."""


ChildExit: TypeAlias = ExitedWith | KilledBySignal | ExitUnknown


def child_exit_from_returncode(returncode: int) -> ChildExit:
    """Read a ``process.returncode``: negative means a signal ended the process."""
    if returncode < 0:
        return KilledBySignal(-returncode)
    return ExitedWith(returncode)


def describe_exit(child_exit: ChildExit) -> str:
    """Return text for logs and crash reasons, for example ``signal 11 (SIGSEGV)``."""
    match child_exit:
        case ExitedWith(code=code):
            return f'exit code {code}'
        case KilledBySignal(signal_number=number):
            try:
                return f'signal {number} ({signal.Signals(number).name})'
            except ValueError:
                return f'signal {number}'
        case ExitUnknown():
            return 'no exit seen after SIGKILL'
        case _ as unreachable:
            assert_never(unreachable)


class LossCause(str, Enum):
    """Why a child ended without a result."""

    CHILD_DIED = 'child died'
    CHANNEL_LOST = 'channel to the child was lost'
    PROTOCOL_VIOLATION = 'child sent a bad frame'
    KILLED_BY_PARENT = 'killed by the worker'


@dataclass(frozen=True, slots=True)
class TaskFinished:
    """The child reported a result. ``ok`` has the ``_run_task_entry`` meaning."""

    ok: bool
    result_json: str
    failed_reason: str | None
    rss: RssReading


@dataclass(frozen=True, slots=True)
class ChildLost:
    """The child ended, or was ended, with no complete result."""

    cause: LossCause
    child_exit: ChildExit
    partial_frame: bool

    def describe(self) -> str:
        """Return text for the task's crash reason."""
        partial = ', partial result discarded' if self.partial_frame else ''
        return f'{self.cause.value}: {describe_exit(self.child_exit)}{partial}'


class NotRunCause(str, Enum):
    """Why a task never reached a child."""

    POOL_STOPPING = 'worker is stopping'
    POOL_FAILED = 'no child could start'


@dataclass(frozen=True, slots=True)
class NotDispatched:
    """The task never reached a child. Its row is still ``CLAIMED``."""

    cause: NotRunCause


SlotOutcome: TypeAlias = TaskFinished | ChildLost | NotDispatched


@dataclass(frozen=True, slots=True)
class WaitExpired:
    """A bounded wait for a frame ended with no frame and no death."""


class RecycleReason(str, Enum):
    """Why the parent retires a healthy child."""

    TASK_COUNT = 'max_tasks_per_child reached'
    MEMORY_LIMIT = 'max_memory_per_child_mb reached'


@dataclass(frozen=True, slots=True)
class Starting:
    """A child is booting for this slot."""


@dataclass(frozen=True, slots=True)
class Idle:
    """The child is ready, and it holds no task."""


@dataclass(frozen=True, slots=True)
class Busy:
    """The child holds this task."""

    task_id: str


@dataclass(frozen=True, slots=True)
class Retiring:
    """The child is being replaced or stopped. It takes no more tasks."""


SlotState: TypeAlias = Starting | Idle | Busy | Retiring


@dataclass(frozen=True, slots=True)
class SpawnFailed:
    """The OS refused to start a child. Retrying cannot be expected to help."""

    detail: str


@dataclass(frozen=True, slots=True)
class ChildHookFailed:
    """An ``on_child_process_start`` hook failed. Retrying reruns the same hook."""


@dataclass(frozen=True, slots=True)
class BootAttemptsExhausted:
    """Every boot attempt ended before the child was ready."""

    attempts: int
    last_detail: str


PoolFailure: TypeAlias = SpawnFailed | ChildHookFailed | BootAttemptsExhausted


@dataclass(frozen=True, slots=True)
class ChildBaseline:
    """A booted child's settled RSS, for the baseline memory guard."""

    pid: int
    rss: RssReading


@dataclass(frozen=True, slots=True)
class KillSent:
    """SIGKILL went to the child that holds this task."""


@dataclass(frozen=True, slots=True)
class NothingToKill:
    """The task is not on a child: it finished, or it never started."""


KillOutcome: TypeAlias = KillSent | NothingToKill


@dataclass(frozen=True, slots=True)
class Assigned:
    """The task frame reached a child."""


@dataclass(frozen=True, slots=True)
class StopReport:
    """What ``SlotPool.stop`` had to do. Frozen once built."""

    killed_task_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SlotPoolConfig:
    """Fixed settings of one pool."""

    slots: int
    init: InitFrame
    max_tasks_per_child: int
    max_memory_per_child_mb: int | None
    child_entry: tuple[str, ...] = CHILD_ENTRY


def interpreter_flags() -> tuple[str, ...]:
    """Rebuild this interpreter's command-line flags from public ``sys`` state.

    A plain ``exec`` does not inherit them, and ``-O`` changes what ``assert``
    does in task code.
    """
    flags = sys.flags
    args: list[str] = []
    if flags.optimize:
        args.append('-' + 'O' * flags.optimize)
    if flags.dont_write_bytecode:
        args.append('-B')
    if flags.isolated:
        args.append('-I')
    else:
        if flags.ignore_environment:
            args.append('-E')
        if flags.no_user_site:
            args.append('-s')
        if flags.safe_path:
            args.append('-P')
    if flags.no_site:
        args.append('-S')
    if flags.dev_mode:
        args.extend(('-X', 'dev'))
    args.extend(arg for option in sys.warnoptions for arg in ('-W', option))
    return tuple(args)


class FrameProtocol(asyncio.BufferedProtocol):
    """Reads frames into preallocated buffers, so a large body is not copied."""

    def __init__(
        self,
        on_frame: Callable[[ChildFrame], None],
        on_closed: Callable[[], None],
    ) -> None:
        self._on_frame = on_frame
        self._on_closed = on_closed
        self._transport: asyncio.Transport | None = None
        self._header = bytearray(FRAME_HEADER.size)
        self._meta = bytearray()
        self._body = bytearray()
        self._target = memoryview(self._header)
        self._filled = 0
        self._stage = 'header'
        self.bytes_received = 0
        self.violation: FrameOverBound | MalformedFrame | None = None

    def _expect(self, buffer: bytearray, stage: str) -> None:
        self._target = memoryview(buffer)
        self._filled = 0
        self._stage = stage

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = cast('asyncio.Transport', transport)

    def get_buffer(self, sizehint: int) -> memoryview:
        return self._target[self._filled :]

    def buffer_updated(self, nbytes: int) -> None:
        self._filled += nbytes
        self.bytes_received += nbytes
        if self._filled == len(self._target):
            self._advance()

    def _advance(self) -> None:
        match self._stage:
            case 'header':
                match parse_header(bytes(self._header)):
                    case Ok(sizes):
                        self._body = bytearray(sizes.body_bytes)
                        self._meta = bytearray(sizes.meta_bytes)
                        self._expect(self._meta, 'meta')
                    case Err(over):
                        self._fail(over)
            case 'meta':
                if self._body:
                    self._expect(self._body, 'body')
                else:
                    self._finish_frame()
            case 'body':
                self._finish_frame()
            case _:
                raise AssertionError(f'unknown frame stage {self._stage!r}')

    def _finish_frame(self) -> None:
        match decode_child_frame(bytes(self._meta), bytes(self._body)):
            case Ok(frame):
                self._body = bytearray()
                self._expect(self._header, 'header')
                self._on_frame(frame)
            case Err(malformed):
                self._fail(malformed)

    def _fail(self, violation: FrameOverBound | MalformedFrame) -> None:
        self.violation = violation
        assert self._transport is not None
        self._transport.abort()

    def has_partial_frame(self) -> bool:
        return self._stage != 'header' or self._filled > 0

    def eof_received(self) -> bool:
        return False

    def connection_lost(self, exc: Exception | None) -> None:
        self._on_closed()


class Child:
    """One running process and its socket. Does not know about tasks."""

    def __init__(
        self,
        process: asyncio.subprocess.Process,
        protocol: FrameProtocol,
        transport: asyncio.Transport,
        frames: deque[ChildFrame],
        wake: asyncio.Event,
    ) -> None:
        self.process = process
        self.pid = process.pid
        self._protocol = protocol
        self._transport = transport
        self._frames = frames
        self._wake = wake
        self._exit = asyncio.ensure_future(process.wait())
        self._exit.add_done_callback(lambda _: wake.set())
        self.channel_closed = False
        self.killed_by_parent = False

    @classmethod
    async def start(
        cls,
        config: SlotPoolConfig,
        init: InitFrame,
    ) -> Result['Child', SpawnFailed]:
        """Start a process and send its init frame.

        Must run on a long-lived thread: on Linux the child's parent-death
        signal fires when the spawning thread ends. Does not wait for the
        child to be ready.
        """
        parent_sock, child_sock = socket.socketpair()
        try:
            for sock in (parent_sock, child_sock):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUFFER_BYTES)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUFFER_BYTES)
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                *interpreter_flags(),
                *config.child_entry,
                str(child_sock.fileno()),
                str(os.getpid()),
                stdin=asyncio.subprocess.DEVNULL,
                pass_fds=(child_sock.fileno(),),
                start_new_session=True,
            )
        except OSError as exc:
            parent_sock.close()
            return Err(SpawnFailed(f'{type(exc).__name__}: {exc}'))
        finally:
            child_sock.close()
        return Ok(await cls._attach(process, parent_sock, init))

    @classmethod
    async def _attach(
        cls,
        process: asyncio.subprocess.Process,
        parent_sock: socket.socket,
        init: InitFrame,
    ) -> 'Child':
        frames: deque[ChildFrame] = deque()
        wake = asyncio.Event()
        closed: list[Child] = []

        def on_frame(frame: ChildFrame) -> None:
            frames.append(frame)
            wake.set()

        def on_closed() -> None:
            closed[0].channel_closed = True
            wake.set()

        protocol = FrameProtocol(on_frame, on_closed)
        parent_sock.setblocking(False)
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_unix_connection(
            lambda: protocol,
            sock=parent_sock,
        )
        child = cls(process, protocol, transport, frames, wake)
        closed.append(child)
        child.send(init)
        return child

    def send(self, frame: ParentFrame) -> None:
        """Queue a frame. The transport buffers it: one frame per slot at a time."""
        if self._transport.is_closing():
            return
        encoded = encode_parent_frame(frame)
        self._transport.write(encoded.head)
        self._transport.write(encoded.body)

    def kill_group(self) -> None:
        """SIGKILL the child's session, so task subprocesses go with it.

        The session id is the child's pid. A task that calls ``setsid`` itself
        leaves the group.
        """
        try:
            os.killpg(self.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            return

    def kill_for_caller(self) -> None:
        """Kill on the caller's request, and report it as such."""
        self.killed_by_parent = True
        self.kill_group()

    async def await_frame(
        self,
        timeout_s: float | None,
    ) -> ChildFrame | ChildLost | WaitExpired:
        """Wait for the next frame, or for a verdict that none will come.

        One waiter at a time: the slot coroutine that owns this child.
        ``timeout_s=None`` waits until a frame or a death.
        """
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        while True:
            if self._frames:
                return self._frames.popleft()
            if self._exit.done():
                return await self._verdict_after_exit()
            if self.channel_closed:
                return await self._channel_lost()
            self._wake.clear()
            if self._frames or self._exit.done() or self.channel_closed:
                continue
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                return WaitExpired()
            try:
                await asyncio.wait_for(self._wake.wait(), remaining)
            except TimeoutError:
                return WaitExpired()

    async def _verdict_after_exit(self) -> ChildFrame | ChildLost:
        """Read what the dead child already wrote, then decide.

        Bytes it wrote sit in the kernel buffer, and the transport reads them
        on later loop passes. A forked helper could keep writing, so the time
        is bounded as well.
        """
        deadline = time.monotonic() + DRAIN_DEADLINE_S
        seen = -1
        quiet = 0
        while quiet < DRAIN_QUIET_PASSES and time.monotonic() < deadline:
            if self._frames:
                return self._frames.popleft()
            received = self._protocol.bytes_received
            quiet = quiet + 1 if received == seen else 0
            seen = received
            await asyncio.sleep(0)
        if self._frames:
            return self._frames.popleft()
        return self._lost(self._loss_cause())

    def _loss_cause(self) -> LossCause:
        if self._protocol.violation is not None:
            return LossCause.PROTOCOL_VIOLATION
        if self.killed_by_parent:
            return LossCause.KILLED_BY_PARENT
        if self.channel_closed and self.process.returncode is None:
            return LossCause.CHANNEL_LOST
        return LossCause.CHILD_DIED

    def _lost(self, cause: LossCause) -> ChildLost:
        return ChildLost(
            cause=cause,
            child_exit=self._current_exit(),
            partial_frame=self._protocol.has_partial_frame(),
        )

    def _current_exit(self) -> ChildExit:
        returncode = self.process.returncode
        return ExitUnknown() if returncode is None else child_exit_from_returncode(returncode)

    async def _channel_lost(self) -> ChildFrame | ChildLost:
        """The socket closed. A live child can never report a result now."""
        cause = LossCause.CHANNEL_LOST
        if self._protocol.violation is not None:
            cause = LossCause.PROTOCOL_VIOLATION
        else:
            await self._wait_exit(CHANNEL_LOST_GRACE_S)
            if self._exit.done():
                return await self._verdict_after_exit()
        self.kill_group()
        await self._wait_exit(CHILD_KILL_WAIT_S)
        return self._lost(cause)

    async def _wait_exit(self, timeout_s: float) -> None:
        await asyncio.wait({self._exit}, timeout=timeout_s)

    def discard_violating_child(self) -> ChildLost:
        """Kill a child that sent a valid frame at the wrong time."""
        self.kill_group()
        return self._lost(LossCause.PROTOCOL_VIOLATION)

    @property
    def has_exited(self) -> bool:
        return self._exit.done()

    def notify_on_exit(self, callback: Callable[[], None]) -> None:
        """Run ``callback`` on the loop when the process ends."""
        self._exit.add_done_callback(lambda _: callback())

    async def retire(self) -> None:
        """Ask a child with no task to exit, bounded.

        A child that exits by itself keeps any background process that a task
        started on purpose, so this does not signal the group. Only a child
        that ignores the request is killed.
        """
        self.send(StopFrame())
        await self._wait_exit(CHILD_STOP_TIMEOUT_S)
        if not self._exit.done():
            logger.warning(
                'Child pid=%s ignored the stop frame for %ss; killing it',
                self.pid,
                CHILD_STOP_TIMEOUT_S,
            )
            self.kill_group()
        await self.dispose()

    async def dispose(self) -> None:
        """Close the socket and wait for the process, bounded."""
        self._transport.abort()
        await self._wait_exit(CHILD_KILL_WAIT_S)
        if not self._exit.done():
            logger.critical(
                'Child pid=%s did not end %ss after SIGKILL',
                self.pid,
                CHILD_KILL_WAIT_S,
            )
            self._exit.cancel()

    async def dispose_lost(self) -> None:
        """Clean up after a lost child, with any task subprocesses it left."""
        self.kill_group()
        await self.dispose()


class SlotHandle:
    """One submitted task, as the caller sees it."""

    def __init__(self, frame: RunFrame, loop: asyncio.AbstractEventLoop) -> None:
        self.frame = frame
        self.assigned: asyncio.Future[Assigned | NotDispatched] = loop.create_future()
        self.outcome: asyncio.Future[SlotOutcome] = loop.create_future()
        self._child: Child | None = None

    def bind_child(self, child: Child) -> None:
        """Pool only: the task frame was sent to this child."""
        self._child = child
        if not self.assigned.done():
            self.assigned.set_result(Assigned())

    def finish(self, outcome: SlotOutcome) -> None:
        """Pool only: the task's slot work is over."""
        self._child = None
        if not self.assigned.done():
            match outcome:
                case NotDispatched() as not_run:
                    self.assigned.set_result(not_run)
                case TaskFinished() | ChildLost():
                    self.assigned.set_result(Assigned())
                case _ as unreachable:
                    assert_never(unreachable)
        # A caller that cancelled this future must not take the slot down.
        if not self.outcome.done():
            self.outcome.set_result(outcome)

    def kill(self) -> KillOutcome:
        """Kill the child only while it holds this task.

        The binding ends when the result is read, so a late timer cannot reach
        the next task on the same slot.
        """
        child = self._child
        if child is None:
            return NothingToKill()
        child.kill_for_caller()
        return KillSent()


@dataclass(slots=True)
class _Slot:
    index: int
    state: SlotState = field(default_factory=Starting)
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    child: Child | None = None
    handle: SlotHandle | None = None


class SlotPool:
    """A FIFO in the parent and one coroutine per slot.

    ``submit`` never blocks and never fails. The claim budget bounds the FIFO,
    so ``pending_limit`` guards a defect and is not flow control.
    """

    def __init__(self, config: SlotPoolConfig, pending_limit: int) -> None:
        self._config = config
        self._pending_limit = pending_limit
        self._pending: deque[SlotHandle] = deque()
        self._slots = [_Slot(index) for index in range(config.slots)]
        self._tasks: list[asyncio.Task[None]] = []
        self._loop = asyncio.get_running_loop()
        self._stopping = False
        self._failure: PoolFailure | None = None
        self._failed: asyncio.Future[PoolFailure] = self._loop.create_future()
        self._baselines: list[ChildBaseline] = []
        self._first_generation: asyncio.Future[None] = self._loop.create_future()

    async def start(self) -> Result[tuple[ChildBaseline, ...], PoolFailure]:
        """Boot every slot. Returns when all are ready, or on the first failure.

        After a failure, ``stop`` is still required.
        """
        self._tasks = [
            asyncio.ensure_future(self._serve_slot(slot)) for slot in self._slots
        ]
        await asyncio.wait(
            {self._first_generation, self._failed},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if self._failure is not None:
            return Err(self._failure)
        return Ok(tuple(self._baselines))

    def submit(self, frame: RunFrame) -> SlotHandle:
        """Queue a task. A stopped or failed pool answers ``NotDispatched`` at once."""
        handle = SlotHandle(frame, self._loop)
        if self._stopping or self._failure is not None:
            handle.finish(NotDispatched(self._not_run_cause()))
            return handle
        assert len(self._pending) < self._pending_limit, (
            f'{len(self._pending)} tasks wait for a slot, above the claim budget '
            f'of {self._pending_limit}'
        )
        self._pending.append(handle)
        self._wake_slots()
        return handle

    async def wait_failed(self) -> PoolFailure:
        """Wait until no replacement child can start. Fatal to the worker."""
        return await asyncio.shield(self._failed)

    def states(self) -> tuple[SlotState, ...]:
        """Return each slot's state, for health reports and tests."""
        return tuple(slot.state for slot in self._slots)

    def _not_run_cause(self) -> NotRunCause:
        return (
            NotRunCause.POOL_FAILED
            if self._failure is not None
            else NotRunCause.POOL_STOPPING
        )

    def _wake_slots(self) -> None:
        for slot in self._slots:
            slot.wake.set()

    def _release_pending(self) -> None:
        while self._pending:
            self._pending.popleft().finish(NotDispatched(self._not_run_cause()))

    def _record_failure(self, failure: PoolFailure) -> None:
        if self._failure is not None:
            return
        logger.critical('Worker cannot start a child: %s', failure)
        self._failure = failure
        self._failed.set_result(failure)
        self._release_pending()
        self._wake_slots()

    async def _serve_slot(self, slot: _Slot) -> None:
        """Keep one child alive on this slot until the pool stops or fails.

        A slot is a service loop. An unexpected exception here is a defect, so
        it is recorded as a pool failure: a slot must never stop silently
        while work waits for it.
        """
        try:
            while not self._stopping and self._failure is None:
                booted = await self._boot_child(slot)
                match booted:
                    case Err(failure):
                        self._record_failure(failure)
                        return
                    case Ok(child):
                        await self._run_generation(slot, child)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.critical('Slot %s stopped on a defect', slot.index, exc_info=True)
            self._record_failure(
                BootAttemptsExhausted(0, f'{type(exc).__name__}: {exc}'),
            )

    async def _boot_child(self, slot: _Slot) -> Result[Child, PoolFailure]:
        """Start one ready child, with the failure classification in the type."""
        slot.state = Starting()
        last_detail = 'no attempt ran'
        init = replace(
            self._config.init,
            process_name=f'{self._config.init.process_name}-{slot.index}',
        )
        for attempt in range(1, CHILD_BOOT_ATTEMPTS + 1):
            match await Child.start(self._config, init):
                case Err(spawn_failed):
                    return Err(spawn_failed)
                case Ok(child):
                    slot.child = child
                    child.notify_on_exit(slot.wake.set)
            ready = await child.await_frame(CHILD_READY_TIMEOUT_S)
            match ready:
                case ReadyFrame():
                    self._note_ready(ChildBaseline(pid=ready.pid, rss=ready.rss))
                    return Ok(child)
                case ChildLost(child_exit=ExitedWith(code=code)) if (
                    code == CHILD_HOOK_FAILURE_EXIT_CODE
                ):
                    await child.dispose_lost()
                    slot.child = None
                    return Err(ChildHookFailed())
                case ChildLost():
                    last_detail = ready.describe()
                case WaitExpired():
                    last_detail = f'not ready after {CHILD_READY_TIMEOUT_S}s'
                case ResultFrame():
                    last_detail = 'result frame before the ready frame'
                case _ as unreachable:
                    assert_never(unreachable)
            await child.dispose_lost()
            slot.child = None
            logger.warning(
                'Child boot attempt %s/%s failed on slot %s: %s',
                attempt,
                CHILD_BOOT_ATTEMPTS,
                slot.index,
                last_detail,
            )
            if attempt < CHILD_BOOT_ATTEMPTS:
                await asyncio.sleep(CHILD_BOOT_RETRY_DELAY_S * attempt)
        return Err(BootAttemptsExhausted(CHILD_BOOT_ATTEMPTS, last_detail))

    def _note_ready(self, baseline: ChildBaseline) -> None:
        if self._first_generation.done():
            return
        self._baselines.append(baseline)
        if len(self._baselines) == self._config.slots:
            self._first_generation.set_result(None)

    async def _run_generation(self, slot: _Slot, child: Child) -> None:
        """Serve tasks on one child until it dies, retires, or the pool stops."""
        tasks_done = 0
        while True:
            slot.state = Idle()
            handle = await self._next_task(slot, child)
            if handle is None:
                break
            outcome = await self._run_task(slot, child, handle)
            tasks_done += 1
            match outcome:
                case TaskFinished(rss=rss):
                    reason = self._recycle_reason(tasks_done, rss)
                    if reason is not None:
                        logger.debug('Retiring child pid=%s: %s', child.pid, reason.value)
                        break
                case ChildLost():
                    logger.warning(
                        'Lost child pid=%s on slot %s: %s',
                        child.pid,
                        slot.index,
                        outcome.describe(),
                    )
                    slot.state = Retiring()
                    await child.dispose_lost()
                    slot.child = None
                    return
                case _ as unreachable:
                    assert_never(unreachable)
        slot.state = Retiring()
        if child.has_exited:
            await child.dispose_lost()
        else:
            await child.retire()
        slot.child = None

    async def _next_task(self, slot: _Slot, child: Child) -> SlotHandle | None:
        """Wait for work. None means this child must not take any."""
        while True:
            if self._stopping or self._failure is not None or child.has_exited:
                return None
            if self._pending:
                return self._pending.popleft()
            slot.wake.clear()
            if self._pending or child.has_exited:
                continue
            await slot.wake.wait()

    async def _run_task(
        self,
        slot: _Slot,
        child: Child,
        handle: SlotHandle,
    ) -> TaskFinished | ChildLost:
        """Run one task and finish its handle. The state stays ``Busy`` until then."""
        frame = handle.frame
        slot.state = Busy(frame.task_id)
        slot.handle = handle
        child.send(frame)
        handle.bind_child(child)
        answer = await child.await_frame(None)
        outcome: TaskFinished | ChildLost
        match answer:
            case ResultFrame(task_id=task_id) if task_id == frame.task_id:
                outcome = TaskFinished(
                    answer.ok,
                    answer.result_json,
                    answer.failed_reason,
                    answer.rss,
                )
            case ResultFrame() | ReadyFrame():
                outcome = child.discard_violating_child()
            case ChildLost():
                outcome = answer
            case WaitExpired():
                raise AssertionError('an unbounded wait cannot expire')
            case _ as unreachable:
                assert_never(unreachable)
        slot.handle = None
        handle.finish(outcome)
        return outcome

    def _recycle_reason(self, tasks_done: int, rss: RssReading) -> RecycleReason | None:
        limit = self._config.max_tasks_per_child
        if limit > 0 and tasks_done >= limit:
            return RecycleReason.TASK_COUNT
        memory_limit = self._config.max_memory_per_child_mb
        match rss:
            case RssMb(value=value) if memory_limit is not None and value >= memory_limit:
                return RecycleReason.MEMORY_LIMIT
            case RssMb() | RssUnreadable():
                return None
            case _ as unreachable:
                assert_never(unreachable)

    def _kill_at_deadline(self, slot: _Slot) -> tuple[str, ...]:
        if slot.handle is not None:
            match slot.handle.kill():
                case KillSent():
                    return (slot.handle.frame.task_id,)
                case NothingToKill():
                    return ()
                case _ as unreachable:
                    assert_never(unreachable)
        if slot.child is not None and isinstance(slot.state, Starting):
            slot.child.kill_group()
        return ()

    async def stop(self, drain_timeout_s: float) -> StopReport:
        """Stop the pool: release queued tasks, drain busy ones, then kill.

        Idle children exit by stop frame. A task still running at the deadline
        is killed with its session, and its handle gets ``ChildLost``. Cancels
        no task early. Does not touch task rows.
        """
        self._stopping = True
        self._release_pending()
        self._wake_slots()
        if not self._tasks:
            return StopReport(())
        _, running = await asyncio.wait(self._tasks, timeout=drain_timeout_s)
        killed: list[str] = []
        if running:
            for slot in self._slots:
                killed.extend(self._kill_at_deadline(slot))
            _, stuck = await asyncio.wait(self._tasks, timeout=CHILD_KILL_WAIT_S)
            for task in stuck:
                task.cancel()
            if stuck:
                await asyncio.wait(stuck, timeout=CHILD_KILL_WAIT_S)
        for task in self._tasks:
            if task.done() and not task.cancelled() and task.exception() is not None:
                logger.error('Slot task ended with %r', task.exception())
        return StopReport(tuple(killed))
