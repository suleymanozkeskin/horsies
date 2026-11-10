"""Worker-owned child processes: one process and one private socket per slot.

An isolated slot runs one task at a time. A shared async slot runs up to its
seats of ``SHARED_ASYNC`` tasks at once in one child, and places a new task
on the open shared child with the fewest tasks. A crash, a timeout kill, or a
bad frame reaches that slot only, because slots share no queue, lock, or
pipe; in a shared slot it reaches every task of that child.

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
from datetime import datetime, timezone
from enum import Enum
from typing import Final, TypeAlias, assert_never, cast

from horsies.core.logging import get_logger
from horsies.core.models.boot_failure import (
    MAX_BOOT_FAILURE_DETAIL_CHARS,
    AppLoadFailed,
    ChildPoolSetupFailed,
    ClearableBootCause,
    ResourceShortage,
    SpawnFailed,
    TaskLoopCreateFailed,
    ThreadStartRefused,
    cut_text,
    describe_boot_cause,
)
from horsies.core.models.health import DownSlot
from horsies.core.models.tasks import (
    MAX_CANCEL_GRACE_MS,
    MAX_TASK_TIMEOUT_MS,
    MIN_CANCEL_GRACE_MS,
    MIN_TASK_TIMEOUT_MS,
)
from horsies.core.models.worker_profile import (
    MemoryLimit,
    NoMemoryLimit,
    NoSharedAsyncChildren,
    NoTaskLimit,
    SharedAsyncChildren,
    TaskLimit,
)
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.child_runner import CHILD_HOOK_FAILURE_EXIT_CODE
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.live_dispatches import (
    Held,
    HoldOutcome,
    LiveDispatches,
    RegistryClosed,
)
from horsies.core.worker.preload_main import REPARENT_FAILED_EXIT_CODE
from horsies.core.worker.preload import (
    ChildProcess,
    PreloadNotLinux,
    PreloadParent,
    PreloadRefused,
    PreloadStartFailed,
    PreloadSupported,
    PreloadUnsupported,
    preload_support,
)
from horsies.core.worker.child_protocol import (
    FRAME_HEADER,
    BlockingRun,
    BootFailedFrame,
    CulpritUnknown,
    ChildFrame,
    ChildKind,
    FrameOverBound,
    InitFrame,
    LoopHoldsNoTask,
    LoopHoldsTasks,
    MalformedFrame,
    NoTaskLoop,
    ParentFrame,
    ReadyFrame,
    DeadlineFiredFrame,
    LoopBlockedFrame,
    ResultFrame,
    RunStuckFrame,
    SecondCancelFrame,
    RssMb,
    RssReading,
    TaskLoopReport,
    RssUnreadable,
    RunFrame,
    SeatFinished,
    SeatRefusal,
    SeatTask,
    SeatTimedOut,
    SeatWork,
    SharedAsyncChild,
    SharedResultFrame,
    SharedRunFrame,
    StopFrame,
    decode_child_frame,
    encode_parent_frame,
    parse_header,
)
from horsies.core.worker.loop_watchdog import LOOP_BLOCK_LIMIT_MS
from horsies.core.worker.shared_stop import (
    BlockedLoopKill,
    ChildKill,
    CrashPath,
    DeadlineState,
    KilledRunVerdict,
    LoopHeldBy,
    LoopHolderUnknown,
    NoNamedKill,
    RetryPlacement,
    StopState,
    StuckRunsKill,
    retry_placement,
    verdict_at_kill,
)

logger = get_logger('worker')

CHILD_ENTRY: Final[tuple[str, ...]] = ('-m', 'horsies.core.worker.child_main')
PRELOAD_ENTRY: Final[tuple[str, ...]] = ('-m', 'horsies.core.worker.preload_main')
PRELOAD_RESTART_LIMIT: Final = 5
PRELOAD_RESTART_DELAY_S: Final = 5.0
SOCKET_BUFFER_BYTES: Final = 4 << 20
CHILD_BOOT_ATTEMPTS: Final = 3
CHILD_BOOT_RETRY_DELAY_S: Final = 0.5
CHILD_BOOT_KILL_DELAY_MAX_S: Final = 5.0
# A slot that is down boots again after a wait. The wait starts at
# SLOT_DOWN_FIRST_DELAY_S, doubles with each failed boot, and stays at
# SLOT_DOWN_MAX_DELAY_S. So the log line of each failed boot comes at most once
# in that wait, per slot. SLOT_DOWN_DELAY_DOUBLINGS bounds the exponent:
# 1 s * 2**6 = 64 s is past the cap.
SLOT_DOWN_FIRST_DELAY_S: Final = 1.0
SLOT_DOWN_MAX_DELAY_S: Final = 60.0
SLOT_DOWN_DELAY_DOUBLINGS: Final = 6
CHILD_READY_TIMEOUT_S: Final = 120.0
CHILD_STOP_TIMEOUT_S: Final = 10.0
CHILD_KILL_WAIT_S: Final = 30.0
SLOT_DEFECT_LIMIT: Final = 3
CHANNEL_LOST_GRACE_S: Final = 0.2
DRAIN_QUIET_PASSES: Final = 3
DRAIN_DEADLINE_S: Final = 5.0
# A shared child reports the end of the cancel grace of a run at once
# (SecondCancelFrame), and its watchdog reports a loop blocked for the block
# limit. The parent checks a run that has not ended this long after its
# deadline and its cancel grace: a child that has reported neither the end of
# the grace nor the end of the run by then has a frozen process, and the parent
# kills it. The margin is past the block limit, so a block that the watchdog
# can report is reported first, with its culprit.
SHARED_BACKSTOP_MARGIN_S: Final = LOOP_BLOCK_LIMIT_MS / 1000 + 5.0
# A refusal runs only the RUNNING update and the finalizing marker: a pool
# wait of at most 30 s and a marker of at most 10 s, with the statements
# between them. A refusal still in flight this long after its placement means
# a frozen child or a database that does not answer: the parent kills the
# child, and the refusal takes the crash path.
SHARED_REFUSAL_BACKSTOP_S: Final = 120.0


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
    SLOT_DEFECT = 'the worker slot failed'


@dataclass(frozen=True, slots=True)
class TaskFinished:
    """The child reported a result. ``ok`` has the ``_run_task_entry`` meaning.

    ``rss`` and ``task_loop`` decide whether the child takes another task.
    """

    ok: bool
    result_json: str
    failed_reason: str | None
    rss: RssReading
    task_loop: TaskLoopReport


@dataclass(frozen=True, slots=True)
class ChildLost:
    """The child ended, or was ended, with no complete result."""

    cause: LossCause
    child_exit: ChildExit
    partial_frame: bool
    pid: int

    def describe(self) -> str:
        """Return text for the task's crash reason."""
        partial = ', partial result discarded' if self.partial_frame else ''
        return f'{self.cause.value}: {describe_exit(self.child_exit)}{partial}'


class NotRunCause(str, Enum):
    """Why a task never reached a child."""

    POOL_STOPPING = 'worker is stopping'
    POOL_FAILED = 'no child could start'
    QUEUE_FULL = 'too many tasks wait for a slot'
    NO_ISOLATED_CHILD = 'this worker has no isolated child'


@dataclass(frozen=True, slots=True)
class NotDispatched:
    """The task never reached a child. Its row is still ``CLAIMED``."""

    cause: NotRunCause


SlotOutcome: TypeAlias = TaskFinished | ChildLost | NotDispatched


@dataclass(frozen=True, slots=True)
class SeatTaskFinished:
    """The task of a seat ended before its deadline. ``ok`` has the
    ``_run_task_entry`` meaning."""

    ok: bool
    result_json: str
    failed_reason: str | None


@dataclass(frozen=True, slots=True)
class SeatDeadlineEnded:
    """The deadline of the task fired in the child, the child cancelled the
    task, and its coroutine and its marked work have ended. The outcome is
    the timeout. ``retry`` is ``ISOLATED`` when the child reported the run
    stuck before it ended."""

    retry: RetryPlacement


@dataclass(frozen=True, slots=True)
class SeatKilled:
    """The worker killed the child of this run for a named cause or after
    its deadline, and saw its exit. ``verdict`` decides the outcome."""

    verdict: KilledRunVerdict
    pid: int
    child_exit: ExitedWith | KilledBySignal


SharedOutcome: TypeAlias = (
    SeatTaskFinished | SeatDeadlineEnded | SeatKilled | ChildLost | NotDispatched
)


@dataclass(frozen=True, slots=True)
class SharedRunRequest:
    """A ``SHARED_ASYNC`` task to run, before the pool numbers its run.

    The ``timeout_ms`` and ``cancel_grace_ms`` of a ``SeatTask`` are within
    the bounds of ``SharedRunFrame``.
    """

    task_id: str
    task_name: str
    args_json: str | None
    kwargs_json: str | None
    worker_id: str
    is_workflow_task: bool
    claimed_at: datetime | None
    work: SeatWork

    def __post_init__(self) -> None:
        match self.work:
            case SeatTask(timeout_ms=timeout_ms, cancel_grace_ms=cancel_grace_ms):
                assert (
                    MIN_TASK_TIMEOUT_MS <= timeout_ms <= MAX_TASK_TIMEOUT_MS
                ), f'timeout_ms {timeout_ms} of task {self.task_id} is out of bounds'
                assert MIN_CANCEL_GRACE_MS <= cancel_grace_ms <= MAX_CANCEL_GRACE_MS, (
                    f'cancel_grace_ms {cancel_grace_ms} of task {self.task_id} is '
                    'out of bounds'
                )
            case SeatRefusal():
                pass
            case _ as unreachable:
                assert_never(unreachable)

    def frame_for(self, dispatch: int) -> SharedRunFrame:
        """The run frame of run ``dispatch``."""
        return SharedRunFrame(
            dispatch=dispatch,
            task_id=self.task_id,
            task_name=self.task_name,
            args_json=self.args_json,
            kwargs_json=self.kwargs_json,
            worker_id=self.worker_id,
            is_workflow_task=self.is_workflow_task,
            claimed_at=self.claimed_at,
            work=self.work,
        )


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


@dataclass(frozen=True, slots=True)
class Down:
    """The last boot of this slot failed for a cause that can clear.

    The slot takes no task, and the claim budget leaves it out. It boots
    again after ``slot_down_delay_s(failed_boots)``, and keeps this state
    while that boot runs. A boot that succeeds returns it to service.
    ``failed_boots`` counts the failed boots since the slot went down, at
    least 1. ``detail`` has at most ``MAX_BOOT_FAILURE_DETAIL_CHARS``
    characters.
    """

    cause: ClearableBootCause
    detail: str
    down_since: datetime
    failed_boots: int


SlotState: TypeAlias = Starting | Idle | Busy | Retiring | Down


class CloseReason(str, Enum):
    """Why a shared child takes no new task."""

    TASK_COUNT = 'the task limit per seat was reached'
    MEMORY_LIMIT = 'the memory limit was reached'
    POOL_STOPPING = 'the worker is stopping'
    SECOND_CANCEL = 'a task needed a second cancel'
    TASK_STUCK = 'a task did not stop after its second cancel'
    LOOP_BLOCKED = 'a task blocked the event loop'


@dataclass(frozen=True, slots=True)
class DeadlineNotReported:
    """The child has not reported that the deadline of this run fired."""


@dataclass(frozen=True, slots=True)
class DeadlineReported:
    """The child reported that the deadline of this run fired, and that it
    cancelled the task: its loop was alive at the deadline."""


@dataclass(frozen=True, slots=True)
class SecondCancelReported:
    """The child reported that the cancel grace of this run ran out while
    the run was alive, and that it sent the second cancel."""


@dataclass(frozen=True, slots=True)
class StuckReported:
    """The child reported that this run did not stop after its second cancel
    and its cancel rounds. The run is a culprit: its retry runs isolated."""


DeadlineReport: TypeAlias = (
    DeadlineNotReported | DeadlineReported | SecondCancelReported | StuckReported
)


@dataclass(slots=True)
class _SeatRun:
    """One task in flight on a shared child. ``deadline`` moves forward, one
    report at a time, in the order of the ``DeadlineReport`` members.
    ``sent_at`` is the monotonic time of the send: the deadline of the run
    in the child is ``timeout_ms`` after a start that is not earlier."""

    handle: SharedHandle
    held: HoldOutcome
    deadline: DeadlineReport
    sent_at: float


class SeatRuns:
    """The tasks in flight on one shared child, by dispatch number. At most
    ``seats`` of them. ``kill`` names the cause of a kill that the worker
    sent to the child: it changes at most once, from ``NoNamedKill``."""

    def __init__(self, seats: int) -> None:
        self.seats = seats
        self._by_dispatch: dict[int, _SeatRun] = {}
        self.kill: ChildKill = NoNamedKill()

    def count(self) -> int:
        return len(self._by_dispatch)

    def free(self) -> int:
        return self.seats - len(self._by_dispatch)

    def add(self, dispatch: int, run: _SeatRun) -> None:
        """Hold a new run. A full table or a number in use is a defect."""
        assert self.free() > 0, f'run {dispatch} placed on a child with no free seat'
        assert dispatch not in self._by_dispatch, f'run {dispatch} placed twice'
        self._by_dispatch[dispatch] = run

    def get(self, dispatch: int) -> _SeatRun | None:
        return self._by_dispatch.get(dispatch)

    def remove(self, dispatch: int) -> _SeatRun:
        """Drop a run that ``get`` found. A missing number is a defect."""
        return self._by_dispatch.pop(dispatch)

    def drain(self) -> tuple[_SeatRun, ...]:
        """Drop every run, and return them in dispatch order."""
        runs = tuple(self._by_dispatch[key] for key in sorted(self._by_dispatch))
        self._by_dispatch.clear()
        return runs

    def task_ids(self) -> tuple[str, ...]:
        return tuple(
            self._by_dispatch[key].handle.frame.task_id
            for key in sorted(self._by_dispatch)
        )

    def all_stuck(self) -> bool:
        """Whether at least one run is in flight, and each was reported stuck."""
        return bool(self._by_dispatch) and all(
            isinstance(run.deadline, StuckReported)
            for run in self._by_dispatch.values()
        )


@dataclass(frozen=True, slots=True)
class Open:
    """The shared child is ready and takes tasks. ``runs`` holds the tasks in
    flight."""

    runs: SeatRuns


@dataclass(frozen=True, slots=True)
class Closing:
    """The shared child takes no new task. Its tasks in flight finish, then
    it retires and a new child boots."""

    reason: CloseReason
    runs: SeatRuns


SharedSlotState: TypeAlias = Starting | Open | Closing | Retiring | Down


@dataclass(frozen=True, slots=True)
class LostBeforeReady:
    """The child ended, or was ended, before its ready frame, and sent no
    boot failure frame.

    The worker cannot tell what failed: a crash in native code, an exit from
    app code, or a defect. So this cause counts as one that repeats.
    """

    loss: LossCause
    child_exit: ChildExit


@dataclass(frozen=True, slots=True)
class NotReadyInTime:
    """The child sent no frame within ``CHILD_READY_TIMEOUT_S``. This cause repeats."""


RepeatingBootCause: TypeAlias = (
    AppLoadFailed | ChildPoolSetupFailed | LostBeforeReady | NotReadyInTime
)
BootFailureCause: TypeAlias = RepeatingBootCause | ClearableBootCause


def describe_failure_cause(cause: BootFailureCause) -> str:
    """Return the cause as short text for logs and error messages."""
    match cause:
        case LostBeforeReady():
            return 'the child ended before it was ready'
        case NotReadyInTime():
            return 'the child was not ready in time'
        case (
            AppLoadFailed()
            | ChildPoolSetupFailed()
            | ResourceShortage()
            | TaskLoopCreateFailed()
            | ThreadStartRefused()
            | SpawnFailed()
        ):
            return describe_boot_cause(cause)
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class BootFailure:
    """One boot that ended before the child was ready.

    ``detail`` is the error text, at most ``MAX_BOOT_FAILURE_DETAIL_CHARS``
    characters. Build it with ``boot_failure``.
    """

    cause: BootFailureCause
    detail: str

    def describe(self) -> str:
        """Subject and cause, for a log line."""
        return f'{describe_failure_cause(self.cause)}: {self.detail}'


def boot_failure(cause: BootFailureCause, detail: str) -> BootFailure:
    """Build a ``BootFailure`` with its detail cut to the bound."""
    return BootFailure(cause, cut_text(detail, MAX_BOOT_FAILURE_DETAIL_CHARS))


@dataclass(frozen=True, slots=True)
class ChildHookFailed:
    """An ``on_child_process_start`` hook failed. Retrying reruns the same hook."""


@dataclass(frozen=True, slots=True)
class BootAttemptsExhausted:
    """``attempts`` counted boot attempts ended before the child was ready.

    ``cause`` and ``detail`` are those of the last attempt. An attempt
    counts when its cause repeats, or when every other slot is down.
    """

    attempts: int
    cause: BootFailureCause
    detail: str


@dataclass(frozen=True, slots=True)
class SlotDefectsRepeated:
    """Slot ``slot`` hit ``SLOT_DEFECT_LIMIT`` defects with no finished task
    between them. ``detail`` names the last one."""

    slot: int
    detail: str


PoolFailure: TypeAlias = (
    ChildHookFailed | BootAttemptsExhausted | PreloadRefused | SlotDefectsRepeated
)


def describe_pool_failure(failure: PoolFailure) -> str:
    """Return a pool failure as text, for logs and the worker's error."""
    match failure:
        case ChildHookFailed():
            return 'an on_child_process_start hook failed'
        case BootAttemptsExhausted(attempts=attempts, cause=cause, detail=detail):
            return (
                f'{attempts} child boot attempts failed; the last one: '
                f'{BootFailure(cause, detail).describe()}'
            )
        case PreloadRefused(reason=reason):
            return f'the preload parent refused: {reason}'
        case SlotDefectsRepeated(slot=slot, detail=detail):
            return f'slot {slot} hit {SLOT_DEFECT_LIMIT} defects in a row: {detail}'
        case _ as unreachable:
            assert_never(unreachable)


class _AttemptCount(Enum):
    """Whether a failed boot uses one of ``CHILD_BOOT_ATTEMPTS``."""

    COUNTS = 'counts'
    SPARED = 'spared'


class _ChildAfterTask(Enum):
    """Whether a child takes another task after a finished one."""

    SERVE = 'serve'
    RETIRE = 'retire'


@dataclass(frozen=True, slots=True)
class PoolStopped:
    """``stop`` ran while the pool was starting. This is no failure."""


@dataclass(frozen=True, slots=True)
class ChildBaseline:
    """A booted child's settled RSS, for the baseline memory guard of its kind."""

    pid: int
    rss: RssReading
    child_kind: ChildKind


@dataclass(frozen=True, slots=True)
class KillSent:
    """SIGKILL went to the child that holds this task."""


@dataclass(frozen=True, slots=True)
class NothingToKill:
    """The task is not on a child: it finished, or it never started."""


KillOutcome: TypeAlias = KillSent | NothingToKill


@dataclass(frozen=True, slots=True)
class ChildEnded:
    """SIGKILL went to the child that held this task, and its exit was seen."""

    pid: int
    child_exit: ExitedWith | KilledBySignal


@dataclass(frozen=True, slots=True)
class ChildEndUnknown:
    """SIGKILL went to the child that held this task. No exit was seen in the wait."""

    pid: int


@dataclass(frozen=True, slots=True)
class SlotAnswered:
    """The slot already finished this task, so nothing was killed."""

    outcome: SlotOutcome


KillWaitOutcome: TypeAlias = ChildEnded | ChildEndUnknown | SlotAnswered | NothingToKill


@dataclass(frozen=True, slots=True)
class SharedAnswered:
    """The run already ended, so nothing was killed."""

    outcome: SharedOutcome


@dataclass(frozen=True, slots=True)
class DeadlineAcknowledged:
    """The child reported the end of the cancel grace of this run: its loop
    was alive then, and the run has not ended since. Nothing was killed: the
    run ends, or the child is killed when every run left in it is stuck."""

    pid: int


BackstopOutcome: TypeAlias = (
    ChildEnded | ChildEndUnknown | SharedAnswered | DeadlineAcknowledged | NothingToKill
)


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
    child_preload: ChildPreload = ChildPreload.NONE
    preload_entry: tuple[str, ...] = PRELOAD_ENTRY
    shared_async: SharedAsyncChildren | NoSharedAsyncChildren = field(
        default_factory=NoSharedAsyncChildren,
    )

    def shared_slots(self) -> int:
        """The number of shared async slots."""
        match self.shared_async:
            case SharedAsyncChildren(count=count):
                return count
            case NoSharedAsyncChildren():
                return 0
            case _ as unreachable:
                assert_never(unreachable)

    def seat_count(self) -> int:
        """One seat per isolated slot, and the seats of each shared slot."""
        match self.shared_async:
            case SharedAsyncChildren(count=count, seats=seats):
                return self.slots + count * seats
            case NoSharedAsyncChildren():
                return self.slots
            case _ as unreachable:
                assert_never(unreachable)


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


def _socket_pair() -> tuple[socket.socket, socket.socket]:
    """Return a new (parent, child) pair with large buffers."""
    parent_sock, child_sock = socket.socketpair()
    for sock in (parent_sock, child_sock):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUFFER_BYTES)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUFFER_BYTES)
    return parent_sock, child_sock


class Child:
    """One running process and its socket. Does not know about tasks."""

    def __init__(
        self,
        process: ChildProcess,
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
        self.on_channel_closed: Callable[[], None] | None = None
        self.on_kill_sent: Callable[[], None] | None = None
        self.on_frame: Callable[[], None] | None = None

    @classmethod
    async def start(
        cls,
        config: SlotPoolConfig,
        init: InitFrame,
        preload: PreloadParent | None = None,
        live_pids: set[int] | None = None,
    ) -> Result['Child', BootFailure]:
        """Start a process and send its init frame.

        A child comes from ``preload`` when one is given and it can serve the
        request, otherwise from ``exec``. ``live_pids`` holds its pid until it
        exits. Must run on a long-lived thread: on Linux the child's
        parent-death signal fires when the spawning thread ends. Does not wait
        for the child to be ready.

        ``Err``: the OS refused the ``exec`` spawn. The cause is always
        ``SpawnFailed``, and no process or socket is left.
        """
        registry = set[int]() if live_pids is None else live_pids
        if preload is not None:
            forked = await cls._start_forked(init, preload, registry)
            if forked is not None:
                return Ok(forked)
        return await cls._start_exec(config, init, registry)

    @classmethod
    async def _start_forked(
        cls,
        init: InitFrame,
        preload: PreloadParent,
        registry: set[int],
    ) -> 'Child | None':
        """Return a forked child, or None.

        After a failure this socket pair must not be used again: a forked child
        may exist and hold it. Closing our end gives that child EOF.
        """
        parent_sock, child_sock = _socket_pair()
        try:
            reply = await preload.fork_child(child_sock, registry)
        finally:
            child_sock.close()
        match reply:
            case Ok(process):
                return await cls._attach_registered(process, parent_sock, init, registry)
            case Err(down):
                parent_sock.close()
                logger.warning('Preload parent gave no child: %s', down.detail)
                return None

    @classmethod
    async def _start_exec(
        cls,
        config: SlotPoolConfig,
        init: InitFrame,
        registry: set[int],
    ) -> Result['Child', BootFailure]:
        parent_sock, child_sock = _socket_pair()
        try:
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
            return Err(boot_failure(SpawnFailed(), f'{type(exc).__name__}: {exc}'))
        finally:
            child_sock.close()
        return Ok(await cls._attach_registered(process, parent_sock, init, registry))

    @classmethod
    async def _attach_registered(
        cls,
        process: ChildProcess,
        parent_sock: socket.socket,
        init: InitFrame,
        registry: set[int],
    ) -> 'Child':
        registry.add(process.pid)
        child = await cls._attach(process, parent_sock, init)
        child.notify_on_exit(lambda: registry.discard(process.pid))
        return child

    @classmethod
    async def _attach(
        cls,
        process: ChildProcess,
        parent_sock: socket.socket,
        init: InitFrame,
    ) -> 'Child':
        frames: deque[ChildFrame] = deque()
        wake = asyncio.Event()
        closed: list[Child] = []

        def on_frame(frame: ChildFrame) -> None:
            frames.append(frame)
            wake.set()
            if closed[0].on_frame is not None:
                closed[0].on_frame()

        def on_closed() -> None:
            closed[0].channel_closed = True
            wake.set()
            if closed[0].on_channel_closed is not None:
                closed[0].on_channel_closed()

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
        leaves the group. ``on_kill_sent`` runs first, so no runner heartbeat
        covers the child's task once the kill is sent.
        """
        if self.on_kill_sent is not None:
            self.on_kill_sent()
        try:
            os.killpg(self.pid, signal.SIGKILL)
        except ProcessLookupError:
            self._kill_before_session()
        except PermissionError:
            return

    def _kill_before_session(self) -> None:
        """A forked child is not its own group until it calls ``setsid``.

        Only an unreaped child is signaled, so its pid cannot be another
        process's.
        """
        if self.process.returncode is not None:
            return
        try:
            os.kill(self.pid, signal.SIGKILL)
        except ProcessLookupError:
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
            pid=self.pid,
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
        return self._lost(LossCause.KILLED_BY_PARENT if self.killed_by_parent else cause)

    async def _wait_exit(self, timeout_s: float) -> None:
        await asyncio.wait({self._exit}, timeout=timeout_s)

    async def wait_exit(self, timeout_s: float) -> ChildExit:
        """Wait up to ``timeout_s`` for the process to end, and report its exit.

        ``ExitUnknown`` means the process was not reaped in that time. Sends
        no signal.
        """
        await self._wait_exit(timeout_s)
        return self._current_exit()

    async def discard_violating_child(self) -> ChildLost:
        """Kill a child that sent a valid frame at the wrong time, and wait for it."""
        self.kill_group()
        await self._wait_exit(CHILD_KILL_WAIT_S)
        return self._lost(LossCause.PROTOCOL_VIOLATION)

    def lost_for(self, cause: LossCause) -> ChildLost:
        """Describe this child's end, for a cause that the caller knows."""
        return self._lost(cause)

    @property
    def has_exited(self) -> bool:
        return self._exit.done()

    def queued_frames(self) -> tuple[ChildFrame, ...]:
        """The frames that arrived and that no ``await_frame`` took yet. Reads only."""
        return tuple(self._frames)

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

    async def kill_and_wait(self, wait_s: float) -> KillWaitOutcome:
        """Kill the child only while it holds this task, then wait for its exit.

        The kill is the one that ``kill`` sends. The wait follows the child
        that got the kill, also after the slot ends the binding, and it ends
        after ``wait_s``. Does not resolve ``outcome``: the slot still does
        that. A handle with no child gives ``SlotAnswered`` when the slot
        finished it, and ``NothingToKill`` when no slot took it yet.
        """
        child = self._child
        if child is None:
            return self._answer_without_child()
        child.kill_for_caller()
        match await child.wait_exit(wait_s):
            case ExitedWith() | KilledBySignal() as child_exit:
                return ChildEnded(pid=child.pid, child_exit=child_exit)
            case ExitUnknown():
                return ChildEndUnknown(pid=child.pid)
            case _ as unreachable:
                assert_never(unreachable)

    def _answer_without_child(self) -> SlotAnswered | NothingToKill:
        """Report a handle that holds no child. A cancelled outcome has no answer."""
        if self.outcome.done() and not self.outcome.cancelled():
            return SlotAnswered(self.outcome.result())
        return NothingToKill()


@dataclass(frozen=True, slots=True)
class _SeatBinding:
    """The child that runs a shared task, and its run there."""

    child: Child
    run: _SeatRun


def _reported_in(frames: tuple[ChildFrame, ...], dispatch: int) -> bool:
    """Whether a frame that the slot has not read yet reports the end of the
    cancel grace of this run, or the run's end."""
    return any(
        isinstance(frame, SecondCancelFrame | RunStuckFrame | SharedResultFrame)
        and frame.dispatch == dispatch
        for frame in frames
    )


class SharedHandle:
    """One submitted shared async task, as the caller sees it."""

    def __init__(self, frame: SharedRunFrame, loop: asyncio.AbstractEventLoop) -> None:
        self.frame = frame
        self.assigned: asyncio.Future[Assigned | NotDispatched] = loop.create_future()
        self.outcome: asyncio.Future[SharedOutcome] = loop.create_future()
        self._binding: _SeatBinding | None = None

    def bind(self, child: Child, run: _SeatRun) -> None:
        """Pool only: the run frame was sent to this child."""
        self._binding = _SeatBinding(child, run)
        if not self.assigned.done():
            self.assigned.set_result(Assigned())

    def finish(self, outcome: SharedOutcome) -> None:
        """Pool only: the run is over. Ends the binding."""
        self._binding = None
        if not self.assigned.done():
            match outcome:
                case NotDispatched() as not_run:
                    self.assigned.set_result(not_run)
                case (
                    SeatTaskFinished()
                    | SeatDeadlineEnded()
                    | SeatKilled()
                    | ChildLost()
                ):
                    self.assigned.set_result(Assigned())
                case _ as unreachable:
                    assert_never(unreachable)
        if not self.outcome.done():
            self.outcome.set_result(outcome)

    async def backstop_kill(self, wait_s: float) -> BackstopOutcome:
        """Kill the child of this run if it is frozen, then wait for its exit.

        The child counts as frozen when the run is still on it and the child
        has reported neither the end of its cancel grace nor its end, also
        not in a frame that the slot has not read yet. A run without a
        deadline (a refusal) reports neither, so its backstop always kills.
        The kill is bound to this run: a run that ended gives
        ``SharedAnswered``, and no later kill can reach another run. The wait
        follows the killed child, also after the slot ends the binding, and
        ends after ``wait_s``. Every task of that child ends with it.
        """
        binding = self._binding
        if binding is None:
            if self.outcome.done() and not self.outcome.cancelled():
                return SharedAnswered(self.outcome.result())
            return NothingToKill()
        child = binding.child
        match binding.run.deadline:
            case SecondCancelReported() | StuckReported():
                return DeadlineAcknowledged(pid=child.pid)
            case DeadlineNotReported() | DeadlineReported():
                if _reported_in(child.queued_frames(), self.frame.dispatch):
                    return DeadlineAcknowledged(pid=child.pid)
            case _ as unreachable:
                assert_never(unreachable)
        child.kill_for_caller()
        match await child.wait_exit(wait_s):
            case ExitedWith() | KilledBySignal() as child_exit:
                return ChildEnded(pid=child.pid, child_exit=child_exit)
            case ExitUnknown():
                return ChildEndUnknown(pid=child.pid)
            case _ as unreachable:
                assert_never(unreachable)


@dataclass(slots=True)
class _Slot:
    index: int
    state: SlotState = field(default_factory=Starting)
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    child: Child | None = None
    handle: SlotHandle | None = None
    defects: int = 0

    def init_for(self, template: InitFrame) -> InitFrame:
        """The init frame of a child of this slot."""
        return replace(template, process_name=f'{template.process_name}-{self.index}')


@dataclass(slots=True)
class _SharedSlot:
    """A shared async slot. ``index`` is unique in the pool, after the
    isolated slots."""

    index: int
    seats: int
    state: SharedSlotState = field(default_factory=Starting)
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    child: Child | None = None
    defects: int = 0

    def init_for(self, template: InitFrame) -> InitFrame:
        """The init frame of a child of this slot: a shared async child."""
        return replace(
            template,
            process_name=f'{template.process_name}-shared-{self.index}',
            child_kind=SharedAsyncChild(seats=self.seats),
        )


@dataclass(frozen=True, slots=True)
class OpenChildLoad:
    """What placement reads of one shared slot: its index, the tasks in
    flight on its child, and its seats."""

    index: int
    runs: int
    seats: int


@dataclass(frozen=True, slots=True)
class NoSeatFree:
    """No open shared child has a free seat."""


def place_on_shared_child(loads: tuple[OpenChildLoad, ...]) -> int | NoSeatFree:
    """The index of the open shared child with the fewest tasks in flight and
    a free seat; the lowest index among equals. Pure.

    Spreading tasks over the children keeps the tasks that one crash or one
    kill ends few: about the tasks in flight divided by the children.
    """
    candidates = [load for load in loads if load.runs < load.seats]
    if not candidates:
        return NoSeatFree()
    return min(candidates, key=lambda load: (load.runs, load.index)).index


def outside_kill_delay_s(kills_in_a_row: int) -> float:
    """Return the wait before the next child. It grows, and it stays bounded."""
    return min(CHILD_BOOT_RETRY_DELAY_S * kills_in_a_row, CHILD_BOOT_KILL_DELAY_MAX_S)


def slot_down_delay_s(failed_boots: int) -> float:
    """Return the wait before the next boot of a slot that is down.

    ``failed_boots`` is at least 1. The wait is ``SLOT_DOWN_FIRST_DELAY_S``
    after the first failed boot, doubles after each next one, and stays at
    ``SLOT_DOWN_MAX_DELAY_S``.
    """
    doublings = min(failed_boots - 1, SLOT_DOWN_DELAY_DOUBLINGS)
    return min(SLOT_DOWN_FIRST_DELAY_S * 2**doublings, SLOT_DOWN_MAX_DELAY_S)


def boot_failure_of(
    reply: BootFailedFrame | ChildLost | WaitExpired,
) -> BootFailure | ChildHookFailed:
    """Name the cause of a boot that ended with ``reply`` in place of a ready frame.

    A boot failure frame carries its cause. A child with no frame is read by
    its exit: ``CHILD_HOOK_FAILURE_EXIT_CODE`` is a failed start hook, and
    ``REPARENT_FAILED_EXIT_CODE`` is a forked child that did not reach the
    worker, so a spawn failure. Any other end is ``LostBeforeReady``, which
    repeats. No reply in time is ``NotReadyInTime``. Pure.
    """
    match reply:
        case BootFailedFrame(cause=cause, error_name=error_name, detail=detail):
            return boot_failure(cause, f'{error_name}: {detail}')
        case ChildLost(child_exit=ExitedWith(code=code)) if (
            code == CHILD_HOOK_FAILURE_EXIT_CODE
        ):
            return ChildHookFailed()
        case ChildLost(child_exit=ExitedWith(code=code)) if (
            code == REPARENT_FAILED_EXIT_CODE
        ):
            return boot_failure(
                SpawnFailed(),
                f'the forked child did not reach the worker: {reply.describe()}',
            )
        case ChildLost(cause=loss, child_exit=child_exit):
            return boot_failure(LostBeforeReady(loss, child_exit), reply.describe())
        case WaitExpired():
            return boot_failure(
                NotReadyInTime(),
                f'no ready frame after {CHILD_READY_TIMEOUT_S}s',
            )
        case _ as unreachable:
            assert_never(unreachable)


def _shared_seat_counts(
    shared_async: SharedAsyncChildren | NoSharedAsyncChildren,
) -> tuple[int, ...]:
    """The seats of each shared slot."""
    match shared_async:
        case SharedAsyncChildren(count=count, seats=seats):
            return (seats,) * count
        case NoSharedAsyncChildren():
            return ()
        case _ as unreachable:
            assert_never(unreachable)


def _shared_capacity_of(slot: _SharedSlot) -> int:
    """The tasks that one shared slot can hold now; see
    ``SlotPool.shared_async_capacity``."""
    match slot.state:
        case Starting() | Open() | Retiring():
            return slot.seats
        case Closing(runs=runs):
            return runs.count()
        case Down():
            return 0
        case _ as unreachable:
            assert_never(unreachable)


def _runs_of(state: SharedSlotState) -> SeatRuns:
    """The tasks in flight of a shared slot in this state: an empty table for
    a state that holds none."""
    match state:
        case Open(runs=runs) | Closing(runs=runs):
            return runs
        case Starting() | Retiring() | Down():
            return SeatRuns(0)
        case _ as unreachable:
            assert_never(unreachable)


def shared_recycle_reason(
    shared_async: SharedAsyncChildren,
    tasks_done: int,
    rss: RssReading,
) -> CloseReason | None:
    """Why a shared child closes after a task, or None when it serves on.

    ``tasks_done`` counts the tasks that ended on the child. The task limit is
    ``after_per_seat.tasks × seats``. The memory limit compares the RSS that
    the child reported with the task. Pure.
    """
    match shared_async.recycle.after_per_seat:
        case TaskLimit(tasks=per_seat) if tasks_done >= per_seat * shared_async.seats:
            return CloseReason.TASK_COUNT
        case TaskLimit() | NoTaskLimit():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    match shared_async.recycle.above, rss:
        case MemoryLimit(mb=limit_mb), RssMb(value=value) if value >= limit_mb:
            return CloseReason.MEMORY_LIMIT
        case (MemoryLimit() | NoMemoryLimit(), RssMb() | RssUnreadable()):
            return None
        case _ as unreachable:
            assert_never(unreachable)


def _deadline_state(run: _SeatRun, now: float) -> DeadlineState:
    """Whether the deadline of a run passed: the child reported it, or
    ``timeout_ms`` passed since the send. The child starts the run after the
    send, so its own deadline is not earlier."""
    match run.deadline:
        case DeadlineReported() | SecondCancelReported() | StuckReported():
            return DeadlineState.PASSED
        case DeadlineNotReported():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    match run.handle.frame.work:
        case SeatTask(timeout_ms=timeout_ms) if now >= run.sent_at + timeout_ms / 1000:
            return DeadlineState.PASSED
        case SeatTask() | SeatRefusal():
            return DeadlineState.PENDING
        case _ as unreachable:
            assert_never(unreachable)


def _stop_state(run: _SeatRun) -> StopState:
    match run.deadline:
        case StuckReported():
            return StopState.STUCK
        case DeadlineNotReported() | DeadlineReported() | SecondCancelReported():
            return StopState.NOT_STUCK
        case _ as unreachable:
            assert_never(unreachable)


def _outcome_at_loss(
    kill: ChildKill,
    run: _SeatRun,
    lost: ChildLost,
    now: float,
) -> SeatKilled | ChildLost:
    """The outcome of a run of a lost shared child.

    A kill by the worker whose exit was seen gives the verdict of
    ``verdict_at_kill``; a verdict of the crash path gives the loss. A death
    that the worker did not cause, and an exit not seen, give the loss.
    """
    match lost.cause, lost.child_exit:
        case LossCause.KILLED_BY_PARENT, ExitedWith() | KilledBySignal() as child_exit:
            pass
        case _:
            return lost
    verdict = verdict_at_kill(
        kill,
        run.handle.frame.dispatch,
        _deadline_state(run, now),
        _stop_state(run),
    )
    match verdict:
        case CrashPath():
            return lost
        case _:
            return SeatKilled(verdict=verdict, pid=lost.pid, child_exit=child_exit)


def _describe_holder(holder: LoopHeldBy | LoopHolderUnknown) -> str:
    match holder:
        case LoopHeldBy(task_id=task_id):
            return f'task {task_id} held it'
        case LoopHolderUnknown():
            return 'code of no task held it'
        case _ as unreachable:
            assert_never(unreachable)


def _in_service(state: SlotState) -> bool:
    """Whether a slot in this state counts in the claim budget."""
    match state:
        case Starting() | Idle() | Busy() | Retiring():
            return True
        case Down():
            return False
        case _ as unreachable:
            assert_never(unreachable)


class SlotPool:
    """A FIFO in the parent per kind of slot, and one coroutine per slot.

    ``submit`` and ``submit_shared`` never block. The claim budget already
    bounds each FIFO, so ``pending_limit`` is a backstop: past it a task is
    refused, not queued, and its row returns to the queue through the
    caller. Isolated slots take tasks from their FIFO one at a time. A shared
    task goes to the open shared child with the fewest tasks in flight and a
    free seat (``place_on_shared_child``), at its submit, when a seat frees,
    and when a shared child opens.
    """

    def __init__(self, config: SlotPoolConfig, pending_limit: int) -> None:
        self._config = config
        self._pending_limit = pending_limit
        self._pending: deque[SlotHandle] = deque()
        self._shared_pending: deque[SharedHandle] = deque()
        self._next_dispatch = 0
        self._slots = [_Slot(index) for index in range(config.slots)]
        self._shared_slots = [
            _SharedSlot(index=config.slots + offset, seats=seats)
            for offset, seats in enumerate(_shared_seat_counts(config.shared_async))
        ]
        self._tasks: list[asyncio.Task[None]] = []
        self._loop = asyncio.get_running_loop()
        self._stopping = False
        self._failure: PoolFailure | None = None
        self._failed: asyncio.Future[PoolFailure] = self._loop.create_future()
        self._baselines: dict[int, ChildBaseline] = {}
        self._stop_requested: asyncio.Future[None] = self._loop.create_future()
        self._first_generation: asyncio.Future[None] = self._loop.create_future()
        self._preload: PreloadParent | None = None
        self._preload_watch: asyncio.Task[None] | None = None
        # Every child that has not exited, from any preload parent or ``exec``.
        self._live_pids: set[int] = set()
        self._live_dispatches = LiveDispatches(config.seat_count())

    async def start(
        self,
    ) -> Result[tuple[ChildBaseline, ...], PoolFailure | PoolStopped]:
        """Boot every slot. Returns when the first generation is in, on the
        first failure, or at a stop.

        The first generation is in when every slot booted once or is
        ``Down``, and at least one slot booted. The pool then starts
        degraded: ``Ok`` holds one baseline per booted slot, and each slot
        that is down keeps booting. When every slot is down, failed boots
        count, so the pool fails after ``CHILD_BOOT_ATTEMPTS`` of them.

        After a failure, ``stop`` is still required.
        """
        refused = await self._start_preload()
        if refused is not None:
            self._record_failure(refused)
            return Err(refused)
        self._tasks = [
            asyncio.ensure_future(self._serve_slot(slot)) for slot in self._all_slots()
        ]
        await asyncio.wait(
            {self._first_generation, self._failed, self._stop_requested},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if self._failure is not None:
            return Err(self._failure)
        if self._stopping:
            return Err(PoolStopped())
        self._log_degraded_start()
        return Ok(tuple(self._baselines.values()))

    @property
    def live_dispatches(self) -> LiveDispatches:
        """The tasks that this pool's children hold, for the runner heartbeat."""
        return self._live_dispatches

    def _log_degraded_start(self) -> None:
        down = self.down_slots()
        if not down:
            return
        total = len(self._all_slots())
        logger.warning(
            'Worker starts degraded: %s of %s slots serve; down: %s',
            total - len(down),
            total,
            '; '.join(
                f'slot {item.slot}: {describe_boot_cause(item.cause)}' for item in down
            ),
        )

    def submit(self, frame: RunFrame) -> SlotHandle:
        """Queue a task for an isolated slot. A pool that cannot take it
        answers ``NotDispatched`` at once: also a pool with no isolated slot."""
        handle = SlotHandle(frame, self._loop)
        if self._stopping or self._failure is not None:
            handle.finish(NotDispatched(self._not_run_cause()))
            return handle
        if not self._slots:
            handle.finish(NotDispatched(NotRunCause.NO_ISOLATED_CHILD))
            return handle
        if len(self._pending) >= self._pending_limit:
            logger.error(
                'Refusing task %s: %s tasks already wait for a slot',
                frame.task_id,
                len(self._pending),
            )
            handle.finish(NotDispatched(NotRunCause.QUEUE_FULL))
            return handle
        self._pending.append(handle)
        self._wake_slots()
        return handle

    def submit_shared(
        self,
        request: SharedRunRequest,
    ) -> SharedHandle:
        """Queue a shared async task, and place it at once when a seat is free.

        The pool numbers the run. A pool that cannot take it answers
        ``NotDispatched`` at once. A pool with no shared slot is a defect of
        the caller, which routes a task here only when the worker has shared
        async children: ``AssertionError``.
        """
        assert (
            self._shared_slots
        ), 'a shared task was submitted to a pool with no shared slot'
        self._next_dispatch += 1
        handle = SharedHandle(request.frame_for(self._next_dispatch), self._loop)
        if self._stopping or self._failure is not None:
            handle.finish(NotDispatched(self._not_run_cause()))
            return handle
        if len(self._shared_pending) >= self._pending_limit:
            logger.error(
                'Refusing shared task %s: %s tasks already wait for a seat',
                request.task_id,
                len(self._shared_pending),
            )
            handle.finish(NotDispatched(NotRunCause.QUEUE_FULL))
            return handle
        self._shared_pending.append(handle)
        self._place_shared()
        return handle

    async def _start_preload(self) -> PreloadRefused | None:
        """Start the preload parent, if this mode and this host use one.

        Only the app mode can refuse: the user chose it, so a failure there is
        reported. In the library mode a failure costs child start time, so
        children use ``exec`` and the worker continues. A Linux host that
        cannot run the parent logs a warning, because children then start
        slower and use more memory. Other platforms use ``exec`` by design.
        """
        mode = self._config.child_preload
        if mode is ChildPreload.NONE:
            return None
        match preload_support():
            case PreloadSupported():
                pass
            case PreloadNotLinux(platform=platform):
                if mode is ChildPreload.APP:
                    logger.warning(
                        'App preload needs Linux, this is %s. Using exec.',
                        platform,
                    )
                return None
            case PreloadUnsupported(reason=reason):
                logger.warning(
                    'Child preload cannot run on this host: %s. Using exec.',
                    reason,
                )
                return None
            case _ as unreachable:
                assert_never(unreachable)
        match await self._launch_preload():
            case Ok(parent):
                self._preload = parent
                self._preload_watch = asyncio.ensure_future(self._watch_preload(parent))
                return None
            case Err(failure) if mode is ChildPreload.APP:
                match failure:
                    case PreloadRefused():
                        return failure
                    case PreloadStartFailed(detail=detail):
                        return PreloadRefused(detail)
                    case _ as unreachable:
                        assert_never(unreachable)
            case Err(failure):
                logger.warning('No preload parent, using exec: %s', failure)
                return None

    async def _launch_preload(
        self,
    ) -> Result[PreloadParent, PreloadRefused | PreloadStartFailed]:
        return await PreloadParent.start(
            (*interpreter_flags(), *self._config.preload_entry),
            self._config.child_preload,
            self._config.init,
        )

    async def _watch_preload(self, parent: PreloadParent) -> None:
        """Replace a dead preload parent, bounded.

        Running children are not affected: they are this process's children.
        New children use ``exec`` until a replacement is up. A failed restart
        spends one attempt. At the bound, children use ``exec`` for the rest
        of this worker's life.
        """
        current: PreloadParent | None = parent
        for attempt in range(1, PRELOAD_RESTART_LIMIT + 1):
            if current is not None:
                await current.process.wait()
                self._preload = None
                logger.error(
                    'Preload parent pid=%s exited with %s; new children use exec',
                    current.process.pid,
                    current.process.returncode,
                )
            if self._stopping:
                return
            await asyncio.sleep(PRELOAD_RESTART_DELAY_S)
            current = await self._restart_preload(attempt)
            if self._stopping:
                return
        if current is not None:
            await current.process.wait()
            self._preload = None
        logger.critical(
            'Preload parent is down after %s restarts; children use exec from now on',
            PRELOAD_RESTART_LIMIT,
        )

    async def _restart_preload(self, attempt: int) -> PreloadParent | None:
        match await self._launch_preload():
            case Ok(replacement):
                if self._stopping:
                    await replacement.stop()
                    return None
                self._preload = replacement
                logger.info('Preload parent restarted, attempt %s', attempt)
                return replacement
            case Err(failure):
                logger.error('Preload parent restart %s failed: %s', attempt, failure)
                return None

    async def wait_failed(self) -> PoolFailure:
        """Wait until no replacement child can start. Fatal to the worker."""
        return await asyncio.shield(self._failed)

    def states(self) -> tuple[SlotState, ...]:
        """Return the state of each isolated slot, for health reports and tests."""
        return tuple(slot.state for slot in self._slots)

    def shared_states(self) -> tuple[SharedSlotState, ...]:
        """Return the state of each shared slot, for health reports and tests."""
        return tuple(slot.state for slot in self._shared_slots)

    def slots_in_service(self) -> int:
        """Count the isolated slots that the claim budget counts: every one
        not ``Down``.

        A slot that boots, serves, or retires counts, because it takes a task
        once its child is ready. Reads only.
        """
        return sum(1 for slot in self._slots if _in_service(slot.state))

    def shared_async_capacity(self) -> int:
        """The ``SHARED_ASYNC`` tasks that the shared slots can hold now.

        Every seat of a slot that boots, is open, or retires counts: it takes
        a task once its child is ready. A closing slot counts only the tasks
        it still runs: its free seats take no task, and the claim counts its
        running tasks as use. A slot that is down counts nothing. Reads only.
        """
        return sum(_shared_capacity_of(slot) for slot in self._shared_slots)

    def down_slots(self) -> tuple[DownSlot, ...]:
        """Each slot in ``Down``, in slot order, for the health report. Reads only."""
        return tuple(
            DownSlot(
                slot=slot.index,
                cause=state.cause,
                detail=state.detail,
                down_since=state.down_since,
                failed_boots=state.failed_boots,
            )
            for slot in self._all_slots()
            if isinstance(state := slot.state, Down)
        )

    def _all_slots(self) -> tuple[_Slot | _SharedSlot, ...]:
        """Every slot of the pool, isolated first, in index order."""
        return (*self._slots, *self._shared_slots)

    def _not_run_cause(self) -> NotRunCause:
        return (
            NotRunCause.POOL_FAILED
            if self._failure is not None
            else NotRunCause.POOL_STOPPING
        )

    def _wake_slots(self) -> None:
        for slot in self._all_slots():
            slot.wake.set()

    def _release_pending(self) -> None:
        while self._pending:
            self._pending.popleft().finish(NotDispatched(self._not_run_cause()))
        while self._shared_pending:
            self._shared_pending.popleft().finish(NotDispatched(self._not_run_cause()))

    def _record_failure(self, failure: PoolFailure) -> None:
        if self._failure is not None:
            return
        logger.critical(
            'Worker cannot start a child: %s',
            describe_pool_failure(failure),
        )
        self._failure = failure
        self._failed.set_result(failure)
        self._release_pending()
        self._wake_slots()

    async def _serve_slot(self, slot: _Slot | _SharedSlot) -> None:
        """Keep one child alive on this slot until the pool stops or fails.

        A slot is a service loop. An unexpected exception here is a defect: it
        costs that slot's task and child, and the slot goes on. Repeated
        defects with no finished task between them fail the pool, so a slot
        cannot stop silently or spin while work waits for it.
        """
        while not self._stopping and self._failure is None:
            try:
                match await self._boot_child(slot):
                    case Ok(child):
                        await self._run_generation_of(slot, child)
                    case Err(error):
                        self._end_after_boot_error(error)
                        return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.critical('Slot %s hit a defect', slot.index, exc_info=True)
                await self._clear_slot(slot)
                slot.defects += 1
                if slot.defects >= SLOT_DEFECT_LIMIT:
                    detail = cut_text(
                        f'{type(exc).__name__}: {exc}',
                        MAX_BOOT_FAILURE_DETAIL_CHARS,
                    )
                    self._record_failure(SlotDefectsRepeated(slot.index, detail))
                    return

    def _end_after_boot_error(self, error: PoolFailure | PoolStopped) -> None:
        match error:
            case PoolStopped():
                return
            case (
                ChildHookFailed()
                | BootAttemptsExhausted()
                | PreloadRefused()
                | SlotDefectsRepeated()
            ):
                self._record_failure(error)
            case _ as unreachable:
                assert_never(unreachable)

    async def _run_generation_of(self, slot: _Slot | _SharedSlot, child: Child) -> None:
        match slot:
            case _Slot():
                await self._run_generation(slot, child)
            case _SharedSlot():
                await self._run_shared_generation(slot, child)
            case _ as unreachable:
                assert_never(unreachable)

    async def _clear_slot(self, slot: _Slot | _SharedSlot) -> None:
        """End this slot's child, and answer its tasks, after a defect."""
        match slot:
            case _Slot():
                slot.state = Retiring()
                child, slot.child = slot.child, None
                handle, slot.handle = slot.handle, None
                if child is not None:
                    await child.dispose_lost()
                if handle is not None and child is not None:
                    handle.finish(child.lost_for(LossCause.SLOT_DEFECT))
            case _SharedSlot():
                runs = _runs_of(slot.state)
                slot.state = Retiring()
                child, slot.child = slot.child, None
                if child is not None:
                    await child.dispose_lost()
                    self._end_all_runs(runs, child.lost_for(LossCause.SLOT_DEFECT))
            case _ as unreachable:
                assert_never(unreachable)

    async def _boot_child(
        self,
        slot: _Slot | _SharedSlot,
    ) -> Result[Child, PoolFailure | PoolStopped]:
        """Start one ready child. The cause of each failed boot decides what follows.

        - A cause that repeats counts one of ``CHILD_BOOT_ATTEMPTS``.
        - A cause that can clear puts the slot in ``Down``, and the slot boots
          again after ``slot_down_delay_s``. It counts only while every other
          slot is ``Down`` too: a worker where no slot can serve must fail
          where its supervisor sees it.
        - A SIGKILL from outside does not count while another child serves
          (``_kill_spares_the_attempt``).
        - A failed start hook fails the pool at once.

        The last counted attempt gives ``BootAttemptsExhausted``. Returns
        ``Err(PoolStopped)`` once ``stop`` runs, and the pool's failure once
        another slot failed it. After any ``Err`` the slot holds no child.

        The loop ends at a ready child, at the counted bound, at a stop, or at
        a pool failure. Attempts that do not count have no count bound, as a
        slot that is down keeps booting while another slot serves; the waits
        of ``slot_down_delay_s`` and ``outside_kill_delay_s`` pace them.
        """
        slot.state = Starting()
        init = slot.init_for(self._config.init)
        counted = 0
        outside_kills = 0
        while True:
            if self._stopping:
                return Err(PoolStopped())
            if self._failure is not None:
                return Err(self._failure)
            match await self._boot_attempt(slot, init):
                case Ok(child):
                    self._log_back_in_service(slot)
                    return Ok(child)
                case Err(ended):
                    pass
            match ended:
                case ChildHookFailed() | PoolStopped():
                    return Err(ended)
                case BootFailure():
                    failed = ended
                case _ as unreachable:
                    assert_never(unreachable)
            if self._kill_spares_the_attempt(slot, failed.cause):
                outside_kills += 1
                logger.warning(
                    'Replacement child on slot %s was killed during boot, %s in a row',
                    slot.index,
                    outside_kills,
                )
                await self._pause(outside_kill_delay_s(outside_kills))
                continue
            match self._attempt_count(slot, failed.cause):
                case _AttemptCount.COUNTS:
                    counted += 1
                case _AttemptCount.SPARED:
                    pass
            delay_s = self._record_failed_boot(slot, failed, counted)
            if counted >= CHILD_BOOT_ATTEMPTS:
                return Err(BootAttemptsExhausted(counted, failed.cause, failed.detail))
            await self._pause(delay_s)

    def _release_on_kill_or_exit(self, child: Child) -> None:
        """Stop the runner heartbeat of this child's task at a kill or an exit."""

        def release() -> None:
            _ = self._live_dispatches.release_child(child.pid)

        child.on_kill_sent = release
        child.notify_on_exit(release)

    async def _boot_attempt(
        self,
        slot: _Slot | _SharedSlot,
        init: InitFrame,
    ) -> Result[Child, BootFailure | ChildHookFailed | PoolStopped]:
        """Start one child and wait for its ready frame, bounded.

        ``Ok``: the child is ready, it is ``slot.child``, and its baseline is
        noted. ``Err``: the slot holds no child, and every process of this
        attempt is ended. Does not change ``slot.state``.
        """
        match await Child.start(self._config, init, self._preload, self._live_pids):
            case Err(spawn_failed):
                return Err(spawn_failed)
            case Ok(child):
                pass
        slot.child = child
        child.notify_on_exit(slot.wake.set)
        child.on_channel_closed = slot.wake.set
        self._release_on_kill_or_exit(child)
        if self._stopping:
            # ``stop`` ran during that await, so it did not see this child.
            await child.dispose_lost()
            slot.child = None
            return Err(PoolStopped())
        reply = await child.await_frame(CHILD_READY_TIMEOUT_S)
        match reply:
            case ReadyFrame(pid=pid, rss=rss):
                self._note_ready(slot, ChildBaseline(pid, rss, init.child_kind))
                return Ok(child)
            case (
                ResultFrame()
                | SharedResultFrame()
                | DeadlineFiredFrame()
                | SecondCancelFrame()
                | RunStuckFrame()
                | LoopBlockedFrame()
            ):
                failed = boot_failure_of(await child.discard_violating_child())
            case BootFailedFrame() | ChildLost() | WaitExpired():
                failed = boot_failure_of(reply)
            case _ as unreachable:
                assert_never(unreachable)
        await child.dispose_lost()
        slot.child = None
        if self._stopping:
            return Err(PoolStopped())
        return Err(failed)

    def _attempt_count(
        self,
        slot: _Slot | _SharedSlot,
        cause: BootFailureCause,
    ) -> _AttemptCount:
        """A cause that repeats counts. A cause that can clear counts only
        while every other slot is ``Down``."""
        match cause:
            case (
                AppLoadFailed()
                | ChildPoolSetupFailed()
                | LostBeforeReady()
                | NotReadyInTime()
            ):
                return _AttemptCount.COUNTS
            case (
                ResourceShortage()
                | TaskLoopCreateFailed()
                | ThreadStartRefused()
                | SpawnFailed()
            ):
                every_other_down = all(
                    isinstance(other.state, Down)
                    for other in self._all_slots()
                    if other is not slot
                )
                return (
                    _AttemptCount.COUNTS if every_other_down else _AttemptCount.SPARED
                )
            case _ as unreachable:
                assert_never(unreachable)

    def _record_failed_boot(
        self, slot: _Slot | _SharedSlot, failed: BootFailure, counted: int
    ) -> float:
        """Set the slot state after a failed boot, log it, and return the wait
        before the next boot."""
        match failed.cause:
            case (
                ResourceShortage()
                | TaskLoopCreateFailed()
                | ThreadStartRefused()
                | SpawnFailed()
            ):
                return self._mark_down(slot, failed.cause, failed.detail, counted)
            case (
                AppLoadFailed()
                | ChildPoolSetupFailed()
                | LostBeforeReady()
                | NotReadyInTime()
            ):
                slot.state = Starting()
                logger.warning(
                    'Child boot attempt %s/%s failed on slot %s: %s',
                    counted,
                    CHILD_BOOT_ATTEMPTS,
                    slot.index,
                    failed.describe(),
                )
                return CHILD_BOOT_RETRY_DELAY_S * counted
            case _ as unreachable:
                assert_never(unreachable)

    def _mark_down(
        self,
        slot: _Slot | _SharedSlot,
        cause: ClearableBootCause,
        detail: str,
        counted: int,
    ) -> float:
        """Put the slot in ``Down``, log the failed boot, and return the wait
        before the next boot.

        A slot that is already down keeps its ``down_since`` and counts one
        more failed boot. One log line per failed boot: the wait between
        boots bounds the rate. The line names the counted boot attempts of
        this slot, which grow only while every other slot is down.
        """
        match slot.state:
            case Down(down_since=since, failed_boots=failed_boots):
                down = Down(cause, detail, since, failed_boots + 1)
            case Starting() | Idle() | Busy() | Retiring() | Open() | Closing():
                down = Down(cause, detail, datetime.now(timezone.utc), 1)
            case _ as unreachable:
                assert_never(unreachable)
        slot.state = down
        delay_s = slot_down_delay_s(down.failed_boots)
        logger.warning(
            'Slot %s is down: %s: %s. Failed boots: %s, counted attempts: %s/%s. '
            'Next boot in %.1fs',
            slot.index,
            describe_boot_cause(cause),
            detail,
            down.failed_boots,
            counted,
            CHILD_BOOT_ATTEMPTS,
            delay_s,
        )
        self._check_first_generation()
        return delay_s

    def _log_back_in_service(self, slot: _Slot | _SharedSlot) -> None:
        match slot.state:
            case Down(cause=cause, failed_boots=failed_boots):
                logger.warning(
                    'Slot %s is back in service after %s failed boots (%s)',
                    slot.index,
                    failed_boots,
                    describe_boot_cause(cause),
                )
            case Starting() | Idle() | Busy() | Retiring() | Open() | Closing():
                pass
            case _ as unreachable:
                assert_never(unreachable)

    async def _pause(self, delay_s: float) -> None:
        """Wait ``delay_s``, or less when the pool stops or fails."""
        await asyncio.wait(
            {self._stop_requested, self._failed},
            timeout=delay_s,
            return_when=asyncio.FIRST_COMPLETED,
        )

    def _kill_spares_the_attempt(
        self,
        slot: _Slot | _SharedSlot,
        cause: BootFailureCause,
    ) -> bool:
        """Tell a SIGKILL from outside apart from a child that cannot boot.

        Such a kill says nothing about the code, so it must not stop a worker
        with healthy tasks. It still counts in two cases: at the first start,
        and when no other child is serving. Then no healthy task is at risk,
        and a worker that does no work must fail where a supervisor can see it.
        """
        match cause:
            case LostBeforeReady(
                child_exit=KilledBySignal(signal_number=signal.SIGKILL)
            ):
                first_start_done = self._first_generation.done()
                return first_start_done and self._another_child_serves(slot)
            case _:
                return False

    def _another_child_serves(self, slot: _Slot | _SharedSlot) -> bool:
        return any(
            other is not slot and isinstance(other.state, Idle | Busy | Open | Closing)
            for other in self._all_slots()
        )

    def _note_ready(self, slot: _Slot | _SharedSlot, baseline: ChildBaseline) -> None:
        if self._first_generation.done():
            return
        self._baselines[slot.index] = baseline
        self._check_first_generation()

    def _check_first_generation(self) -> None:
        """The first generation is in once every slot booted once or is
        ``Down``, and at least one slot booted."""
        if self._first_generation.done():
            return
        booted = len(self._baselines)
        down = sum(
            1
            for slot in self._all_slots()
            if slot.index not in self._baselines and isinstance(slot.state, Down)
        )
        if booted > 0 and booted + down == len(self._all_slots()):
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
                case TaskFinished():
                    slot.defects = 0
                    match self._child_after_task(child, handle, outcome, tasks_done):
                        case _ChildAfterTask.RETIRE:
                            break
                        case _ChildAfterTask.SERVE:
                            pass
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
            if self._child_cannot_serve(child):
                return None
            if self._pending:
                return self._pending.popleft()
            slot.wake.clear()
            if self._pending or self._child_cannot_serve(child):
                continue
            await slot.wake.wait()

    def _child_cannot_serve(self, child: Child) -> bool:
        """A child that the parent killed takes no task.

        The kill can land after the child's result frame arrived and before
        the slot read it. The slot then reports that result, and the child is
        still dying, not yet seen as exited.
        """
        return (
            self._stopping
            or self._failure is not None
            or child.has_exited
            or child.channel_closed
            or child.killed_by_parent
        )

    async def _run_task(
        self,
        slot: _Slot,
        child: Child,
        handle: SlotHandle,
    ) -> TaskFinished | ChildLost:
        """Run one task and finish its handle. The state stays ``Busy`` until then.

        The task is in the runner heartbeat registry from the send until the
        slot reads the answer. A closed registry means the worker failed
        closed: the child is killed, because no heartbeat would cover it.
        """
        frame = handle.frame
        slot.state = Busy(frame.task_id)
        slot.handle = handle
        child.send(frame)
        handle.bind_child(child)
        held = self._live_dispatches.hold(frame, child.pid, seats=1)
        match held:
            case Held():
                pass
            case RegistryClosed():
                child.kill_for_caller()
            case _ as unreachable:
                assert_never(unreachable)
        answer = await child.await_frame(None)
        outcome: TaskFinished | ChildLost
        match answer:
            case ResultFrame(task_id=task_id) if task_id == frame.task_id:
                outcome = TaskFinished(
                    answer.ok,
                    answer.result_json,
                    answer.failed_reason,
                    answer.rss,
                    answer.task_loop,
                )
            case (
                ResultFrame()
                | ReadyFrame()
                | BootFailedFrame()
                | SharedResultFrame()
                | DeadlineFiredFrame()
                | SecondCancelFrame()
                | RunStuckFrame()
                | LoopBlockedFrame()
            ):
                outcome = await child.discard_violating_child()
            case ChildLost():
                outcome = answer
            case WaitExpired():
                raise AssertionError('an unbounded wait cannot expire')
            case _ as unreachable:
                assert_never(unreachable)
        slot.handle = None
        match held:
            case Held(ticket=ticket):
                _ = self._live_dispatches.release(ticket)
            case RegistryClosed():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        handle.finish(outcome)
        return outcome

    def _child_after_task(
        self,
        child: Child,
        handle: SlotHandle,
        finished: TaskFinished,
        tasks_done: int,
    ) -> _ChildAfterTask:
        """Read the task loop report of a finished task, and decide whether
        the child takes another task."""
        match finished.task_loop:
            case LoopHoldsTasks() as leftover:
                # No later task may run beside these asyncio tasks. The stop
                # frame makes the child cancel them.
                logger.warning(
                    'Retiring child pid=%s: task %s left %d task(s) on its event '
                    'loop: %s',
                    child.pid,
                    handle.frame.task_id,
                    leftover.count,
                    leftover.names_text(),
                )
                return _ChildAfterTask.RETIRE
            case NoTaskLoop() | LoopHoldsNoTask():
                reason = self._recycle_reason(tasks_done, finished.rss)
                if reason is None:
                    return _ChildAfterTask.SERVE
                logger.debug('Retiring child pid=%s: %s', child.pid, reason.value)
                return _ChildAfterTask.RETIRE

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

    def _kill_at_deadline(self, slot: _Slot | _SharedSlot) -> tuple[str, ...]:
        """Kill the child of a slot that still runs a task at the drain
        deadline, or that still boots. Returns the ids of the tasks it held."""
        match slot:
            case _Slot():
                if slot.handle is not None:
                    match slot.handle.kill():
                        case KillSent():
                            return (slot.handle.frame.task_id,)
                        case NothingToKill():
                            return ()
                        case _ as unreachable:
                            assert_never(unreachable)
            case _SharedSlot():
                runs = _runs_of(slot.state)
                if slot.child is not None and runs.count() > 0:
                    task_ids = runs.task_ids()
                    slot.child.kill_for_caller()
                    return task_ids
            case _ as unreachable:
                assert_never(unreachable)
        if slot.child is not None and isinstance(slot.state, Starting | Down):
            slot.child.kill_group()
        return ()

    # ----- shared async slots -----

    def _shared_slot_at(self, index: int) -> _SharedSlot:
        slot = self._shared_slots[index - self._config.slots]
        assert slot.index == index, f'shared slot {index} is not at its place'
        return slot

    def _shared_loads(self) -> tuple[OpenChildLoad, ...]:
        """The load of each open shared child that can take a task."""
        loads: list[OpenChildLoad] = []
        for slot in self._shared_slots:
            match slot.state:
                case Open(runs=runs) if (
                    slot.child is not None and not self._child_cannot_serve(slot.child)
                ):
                    loads.append(OpenChildLoad(slot.index, runs.count(), runs.seats))
                case Starting() | Open() | Closing() | Retiring() | Down():
                    continue
                case _ as unreachable:
                    assert_never(unreachable)
        return tuple(loads)

    def _place_shared(self) -> None:
        """Send queued shared tasks to open shared children with a free seat.

        Each task goes to the open child with the fewest tasks in flight
        (``place_on_shared_child``). A closing child, a child that the parent
        killed or that exited, and every child of a stopping pool take none.
        Sends without waiting: the transport buffers the frame.
        """
        while self._shared_pending:
            match place_on_shared_child(self._shared_loads()):
                case NoSeatFree():
                    return
                case int() as index:
                    slot = self._shared_slot_at(index)
            self._send_to_seat(slot, self._shared_pending.popleft())

    def _send_to_seat(self, slot: _SharedSlot, handle: SharedHandle) -> None:
        """Send one task to the open child of ``slot``, and hold it there.

        The task is in the runner heartbeat registry from the send until the
        slot reads its end. A closed registry means the worker failed closed:
        the child is killed, because no heartbeat would cover its tasks.
        """
        child = slot.child
        assert (
            child is not None
        ), f'shared slot {slot.index} placed a task with no child'
        match slot.state:
            case Open(runs=runs):
                pass
            case Starting() | Closing() | Retiring() | Down():
                raise AssertionError(f'shared slot {slot.index} is not open')
            case _ as unreachable:
                assert_never(unreachable)
        frame = handle.frame
        child.send(frame)
        held = self._live_dispatches.hold(frame, child.pid, seats=slot.seats)
        run = _SeatRun(
            handle=handle,
            held=held,
            deadline=DeadlineNotReported(),
            sent_at=time.monotonic(),
        )
        runs.add(frame.dispatch, run)
        handle.bind(child, run)
        match held:
            case Held():
                pass
            case RegistryClosed():
                child.kill_for_caller()
            case _ as unreachable:
                assert_never(unreachable)

    def _release_held(self, held: HoldOutcome) -> None:
        match held:
            case Held(ticket=ticket):
                _ = self._live_dispatches.release(ticket)
            case RegistryClosed():
                pass
            case _ as unreachable:
                assert_never(unreachable)

    def _end_all_runs(self, runs: SeatRuns, lost: ChildLost) -> None:
        """Answer every task in flight of a lost child: with the verdict of a
        kill by the worker whose exit was seen (``_outcome_at_loss``), else
        with the loss."""
        now = time.monotonic()
        for run in runs.drain():
            self._release_held(run.held)
            run.handle.finish(_outcome_at_loss(runs.kill, run, lost, now))

    async def _run_shared_generation(self, slot: _SharedSlot, child: Child) -> None:
        """Serve shared tasks on one child until it closes and drains, until it
        is lost, or until the pool stops and its tasks end.

        Reads each frame of the child as it arrives: a result ends its task
        and frees its seat, and a deadline report marks its task. A frame that
        names no task in flight, or a frame of another kind of child, is a
        protocol violation: the child is killed, and every task of it ends
        with the loss. A lost child ends every task of it with that loss.
        """
        runs = SeatRuns(slot.seats)
        slot.state = Open(runs)
        child.on_frame = slot.wake.set
        self._place_shared()
        tasks_done = 0
        while True:
            slot.wake.clear()
            event = await child.await_frame(0)
            match event:
                case SharedResultFrame():
                    if not self._end_run(runs, event):
                        lost = await child.discard_violating_child()
                        await self._lose_shared_child(slot, child, runs, lost)
                        return
                    tasks_done += 1
                    slot.defects = 0
                    self._close_after_task(slot, runs, tasks_done, event.rss)
                    self._kill_when_only_stuck_runs_left(child, runs)
                    self._place_shared()
                case DeadlineFiredFrame():
                    if not self._note_deadline(child, runs, event):
                        lost = await child.discard_violating_child()
                        await self._lose_shared_child(slot, child, runs, lost)
                        return
                case SecondCancelFrame() | RunStuckFrame():
                    if not self._note_stop_report(slot, child, runs, event):
                        lost = await child.discard_violating_child()
                        await self._lose_shared_child(slot, child, runs, lost)
                        return
                case LoopBlockedFrame():
                    if not self._kill_for_blocked_loop(slot, child, runs, event):
                        lost = await child.discard_violating_child()
                        await self._lose_shared_child(slot, child, runs, lost)
                        return
                case ReadyFrame() | BootFailedFrame() | ResultFrame():
                    lost = await child.discard_violating_child()
                    await self._lose_shared_child(slot, child, runs, lost)
                    return
                case ChildLost():
                    await self._lose_shared_child(slot, child, runs, event)
                    return
                case WaitExpired():
                    if self._shared_child_drained(slot, runs):
                        break
                    await slot.wake.wait()
                case _ as unreachable:
                    assert_never(unreachable)
        slot.state = Retiring()
        if child.has_exited:
            await child.dispose_lost()
        else:
            await child.retire()
        slot.child = None

    def _end_run(self, runs: SeatRuns, frame: SharedResultFrame) -> bool:
        """End the task of a result frame. False: the frame names no task in
        flight, or reports a timeout of a run without a deadline, a protocol
        violation."""
        run = runs.get(frame.dispatch)
        if run is None or run.handle.frame.task_id != frame.task_id:
            return False
        outcome: SeatTaskFinished | SeatDeadlineEnded
        match frame.end:
            case SeatFinished(ok=ok, result_json=result_json, failed_reason=reason):
                outcome = SeatTaskFinished(ok, result_json, reason)
            case SeatTimedOut():
                match run.handle.frame.work:
                    case SeatTask():
                        outcome = SeatDeadlineEnded(
                            retry=retry_placement(_stop_state(run)),
                        )
                    case SeatRefusal():
                        return False
                    case _ as unreachable:
                        assert_never(unreachable)
            case _ as unreachable:
                assert_never(unreachable)
        _ = runs.remove(frame.dispatch)
        self._release_held(run.held)
        run.handle.finish(outcome)
        return True

    def _note_deadline(
        self,
        child: Child,
        runs: SeatRuns,
        frame: DeadlineFiredFrame,
    ) -> bool:
        """Mark that the child cancelled a task at its deadline. False: the
        frame names no task in flight, or a run without a deadline, a protocol
        violation."""
        run = runs.get(frame.dispatch)
        if run is None or run.handle.frame.task_id != frame.task_id:
            return False
        match run.handle.frame.work, run.deadline:
            case SeatTask(timeout_ms=timeout_ms), DeadlineNotReported():
                pass
            case ((SeatTask() | SeatRefusal()), _):
                return False
            case _ as unreachable:
                assert_never(unreachable)
        run.deadline = DeadlineReported()
        logger.info(
            'Task %s reached its timeout_ms=%s; child pid=%s cancelled it',
            frame.task_id,
            timeout_ms,
            child.pid,
        )
        return True

    def _note_stop_report(
        self,
        slot: _SharedSlot,
        child: Child,
        runs: SeatRuns,
        frame: SecondCancelFrame | RunStuckFrame,
    ) -> bool:
        """Record that a run needed its second cancel, or that it is stuck,
        and close the child: no new task goes to it. A stuck report may
        leave only stuck runs: then the child is killed. False: the frame
        names no task in flight, or comes out of order, a protocol
        violation."""
        run = runs.get(frame.dispatch)
        if run is None or run.handle.frame.task_id != frame.task_id:
            return False
        match frame, run.deadline:
            case SecondCancelFrame(), DeadlineReported():
                run.deadline = SecondCancelReported()
                self._close_for_stop(slot, runs, CloseReason.SECOND_CANCEL)
                logger.warning(
                    'Task %s did not stop within its cancel_grace_ms; child pid=%s '
                    'sent a second cancel. The child takes no new task and is '
                    'replaced when its tasks end',
                    frame.task_id,
                    child.pid,
                )
            case RunStuckFrame() as stuck, SecondCancelReported():
                run.deadline = StuckReported()
                self._close_for_stop(slot, runs, CloseReason.TASK_STUCK)
                logger.warning(
                    'Task %s did not stop after its second cancel in child pid=%s '
                    '(%s asyncio task(s): %s; %s executor item(s)); the child is '
                    'killed when every task left in it is stuck',
                    frame.task_id,
                    child.pid,
                    stuck.tasks,
                    ', '.join(stuck.names),
                    stuck.executor_items,
                )
                self._kill_when_only_stuck_runs_left(child, runs)
            case ((SecondCancelFrame() | RunStuckFrame()), _):
                return False
            case _ as unreachable:
                assert_never(unreachable)
        return True

    def _close_for_stop(
        self,
        slot: _SharedSlot,
        runs: SeatRuns,
        reason: CloseReason,
    ) -> None:
        """Close the child because of the stop of a run: its free seats
        leave the claim budget, and its runs finish in their own time."""
        match slot.state:
            case Open() | Closing():
                slot.state = Closing(reason, runs)
            case Starting() | Retiring() | Down():
                raise AssertionError(
                    f'shared slot {slot.index} read a stop report while not open'
                )
            case _ as unreachable:
                assert_never(unreachable)

    def _kill_when_only_stuck_runs_left(self, child: Child, runs: SeatRuns) -> None:
        """Kill the child once every run left in it is stuck: no run in it
        has time left then. The verdicts come when its exit is seen."""
        match runs.kill:
            case NoNamedKill() if runs.all_stuck() and not child.killed_by_parent:
                pass
            case NoNamedKill() | StuckRunsKill() | BlockedLoopKill():
                return
            case _ as unreachable:
                assert_never(unreachable)
        runs.kill = StuckRunsKill()
        logger.warning(
            'Killing shared child pid=%s: every task left in it is stuck: %s',
            child.pid,
            ', '.join(runs.task_ids()),
        )
        child.kill_for_caller()

    def _kill_for_blocked_loop(
        self,
        slot: _SharedSlot,
        child: Child,
        runs: SeatRuns,
        frame: LoopBlockedFrame,
    ) -> bool:
        """Kill a child whose loop did not turn for the block limit, at once:
        no task of it can move. The run that held the loop is named when it
        is still in flight. False: the report names a run in flight with
        another task, a protocol violation."""
        holder: LoopHeldBy | LoopHolderUnknown
        match frame.culprit:
            case BlockingRun(dispatch=dispatch, task_id=task_id):
                match runs.get(dispatch):
                    case None:
                        holder = LoopHolderUnknown()
                    case run if run.handle.frame.task_id != task_id:
                        return False
                    case _:
                        holder = LoopHeldBy(dispatch=dispatch, task_id=task_id)
            case CulpritUnknown():
                holder = LoopHolderUnknown()
            case _ as unreachable:
                assert_never(unreachable)
        match runs.kill:
            case NoNamedKill():
                runs.kill = BlockedLoopKill(holder, frame.blocked_ms, frame.where)
            case StuckRunsKill() | BlockedLoopKill():
                return True
            case _ as unreachable:
                assert_never(unreachable)
        self._close_for_stop(slot, runs, CloseReason.LOOP_BLOCKED)
        logger.critical(
            'The event loop of shared child pid=%s did not turn for %s ms (%s at '
            '%s); killing the child with its %s task(s)',
            child.pid,
            frame.blocked_ms,
            _describe_holder(holder),
            frame.where,
            runs.count(),
        )
        child.kill_for_caller()
        return True

    def _close_after_task(
        self,
        slot: _SharedSlot,
        runs: SeatRuns,
        tasks_done: int,
        rss: RssReading,
    ) -> None:
        """Close an open shared child that reached a recycle limit."""
        match self._config.shared_async:
            case SharedAsyncChildren() as shared_async:
                pass
            case NoSharedAsyncChildren():
                raise AssertionError('a shared slot exists with no shared config')
            case _ as unreachable:
                assert_never(unreachable)
        match slot.state:
            case Open():
                reason = shared_recycle_reason(shared_async, tasks_done, rss)
                if reason is not None:
                    logger.debug(
                        'Closing shared child on slot %s: %s',
                        slot.index,
                        reason.value,
                    )
                    slot.state = Closing(reason, runs)
            case Closing():
                pass
            case Starting() | Retiring() | Down():
                raise AssertionError(
                    f'shared slot {slot.index} read a result while not open'
                )
            case _ as unreachable:
                assert_never(unreachable)

    def _shared_child_drained(self, slot: _SharedSlot, runs: SeatRuns) -> bool:
        """Close the child when the pool stops or fails. True: the child is
        closed and runs no task, so it can retire."""
        if (self._stopping or self._failure is not None) and isinstance(
            slot.state,
            Open,
        ):
            slot.state = Closing(CloseReason.POOL_STOPPING, runs)
        return isinstance(slot.state, Closing) and runs.count() == 0

    async def _lose_shared_child(
        self,
        slot: _SharedSlot,
        child: Child,
        runs: SeatRuns,
        lost: ChildLost,
    ) -> None:
        """End every task of a lost shared child, then clean up the child."""
        logger.warning(
            'Lost shared child pid=%s on slot %s with %s task(s): %s',
            child.pid,
            slot.index,
            runs.count(),
            lost.describe(),
        )
        slot.state = Retiring()
        self._end_all_runs(runs, lost)
        await child.dispose_lost()
        slot.child = None

    async def stop(self, drain_timeout_s: float) -> StopReport:
        """Stop the pool: release queued tasks, drain busy ones, then kill.

        Idle children exit by stop frame. A shared child closes: it takes no
        new task, and it exits by stop frame when its tasks ended. A task still
        running at the deadline is killed with its child's session, and its
        handle gets ``ChildLost``; in a shared child, every task of the child
        does. Cancels no task early. Does not touch task rows.
        """
        report = await self._stop_slots(drain_timeout_s)
        await self._stop_preload()
        return report

    async def _stop_preload(self) -> None:
        """Stop the preload parent after the slots, so that no fork is in flight."""
        if self._preload_watch is not None:
            self._preload_watch.cancel()
            await asyncio.wait({self._preload_watch})
            self._preload_watch = None
        parent, self._preload = self._preload, None
        if parent is not None:
            await parent.stop()

    async def _stop_slots(self, drain_timeout_s: float) -> StopReport:
        self._stopping = True
        if not self._stop_requested.done():
            self._stop_requested.set_result(None)
        self._release_pending()
        for slot in self._all_slots():
            # A booting child holds no task, so it does not wait for the drain.
            # A slot that is down keeps that state while its next child boots.
            if slot.child is not None and isinstance(slot.state, Starting | Down):
                slot.child.kill_group()
        self._wake_slots()
        if not self._tasks:
            return StopReport(())
        _, running = await asyncio.wait(self._tasks, timeout=drain_timeout_s)
        killed: list[str] = []
        if running:
            for slot in self._all_slots():
                killed.extend(self._kill_at_deadline(slot))
            _, stuck = await asyncio.wait(self._tasks, timeout=CHILD_KILL_WAIT_S)
            for task in stuck:
                task.cancel()
            if stuck:
                await asyncio.wait(stuck, timeout=CHILD_KILL_WAIT_S)
            for slot in self._all_slots():
                await self._clear_slot(slot)
        for task in self._tasks:
            if task.done() and not task.cancelled() and task.exception() is not None:
                logger.error('Slot task ended with %r', task.exception())
        return StopReport(tuple(killed))
