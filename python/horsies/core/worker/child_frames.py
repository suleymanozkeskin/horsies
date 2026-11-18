"""The frames of a worker child and of the preload process, on the core frame
codec at version 2 (``horsies._core``).

The parent frames come from the core as its classes: ``core.InitFrame``,
``core.RunFrame``, ``core.SharedRunFrame`` and ``core.StopFrame``
(``core.FrameReader``, ``core.decode_parent_frame``). The init frame keeps
the database URL and the connection settings in the core; the child builds
its core handle from it (``CoreChildHandle.from_init``).

The frames that a child sends are the frozen values of this module. Child
code builds and matches them; ``encode_child_frame`` and
``encode_preload_frame`` turn each into its core class and its bytes. Every
text is cut to its bound by the core class.

This module holds no state. ``encode_child_frame`` and
``encode_preload_frame`` call the core codec, which starts nothing.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Final, Literal, assert_never

from horsies import _core as core
from horsies.core.models.boot_failure import (
    AppLoadFailed,
    ChildPoolSetupFailed,
    ReportedBootCause,
    ResourceShortage,
    TaskLoopCreateFailed,
    ThreadStartRefused,
    cut_text,
    describe_boot_cause,
)
from horsies.core.worker.run_return import (
    CoreStoppedBeforeStart,
    ResultEnvelope,
    StoppedBeforeRunning,
)
from horsies.core.worker.stored_timeout import (
    CancelGraceInvalid,
    OptionsUnreadable,
    TimeoutRefusal,
    TimeoutValueInvalid,
)

PROTOCOL_NUMBERS: Final = core.protocol_numbers()
"""The protocol version, the exit codes and the bounds of the core codec."""

MAX_LEFTOVER_TASK_NAMES: Final = PROTOCOL_NUMBERS.max_leftover_task_names
"""A report of left work names ``min(count, MAX_LEFTOVER_TASK_NAMES)``
tasks."""

MAX_BODY_BYTES: Final = PROTOCOL_NUMBERS.max_body_bytes
"""The largest body of a frame, in bytes: the largest result envelope.
PostgreSQL cannot store a field of 1 GiB or more, so a larger result could
never be saved."""

MAX_LEFTOVER_NAME_CHARS: Final = 200
"""The longest task name of a report of left work (the core cuts the
same); the child cuts it for its own log lines too."""

MAX_BLOCKED_WHERE_CHARS: Final = 500
"""The longest source line of a report of a blocked loop (the core cuts the
same); the child cuts it for its own log lines too."""

MAX_BOOT_ERROR_NAME_CHARS: Final = 200
"""The longest exception name of a boot failure frame (the core cuts the
same)."""

MAX_BOOT_DETAIL_CHARS: Final = 2000
"""The longest exception text of a boot failure frame (the core cuts the
same)."""

type ParentFrame = core.InitFrame | core.RunFrame | core.SharedRunFrame | core.StopFrame
"""A frame from the worker parent."""

type WorkFrame = core.RunFrame | core.SharedRunFrame | core.StopFrame
"""The parent frames after the init frame."""

type ThreadSeatWork = core.ThreadTaskWork | core.StoredOptionsRefused
"""The work of a run on a seat of a shared thread child."""


def stored_refusal_of(refused: core.StoredOptionsRefused) -> TimeoutRefusal:
    """The refusal of the stored options that a run frame carries. Pure."""
    match refused.kind:
        case 'OPTIONS_UNREADABLE':
            return OptionsUnreadable(reason=refused.reason)
        case 'TIMEOUT_VALUE_INVALID':
            return TimeoutValueInvalid(reason=refused.reason)
        case 'CANCEL_GRACE_INVALID':
            return CancelGraceInvalid(reason=refused.reason)
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class RssMb:
    """The child's resident set size."""

    value: float


@dataclass(frozen=True, slots=True)
class RssUnreadable:
    """The child could not read its resident set size."""


type RssReading = RssMb | RssUnreadable


@dataclass(frozen=True, slots=True)
class ReadyFrame:
    """Boot finished, the child can take a task. The core adds the protocol
    version."""

    pid: int
    rss: RssReading


@dataclass(frozen=True, slots=True)
class BootFailedFrame:
    """In place of the ready frame: the boot failed.

    The child exits after this frame. ``cause`` decides how the parent
    retries. ``error_name`` and ``detail`` are the type and the text of the
    exception, at most ``MAX_BOOT_ERROR_NAME_CHARS`` and
    ``MAX_BOOT_DETAIL_CHARS`` characters. Build it with ``boot_failed_frame``,
    which cuts them to those bounds.
    """

    cause: ReportedBootCause
    error_name: str
    detail: str

    def describe(self) -> str:
        """Subject and cause, for the log line of the child."""
        return f'{describe_boot_cause(self.cause)}: {self.error_name}: {self.detail}'


def boot_failed_frame(
    cause: ReportedBootCause,
    error_name: str,
    detail: str,
) -> BootFailedFrame:
    """Build a boot failure frame. Each text becomes valid UTF-8, cut to its
    bound. Pure."""
    return BootFailedFrame(
        cause=cause,
        error_name=cut_text(error_name, MAX_BOOT_ERROR_NAME_CHARS),
        detail=cut_text(detail, MAX_BOOT_DETAIL_CHARS),
    )


@dataclass(frozen=True, slots=True)
class NoTaskLoop:
    """After the task, the child has no task loop: its app has no
    ``async def`` task."""


@dataclass(frozen=True, slots=True)
class LoopHoldsNoTask:
    """After the task, the child's task loop holds no task that is not done."""


@dataclass(frozen=True, slots=True)
class LoopHoldsTasks:
    """At the report, ``count`` asyncio tasks that are not done are on the
    child's task loop: work that a task started and did not await.

    ``names`` holds the coroutine ``__qualname__`` of
    ``min(count, MAX_LEFTOVER_TASK_NAMES)`` of those tasks, sorted.
    """

    count: int
    names: tuple[str, ...]

    def names_text(self) -> str:
        """The names, and how many tasks they leave out, for a log line."""
        named = ', '.join(self.names)
        left_out = self.count - len(self.names)
        match left_out:
            case 0:
                return named
            case _:
                return f'{named}, and {left_out} more'


type TaskLoopReport = NoTaskLoop | LoopHoldsNoTask | LoopHoldsTasks

type NoLeftWork = NoTaskLoop | LoopHoldsNoTask
"""After the task, no asyncio task that it started is on a task loop of the
child: the child has no loop, or its loop holds no task."""

type RunEnd = ResultEnvelope | StoppedBeforeRunning | CoreStoppedBeforeStart
"""How a run ended: its result envelope, or a stop before its start. A
start that the stopped core of the child refused (``CoreStoppedBeforeStart``)
goes out as ``OWNERSHIP_UNCONFIRMED``; the child ends itself once its frame
is written."""


@dataclass(frozen=True, slots=True)
class ResultFrame:
    """An isolated task ended and left no work on a task loop."""

    task_id: uuid.UUID
    end: RunEnd
    rss: RssReading
    task_loop: NoLeftWork


@dataclass(frozen=True, slots=True)
class SeatTimedOut:
    """The deadline of the task fired. Anything the task returned is
    dropped: the outcome is the timeout."""


type SeatEnd = RunEnd | SeatTimedOut
"""How a run of a shared child ended."""


@dataclass(frozen=True, slots=True)
class SharedResultFrame:
    """The task of run ``dispatch`` has ended, and its seat is free. ``rss``
    is the RSS of the child after it."""

    dispatch: int
    task_id: uuid.UUID
    end: SeatEnd
    rss: RssReading


@dataclass(frozen=True, slots=True)
class DeadlineFiredFrame:
    """The deadline of run ``dispatch`` fired. A shared async child cancelled
    its task; a shared thread child reports it, since a thread cannot be
    cancelled."""

    dispatch: int
    task_id: uuid.UUID


@dataclass(frozen=True, slots=True)
class SecondCancelFrame:
    """The cancel grace of run ``dispatch`` ran out while it was alive: the
    child cancels its task and its marked work again."""

    dispatch: int
    task_id: uuid.UUID


@dataclass(frozen=True, slots=True)
class RunStuckFrame:
    """Run ``dispatch`` did not stop after its second cancel and its cancel
    rounds: ``tasks`` asyncio tasks and ``executor_items`` executor items of
    the run are alive; ``names`` holds ``min(tasks,
    MAX_LEFTOVER_TASK_NAMES)`` task names."""

    dispatch: int
    task_id: uuid.UUID
    tasks: int
    executor_items: int
    names: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class LeftoverCancelFrame:
    """The body of run ``dispatch`` returned before its deadline, and its
    marked work was alive one cancel grace later: the child cancels that
    work. ``returned`` is what the body returned, the end of the run."""

    dispatch: int
    task_id: uuid.UUID
    returned: ResultEnvelope


@dataclass(frozen=True, slots=True)
class LeftoverStuckFrame:
    """The work that run ``dispatch`` left after its return did not stop
    after its cancel rounds. ``returned`` is the end of the run."""

    dispatch: int
    task_id: uuid.UUID
    tasks: int
    executor_items: int
    names: tuple[str, ...]
    returned: ResultEnvelope


@dataclass(frozen=True, slots=True)
class BodyReturnedFrame:
    """The body of an isolated task returned, and work it started is still on
    the task loop. ``returned`` is the end of the run; the child reports the
    end of that work next."""

    task_id: uuid.UUID
    returned: ResultEnvelope
    left: LoopHoldsTasks


class LeftWorkEnd(Enum):
    """How the work that an isolated task left on the task loop ended."""

    IN_GRACE = 'IN_GRACE'
    """It ended on its own within the grace after the return."""

    CANCELLED = 'CANCELLED'
    """It ended after the child cancelled it."""


@dataclass(frozen=True, slots=True)
class LeftWorkEndedFrame:
    """The work that an isolated task left has ended."""

    task_id: uuid.UUID
    end: LeftWorkEnd
    rss: RssReading


@dataclass(frozen=True, slots=True)
class LeftWorkStuckFrame:
    """The work that an isolated task left did not stop after the cancel
    rounds; the parent kills the child."""

    task_id: uuid.UUID
    still: LoopHoldsTasks


@dataclass(frozen=True, slots=True)
class BlockingRun:
    """The run whose task held the blocked loop."""

    dispatch: int
    task_id: uuid.UUID


@dataclass(frozen=True, slots=True)
class CulpritUnknown:
    """No single run holds the blocked loop: none, or more than one, ran at
    the last turn."""


type LoopCulprit = BlockingRun | CulpritUnknown


@dataclass(frozen=True, slots=True)
class LoopBlockedFrame:
    """The event loop of a shared async child has not turned for
    ``blocked_ms``. ``where`` is the source line of the blocking frame; the
    core cuts it to its bound. The parent kills the child at once: no task
    of a blocked loop can move."""

    culprit: LoopCulprit
    blocked_ms: int
    where: str


type ChildFrame = (
    ReadyFrame
    | BootFailedFrame
    | ResultFrame
    | BodyReturnedFrame
    | LeftWorkEndedFrame
    | LeftWorkStuckFrame
    | SharedResultFrame
    | DeadlineFiredFrame
    | SecondCancelFrame
    | RunStuckFrame
    | LeftoverCancelFrame
    | LeftoverStuckFrame
    | LoopBlockedFrame
)
"""A frame from a child to the worker parent."""


@dataclass(frozen=True, slots=True)
class PreloadReadyFrame:
    """Imports are done, so forks can start."""

    pid: int


@dataclass(frozen=True, slots=True)
class ChildForkedFrame:
    """The child exists, and it has left the preload process."""

    pid: int


@dataclass(frozen=True, slots=True)
class ForkFailedFrame:
    """This fork failed. The preload process stays up."""

    reason: str


@dataclass(frozen=True, slots=True)
class PreloadRefusedFrame:
    """The preload process cannot fork safely, and it exits."""

    reason: str


type PreloadFrame = PreloadReadyFrame | ChildForkedFrame | ForkFailedFrame | PreloadRefusedFrame
"""A frame from the preload process to the worker parent."""


def _rss(rss: RssReading) -> float | Literal['UNREADABLE']:
    match rss:
        case RssMb(value=value):
            return value
        case RssUnreadable():
            return 'UNREADABLE'
        case _ as unreachable:
            assert_never(unreachable)


def _cause(
    cause: ReportedBootCause,
) -> (
    core.ResourceShortage
    | Literal[
        'APP_LOAD_FAILED',
        'CHILD_POOL_SETUP_FAILED',
        'TASK_LOOP_CREATE_FAILED',
        'THREAD_START_REFUSED',
    ]
):
    match cause:
        case AppLoadFailed():
            return 'APP_LOAD_FAILED'
        case ChildPoolSetupFailed():
            return 'CHILD_POOL_SETUP_FAILED'
        case TaskLoopCreateFailed():
            return 'TASK_LOOP_CREATE_FAILED'
        case ThreadStartRefused():
            return 'THREAD_START_REFUSED'
        case ResourceShortage(errno_name=errno_name):
            return core.ResourceShortage(errno=errno_name.value)
        case _ as unreachable:
            assert_never(unreachable)


def _left(held: LoopHoldsTasks) -> core.LoopHoldsTasks:
    return core.LoopHoldsTasks(count=held.count, names=list(held.names))


def _task_loop(task_loop: NoLeftWork) -> Literal['NO_TASK_LOOP', 'LOOP_HOLDS_NO_TASK']:
    match task_loop:
        case NoTaskLoop():
            return 'NO_TASK_LOOP'
        case LoopHoldsNoTask():
            return 'LOOP_HOLDS_NO_TASK'
        case _ as unreachable:
            assert_never(unreachable)


def _returned(returned: ResultEnvelope) -> core.TaskResult:
    return core.TaskResult(envelope=returned.text)


def _run_end(end: RunEnd) -> core.TaskResult | core.StoppedBeforeRunning:
    match end:
        case ResultEnvelope():
            return _returned(end)
        case StoppedBeforeRunning(reason=reason):
            return core.StoppedBeforeRunning(reason=reason)
        case CoreStoppedBeforeStart():
            return core.StoppedBeforeRunning(reason='OWNERSHIP_UNCONFIRMED')
        case _ as unreachable:
            assert_never(unreachable)


def _seat_end(
    end: SeatEnd,
) -> core.TaskResult | core.StoppedBeforeRunning | Literal['TIMED_OUT']:
    match end:
        case ResultEnvelope() | StoppedBeforeRunning() | CoreStoppedBeforeStart():
            return _run_end(end)
        case SeatTimedOut():
            return 'TIMED_OUT'
        case _ as unreachable:
            assert_never(unreachable)


def _stuck(tasks: int, executor_items: int, names: tuple[str, ...]) -> core.StuckWork:
    return core.StuckWork(tasks=tasks, executor_items=executor_items, names=list(names))


def _culprit(culprit: LoopCulprit) -> core.BlockingRun | Literal['UNKNOWN']:
    match culprit:
        case BlockingRun(dispatch=dispatch, task_id=task_id):
            return core.BlockingRun(dispatch=dispatch, task_id=task_id)
        case CulpritUnknown():
            return 'UNKNOWN'
        case _ as unreachable:
            assert_never(unreachable)


type CoreChildFrame = (
    core.ReadyFrame
    | core.BootFailedFrame
    | core.ResultFrame
    | core.BodyReturnedFrame
    | core.LeftWorkEndedFrame
    | core.LeftWorkStuckFrame
    | core.SharedResultFrame
    | core.DeadlineFiredFrame
    | core.SecondCancelFrame
    | core.RunStuckFrame
    | core.LeftoverCancelFrame
    | core.LeftoverStuckFrame
    | core.LoopBlockedFrame
)


def core_child_frame(frame: ChildFrame) -> CoreChildFrame:
    """The core class of a child frame. A value outside its bound (a pid of
    0, a report whose names do not match its count) is a defect of the child:
    the core class raises ``ValueError``."""
    match frame:
        case ReadyFrame(pid=pid, rss=rss):
            return core.ReadyFrame(pid=pid, rss_mb=_rss(rss))
        case BootFailedFrame(cause=cause, error_name=error_name, detail=detail):
            return core.BootFailedFrame(
                cause=_cause(cause), error_name=error_name, detail=detail,
            )
        case ResultFrame(task_id=task_id, end=end, rss=rss, task_loop=task_loop):
            return core.ResultFrame(
                task_id=task_id,
                end=_run_end(end),
                rss_mb=_rss(rss),
                task_loop=_task_loop(task_loop),
            )
        case BodyReturnedFrame(task_id=task_id, returned=returned, left=left):
            return core.BodyReturnedFrame(
                task_id=task_id, returned=_returned(returned), left=_left(left),
            )
        case LeftWorkEndedFrame(task_id=task_id, end=end, rss=rss):
            return core.LeftWorkEndedFrame(task_id=task_id, end=end.value, rss_mb=_rss(rss))
        case LeftWorkStuckFrame(task_id=task_id, still=still):
            return core.LeftWorkStuckFrame(task_id=task_id, still=_left(still))
        case SharedResultFrame(dispatch=dispatch, task_id=task_id, end=end, rss=rss):
            return core.SharedResultFrame(
                dispatch=dispatch, task_id=task_id, end=_seat_end(end), rss_mb=_rss(rss),
            )
        case DeadlineFiredFrame(dispatch=dispatch, task_id=task_id):
            return core.DeadlineFiredFrame(dispatch=dispatch, task_id=task_id)
        case SecondCancelFrame(dispatch=dispatch, task_id=task_id):
            return core.SecondCancelFrame(dispatch=dispatch, task_id=task_id)
        case RunStuckFrame(
            dispatch=dispatch,
            task_id=task_id,
            tasks=tasks,
            executor_items=executor_items,
            names=names,
        ):
            return core.RunStuckFrame(
                dispatch=dispatch,
                task_id=task_id,
                work=_stuck(tasks, executor_items, names),
            )
        case LeftoverCancelFrame(dispatch=dispatch, task_id=task_id, returned=returned):
            return core.LeftoverCancelFrame(
                dispatch=dispatch, task_id=task_id, returned=_returned(returned),
            )
        case LeftoverStuckFrame(
            dispatch=dispatch,
            task_id=task_id,
            tasks=tasks,
            executor_items=executor_items,
            names=names,
            returned=returned,
        ):
            return core.LeftoverStuckFrame(
                dispatch=dispatch,
                task_id=task_id,
                work=_stuck(tasks, executor_items, names),
                returned=_returned(returned),
            )
        case LoopBlockedFrame(culprit=culprit, blocked_ms=blocked_ms, where=where):
            return core.LoopBlockedFrame(
                culprit=_culprit(culprit), blocked_ms=blocked_ms, location=where,
            )
        case _ as unreachable:
            assert_never(unreachable)


def encode_child_frame(frame: ChildFrame) -> bytes | core.FrameTooLarge:
    """The bytes of a child frame, or ``FrameTooLarge`` when its metadata or
    its body is over a bound of the header (nothing is encoded then). A
    value outside its bound raises ``ValueError`` (``core_child_frame``)."""
    return core.encode_child_frame(core_child_frame(frame))


def encode_preload_frame(frame: PreloadFrame) -> bytes:
    """The bytes of a preload frame. A pid of 0 raises ``ValueError``."""
    match frame:
        case PreloadReadyFrame(pid=pid):
            return core.encode_preload_frame(core.PreloadReadyFrame(pid=pid))
        case ChildForkedFrame(pid=pid):
            return core.encode_preload_frame(core.ChildForkedFrame(pid=pid))
        case ForkFailedFrame(reason=reason):
            return core.encode_preload_frame(core.ForkFailedFrame(reason=reason))
        case PreloadRefusedFrame(reason=reason):
            return core.encode_preload_frame(core.PreloadRefusedFrame(reason=reason))
        case _ as unreachable:
            assert_never(unreachable)
