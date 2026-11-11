"""Why a worker child failed to boot, as closed cause types.

A child reports the causes that it can see in its boot failure frame. The
worker parent sees a spawn failure itself. Each cause here is either one that
can clear by itself, or one that is the same on every boot of the same code
and settings. ``ClearableBootCause`` names the first group. The worker parent
adds the causes that it alone sees (``horsies.core.worker.slots``).

This module is pure. It does no I/O and holds no state.
"""

from __future__ import annotations

import errno
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Final, TypeAlias, assert_never, cast

from horsies.core.types.result import Err, Ok, Result


class ShortageErrno(Enum):
    """An errno that names an OS resource limit.

    A boot that fails with one can succeed later, when the resource is free
    again. ``CLEARABLE_ERRNOS`` maps the errno numbers to these names.
    """

    EMFILE = 'EMFILE'
    ENFILE = 'ENFILE'
    ENOMEM = 'ENOMEM'
    EAGAIN = 'EAGAIN'


# The only errno values that make a boot failure one that can clear:
# EMFILE: this process holds its limit of open file descriptors.
# ENFILE: the system holds its limit of open files.
# ENOMEM: the kernel could not allocate memory.
# EAGAIN: a process or thread limit, or a short kernel shortage. EWOULDBLOCK
# has the same number on Linux and macOS.
# Any other errno is a cause that repeats.
CLEARABLE_ERRNOS: Final[Mapping[int, ShortageErrno]] = MappingProxyType(
    {
        errno.EMFILE: ShortageErrno.EMFILE,
        errno.ENFILE: ShortageErrno.ENFILE,
        errno.ENOMEM: ShortageErrno.ENOMEM,
        errno.EAGAIN: ShortageErrno.EAGAIN,
    },
)
# The text that goes with a boot failure cause, in the worker state and in its
# health report, has at most this many characters.
MAX_BOOT_FAILURE_DETAIL_CHARS: Final = 2400


def cut_text(text: str, limit: int) -> str:
    """Return ``text`` as valid UTF-8, cut to at most ``limit`` characters.

    Text from an exception can hold a lone surrogate, which JSON text cannot
    encode. Pure.
    """
    return text.encode('utf-8', 'backslashreplace').decode('utf-8')[:limit]


@dataclass(frozen=True, slots=True)
class AppLoadFailed:
    """Locating or importing the app or its task modules raised.

    The same code raises again, so this cause repeats.
    """


@dataclass(frozen=True, slots=True)
class ChildPoolSetupFailed:
    """The child could not set up its database connection pool, for example
    because the URL does not parse.

    The same settings fail again, so this cause repeats.
    """


@dataclass(frozen=True, slots=True)
class ResourceShortage:
    """A boot step failed with an errno in ``CLEARABLE_ERRNOS``.

    This cause can clear.
    """

    errno_name: ShortageErrno


@dataclass(frozen=True, slots=True)
class TaskLoopCreateFailed:
    """The app has an ``async def`` task, and the child could not create the
    event loop for it.

    This cause can clear.
    """


@dataclass(frozen=True, slots=True)
class ThreadStartRefused:
    """The OS refused the thread that reads frames from the worker.

    This cause can clear.
    """


@dataclass(frozen=True, slots=True)
class SpawnFailed:
    """The OS refused to start the child process, or a forked child did not
    reach the worker in time. The worker parent sees this cause, not the child.

    This cause can clear.
    """


def shortage_or[StepCause](
    exc: BaseException,
    step_cause: StepCause,
) -> ResourceShortage | StepCause:
    """Name the cause of an exception that a boot step raised.

    An ``OSError`` whose errno is in ``CLEARABLE_ERRNOS`` is a
    ``ResourceShortage``, which can clear. Any other exception is
    ``step_cause``, the cause of that step. Only the exception itself is
    read, not its ``__cause__``. Pure.
    """
    match exc:
        case OSError(errno=int() as number) if number in CLEARABLE_ERRNOS:
            return ResourceShortage(CLEARABLE_ERRNOS[number])
        case _:
            return step_cause


ReportedBootCause: TypeAlias = (
    AppLoadFailed
    | ChildPoolSetupFailed
    | ResourceShortage
    | TaskLoopCreateFailed
    | ThreadStartRefused
)
ClearableBootCause: TypeAlias = (
    ResourceShortage | TaskLoopCreateFailed | ThreadStartRefused | SpawnFailed
)
ModelBootCause: TypeAlias = ReportedBootCause | SpawnFailed


@dataclass(frozen=True, slots=True)
class BootCauseUnreadable:
    """A stored or received cause is not one of the known shapes."""

    reason: str


def describe_boot_cause(cause: ModelBootCause) -> str:
    """Return the cause as short text for logs and error messages."""
    match cause:
        case AppLoadFailed():
            return 'the app or a task module could not be imported'
        case ChildPoolSetupFailed():
            return 'the child could not set up its database connection pool'
        case ResourceShortage(errno_name=errno_name):
            return f'the OS ran short of a resource ({errno_name.value})'
        case TaskLoopCreateFailed():
            return 'the child could not create the event loop for async tasks'
        case ThreadStartRefused():
            return 'the OS refused the control thread of the child'
        case SpawnFailed():
            return 'the OS refused to start the child process'
        case _ as unreachable:
            assert_never(unreachable)


def boot_cause_json(cause: ModelBootCause) -> dict[str, str]:
    """Encode a cause as a JSON object, for a frame or a stored row."""
    match cause:
        case AppLoadFailed():
            return {'kind': 'app_load_failed'}
        case ChildPoolSetupFailed():
            return {'kind': 'child_pool_setup_failed'}
        case ResourceShortage(errno_name=errno_name):
            return {'kind': 'resource_shortage', 'errno': errno_name.value}
        case TaskLoopCreateFailed():
            return {'kind': 'task_loop_create_failed'}
        case ThreadStartRefused():
            return {'kind': 'thread_start_refused'}
        case SpawnFailed():
            return {'kind': 'spawn_failed'}
        case _ as unreachable:
            assert_never(unreachable)


def parse_boot_cause(value: object) -> Result[ModelBootCause, BootCauseUnreadable]:
    """Build a cause from the JSON object that ``boot_cause_json`` made.

    ``Err(BootCauseUnreadable)``: the value is not an object, or its ``kind``
    or ``errno`` is not a known name. Reads only.
    """
    if not isinstance(value, dict):
        return Err(BootCauseUnreadable('the cause is not a JSON object'))
    fields = cast('Mapping[str, Any]', value)
    match fields.get('kind'):
        case 'app_load_failed':
            return Ok(AppLoadFailed())
        case 'child_pool_setup_failed':
            return Ok(ChildPoolSetupFailed())
        case 'resource_shortage':
            return _parse_shortage(fields.get('errno'))
        case 'task_loop_create_failed':
            return Ok(TaskLoopCreateFailed())
        case 'thread_start_refused':
            return Ok(ThreadStartRefused())
        case 'spawn_failed':
            return Ok(SpawnFailed())
        case other:
            return Err(BootCauseUnreadable(f'unknown cause kind {other!r}'))


def _parse_shortage(value: object) -> Result[ModelBootCause, BootCauseUnreadable]:
    known = {member.value: member for member in ShortageErrno}
    match value:
        case str() if value in known:
            return Ok(ResourceShortage(known[value]))
        case _:
            return Err(BootCauseUnreadable(f'unknown errno name {value!r}'))
