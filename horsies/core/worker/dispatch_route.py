"""Which kind of child runs a claimed row.

A row runs on a shared async child when the worker has shared async children
and the row is a ``SHARED_ASYNC`` task with a usable timeout and cancel grace.
Every other row runs in an isolated child. A worker may run a task more
isolated than its row says, never less: a worker with no shared async
children runs ``SHARED_ASYNC`` rows in isolated children.

The claim returns the stored ``task_options`` of each row, and this module
reads the class and the grace from them, as enqueue stored them. Pure.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias, assert_never

from horsies.core.codec.json_io import loads_json
from horsies.core.codec.task_options import stored_execution
from horsies.core.models.tasks import Execution
from horsies.core.models.worker_profile import (
    NoSharedAsyncChildren,
    SharedAsyncChildren,
)
from horsies.core.types.result import Err, Ok
from horsies.core.worker.stored_timeout import (
    NoTimeout,
    OptionsUnreadable,
    StoredTimeout,
    TaskTimeout,
    TimeoutValueInvalid,
)


@dataclass(frozen=True, slots=True)
class RunIsolated:
    """An isolated child runs the row, one task in its process."""


@dataclass(frozen=True, slots=True)
class RunShared:
    """A shared async child runs the row, on a seat. ``timeout_ms`` and
    ``cancel_grace_ms`` are the stored values, within their bounds."""

    timeout_ms: int
    cancel_grace_ms: int


DispatchRoute: TypeAlias = RunIsolated | RunShared


def _stored_cancel_grace_ms(task_options: str) -> int | None:
    """The cancel grace of stored ``SHARED_ASYNC`` options, or None when the
    options do not store a valid ``SHARED_ASYNC`` class with its grace."""
    match loads_json(task_options):
        case Ok(ok_value=dict() as options):
            pass
        case Ok() | Err():
            return None
    match stored_execution(options):
        case Ok(ok_value=execution):
            pass
        case Err():
            return None
    match execution:
        case Execution.SHARED_ASYNC:
            grace = options.get('cancel_grace_ms')
            return grace if isinstance(grace, int) else None
        case Execution.ISOLATED | Execution.SHARED_THREAD:
            return None
        case _ as unreachable:
            assert_never(unreachable)


def dispatch_route(
    task_options: object,
    timeout: StoredTimeout,
    shared_async: SharedAsyncChildren | NoSharedAsyncChildren,
) -> DispatchRoute:
    """The kind of child that runs a claimed row.

    ``task_options`` is the stored column as the claim returns it (text, or
    None), and ``timeout`` is ``read_stored_timeout`` of it. ``RunShared``
    needs all of: shared async children on this worker, the stored class
    ``SHARED_ASYNC``, a ``TaskTimeout``, and a stored grace that
    ``stored_execution`` accepts. Any other row is ``RunIsolated``; a stored
    timeout that the worker refuses then fails the task in the isolated
    child, as for any row.
    """
    match shared_async:
        case SharedAsyncChildren():
            pass
        case NoSharedAsyncChildren():
            return RunIsolated()
        case _ as unreachable:
            assert_never(unreachable)
    match timeout:
        case TaskTimeout(ms=timeout_ms):
            pass
        case NoTimeout() | OptionsUnreadable() | TimeoutValueInvalid():
            return RunIsolated()
        case _ as unreachable:
            assert_never(unreachable)
    match task_options:
        case str() as text:
            grace_ms = _stored_cancel_grace_ms(text)
        case _:
            return RunIsolated()
    match grace_ms:
        case int():
            return RunShared(timeout_ms=timeout_ms, cancel_grace_ms=grace_ms)
        case None:
            return RunIsolated()
