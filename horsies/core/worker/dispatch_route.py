"""Which kind of child runs a claimed row.

The claim plan of the worker claimed the row into the budget of a class
(``ClaimPlan.route``). A row of the ``SHARED_ASYNC`` budget runs on a seat
of a shared async child; a row of the ``ISOLATED`` budget runs in an isolated
child. The budget decides, so dispatch and claim agree on every row.

A seat needs the timeout and the cancel grace of the row, which enqueue
stored in its ``task_options``. A row of the ``SHARED_ASYNC`` budget whose
stored options give no usable timeout or grace runs in an isolated child
instead, where a refused stored timeout fails the task as for any row. Pure.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias, assert_never

from horsies.core.codec.json_io import loads_json
from horsies.core.models.tasks import (
    MAX_CANCEL_GRACE_MS,
    MIN_CANCEL_GRACE_MS,
    Execution,
)
from horsies.core.types.result import Err, Ok
from horsies.core.worker.claim_plan import ClaimPlan, NotServed, ServedBy
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


def _stored_cancel_grace_ms(task_options: object) -> int | None:
    """The ``cancel_grace_ms`` of stored task options, when it is an int in
    its bounds; None otherwise."""
    match task_options:
        case str() as text:
            pass
        case _:
            return None
    match loads_json(text):
        case Ok(ok_value=dict() as options):
            grace = options.get('cancel_grace_ms')
        case Ok() | Err():
            return None
    match grace:
        case bool():
            return None
        case int() if MIN_CANCEL_GRACE_MS <= grace <= MAX_CANCEL_GRACE_MS:
            return grace
        case _:
            return None


def _shared_or_isolated(task_options: object, timeout: StoredTimeout) -> DispatchRoute:
    """A seat when the row stores a usable timeout and grace; else isolated."""
    match timeout:
        case TaskTimeout(ms=timeout_ms):
            pass
        case NoTimeout() | OptionsUnreadable() | TimeoutValueInvalid():
            return RunIsolated()
        case _ as unreachable:
            assert_never(unreachable)
    match _stored_cancel_grace_ms(task_options):
        case int() as grace_ms:
            return RunShared(timeout_ms=timeout_ms, cancel_grace_ms=grace_ms)
        case None:
            return RunIsolated()


def dispatch_route(
    execution: Execution,
    plan: ClaimPlan,
    task_options: object,
    timeout: StoredTimeout,
) -> DispatchRoute:
    """The kind of child that runs a claimed row of class ``execution``.

    ``task_options`` is the stored column as the claim returns it (text, or
    None), and ``timeout`` is ``read_stored_timeout`` of it. The claim reads
    only classes that ``plan`` serves, and this worker starts no
    ``SHARED_THREAD`` children, so another budget is a defect:
    ``AssertionError``.
    """
    match plan.route(execution):
        case ServedBy(budget=budget):
            pass
        case NotServed():
            raise AssertionError(
                f'a {execution.value} row was claimed by a worker that does '
                'not serve that class',
            )
        case _ as unreachable:
            assert_never(unreachable)
    match budget:
        case Execution.SHARED_ASYNC:
            return _shared_or_isolated(task_options, timeout)
        case Execution.ISOLATED:
            return RunIsolated()
        case Execution.SHARED_THREAD:
            raise AssertionError(
                f'a {execution.value} row was claimed into a SHARED_THREAD '
                'budget, and this worker starts no SHARED_THREAD children',
            )
        case _ as unreachable:
            assert_never(unreachable)
