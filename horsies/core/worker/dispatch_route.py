"""Which kind of child runs a claimed row, and with which stored timeout.

The claim plan of the worker claimed the row into the budget of a class
(``ClaimPlan.route``). A row of the ``SHARED_ASYNC`` budget runs on a seat
of a shared async child; a row of the ``SHARED_THREAD`` budget runs on a
seat of a shared thread child; a row of the ``ISOLATED`` budget runs in an
isolated child. The budget decides, so dispatch and claim agree on every row.

A ``SHARED_ASYNC`` row needs the timeout and the cancel grace that enqueue
stored in its ``task_options``, and a ``SHARED_THREAD`` row needs the
timeout. A row whose stored options give no usable timeout or grace is
refused on every worker: the child that the budget names sets the row
RUNNING and fails the task with ``WORKER_SERIALIZATION_ERROR`` before it
resolves the task, as for an isolated row with a refused stored timeout. It
never runs, and it never returns to the queue. Pure.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias, assert_never

from horsies.core.codec.json_io import loads_json
from horsies.core.codec.task_options import shared_async_grace_error
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Err, Ok
from horsies.core.worker.child_protocol import (
    SeatRefusal,
    SeatTask,
    SeatWork,
    ThreadSeatTask,
    ThreadSeatWork,
)
from horsies.core.worker.claim_plan import ClaimPlan, NotServed, ServedBy
from horsies.core.worker.stored_timeout import (
    CancelGraceInvalid,
    NoTimeout,
    OptionsUnreadable,
    StoredTimeout,
    TaskTimeout,
    TimeoutValueInvalid,
)


@dataclass(frozen=True, slots=True)
class RunIsolated:
    """An isolated child runs the row, one task in its process. ``timeout`` is
    the stored timeout that the child applies; a refusal fails the task."""

    timeout: StoredTimeout


@dataclass(frozen=True, slots=True)
class RunShared:
    """A shared async child takes the row on a seat: ``work`` is the task
    with its deadline, or the refusal that fails it."""

    work: SeatWork


@dataclass(frozen=True, slots=True)
class RunSharedThread:
    """A shared thread child takes the row on a seat: ``work`` is the task
    with its deadline, or the refusal that fails it."""

    work: ThreadSeatWork


DispatchRoute: TypeAlias = RunIsolated | RunShared | RunSharedThread


def _stored_cancel_grace_ms(
    task_options: object,
) -> int | OptionsUnreadable | CancelGraceInvalid:
    """The ``cancel_grace_ms`` of stored task options, checked against the
    enqueue rule of a ``SHARED_ASYNC`` row (``shared_async_grace_error``)."""
    match task_options:
        case None | '':
            return CancelGraceInvalid(
                reason='stored cancel_grace_ms is absent for execution SHARED_ASYNC',
            )
        case str() as text:
            pass
        case _:
            return OptionsUnreadable(
                reason=(
                    'stored task_options are a '
                    f'{type(task_options).__name__}, not text'
                ),
            )
    match loads_json(text):
        case Ok(ok_value=dict() as options):
            raw = options.get('cancel_grace_ms')
        case Ok(ok_value=other):
            return OptionsUnreadable(
                reason=(
                    'stored task_options are a JSON '
                    f'{type(other).__name__}, not an object'
                ),
            )
        case Err(err_value=error):
            return OptionsUnreadable(
                reason=f'stored task_options do not parse: {error}',
            )
    match shared_async_grace_error(raw):
        case None:
            assert isinstance(raw, int), 'the grace rule passes only an int'
            return raw
        case cause:
            return CancelGraceInvalid(reason=f'stored {cause}')


def _shared_async_work(task_options: object, timeout: StoredTimeout) -> SeatWork:
    """The work of a ``SHARED_ASYNC`` row: the task with its stored timeout
    and grace, or the refusal of the first one that the worker cannot use.

    ``task_options`` is the stored column as the claim returns it, and
    ``timeout`` is ``read_stored_timeout`` of it. A row with no stored
    timeout is refused with ``TimeoutValueInvalid``: the class needs one.
    """
    match timeout:
        case TaskTimeout(ms=timeout_ms):
            pass
        case NoTimeout():
            return SeatRefusal(
                refusal=TimeoutValueInvalid(
                    reason='stored timeout_ms is absent for execution SHARED_ASYNC',
                ),
            )
        case OptionsUnreadable() | TimeoutValueInvalid() | CancelGraceInvalid():
            return SeatRefusal(refusal=timeout)
        case _ as unreachable:
            assert_never(unreachable)
    match _stored_cancel_grace_ms(task_options):
        case int() as grace_ms:
            return SeatTask(timeout_ms=timeout_ms, cancel_grace_ms=grace_ms)
        case OptionsUnreadable() | CancelGraceInvalid() as refusal:
            return SeatRefusal(refusal=refusal)
        case _ as unreachable:
            assert_never(unreachable)


def _shared_thread_work(timeout: StoredTimeout) -> ThreadSeatWork:
    """The work of a ``SHARED_THREAD`` row: the task with its stored
    timeout, or the refusal of a timeout that the worker cannot use.

    ``timeout`` is ``read_stored_timeout`` of the stored column. A row with
    no stored timeout is refused with ``TimeoutValueInvalid``: the class
    needs one.
    """
    match timeout:
        case TaskTimeout(ms=timeout_ms):
            return ThreadSeatTask(timeout_ms=timeout_ms)
        case NoTimeout():
            return SeatRefusal(
                refusal=TimeoutValueInvalid(
                    reason='stored timeout_ms is absent for execution SHARED_THREAD',
                ),
            )
        case OptionsUnreadable() | TimeoutValueInvalid() | CancelGraceInvalid():
            return SeatRefusal(refusal=timeout)
        case _ as unreachable:
            assert_never(unreachable)


def _isolated_timeout(
    execution: Execution,
    task_options: object,
    timeout: StoredTimeout,
) -> StoredTimeout:
    """The stored timeout that an isolated child applies to a row of class
    ``execution``. A shared row gets the refusal of its seat work, so its
    outcome does not depend on the kind of child that takes it."""
    match execution:
        case Execution.SHARED_ASYNC:
            match _shared_async_work(task_options, timeout):
                case SeatTask(timeout_ms=timeout_ms):
                    return TaskTimeout(ms=timeout_ms)
                case SeatRefusal(refusal=refusal):
                    return refusal
                case _ as unreachable:
                    assert_never(unreachable)
        case Execution.SHARED_THREAD:
            match _shared_thread_work(timeout):
                case ThreadSeatTask(timeout_ms=timeout_ms):
                    return TaskTimeout(ms=timeout_ms)
                case SeatRefusal(refusal=refusal):
                    return refusal
                case _ as unreachable:
                    assert_never(unreachable)
        case Execution.ISOLATED:
            return timeout
        case _ as unreachable:
            assert_never(unreachable)


def dispatch_route(
    execution: Execution,
    plan: ClaimPlan,
    task_options: object,
    timeout: StoredTimeout,
) -> DispatchRoute:
    """The kind of child that runs a claimed row of class ``execution``.

    ``task_options`` is the stored column as the claim returns it (text, or
    None), and ``timeout`` is ``read_stored_timeout`` of it. The claim reads
    only classes that ``plan`` serves, so a class that it does not serve is
    a defect: ``AssertionError``.
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
            return RunShared(work=_shared_async_work(task_options, timeout))
        case Execution.ISOLATED:
            return RunIsolated(
                timeout=_isolated_timeout(execution, task_options, timeout),
            )
        case Execution.SHARED_THREAD:
            return RunSharedThread(work=_shared_thread_work(timeout))
        case _ as unreachable:
            assert_never(unreachable)
