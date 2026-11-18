"""The task timeout that a claimed row stores, read once by the worker parent.

The parent reads the stored `task_options` text into `StoredTimeout` when it
claims the row, and the run frame carries that value to the child. A refusal
is not dropped: the child sets the row RUNNING and returns
`WORKER_SERIALIZATION_ERROR` before it resolves the task, so the normal
persist path fails the task. The text of each refusal holds no stored value,
because an int with more than 4300 digits cannot be converted to a string.

The claim reads the row with `read_claimed_options`: a stored retry policy
that does not validate (`stored_retry_policy`, the rule of enqueue) is
refused with `OptionsUnreadable` before the timeout is read, so the task fails
before it runs and no retry decision falls back to defaults.

A `SHARED_ASYNC` row also needs a stored `cancel_grace_ms`, and a stored
timeout. `dispatch_route` reads those, and refuses a row without them with
`TimeoutValueInvalid` or `CancelGraceInvalid`; `read_stored_timeout` never
returns `CancelGraceInvalid`.
"""

from __future__ import annotations

from dataclasses import dataclass

from horsies.core.codec.json_io import loads_json
from horsies.core.codec.task_options import InvalidRetryPolicy, NoRetryPolicy
from horsies.core.models.tasks import (
    MAX_TASK_TIMEOUT_MS,
    MIN_TASK_TIMEOUT_MS,
    RetryPolicy,
)
from horsies.core.types.result import Err, Ok
from horsies.core.utils.retry import OptionsNotAnObject, read_stored_retry_policy


@dataclass(frozen=True, slots=True)
class NoTimeout:
    """The row sets no timeout: no options, no `timeout_ms` key, or `null`."""


@dataclass(frozen=True, slots=True)
class TaskTimeout:
    """The row's timeout, from `MIN_TASK_TIMEOUT_MS` to `MAX_TASK_TIMEOUT_MS`."""

    ms: int


@dataclass(frozen=True, slots=True)
class OptionsUnreadable:
    """The stored `task_options` text does not parse, or is not a JSON object."""

    reason: str


@dataclass(frozen=True, slots=True)
class TimeoutValueInvalid:
    """The stored `timeout_ms` is not an int inside the bounds."""

    reason: str


@dataclass(frozen=True, slots=True)
class CancelGraceInvalid:
    """The stored `cancel_grace_ms` of a `SHARED_ASYNC` row is not an int
    from `MIN_CANCEL_GRACE_MS` to `MAX_CANCEL_GRACE_MS`."""

    reason: str


type TimeoutRefusal = OptionsUnreadable | TimeoutValueInvalid | CancelGraceInvalid
"""A stored timeout, or the stored cancel grace of a `SHARED_ASYNC` row, that
the worker cannot use. The task fails with it."""

type StoredTimeout = NoTimeout | TaskTimeout | TimeoutRefusal
"""Everything a claimed row can store as its timeout."""


def read_claimed_options(task_options: object) -> StoredTimeout:
    """Read the stored options of a claimed row that its run needs.

    The retry policy must validate (``read_stored_retry_policy``); one that
    does not is ``OptionsUnreadable`` naming ``retry_policy``. Then the
    timeout is read (``read_stored_timeout``). Options text that does not
    parse is refused by ``read_stored_timeout`` with its own reason. Does no
    I/O and logs nothing.
    """
    match task_options:
        case str():
            match read_stored_retry_policy(task_options):
                case InvalidRetryPolicy(detail=detail):
                    return OptionsUnreadable(
                        reason=f'stored retry_policy does not validate: {detail}',
                    )
                case RetryPolicy() | NoRetryPolicy() | OptionsNotAnObject():
                    pass
        case _:
            pass
    return read_stored_timeout(task_options)


def read_stored_timeout(task_options: object) -> StoredTimeout:
    """Read the timeout of a claimed row from its stored `task_options`.

    `task_options` is the column value as the claim returns it: text, or None
    when the row has no options. No writer stores an empty text (the send
    and the workflow node promotion refuse it), so `loads_json` refuses it
    and it reads as `OptionsUnreadable`, as the core's worker read of the
    options (`read_stored_options`) reads it. A stored int with more than
    4300 digits makes the whole text fail to parse, so it reads as
    `OptionsUnreadable`, not as `TimeoutValueInvalid`; the task fails the
    same way. Does no I/O and logs nothing.
    """
    match task_options:
        case None:
            return NoTimeout()
        case str():
            pass
        case _:
            return OptionsUnreadable(
                reason=(
                    'stored task_options are a '
                    f'{type(task_options).__name__}, not text'
                ),
            )
    match loads_json(task_options):
        case Err(err_value=error):
            return OptionsUnreadable(
                reason=f'stored task_options do not parse: {error}',
            )
        case Ok(ok_value=dict() as options):
            return _timeout_value(options.get('timeout_ms'))
        case Ok(ok_value=other):
            return OptionsUnreadable(
                reason=(
                    'stored task_options are a JSON '
                    f'{type(other).__name__}, not an object'
                ),
            )


def _timeout_value(raw: object) -> NoTimeout | TaskTimeout | TimeoutValueInvalid:
    """Check the stored `timeout_ms` value against the rule that wrote it."""
    match raw:
        case None:
            return NoTimeout()
        case bool():
            return TimeoutValueInvalid(reason='stored timeout_ms is a bool, not an int')
        case int() if raw < MIN_TASK_TIMEOUT_MS:
            return TimeoutValueInvalid(
                reason=f'stored timeout_ms is below {MIN_TASK_TIMEOUT_MS} ms',
            )
        case int() if raw > MAX_TASK_TIMEOUT_MS:
            return TimeoutValueInvalid(
                reason=(
                    f'stored timeout_ms is above the limit of {MAX_TASK_TIMEOUT_MS} '
                    'ms (about 49.7 days)'
                ),
            )
        case int():
            return TaskTimeout(ms=raw)
        case _:
            return TimeoutValueInvalid(
                reason=(
                    f'stored timeout_ms is a {type(raw).__name__}, not an int'
                ),
            )
