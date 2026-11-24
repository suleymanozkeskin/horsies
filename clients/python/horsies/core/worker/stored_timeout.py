"""The refusals of the stored task options of a run.

The options do not parse (`OptionsUnreadable`), the timeout value is not a
whole number of milliseconds in range (`TimeoutValueInvalid`), or a
`SHARED_ASYNC` row has no valid `cancel_grace_ms` (`CancelGraceInvalid`). A
refusal is not dropped: the child sets the row RUNNING and returns
`WORKER_SERIALIZATION_ERROR` before it resolves the task, so the normal
persist path fails the task. The text of each refusal holds no stored value,
because an int with more than 4300 digits cannot be converted to a string.
"""

from __future__ import annotations

from dataclasses import dataclass


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
    from `MIN_CANCEL_GRACE_MS` to `MAX_CANCEL_GRACE_MS`, or the stored grace
    of an `ISOLATED` row is set and outside those bounds."""

    reason: str


type TimeoutRefusal = OptionsUnreadable | TimeoutValueInvalid | CancelGraceInvalid
"""A stored timeout, or the stored cancel grace of a `SHARED_ASYNC` row, that
the worker cannot use. The task fails with it."""


