"""The wait budget of a result get, parsed once at each public get."""

from __future__ import annotations

from dataclasses import dataclass

from horsies.core.models.tasks import MAX_TASK_TIMEOUT_MS
from horsies.core.types.result import Err, Ok, Result

WAIT_TIMEOUT_NOT_AN_INT: str = 'wait_ms must be an int or None'
"""The refusal for a bool or a value of another type."""

WAIT_TIMEOUT_NEGATIVE: str = 'wait_ms must not be negative; 0 checks once with no wait'
"""The refusal for a negative value."""

WAIT_TIMEOUT_ABOVE_LIMIT: str = (
    f'wait_ms is above the limit of {MAX_TASK_TIMEOUT_MS} ms (about 49.7 days)'
)
"""The refusal for a value above the limit.

The refusal texts hold no input value: an int with more than 4300 digits
cannot be converted to a string.
"""


@dataclass(frozen=True, slots=True)
class WaitTimeoutRefused:
    """A `wait_ms` that no wait can use. `message` names the rule."""

    message: str


def wait_timeout_seconds(
    wait_ms: object,
) -> Result[float | None, WaitTimeoutRefused]:
    """The wait budget in seconds, or None when the caller sets no limit.

    Takes `object` because the caller's value reaches here unchecked. Accepts
    None and an int from 0 to `MAX_TASK_TIMEOUT_MS`. A wait is not a task
    timeout, but it uses the same limit: the conversion to seconds overflows
    a float above about 1.8e308 ms, and the Rust port waits take a
    `Duration`, which cannot be negative. Does no I/O.
    """
    match wait_ms:
        case None:
            return Ok(None)
        case bool():
            return Err(WaitTimeoutRefused(message=WAIT_TIMEOUT_NOT_AN_INT))
        case int() if wait_ms < 0:
            return Err(WaitTimeoutRefused(message=WAIT_TIMEOUT_NEGATIVE))
        case int() if wait_ms > MAX_TASK_TIMEOUT_MS:
            return Err(WaitTimeoutRefused(message=WAIT_TIMEOUT_ABOVE_LIMIT))
        case int():
            return Ok(wait_ms / 1000.0)
        case _:
            return Err(WaitTimeoutRefused(message=WAIT_TIMEOUT_NOT_AN_INT))
