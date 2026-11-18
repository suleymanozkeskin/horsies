"""The retry rules shared by the worker and the broker.

Pure functions: no database access, no I/O. Callers pass the facts they read.

A stored retry policy is read with the rule of enqueue
(``stored_retry_policy``): it validates as ``RetryPolicy`` or it is refused.
The claim refuses to run a row whose policy is refused (``read_claimed_options``
in ``core/worker/stored_timeout.py``), so a task with such a policy fails
before it runs. No rule here falls back to default intervals, backoff or
jitter.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from enum import Enum
from typing import assert_never

from horsies.core.codec.json_io import loads_json
from horsies.core.codec.task_options import (
    InvalidRetryPolicy,
    NoRetryPolicy,
    stored_retry_policy,
)
from horsies.core.models.tasks import RetryPolicy
from horsies.core.types.result import Err, Ok


@dataclass(frozen=True, slots=True)
class OptionsNotAnObject:
    """The stored ``task_options`` text does not parse, or is not a JSON object.

    ``reason`` names the cause and holds no stored value.
    """

    reason: str


type StoredRetryPolicy = (
    RetryPolicy | NoRetryPolicy | InvalidRetryPolicy | OptionsNotAnObject
)
"""Everything a stored ``task_options`` text gives as its retry policy."""


def read_stored_retry_policy(task_options_json: str | None) -> StoredRetryPolicy:
    """Read the retry policy of a stored ``task_options`` text. Pure.

    None or an empty text is ``NoRetryPolicy``, as the core's worker read of
    the options (``read_stored_options``) reads them.
    A text that does not parse, or is not a JSON object, is
    ``OptionsNotAnObject``. The ``retry_policy`` of an object is read by
    ``stored_retry_policy``.
    """
    match task_options_json:
        case None | '':
            return NoRetryPolicy()
        case str():
            pass
    match loads_json(task_options_json):
        case Err(err_value=error):
            return OptionsNotAnObject(
                reason=f'stored task_options do not parse: {error}',
            )
        case Ok(ok_value=dict() as options):
            pass
        case Ok(ok_value=other):
            return OptionsNotAnObject(
                reason=(
                    'stored task_options are a JSON '
                    f'{type(other).__name__}, not an object'
                ),
            )
    match stored_retry_policy(options):
        case Ok(ok_value=read):
            return read
        case Err(err_value=refused):
            return refused


def calculate_retry_delay(retry_attempt: int, policy: RetryPolicy) -> float:
    """The delay in seconds before retry ``retry_attempt`` (1-based). Pure.

    ``fixed`` takes the interval of the attempt, and the last interval past
    the end of the list; ``exponential`` doubles the one base interval per
    attempt. Jitter adds up to 25% upward. ``max_delay_seconds`` caps the
    result after jitter. The result is at least 1.0.
    """
    intervals = policy.intervals
    match policy.backoff_strategy:
        case 'exponential':
            base_delay = float(intervals[0] * (2 ** (retry_attempt - 1)))
        case 'fixed':
            base_delay = float(intervals[min(retry_attempt - 1, len(intervals) - 1)])

    # Floor before jitter so 1.0 is the bottom of the spread, not a clamp
    # that collapses the lower half of a symmetric window onto 1.0 (which
    # destroys uniformity at small base delays). Jitter is applied upward.
    base_delay = max(1.0, base_delay)

    if policy.jitter:
        base_delay += random.uniform(0.0, base_delay * 0.25)

    match policy.max_delay_seconds:
        case None:
            return base_delay
        case cap:
            return min(base_delay, float(cap))


def admitted_retry_policy(
    *,
    retry_count: int,
    max_retries: int,
    task_options_json: str | None,
    error_code: str | Enum | None,
) -> RetryPolicy | None:
    """The retry policy of a task when it retries ``error_code`` now. Pure.

    None when the retries are spent (``retry_count >= max_retries``, or
    ``max_retries`` is 0), when the options hold no policy or a policy that
    does not validate (the claim refused to run such a task, and its failure
    is terminal), or when ``auto_retry_for`` does not name the code. The
    deadline is not judged here.
    """
    if max_retries == 0 or retry_count >= max_retries:
        return None

    match read_stored_retry_policy(task_options_json):
        case RetryPolicy() as policy:
            pass
        case NoRetryPolicy() | InvalidRetryPolicy() | OptionsNotAnObject():
            return None
        case _ as unreachable:
            assert_never(unreachable)

    code = error_code.value if isinstance(error_code, Enum) else error_code
    if not code:
        return None
    named = {
        entry.value if isinstance(entry, Enum) else entry
        for entry in policy.auto_retry_for
    }
    if code not in named:
        return None
    return policy
