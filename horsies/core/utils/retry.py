"""Shared retry utilities used by both Worker and Broker.

Pure functions — no DB access, no I/O. Callers provide pre-fetched data.
"""

from __future__ import annotations

import math
import random
from datetime import datetime, timezone
from enum import Enum
from typing import Any, cast

from horsies.core.codec.json_io import loads_json
from horsies.core.types.result import is_err

# 1min, 5min, 15min
_DEFAULT_RETRY_INTERVALS: list[float] = [60.0, 300.0, 900.0]


def _sanitize_intervals(raw: Any) -> list[float]:
    """Return validated retry intervals.

    The intervals come from an untrusted deserialized dict; anything that is
    not a non-empty list of finite numbers falls back to the defaults, so
    delay calculation is total.
    """
    if not isinstance(raw, list) or not raw:
        return list(_DEFAULT_RETRY_INTERVALS)
    entries = cast('list[Any]', raw)
    sanitized: list[float] = []
    for value in entries:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return list(_DEFAULT_RETRY_INTERVALS)
        if not math.isfinite(value):
            return list(_DEFAULT_RETRY_INTERVALS)
        sanitized.append(float(value))
    return sanitized


def calculate_retry_delay(
    retry_attempt: int,
    retry_policy_data: dict[str, Any],
) -> float:
    """Calculate the delay in seconds for a retry attempt.

    Total: corrupt policy data (missing, empty, or non-numeric intervals,
    unknown backoff strategy) falls back to defaults instead of raising.

    Args:
        retry_attempt: 1-based attempt number (retry_count + 1).
        retry_policy_data: Deserialized retry_policy dict from task_options.

    Returns:
        Delay in seconds (minimum 1.0).
    """
    intervals = _sanitize_intervals(retry_policy_data.get('intervals'))
    backoff_strategy = retry_policy_data.get('backoff_strategy', 'fixed')
    jitter = retry_policy_data.get('jitter', True)
    max_delay_seconds = retry_policy_data.get('max_delay_seconds')

    match backoff_strategy:
        case 'exponential':
            base_delay = intervals[0] * (2 ** (retry_attempt - 1))
        case 'fixed':
            base_delay = intervals[min(retry_attempt - 1, len(intervals) - 1)]
        case _:
            base_delay = intervals[0]

    # Floor before jitter so 1.0 is the bottom of the spread, not a clamp
    # that collapses the lower half of a symmetric window onto 1.0 (which
    # destroys uniformity at small base delays). Jitter is applied upward.
    base_delay = max(1.0, base_delay)

    if jitter:
        base_delay += random.uniform(0.0, base_delay * 0.25)

    if isinstance(max_delay_seconds, int) and max_delay_seconds > 0:
        base_delay = min(base_delay, float(max_delay_seconds))

    return float(base_delay)


def parse_retry_policy(
    task_options_json: str | None,
) -> dict[str, Any] | None:
    """Extract retry_policy dict from serialized task_options JSON.

    Returns None if task_options is missing, corrupt, or has no retry_policy.
    """
    if not task_options_json:
        return None
    try:
        opts_r = loads_json(task_options_json)
        if is_err(opts_r):
            return None
        task_options_data = opts_r.ok_value
        if not isinstance(task_options_data, dict):
            return None
        retry_policy_raw = task_options_data.get('retry_policy')
        if not isinstance(retry_policy_raw, dict):
            return None
        return retry_policy_raw
    except Exception:
        return None


def check_retry_eligibility(
    *,
    retry_count: int,
    max_retries: int,
    task_options_json: str | None,
    error_code: str | Enum | None,
    good_until: datetime | None,
    db_now: datetime,
) -> bool:
    """Determine if a failed task is eligible for retry.

    Pure check — does not touch the database or modify any state.

    Args:
        retry_count: Current retry count from the task row.
        max_retries: Maximum retries allowed from the task row.
        task_options_json: Serialized task_options (contains retry_policy).
        error_code: The error code from the failure (enum member or string).
        good_until: Task expiry deadline (None = no expiry).
        db_now: Current database time for good_until comparison.

    Returns:
        True if the task should be retried, False otherwise.
    """
    if max_retries == 0 or retry_count >= max_retries:
        return False

    retry_policy = parse_retry_policy(task_options_json)
    if retry_policy is None:
        return False

    auto_retry_for = retry_policy.get('auto_retry_for')
    if not isinstance(auto_retry_for, list) or not auto_retry_for:
        return False

    code = error_code.value if isinstance(error_code, Enum) else error_code
    if not code or code not in auto_retry_for:
        return False

    if good_until is not None:
        _good_until = good_until
        _db_now = db_now
        if _good_until.tzinfo is None:
            _good_until = _good_until.replace(tzinfo=timezone.utc)
        if _db_now.tzinfo is None:
            _db_now = _db_now.replace(tzinfo=timezone.utc)
        if _good_until <= _db_now:
            return False

    return True
