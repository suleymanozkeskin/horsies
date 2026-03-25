"""Shared retry utilities used by both Worker and Broker.

Pure functions — no DB access, no I/O. Callers provide pre-fetched data.
"""

from __future__ import annotations

import random
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from horsies.core.codec.serde import loads_json
from horsies.core.types.result import is_err


def calculate_retry_delay(
    retry_attempt: int,
    retry_policy_data: dict[str, Any],
) -> float:
    """Calculate the delay in seconds for a retry attempt.

    Args:
        retry_attempt: 1-based attempt number (retry_count + 1).
        retry_policy_data: Deserialized retry_policy dict from task_options.

    Returns:
        Delay in seconds (minimum 1.0).
    """
    intervals = retry_policy_data.get(
        'intervals', [60, 300, 900],
    )  # 1min, 5min, 15min
    backoff_strategy = retry_policy_data.get('backoff_strategy', 'fixed')
    jitter = retry_policy_data.get('jitter', True)

    if backoff_strategy == 'fixed':
        clamped_index = min(retry_attempt - 1, len(intervals) - 1)
        base_delay = intervals[clamped_index]
    elif backoff_strategy == 'exponential':
        base_interval = intervals[0] if intervals else 60
        base_delay = base_interval * (2 ** (retry_attempt - 1))
    else:
        base_delay = intervals[0] if intervals else 60

    if jitter:
        jitter_range = base_delay * 0.25
        base_delay += random.uniform(-jitter_range, jitter_range)

    return float(max(1.0, base_delay))


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
