"""The reservation window of an idempotency key: its default, its bound and
its check.

The core frames and hashes the caller's key under the task-name scope
(``ScopedIdempotencyKey``, ``core/src/core/history/identity/keys.rs``).
The client checks the configured window before any write.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Final


IDEMPOTENCY_WINDOW_DEFAULT: Final = timedelta(hours=24)
IDEMPOTENCY_WINDOW_MAX: Final = timedelta(days=30)


def validate_reservation_window(window: timedelta) -> timedelta:
    """Validate a configured reservation duration before any mutation.

    The accepted range is greater than zero through the inclusive 30-day
    maximum; the check runs before any task or registry write so an invalid
    configuration cannot leave partial state.
    """
    if window <= timedelta(0):
        raise ValueError('idempotency reservation window must be positive')
    if window > IDEMPOTENCY_WINDOW_MAX:
        raise ValueError(
            f'idempotency reservation window exceeds the inclusive maximum '
            f'of {IDEMPOTENCY_WINDOW_MAX.days} days'
        )
    return window
