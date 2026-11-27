"""The bound of `RetryPolicy.max_retries` is the stored bound.

The baseline stores `max_retries` from 0 to 20 (`horsies_tasks`, CHECK
`horsies_tasks_retry_range`), and the core reads the stored retry
policy with the same bound (`core/src/worker/retry.rs`,
`stored_max_retries`). A `RetryPolicy` built in Python cannot pass it.
"""

from __future__ import annotations

import pytest

from horsies.core.models.tasks import (
    MAX_RETRY_ATTEMPTS,
    OperationalErrorCode,
    RetryPolicy,
)

pytestmark = [pytest.mark.unit]


class TestStoredMaxRetries:
    def test_the_retry_policy_bound_is_the_stored_bound(self) -> None:
        policy = RetryPolicy.exponential(
            base_seconds=1,
            max_retries=MAX_RETRY_ATTEMPTS,
            auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
        )
        assert policy.max_retries == MAX_RETRY_ATTEMPTS
        with pytest.raises(ValueError, match='less than or equal to 20'):
            RetryPolicy.exponential(
                base_seconds=1,
                max_retries=MAX_RETRY_ATTEMPTS + 1,
                auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
            )


