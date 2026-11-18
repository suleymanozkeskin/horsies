"""Unit tests for horsies.core.utils.db error classification helpers."""

from __future__ import annotations

import pytest

from horsies._core import (
    CoreConnectionError,
    CoreContractViolation,
    CorePreconditionFailed,
    CoreServerError,
    CoreStopped,
    ForkedAfterCoreStart,
    TooManyPendingCalls,
)
from horsies.core.utils.db import (
    is_retryable_connection_error,
)


@pytest.mark.unit
class TestIsRetryableConnectionError:
    """Tests for is_retryable_connection_error."""


    def test_a_core_failure_keeps_the_core_classification(self) -> None:
        lost = CoreConnectionError('release_claims', 'connection lost', True)
        closed = CoreConnectionError('release_claims', 'the main pool is closed', False)
        deadlock = CoreServerError('release_claims', '40P01', 'deadlock detected', True)
        unique = CoreServerError('release_claims', '23505', 'duplicate key value', False)
        held = CorePreconditionFailed('install_schema', 'the install lock is held', True)
        family = CorePreconditionFailed('install_schema', 'another client family', False)
        assert is_retryable_connection_error(lost) is True
        assert is_retryable_connection_error(closed) is False
        assert is_retryable_connection_error(deadlock) is True
        assert is_retryable_connection_error(unique) is False
        assert is_retryable_connection_error(held) is True
        assert is_retryable_connection_error(family) is False

    def test_a_core_contract_violation_is_not_retryable(self) -> None:
        broken = CoreContractViolation('release_claims', 'ordinality 3 has no answer')
        assert is_retryable_connection_error(broken) is False

    def test_a_core_boundary_error_is_retryable_only_when_it_clears(self) -> None:
        busy = TooManyPendingCalls('CoreHandle.release_claims_async', 65_536)
        stopped = CoreStopped('core task', 'panic')
        forked = ForkedAfterCoreStart(100, 101, 'exec the child')
        assert is_retryable_connection_error(busy) is True
        assert is_retryable_connection_error(stopped) is False
        assert is_retryable_connection_error(forked) is False


