"""Unit tests for the payload-size guardrail (PayloadPolicy + enforce)."""

from __future__ import annotations

import logging

import pytest
from pydantic import ValidationError

from horsies.core.codec.payload_guard import (
    enforce_payload_policy,
    reset_payload_warnings,
)
from horsies.core.models.payload import PayloadPolicy

pytestmark = [pytest.mark.unit]

_GUARD_MSG = 'Payload size guardrail'


def _warn_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if _GUARD_MSG in r.getMessage()]


class TestPayloadPolicyModel:
    """PayloadPolicy defaults and bounds."""

    def test_defaults(self) -> None:
        policy = PayloadPolicy()
        assert policy.warn_bytes == 1_048_576
        assert policy.reject_bytes is None

    def test_zero_thresholds_rejected(self) -> None:
        with pytest.raises(ValidationError):
            PayloadPolicy(warn_bytes=0)
        with pytest.raises(ValidationError):
            PayloadPolicy(reject_bytes=0)

    def test_none_disables_each_threshold(self) -> None:
        policy = PayloadPolicy(warn_bytes=None, reject_bytes=None)
        assert policy.warn_bytes is None
        assert policy.reject_bytes is None


class TestEnforcePayloadPolicy:
    """Warn rate-limiting and reject verdicts."""

    def setup_method(self) -> None:
        reset_payload_warnings()

    def test_under_thresholds_is_silent(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        policy = PayloadPolicy(warn_bytes=100, reject_bytes=200)
        with caplog.at_level(logging.WARNING):
            verdict = enforce_payload_policy(
                policy, task_name='t', kind='kwargs', encoded_len=100,
            )
        assert verdict is None
        assert _warn_records(caplog) == []

    def test_warn_fires_once_per_task_and_kind(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        policy = PayloadPolicy(warn_bytes=10, reject_bytes=None)
        with caplog.at_level(logging.WARNING):
            for _ in range(3):
                verdict = enforce_payload_policy(
                    policy, task_name='t', kind='kwargs', encoded_len=50,
                )
                assert verdict is None
        assert len(_warn_records(caplog)) == 1

    def test_distinct_task_and_kind_warn_separately(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        policy = PayloadPolicy(warn_bytes=10, reject_bytes=None)
        with caplog.at_level(logging.WARNING):
            enforce_payload_policy(
                policy, task_name='a', kind='kwargs', encoded_len=50,
            )
            enforce_payload_policy(
                policy, task_name='a', kind='result', encoded_len=50,
            )
            enforce_payload_policy(
                policy, task_name='b', kind='kwargs', encoded_len=50,
            )
        assert len(_warn_records(caplog)) == 3

    def test_over_reject_returns_size_and_still_warns(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        policy = PayloadPolicy(warn_bytes=10, reject_bytes=40)
        with caplog.at_level(logging.WARNING):
            verdict = enforce_payload_policy(
                policy, task_name='t', kind='kwargs', encoded_len=50,
            )
        assert verdict == 50
        assert len(_warn_records(caplog)) == 1

    def test_disabled_thresholds_never_fire(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        policy = PayloadPolicy(warn_bytes=None, reject_bytes=None)
        with caplog.at_level(logging.WARNING):
            verdict = enforce_payload_policy(
                policy,
                task_name='t',
                kind='kwargs',
                encoded_len=10_000_000,
            )
        assert verdict is None
        assert _warn_records(caplog) == []

    def test_boundary_equal_to_threshold_is_allowed(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Thresholds are exclusive: exactly warn/reject bytes passes."""
        policy = PayloadPolicy(warn_bytes=100, reject_bytes=100)
        with caplog.at_level(logging.WARNING):
            verdict = enforce_payload_policy(
                policy, task_name='t', kind='result', encoded_len=100,
            )
        assert verdict is None
        assert _warn_records(caplog) == []
