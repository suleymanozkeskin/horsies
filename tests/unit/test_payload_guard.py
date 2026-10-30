"""Unit tests for the payload-size guardrail (PayloadPolicy, warn, size check)."""

from __future__ import annotations

import logging

import pytest
from pydantic import ValidationError

from horsies.core.codec.payload_guard import (
    PayloadOverLimit,
    PayloadWithinLimit,
    check_payload_size,
    reset_payload_warnings,
    warn_if_payload_large,
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


class TestWarnIfPayloadLarge:
    """Warn threshold and its per-process rate limit."""

    def setup_method(self) -> None:
        reset_payload_warnings()

    def test_under_threshold_is_silent(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        policy = PayloadPolicy(warn_bytes=100, reject_bytes=200)
        with caplog.at_level(logging.WARNING):
            warn_if_payload_large(
                policy, task_name='t', kind='kwargs', encoded_len=99,
            )
        assert _warn_records(caplog) == []

    def test_warn_fires_once_per_task_and_kind(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        policy = PayloadPolicy(warn_bytes=10, reject_bytes=None)
        with caplog.at_level(logging.WARNING):
            for _ in range(3):
                warn_if_payload_large(
                    policy, task_name='t', kind='kwargs', encoded_len=50,
                )
        assert len(_warn_records(caplog)) == 1

    def test_distinct_task_and_kind_warn_separately(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        policy = PayloadPolicy(warn_bytes=10, reject_bytes=None)
        with caplog.at_level(logging.WARNING):
            warn_if_payload_large(
                policy, task_name='a', kind='kwargs', encoded_len=50,
            )
            warn_if_payload_large(
                policy, task_name='a', kind='result', encoded_len=50,
            )
            warn_if_payload_large(
                policy, task_name='b', kind='kwargs', encoded_len=50,
            )
        assert len(_warn_records(caplog)) == 3

    def test_warns_when_also_over_reject_bytes(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The warning does not depend on reject_bytes."""
        policy = PayloadPolicy(warn_bytes=10, reject_bytes=40)
        with caplog.at_level(logging.WARNING):
            warn_if_payload_large(
                policy, task_name='t', kind='kwargs', encoded_len=50,
            )
        assert len(_warn_records(caplog)) == 1

    def test_disabled_threshold_never_fires(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        policy = PayloadPolicy(warn_bytes=None, reject_bytes=None)
        with caplog.at_level(logging.WARNING):
            warn_if_payload_large(
                policy,
                task_name='t',
                kind='kwargs',
                encoded_len=10_000_000,
            )
        assert _warn_records(caplog) == []

    def test_boundary_equal_to_threshold_is_silent(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The threshold is exclusive: exactly warn_bytes does not warn."""
        policy = PayloadPolicy(warn_bytes=100, reject_bytes=None)
        with caplog.at_level(logging.WARNING):
            warn_if_payload_large(
                policy, task_name='t', kind='result', encoded_len=100,
            )
        assert _warn_records(caplog) == []


class TestCheckPayloadSize:
    """One test per outcome of the reject_bytes comparison."""

    def test_no_limit_is_within_limit(self) -> None:
        policy = PayloadPolicy(warn_bytes=None, reject_bytes=None)
        outcome = check_payload_size(policy, encoded_len=10_000_000)
        assert outcome == PayloadWithinLimit()

    def test_under_limit_is_within_limit(self) -> None:
        policy = PayloadPolicy(warn_bytes=None, reject_bytes=100)
        outcome = check_payload_size(policy, encoded_len=99)
        assert outcome == PayloadWithinLimit()

    def test_equal_to_limit_is_within_limit(self) -> None:
        """The limit is exclusive: exactly reject_bytes passes."""
        policy = PayloadPolicy(warn_bytes=None, reject_bytes=100)
        outcome = check_payload_size(policy, encoded_len=100)
        assert outcome == PayloadWithinLimit()

    def test_over_limit_carries_size_and_limit_and_does_not_log(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Over the limit gives both numbers. The check never logs, also
        when the length is above warn_bytes."""
        reset_payload_warnings()
        policy = PayloadPolicy(warn_bytes=10, reject_bytes=40)
        with caplog.at_level(logging.WARNING):
            outcome = check_payload_size(policy, encoded_len=50)
        assert outcome == PayloadOverLimit(size_bytes=50, limit_bytes=40)
        assert _warn_records(caplog) == []
