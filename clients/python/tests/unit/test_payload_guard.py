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
    utf8_byte_length,
    warn_if_payload_large,
)
from horsies.core.models.payload import PayloadPolicy

pytestmark = [pytest.mark.unit]

_GUARD_MSG = 'Payload size guardrail'

# Two bytes of UTF-8 text per character.
_TWO_BYTE_CHARACTER = '\u00e9'


def _ascii(size_bytes: int) -> str:
    """A text of ``size_bytes`` characters and ``size_bytes`` UTF-8 bytes."""
    return 'x' * size_bytes


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
                policy, task_name='t', kind='kwargs', payload_text=_ascii(99),
            )
        assert _warn_records(caplog) == []

    def test_warn_fires_once_per_task_and_kind(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        policy = PayloadPolicy(warn_bytes=10, reject_bytes=None)
        with caplog.at_level(logging.WARNING):
            for _ in range(3):
                warn_if_payload_large(
                    policy, task_name='t', kind='kwargs', payload_text=_ascii(50),
                )
        assert len(_warn_records(caplog)) == 1

    def test_distinct_task_and_kind_warn_separately(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        policy = PayloadPolicy(warn_bytes=10, reject_bytes=None)
        with caplog.at_level(logging.WARNING):
            warn_if_payload_large(
                policy, task_name='a', kind='kwargs', payload_text=_ascii(50),
            )
            warn_if_payload_large(
                policy, task_name='a', kind='result', payload_text=_ascii(50),
            )
            warn_if_payload_large(
                policy, task_name='b', kind='kwargs', payload_text=_ascii(50),
            )
        assert len(_warn_records(caplog)) == 3

    def test_warns_when_also_over_reject_bytes(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The warning does not depend on reject_bytes."""
        policy = PayloadPolicy(warn_bytes=10, reject_bytes=40)
        with caplog.at_level(logging.WARNING):
            warn_if_payload_large(
                policy, task_name='t', kind='kwargs', payload_text=_ascii(50),
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
                payload_text=_ascii(10_000_000),
            )
        assert _warn_records(caplog) == []

    def test_boundary_equal_to_threshold_is_silent(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The threshold is exclusive: exactly warn_bytes does not warn."""
        policy = PayloadPolicy(warn_bytes=100, reject_bytes=None)
        with caplog.at_level(logging.WARNING):
            warn_if_payload_large(
                policy, task_name='t', kind='result', payload_text=_ascii(100),
            )
        assert _warn_records(caplog) == []


class TestCheckPayloadSize:
    """One test per outcome of the reject_bytes comparison."""

    def test_no_limit_is_within_limit(self) -> None:
        policy = PayloadPolicy(warn_bytes=None, reject_bytes=None)
        outcome = check_payload_size(policy, payload_text=_ascii(10_000_000))
        assert outcome == PayloadWithinLimit()

    def test_under_limit_is_within_limit(self) -> None:
        policy = PayloadPolicy(warn_bytes=None, reject_bytes=100)
        outcome = check_payload_size(policy, payload_text=_ascii(99))
        assert outcome == PayloadWithinLimit()

    def test_equal_to_limit_is_within_limit(self) -> None:
        """The limit is exclusive: exactly reject_bytes passes."""
        policy = PayloadPolicy(warn_bytes=None, reject_bytes=100)
        outcome = check_payload_size(policy, payload_text=_ascii(100))
        assert outcome == PayloadWithinLimit()

    def test_over_limit_carries_size_and_limit_and_does_not_log(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Over the limit gives both numbers. The check never logs, also
        when the length is above warn_bytes."""
        reset_payload_warnings()
        policy = PayloadPolicy(warn_bytes=10, reject_bytes=40)
        with caplog.at_level(logging.WARNING):
            outcome = check_payload_size(policy, payload_text=_ascii(50))
        assert outcome == PayloadOverLimit(size_bytes=50, limit_bytes=40)
        assert _warn_records(caplog) == []

    def test_size_is_counted_in_utf8_bytes_not_characters(self) -> None:
        """60 characters of 2 bytes each are 120 bytes: over a 100-byte limit."""
        policy = PayloadPolicy(warn_bytes=None, reject_bytes=100)
        outcome = check_payload_size(
            policy, payload_text=_TWO_BYTE_CHARACTER * 60,
        )
        assert outcome == PayloadOverLimit(size_bytes=120, limit_bytes=100)


class TestWarnCountsUtf8Bytes:
    def setup_method(self) -> None:
        reset_payload_warnings()

    def test_warn_counts_utf8_bytes_not_characters(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """60 characters of 2 bytes each pass a 100-byte warn threshold."""
        policy = PayloadPolicy(warn_bytes=100, reject_bytes=None)
        with caplog.at_level(logging.WARNING):
            warn_if_payload_large(
                policy,
                task_name='t',
                kind='kwargs',
                payload_text=_TWO_BYTE_CHARACTER * 60,
            )
        records = _warn_records(caplog)
        assert len(records) == 1
        assert 'is 120 bytes' in records[0].getMessage()


class TestUtf8ByteLength:
    """The size of a payload is the byte count of its UTF-8 text."""

    @pytest.mark.parametrize(
        ('text', 'size_bytes'),
        [
            ('', 0),
            ('{"a":1}', 7),
            ('\u00e9', 2),
            ('\u20ac', 3),
            ('\U0001f600', 4),
        ],
    )
    def test_counts_bytes_of_the_utf8_text(self, text: str, size_bytes: int) -> None:
        assert utf8_byte_length(text) == size_bytes

    def test_lone_surrogate_counts_its_three_byte_form(self) -> None:
        assert utf8_byte_length('\ud800') == 3
