"""Declared retention classes are refused at config parse, not at use.

A declaration that the registration machinery could not honour must fail
where the adopter wrote it. The alternative is a class key that parses,
reaches enqueue, and only fails when the task terminalizes into a
partition that was never created.

Each refusal here names the declaration; the validator collects every
problem so one bad config reports all of them at once rather than one
per run.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from horsies.core.errors import ConfigurationError, MultipleValidationErrors
from horsies.core.models.recovery import (
    RESERVED_RETENTION_CLASS_KEYS,
    RecoveryConfig,
    RetentionClassConfig,
)


@pytest.mark.unit
class TestDeclaredRetentionClasses:
    def test_no_declarations_is_the_default(self) -> None:
        assert RecoveryConfig().retention_classes == ()

    def test_a_well_formed_declaration_is_accepted(self) -> None:
        config = RecoveryConfig(
            retention_classes=(
                RetentionClassConfig(key='audit_1y', duration=timedelta(days=365)),
            )
        )
        assert config.retention_classes[0].key == 'audit_1y'
        assert config.retention_classes[0].duration == timedelta(days=365)

    @pytest.mark.parametrize('reserved', sorted(RESERVED_RETENTION_CLASS_KEYS))
    def test_reserved_keys_are_refused(self, reserved: str) -> None:
        """The library owns these and fixes their durations."""
        with pytest.raises(ConfigurationError) as caught:
            RecoveryConfig(
                retention_classes=(
                    RetentionClassConfig(key=reserved, duration=timedelta(days=5)),
                )
            )
        assert reserved in str(caught.value)

    @pytest.mark.parametrize(
        'unsafe',
        ['has space', 'has-dash', '1leading_digit', 'quote"', 'semi;colon', ''],
    )
    def test_unusable_identifiers_are_refused(self, unsafe: str) -> None:
        """The key becomes part of a relation name for the class partition."""
        with pytest.raises(ConfigurationError):
            RecoveryConfig(
                retention_classes=(
                    RetentionClassConfig(key=unsafe, duration=timedelta(days=5)),
                )
            )

    @pytest.mark.parametrize(
        'bad_duration', [timedelta(0), timedelta(seconds=-1), timedelta(days=-30)]
    )
    def test_non_positive_durations_are_refused(
        self, bad_duration: timedelta
    ) -> None:
        with pytest.raises(ConfigurationError):
            RecoveryConfig(
                retention_classes=(
                    RetentionClassConfig(key='weekly', duration=bad_duration),
                )
            )

    def test_a_duplicate_key_is_refused(self) -> None:
        """A class has exactly one duration."""
        with pytest.raises(ConfigurationError) as caught:
            RecoveryConfig(
                retention_classes=(
                    RetentionClassConfig(key='weekly', duration=timedelta(days=7)),
                    RetentionClassConfig(key='weekly', duration=timedelta(days=14)),
                )
            )
        assert 'weekly' in str(caught.value)

    def test_every_problem_is_collected_not_just_the_first(self) -> None:
        """One bad config reports all of its problems in one run.

        Per `raise_collected`: a single error raises that error, so the
        single-problem tests above expect `ConfigurationError`; two or
        more raise `MultipleValidationErrors` wrapping the report.
        """
        with pytest.raises(MultipleValidationErrors) as caught:
            RecoveryConfig(
                retention_classes=(
                    RetentionClassConfig(key='forever', duration=timedelta(days=5)),
                    RetentionClassConfig(key='bad key', duration=timedelta(days=5)),
                    RetentionClassConfig(key='ok_key', duration=timedelta(0)),
                )
            )
        reported = str(caught.value)
        assert 'forever' in reported
        assert 'bad key' in reported
        assert 'ok_key' in reported

    def test_a_sub_day_duration_is_accepted(self) -> None:
        """Retention is a minimum; daily leaves cannot under-retain it.

        A one-hour class keeps a leaf until its whole day is an hour
        past, so the row survives longer than declared — never less.
        Accepting it is correct; the guide documents the granularity.
        """
        config = RecoveryConfig(
            retention_classes=(
                RetentionClassConfig(key='hourly_ish', duration=timedelta(hours=1)),
            )
        )
        assert config.retention_classes[0].duration == timedelta(hours=1)
