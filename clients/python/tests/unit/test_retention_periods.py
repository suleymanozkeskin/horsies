"""A retention class is its period: the rules, the key, and the config.

A period is whole days within bounds, or forever. The class key is a
function of the period, so a key always means one duration. Each refusal
happens at config parse, where the adopter wrote the period, and every
problem of one config is reported together.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from horsies.core.errors import ConfigurationError
from horsies.core.models.retention import (
    RetentionConfig,
    resolve_queue_retention_class,
)

pytestmark = [pytest.mark.unit]


class TestRetentionPeriods:
    def test_a_listed_period_is_accepted(self) -> None:
        config = RetentionConfig(retention_periods=(timedelta(days=365),))
        assert config.retention_periods == (timedelta(days=365),)

    @pytest.mark.parametrize(
        'periods',
        [
            pytest.param((timedelta(days=90), timedelta(days=90)), id='same-value'),
            pytest.param((timedelta(days=2), timedelta(hours=48)), id='same-days'),
        ],
    )
    def test_a_repeated_period_is_refused(
        self, periods: tuple[timedelta, ...],
    ) -> None:
        with pytest.raises(ConfigurationError) as caught:
            RetentionConfig(retention_periods=periods)
        assert 'listed twice' in str(caught.value)

    def test_the_removed_named_classes_are_refused_with_the_replacement(
        self,
    ) -> None:
        with pytest.raises(ValueError) as caught:
            RetentionConfig.model_validate(
                {'retention_classes': [{'key': 'audit_1y', 'duration': 'P365D'}]}
            )
        assert 'retention_classes was removed in 0.6.1' in str(caught.value)
        assert 'retention_periods' in str(caught.value)


class TestTheFieldIsTheOnlyCopy:
    """An edit to a field after validation is what the next send stamps.

    `RetentionConfig` is not frozen, its mapping is mutable, and
    `model_copy(update=...)` runs no validator. A second, parsed copy of
    the periods would keep stamping the old class while the field states
    the new period, and rows would be deleted on the old one.
    """

    @staticmethod
    def _mapped(days: int) -> RetentionConfig:
        return RetentionConfig(queue_retention={'emails': timedelta(days=days)})

    def test_an_item_edit_is_followed(self) -> None:
        config = self._mapped(7)
        config.queue_retention['emails'] = timedelta(days=365)
        assert resolve_queue_retention_class(config, 'emails') == 'standard_365d'
        assert config.registrable_periods() == (
            ('standard_365d', timedelta(days=365)),
        )

    def test_a_field_assignment_is_followed(self) -> None:
        config = self._mapped(7)
        config.queue_retention = {'emails': timedelta(days=365)}
        assert resolve_queue_retention_class(config, 'emails') == 'standard_365d'
        assert config.registrable_periods() == (
            ('standard_365d', timedelta(days=365)),
        )

    def test_a_copy_with_an_update_is_followed(self) -> None:
        copied = self._mapped(7).model_copy(
            update={'queue_retention': {'emails': timedelta(days=90)}},
        )
        assert resolve_queue_retention_class(copied, 'emails') == 'standard_90d'
        assert copied.sendable_periods() == frozenset({90, 30})

    def test_an_invalid_edit_after_validation_is_a_defect(self) -> None:
        config = self._mapped(7)
        config.queue_retention['emails'] = timedelta(hours=36)
        with pytest.raises(AssertionError, match='changed after validation'):
            resolve_queue_retention_class(config, 'emails')
