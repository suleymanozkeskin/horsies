"""A retention class is its period: the rules, the key, and the config.

A period is whole days within bounds, or forever. The class key is a
function of the period, so a key always means one duration. Each refusal
happens at config parse, where the adopter wrote the period, and every
problem of one config is reported together.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from horsies.core.errors import ConfigurationError, MultipleValidationErrors
from horsies.core.history.ddl.classes import (
    DEFAULT_RETENTION_CLASS_KEY,
    DEFAULT_RETENTION_DURATION,
)
from horsies.core.history.names import MAX_RETENTION_CLASS_KEY_LENGTH
from horsies.core.history.periods import (
    MAX_RETENTION_DAYS,
    MIN_RETENTION_DAYS,
    PeriodRefused,
    RetentionDays,
    parse_retention_period,
    period_class_key,
)
from horsies.core.models.retention import (
    RetentionConfig,
    resolve_queue_retention_class,
)
from horsies.core.types.result import Err, Ok

pytestmark = [pytest.mark.unit]


class TestParseRetentionPeriod:
    @pytest.mark.parametrize(
        ('period', 'days'),
        [
            pytest.param(timedelta(days=MIN_RETENTION_DAYS), MIN_RETENTION_DAYS, id='minimum'),
            pytest.param(timedelta(hours=48), 2, id='hours-that-are-whole-days'),
            pytest.param(timedelta(days=MAX_RETENTION_DAYS), MAX_RETENTION_DAYS, id='maximum'),
        ],
    )
    def test_whole_days_in_bounds_are_accepted(
        self, period: timedelta, days: int,
    ) -> None:
        assert parse_retention_period(period) == Ok(days)

    @pytest.mark.parametrize(
        ('period', 'reason'),
        [
            pytest.param(timedelta(hours=36), 'not a whole number of days', id='fractional-day'),
            pytest.param(timedelta(days=7, microseconds=1), 'not a whole number of days', id='microsecond'),
            pytest.param(timedelta(0), 'shorter than', id='zero'),
            pytest.param(timedelta(days=-7), 'shorter than', id='negative'),
            pytest.param(timedelta(days=MAX_RETENTION_DAYS + 1), 'longer than', id='above-maximum'),
        ],
    )
    def test_other_periods_are_refused_with_the_reason(
        self, period: timedelta, reason: str,
    ) -> None:
        match parse_retention_period(period):
            case Err(err_value=PeriodRefused(reason=text)):
                assert reason in text, text
            case Ok() as accepted:
                pytest.fail(f'{period} was accepted: {accepted!r}')


class TestPeriodClassKey:
    def test_the_default_period_gives_the_default_key(self) -> None:
        """The default class continues: no default row moves to a new class."""
        assert DEFAULT_RETENTION_CLASS_KEY == 'standard_30d'
        assert DEFAULT_RETENTION_DURATION == timedelta(days=30)

    def test_a_period_names_its_class(self) -> None:
        assert period_class_key(RetentionDays(7)) == 'standard_7d'

    def test_the_longest_key_fits_the_relation_name_budget(self) -> None:
        assert len(period_class_key(RetentionDays(MAX_RETENTION_DAYS))) <= (
            MAX_RETENTION_CLASS_KEY_LENGTH
        )

    @pytest.mark.parametrize('days', [0, MAX_RETENTION_DAYS + 1])
    def test_a_period_out_of_bounds_is_a_defect(self, days: int) -> None:
        with pytest.raises(AssertionError):
            period_class_key(RetentionDays(days))


class TestQueueRetention:
    def test_a_whole_day_period_and_forever_are_accepted(self) -> None:
        config = RetentionConfig(
            queue_retention={'emails': timedelta(days=7), 'audit': None},
        )
        assert config.queue_retention == {
            'emails': timedelta(days=7), 'audit': None,
        }

    def test_any_queue_name_can_be_mapped(self) -> None:
        """The queue name is a lookup key only, so no name rule applies here."""
        name = 'XML-Generation.v2_' + 'q' * 82
        assert len(name) == 100

        config = RetentionConfig(queue_retention={name: timedelta(days=7)})

        assert resolve_queue_retention_class(config, name) == 'standard_7d'

    @pytest.mark.parametrize(
        'period',
        [
            pytest.param(timedelta(hours=36), id='fractional-day'),
            pytest.param(timedelta(0), id='zero'),
            pytest.param(timedelta(days=MAX_RETENTION_DAYS + 1), id='above-maximum'),
        ],
    )
    def test_a_mapped_period_out_of_the_rules_is_refused(
        self, period: timedelta,
    ) -> None:
        with pytest.raises(ConfigurationError) as caught:
            RetentionConfig(queue_retention={'emails': period})
        assert "queue 'emails'" in str(caught.value)

    def test_every_problem_is_collected_not_just_the_first(self) -> None:
        with pytest.raises(MultipleValidationErrors) as caught:
            RetentionConfig(
                queue_retention={
                    'emails': timedelta(hours=1),
                    'reports': timedelta(days=MAX_RETENTION_DAYS + 1),
                },
                retention_periods=(timedelta(0),),
            )
        reported = str(caught.value)
        assert "'emails'" in reported
        assert "'reports'" in reported
        assert 'retention period 0:00:00' in reported


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


class TestRegistrableClasses:
    def test_one_class_per_distinct_period_without_the_default(self) -> None:
        """Queues with one period share its class; the library owns 30 days."""
        config = RetentionConfig(
            queue_retention={
                'emails': timedelta(days=7),
                'alerts': timedelta(days=7),
                'reports': DEFAULT_RETENTION_DURATION,
                'audit': None,
            },
            retention_periods=(timedelta(days=365), timedelta(days=7)),
        )

        assert config.registrable_classes() == (
            ('standard_7d', timedelta(days=7)),
            ('standard_365d', timedelta(days=365)),
        )

    def test_the_sendable_periods_are_the_default_the_mapped_and_the_listed(
        self,
    ) -> None:
        config = RetentionConfig(
            queue_retention={'emails': timedelta(days=7), 'audit': None},
            retention_periods=(timedelta(days=365),),
        )

        assert config.sendable_periods() == frozenset({7, 365, 30})


class TestResolveQueueRetentionClass:
    @pytest.mark.parametrize(
        ('retention', 'queue', 'key'),
        [
            pytest.param(object(), 'emails', DEFAULT_RETENTION_CLASS_KEY, id='no-config'),
            pytest.param(RetentionConfig(), 'emails', DEFAULT_RETENTION_CLASS_KEY, id='unmapped'),
            pytest.param(
                RetentionConfig(queue_retention={'emails': None}), 'emails', None,
                id='forever',
            ),
            pytest.param(
                RetentionConfig(queue_retention={'emails': timedelta(days=7)}),
                'emails', 'standard_7d', id='mapped',
            ),
        ],
    )
    def test_each_outcome(
        self, retention: object, queue: str, key: str | None,
    ) -> None:
        assert resolve_queue_retention_class(retention, queue) == key


@pytest.mark.unit
class TestReportedRetentionEqualsGoverning:
    """The health surface reports what the worker actually runs on.

    The maintenance pass binds `retention_config or RetentionConfig()`,
    so an unset section means the worker runs on DEFAULTS. If the
    snapshot resolved differently it would publish nulls for a worker
    governed by real values — a surface disagreeing with the behaviour
    it describes.
    """

    def test_an_unset_section_reports_the_defaults_it_runs_on(self) -> None:
        from horsies.core.worker.config import WorkerConfig
        from horsies.core.worker.health import _effective_retention

        cfg = WorkerConfig(
            dsn='postgresql+psycopg://u:p@h/d',
            psycopg_dsn='postgresql://u:p@h/d',
            queues=['default'],
        )
        assert cfg.retention_config is None
        effective = _effective_retention(cfg)
        assert effective.terminal_record_retention_hours == (
            RetentionConfig().terminal_record_retention_hours
        ), 'an unset section must report the defaults the pass runs on'
        assert effective.worker_state_retention_hours == (
            RetentionConfig().worker_state_retention_hours
        )

    def test_a_set_section_reports_itself(self) -> None:
        from horsies.core.worker.config import WorkerConfig
        from horsies.core.worker.health import _effective_retention

        cfg = WorkerConfig(
            dsn='postgresql+psycopg://u:p@h/d',
            psycopg_dsn='postgresql://u:p@h/d',
            queues=['default'],
            retention_config=RetentionConfig(
                terminal_record_retention_hours=99
            ),
        )
        assert _effective_retention(cfg).terminal_record_retention_hours == 99
