"""The paused-workflow age of the retention config holds whole microseconds
and is at most ``PAUSED_WORKFLOW_AGE_MAX``.

The core refuses a paused age with a part below one microsecond when its
config is read: the expiry sends the age to the database as a count of
microseconds. A Python ``timedelta`` holds whole microseconds only, so no
Python config can carry such a part: a finer input is rounded by
``timedelta`` itself, before the config check runs. These tests pin that,
so the Python config needs no refusal of its own.

Both configs refuse an age longer than 365,000 days: the expiry subtracts
the age from ``now()``, and the result must stay inside PostgreSQL's
timestamp range.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import ValidationError

from horsies._catalog.worker_manifest import PAUSED_AGE_MICROS_MAX
from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.models.retention import PAUSED_WORKFLOW_AGE_MAX, RetentionConfig

_ONE_MICROSECOND = timedelta(microseconds=1)


def _is_whole_microseconds(age: timedelta) -> bool:
    return timedelta(microseconds=age // _ONE_MICROSECOND) == age


def test_a_timedelta_has_a_resolution_of_one_microsecond() -> None:
    assert timedelta.resolution == _ONE_MICROSECOND
    assert timedelta(microseconds=0.4) == timedelta(0)
    assert timedelta(hours=1, microseconds=1.5) == timedelta(hours=1, microseconds=2)


@pytest.mark.parametrize(
    'given',
    [
        timedelta(hours=1),
        timedelta(hours=1, microseconds=0.4),
        timedelta(hours=1, microseconds=1.5),
        timedelta(days=1, microseconds=250),
    ],
)
def test_a_paused_age_from_a_timedelta_is_whole_microseconds(given: timedelta) -> None:
    config = RetentionConfig(paused_workflow_cancel_after=given)

    age = config.paused_workflow_cancel_after
    assert age is not None
    assert _is_whole_microseconds(age)
    assert age // _ONE_MICROSECOND == given // _ONE_MICROSECOND


@pytest.mark.parametrize(
    ('given', 'micros'),
    [
        (3_600.000_000_4, 3_600_000_000),
        (3_600.000_001_6, 3_600_000_002),
        ('PT1H0.000001S', 3_600_000_001),
        ('PT1H0.0000016S', 3_600_000_002),
    ],
)
def test_a_paused_age_read_from_seconds_or_text_is_whole_microseconds(
    given: float | str,
    micros: int,
) -> None:
    config = RetentionConfig.model_validate({'paused_workflow_cancel_after': given})

    age = config.paused_workflow_cancel_after
    assert age is not None
    assert _is_whole_microseconds(age)
    assert age // _ONE_MICROSECOND == micros


def test_a_paused_age_below_one_microsecond_is_zero_and_refused() -> None:
    with pytest.raises(ValidationError, match='must be a positive duration'):
        RetentionConfig.model_validate({'paused_workflow_cancel_after': 1e-7})


def test_the_bound_is_365000_days_and_the_core_bound() -> None:
    assert PAUSED_WORKFLOW_AGE_MAX == timedelta(days=365_000)
    assert PAUSED_WORKFLOW_AGE_MAX // _ONE_MICROSECOND == PAUSED_AGE_MICROS_MAX


def test_the_longest_paused_age_is_accepted() -> None:
    config = RetentionConfig(paused_workflow_cancel_after=PAUSED_WORKFLOW_AGE_MAX)

    assert config.paused_workflow_cancel_after == PAUSED_WORKFLOW_AGE_MAX


@pytest.mark.parametrize(
    'given',
    [
        PAUSED_WORKFLOW_AGE_MAX + _ONE_MICROSECOND,
        timedelta(days=365_001),
        timedelta.max,
    ],
    ids=['one-microsecond-above', 'one-day-above', 'timedelta-max'],
)
def test_a_paused_age_above_the_bound_is_refused_with_the_retention_code(
    given: timedelta,
) -> None:
    with pytest.raises(ConfigurationError) as raised:
        RetentionConfig(paused_workflow_cancel_after=given)

    assert raised.value.code is ErrorCode.CONFIG_INVALID_RETENTION
    assert 'longer than 365000 days' in raised.value.message
