"""Retention periods: the one rule for how long a finite class keeps a record.

A retention class is identified by its period. The class key is a function of
the period, so a key always means the same duration, and two configurations
can never disagree about a key's duration.

History partitions are daily: a row survives between the period and the
period plus one day. A period is therefore whole days, and two periods that
differ by less than a day would be two classes with the same behaviour.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Final, NewType

from horsies.core.types.result import Err, Ok, Result

MIN_RETENTION_DAYS: Final = 1
MAX_RETENTION_DAYS: Final = 1825
"""Five years, the bound of ``terminal_record_retention_hours``.

One class at this period keeps about 1,830 daily partitions, each with two
indexes."""

PERIOD_CLASS_PREFIX: Final = 'standard_'
"""30 days gives ``standard_30d``, the key of the default class."""

RetentionDays = NewType('RetentionDays', int)
"""Whole days within the bounds. ``parse_retention_period`` builds it.

A NewType is a static marker: anyone can call ``RetentionDays(n)``, and
pyright makes the convention hold for typed callers."""

RetentionClassKey = NewType('RetentionClassKey', str)
"""The key of a finite period class. ``period_class_key`` builds it.

The enqueue paths take this type, so a raw string key is a type error at
a typed call site, not a row that has no partition later. Like
``RetentionDays``, it is static only: ``RetentionClassKey('x')`` runs."""


@dataclass(frozen=True, slots=True)
class PeriodRefused:
    """A finite period outside the rule; ``reason`` names the rule it breaks."""

    reason: str


def parse_retention_period(
    period: timedelta,
) -> Result[RetentionDays, PeriodRefused]:
    """The number of days of a finite period, or why it is not one.

    Refuses a period that is not whole days, and one outside
    ``MIN_RETENTION_DAYS``..``MAX_RETENTION_DAYS``. It never rounds: a
    rounded period would keep records for a time the caller did not ask for.
    """
    match period.seconds, period.microseconds:
        case 0, 0:
            pass
        case _:
            return Err(PeriodRefused(
                f'{period} is not a whole number of days; history '
                'partitions are daily, so a retention period is whole days'
            ))
    match period.days:
        case days if days < MIN_RETENTION_DAYS:
            return Err(PeriodRefused(
                f'{period} is shorter than {MIN_RETENTION_DAYS} day'
            ))
        case days if days > MAX_RETENTION_DAYS:
            return Err(PeriodRefused(
                f'{period} is longer than {MAX_RETENTION_DAYS} days'
            ))
        case days:
            return Ok(RetentionDays(days))


def period_class_key(days: RetentionDays) -> RetentionClassKey:
    """The class key of a finite period of ``days`` whole days.

    ``days`` comes from ``parse_retention_period``; a value out of bounds is
    a defect.
    """
    assert MIN_RETENTION_DAYS <= days <= MAX_RETENTION_DAYS, (
        f'retention period of {days} days is out of bounds'
    )
    return RetentionClassKey(f'{PERIOD_CLASS_PREFIX}{days}d')
