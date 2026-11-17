"""Dual-run of the window switch: Python's resolver against the core's.

Reference: ``tests/dualrun/reference/monitoring_window.py`` (the body before
the switch). Candidate: ``horsies.monitoring.resolve_monitoring_window``,
which the core resolves. The resolver is pure, so each case compares the two
answers for one input: the same window (equal instants) or the same refusal
text. The core gives its bounds in UTC.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import Final
from zoneinfo import ZoneInfo

import pytest

from horsies.monitoring import MonitoringWindow, WindowRefused, resolve_monitoring_window
from tests.dualrun.reference import monitoring_window as reference

pytestmark = [pytest.mark.unit]

_NOW: Final = datetime(2026, 11, 17, 12, 0, 0, 123456, tzinfo=UTC)
_PLUS_THREE: Final = timezone(timedelta(hours=3))
_NEW_YORK: Final = ZoneInfo('America/New_York')
_MAXIMUM: Final = timedelta(days=30)


@dataclass(frozen=True, slots=True)
class _Case:
    name: str
    since: datetime | None
    until: datetime | None


_CASES: Final = (
    _Case('default', None, None),
    _Case('lone since', _NOW - timedelta(hours=6), None),
    _Case('lone until', None, _NOW - timedelta(days=2)),
    _Case('both', _NOW - timedelta(days=3), _NOW - timedelta(days=1)),
    _Case('exactly the maximum', _NOW - _MAXIMUM, _NOW),
    _Case('one microsecond over the maximum', _NOW - _MAXIMUM - timedelta(microseconds=1), _NOW),
    _Case('empty', _NOW, _NOW),
    _Case('reversed', _NOW, _NOW - timedelta(hours=1)),
    _Case('lone since after now', _NOW + timedelta(seconds=1), None),
    _Case('naive since', datetime(2026, 11, 17, 11, 0), None),
    _Case('naive until', None, datetime(2026, 11, 17, 11, 0)),
    _Case('naive since and naive until', datetime(2026, 11, 17, 10, 0), datetime(2026, 11, 17, 11, 0)),
    _Case('fixed offset bounds', datetime(2026, 11, 17, 10, 0, tzinfo=_PLUS_THREE), datetime(2026, 11, 17, 14, 0, tzinfo=_PLUS_THREE)),
    _Case(
        'zone bounds across a daylight saving change',
        datetime(2026, 10, 31, 12, 0, tzinfo=_NEW_YORK),
        datetime(2026, 11, 2, 12, 0, tzinfo=_NEW_YORK),
    ),
)


def _utc_bounds(answer: MonitoringWindow | WindowRefused) -> bool:
    match answer:
        case MonitoringWindow(lower=lower, upper=upper):
            return lower.utcoffset() == timedelta(0) and upper.utcoffset() == timedelta(0)
        case WindowRefused():
            return True


@pytest.mark.parametrize('case', _CASES, ids=[case.name for case in _CASES])
def test_the_core_answers_what_python_answered(case: _Case) -> None:
    python_answer = reference.resolve_monitoring_window(since=case.since, until=case.until, now=_NOW)
    core_answer = resolve_monitoring_window(since=case.since, until=case.until, now=_NOW)

    assert core_answer == python_answer
    assert _utc_bounds(core_answer), core_answer


def test_a_span_in_one_zone_across_a_daylight_saving_change_counts_elapsed_time() -> None:
    """The one known difference, open in #1427: with both bounds in one zone,
    Python's ``datetime`` arithmetic counts wall-clock time; the core counts
    elapsed time. The web routes parse fixed offsets and never meet it."""
    until = datetime(2026, 11, 1, 12, 0, tzinfo=_NEW_YORK)
    python_default = reference.resolve_monitoring_window(since=None, until=until, now=_NOW)
    core_default = resolve_monitoring_window(since=None, until=until, now=_NOW)
    assert isinstance(python_default, MonitoringWindow)
    assert isinstance(core_default, MonitoringWindow)
    assert python_default.upper - python_default.lower.astimezone(UTC) == timedelta(hours=25)
    assert core_default.upper == until
    assert core_default.upper.astimezone(UTC) - core_default.lower == timedelta(hours=24)

    since = datetime(2026, 10, 10, 12, 0, tzinfo=_NEW_YORK)
    late_until = datetime(2026, 11, 9, 11, 30, tzinfo=_NEW_YORK)
    assert isinstance(
        reference.resolve_monitoring_window(since=since, until=late_until, now=_NOW),
        MonitoringWindow,
    )
    assert resolve_monitoring_window(since=since, until=late_until, now=_NOW) == WindowRefused(
        reason='the window exceeds the 30-day maximum',
    )


def test_without_now_both_read_the_clock_of_this_process() -> None:
    before = datetime.now(UTC)
    python_answer = reference.resolve_monitoring_window(since=None, until=None)
    core_answer = resolve_monitoring_window(since=None, until=None)
    after = datetime.now(UTC)

    match (python_answer, core_answer):
        case (MonitoringWindow() as python_window, MonitoringWindow() as core_window):
            pass
        case _:
            raise AssertionError(f'expected two windows: {python_answer!r}, {core_answer!r}')
    assert before <= core_window.upper <= after
    assert core_window.upper - core_window.lower == python_window.upper - python_window.lower


def test_the_comparison_sees_a_window_one_microsecond_off() -> None:
    """The comparison is exact to the microsecond: a shifted bound differs."""
    case = _CASES[3]
    core_answer = resolve_monitoring_window(since=case.since, until=case.until, now=_NOW)
    assert isinstance(core_answer, MonitoringWindow)
    shifted = MonitoringWindow(
        lower=core_answer.lower + timedelta(microseconds=1), upper=core_answer.upper,
    )

    assert shifted != reference.resolve_monitoring_window(since=case.since, until=case.until, now=_NOW)
