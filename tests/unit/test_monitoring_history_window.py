"""The monitoring window resolver: defaults, bounds, typed refusals."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from horsies.core.history.reads.pages import HistoryWindow
from horsies.monitoring.history_window import (
    MONITORING_WINDOW_DEFAULT,
    MONITORING_WINDOW_MAX,
    WindowRefused,
    resolve_monitoring_window,
)

pytestmark = [pytest.mark.unit]

NOW = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)


class TestRatifiedConstants:
    def test_default_and_max_are_the_ruled_values(self) -> None:
        assert MONITORING_WINDOW_DEFAULT == timedelta(hours=24)
        assert MONITORING_WINDOW_MAX == timedelta(days=30)


class TestResolution:
    def test_absent_bounds_take_the_default_ending_now(self) -> None:
        window = resolve_monitoring_window(since=None, until=None, now=NOW)
        assert isinstance(window, HistoryWindow)
        assert window.upper == NOW
        assert window.lower == NOW - MONITORING_WINDOW_DEFAULT

    def test_lone_since_runs_to_now(self) -> None:
        since = NOW - timedelta(hours=6)
        window = resolve_monitoring_window(since=since, until=None, now=NOW)
        assert isinstance(window, HistoryWindow)
        assert (window.lower, window.upper) == (since, NOW)

    def test_lone_until_covers_the_default_span(self) -> None:
        until = NOW - timedelta(hours=1)
        window = resolve_monitoring_window(since=None, until=until, now=NOW)
        assert isinstance(window, HistoryWindow)
        assert window.upper == until
        assert window.lower == until - MONITORING_WINDOW_DEFAULT

    def test_exactly_at_maximum_passes(self) -> None:
        since = NOW - MONITORING_WINDOW_MAX
        window = resolve_monitoring_window(since=since, until=NOW, now=NOW)
        assert isinstance(window, HistoryWindow)


class TestTypedRefusals:
    def test_over_maximum_names_the_bound_never_clamps(self) -> None:
        since = NOW - MONITORING_WINDOW_MAX - timedelta(seconds=1)
        refused = resolve_monitoring_window(since=since, until=NOW, now=NOW)
        assert isinstance(refused, WindowRefused)
        assert '30-day maximum' in refused.reason

    def test_non_increasing_and_naive_bounds_are_refused(self) -> None:
        refused = resolve_monitoring_window(since=NOW, until=NOW, now=NOW)
        assert isinstance(refused, WindowRefused)
        assert 'increasing' in refused.reason
        naive = datetime(2026, 8, 8, 11, 0)
        refused = resolve_monitoring_window(since=naive, until=None, now=NOW)
        assert isinstance(refused, WindowRefused)
        assert 'timezone-aware' in refused.reason
