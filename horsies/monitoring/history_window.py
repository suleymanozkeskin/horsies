"""The monitoring window: named bounds, typed refusal, one resolver.

History reads are anchor-scoped by ratified budget, but the
monitoring routes carry no time parameter — so the window is
resolved here: optional `since`/`until` with a server default when
absent. The constants are MONITORING-OWNED (the reservation
precedent's shape, its constants not reused). A request over the
maximum is refused with the maximum named, never clamped — a caller
asking for ninety days learns the bound, not a silently truncated
answer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Final

from horsies.core.history.reads.pages import HistoryWindow

MONITORING_WINDOW_DEFAULT: Final = timedelta(hours=24)
MONITORING_WINDOW_MAX: Final = timedelta(days=30)


@dataclass(frozen=True, slots=True)
class MonitoringWindow:
    """The terminal-history window that every monitoring read takes.

    ``resolve_monitoring_window`` builds it: bounds timezone-aware,
    increasing, and at most ``MONITORING_WINDOW_MAX`` apart. The type
    carries that bound, so no read checks it again. A direct build with
    other bounds is a defect, and the constructor asserts it.
    """

    lower: datetime
    upper: datetime

    def __post_init__(self) -> None:
        assert self.lower.tzinfo is not None and self.upper.tzinfo is not None, (
            'monitoring window bounds must be timezone-aware'
        )
        assert self.lower < self.upper, (
            f'monitoring window must be increasing: {self.lower} >= {self.upper}'
        )
        assert self.upper - self.lower <= MONITORING_WINDOW_MAX, (
            f'monitoring window {self.upper - self.lower} exceeds '
            f'{MONITORING_WINDOW_MAX}'
        )

    @property
    def history(self) -> HistoryWindow:
        """The same bounds as the history-read window."""
        return HistoryWindow(lower=self.lower, upper=self.upper)


@dataclass(frozen=True, slots=True)
class WindowRefused:
    """The requested window is not servable; the reason names why."""

    reason: str


def resolve_monitoring_window(
    *,
    since: datetime | None,
    until: datetime | None,
    now: datetime | None = None,
) -> MonitoringWindow | WindowRefused:
    """Resolve the terminal-history window for one monitoring request.

    Absent bounds take the default: the last
    ``MONITORING_WINDOW_DEFAULT`` ending now. A lone ``since`` runs to
    now; a lone ``until`` covers the default span ending there. Bounds
    must be timezone-aware, increasing, and within
    ``MONITORING_WINDOW_MAX``.
    """
    anchor = now if now is not None else datetime.now(timezone.utc)
    if since is not None and since.tzinfo is None:
        return WindowRefused(reason='since must be timezone-aware')
    if until is not None and until.tzinfo is None:
        return WindowRefused(reason='until must be timezone-aware')
    upper = until if until is not None else anchor
    lower = since if since is not None else upper - MONITORING_WINDOW_DEFAULT
    if lower >= upper:
        return WindowRefused(
            reason='the window must be increasing (since < until)'
        )
    if upper - lower > MONITORING_WINDOW_MAX:
        maximum_days = MONITORING_WINDOW_MAX.days
        return WindowRefused(
            reason=(
                f'the window exceeds the {maximum_days}-day maximum'
            )
        )
    return MonitoringWindow(lower=lower, upper=upper)
