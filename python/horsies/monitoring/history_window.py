"""The monitoring window: named bounds, typed refusal, one resolver.

History reads are anchor-scoped by ratified budget, but the
monitoring routes carry no time parameter — so the window is
resolved per request, by the core: optional `since`/`until` with a
server default when absent. The constants are MONITORING-OWNED (the reservation
precedent's shape, its constants not reused). A request over the
maximum is refused with the maximum named, never clamped — a caller
asking for ninety days learns the bound, not a silently truncated
answer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final, assert_never

from horsies._core import (
    MonitoringWindowRecord,
    WindowRefusedRecord,
    resolve_monitoring_window as core_resolve_monitoring_window,
)
from horsies.core.history.reads.pages import HistoryWindow

# The core's values (``MONITORING_WINDOW_DEFAULT`` and ``MONITORING_WINDOW_MAX``
# of ``horsies_core::monitoring``), restated until the generated constants of
# #1423 replace them.
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
    ``MONITORING_WINDOW_MAX``. The bounds of the window are in UTC.

    The core resolves the window (``horsies._core.resolve_monitoring_window``);
    the call is pure and starts no runtime.

    Raises:
        CoreError: the core of this process stopped, or the process was
            forked after the core started in its parent. Every core call of
            the process raises it.
        TypeError: ``now`` is naive, or a bound is not a ``datetime``.
    """
    match core_resolve_monitoring_window(since=since, until=until, now=now):
        case MonitoringWindowRecord(lower=lower, upper=upper):
            return MonitoringWindow(lower=lower, upper=upper)
        case WindowRefusedRecord(reason=reason):
            return WindowRefused(reason=reason)
        case _ as unreachable:
            assert_never(unreachable)
