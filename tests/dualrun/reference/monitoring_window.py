"""The monitoring window resolver before the monitoring switch: Python's body.

The dual-run reference of ``horsies.monitoring.history_window.resolve_monitoring_window``,
which the core resolves since the switch. It builds the same public types.
"""

from __future__ import annotations

from datetime import datetime, timezone

from horsies.monitoring.history_window import (
    MONITORING_WINDOW_DEFAULT,
    MONITORING_WINDOW_MAX,
    MonitoringWindow,
    WindowRefused,
)


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
