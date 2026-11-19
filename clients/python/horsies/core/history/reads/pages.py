"""The anchor window of the history reads: a half-open, timezone-aware range
(``HistoryWindow``). The monitoring window (``MonitoringWindow``) bounds it
and hands it to the core's history reads.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class HistoryWindow:
    """Half-open `[lower, upper)` anchor range every statement scopes to."""

    lower: datetime
    upper: datetime

    def __post_init__(self) -> None:
        if self.lower.tzinfo is None or self.upper.tzinfo is None:
            raise ValueError('history window bounds must be timezone-aware')
        if self.lower >= self.upper:
            raise ValueError('history window bounds must be increasing')


