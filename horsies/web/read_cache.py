"""A short-lived shared cache for the task status cards.

Each ``/api/tasks/stats`` poll runs an exact status aggregation over the
live table and the history window. Monitoring clients poll it on a timer,
so its cost grows with the window and with the number of open dashboards.
The cache keeps each scope's last successful answer for a fixed time and
lets one load serve every concurrent miss of the same scope.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from horsies.core.types.result import Ok, is_ok
from horsies.monitoring import ErrorCategory, MonitoringResult, StatusCount

TASK_STATS_TTL_SECONDS: Final = 10.0
"""A cached status count is at most this many seconds old."""

TASK_STATS_MAX_SCOPES: Final = 256
"""At most this many scopes are cached at once."""


@dataclass(frozen=True, slots=True)
class TaskStatsScope:
    """The request parameters of one stats query, exactly as received.

    ``since`` and ``until`` stay unresolved: an open window resolves
    against the current time on each load, and the key names the request,
    not the instants.
    """

    since: datetime | None
    until: datetime | None
    task_names: tuple[str, ...]
    queues: tuple[str, ...]
    workers: tuple[str, ...]
    error_codes: tuple[str, ...]
    error_categories: tuple[ErrorCategory, ...]
    retried_only: bool


@dataclass(frozen=True, slots=True)
class _CachedCounts:
    counts: list[StatusCount]
    expires_at: float


type StatsLoad = Callable[[], Awaitable[MonitoringResult[list[StatusCount]]]]


class TaskStatsCache:
    """Status counts per scope, kept for ``ttl_seconds`` after a load.

    Only successful loads are kept; a failed load is returned to every
    caller that waited on it and is not cached. Expired entries are
    removed before each insert, and when ``max_scopes`` entries remain the
    one that expires first is evicted.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float = TASK_STATS_TTL_SECONDS,
        max_scopes: int = TASK_STATS_MAX_SCOPES,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError(f'ttl_seconds must be positive, got {ttl_seconds}')
        if max_scopes <= 0:
            raise ValueError(f'max_scopes must be positive, got {max_scopes}')
        self._ttl_seconds = ttl_seconds
        self._max_scopes = max_scopes
        self._clock = clock
        self._entries: dict[TaskStatsScope, _CachedCounts] = {}
        self._loads: dict[
            TaskStatsScope,
            asyncio.Task[MonitoringResult[list[StatusCount]]],
        ] = {}

    async def get(
        self,
        scope: TaskStatsScope,
        load: StatsLoad,
    ) -> MonitoringResult[list[StatusCount]]:
        """The scope's cached counts, or the result of one shared load."""
        cached = self._entries.get(scope)
        if cached is not None and cached.expires_at > self._clock():
            return _ok_copy(cached.counts)
        pending = self._loads.get(scope)
        if pending is None:
            pending = asyncio.create_task(self._load(scope, load))
            self._loads[scope] = pending
        # A cancelled caller leaves the shared load running for the others.
        return await asyncio.shield(pending)

    async def _load(
        self,
        scope: TaskStatsScope,
        load: StatsLoad,
    ) -> MonitoringResult[list[StatusCount]]:
        try:
            result = await load()
            if is_ok(result):
                self._store(scope, result.ok_value)
            return result
        finally:
            del self._loads[scope]

    def _store(self, scope: TaskStatsScope, counts: list[StatusCount]) -> None:
        now = self._clock()
        for expired in [
            key for key, entry in self._entries.items() if entry.expires_at <= now
        ]:
            del self._entries[expired]
        self._entries.pop(scope, None)
        while len(self._entries) >= self._max_scopes:
            first_to_expire = min(
                self._entries, key=lambda key: self._entries[key].expires_at,
            )
            del self._entries[first_to_expire]
        self._entries[scope] = _CachedCounts(
            counts=list(counts), expires_at=now + self._ttl_seconds,
        )

    def __len__(self) -> int:
        return len(self._entries)


def _ok_copy(counts: list[StatusCount]) -> MonitoringResult[list[StatusCount]]:
    return Ok(list(counts))
