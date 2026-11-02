"""Short-lived shared caches for the polled task reads.

The status cards, the filter facets and the breakdown each scan the history
window on a load. Each open dashboard polls them on a timer, so without a
cache their cost grows with the window and with the number of dashboards.

A cache keeps each scope's last successful answer for a fixed time, and one
load serves every concurrent miss of that scope. The time is shorter than the
poll of a read, so one dashboard alone misses on each poll: these caches serve
concurrent dashboards, and a read's own statement is what it costs one user.
"""

from __future__ import annotations

import asyncio
import copy
import time
from collections.abc import Awaitable, Callable, Hashable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Final

from horsies.core.types.result import Ok, is_ok
from horsies.core.types.status import TaskStatus
from horsies.monitoring import (
    Breakdown,
    ErrorCategory,
    Facets,
    MonitoringResult,
    StatusCount,
)

READ_TTL_SECONDS: Final = 10.0
"""A cached answer is at most this many seconds old."""

READ_MAX_SCOPES: Final = 256
"""At most this many scopes are cached at once, per cache."""


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
class TaskFacetsScope:
    """The request parameters of one facets query, exactly as received."""

    since: datetime | None
    until: datetime | None
    statuses: tuple[TaskStatus, ...]
    error_categories: tuple[ErrorCategory, ...]
    retried_only: bool


@dataclass(frozen=True, slots=True)
class TaskBreakdownScope:
    """The request parameters of one breakdown query, exactly as received."""

    since: datetime | None
    until: datetime | None
    group_by: str
    statuses: tuple[TaskStatus, ...]
    task_names: tuple[str, ...]
    queues: tuple[str, ...]
    workers: tuple[str, ...]
    error_codes: tuple[str, ...]
    error_categories: tuple[ErrorCategory, ...]
    retried_only: bool
    limit: int


@dataclass(frozen=True, slots=True)
class _Cached[Value]:
    value: Value
    expires_at: float


class ReadCache[Scope: Hashable, Value]:
    """Answers per scope, kept for ``ttl_seconds`` after a load.

    Only successful loads are kept; a failed load is returned to every
    caller that waited on it and is not cached. Expired entries are
    removed before each insert, and when ``max_scopes`` entries remain the
    one that expires first is evicted. A caller gets its own copy.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float = READ_TTL_SECONDS,
        max_scopes: int = READ_MAX_SCOPES,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError(f'ttl_seconds must be positive, got {ttl_seconds}')
        if max_scopes <= 0:
            raise ValueError(f'max_scopes must be positive, got {max_scopes}')
        self._ttl_seconds = ttl_seconds
        self._max_scopes = max_scopes
        self._clock = clock
        self._entries: dict[Scope, _Cached[Value]] = {}
        self._loads: dict[Scope, asyncio.Task[MonitoringResult[Value]]] = {}

    async def get(
        self,
        scope: Scope,
        load: Callable[[], Awaitable[MonitoringResult[Value]]],
    ) -> MonitoringResult[Value]:
        """The scope's cached answer, or the result of one shared load."""
        cached = self._entries.get(scope)
        if cached is not None and cached.expires_at > self._clock():
            return Ok(copy.deepcopy(cached.value))
        pending = self._loads.get(scope)
        if pending is None:
            pending = asyncio.create_task(self._load(scope, load))
            self._loads[scope] = pending
        # A cancelled caller leaves the shared load running for the others.
        return await asyncio.shield(pending)

    async def _load(
        self,
        scope: Scope,
        load: Callable[[], Awaitable[MonitoringResult[Value]]],
    ) -> MonitoringResult[Value]:
        try:
            result = await load()
            if is_ok(result):
                self._store(scope, copy.deepcopy(result.ok_value))
            return result
        finally:
            del self._loads[scope]

    def _store(self, scope: Scope, value: Value) -> None:
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
        self._entries[scope] = _Cached(
            value=value, expires_at=now + self._ttl_seconds,
        )

    def __len__(self) -> int:
        return len(self._entries)


def _stats_cache() -> ReadCache[TaskStatsScope, list[StatusCount]]:
    return ReadCache()


def _facets_cache() -> ReadCache[TaskFacetsScope, Facets]:
    return ReadCache()


def _breakdown_cache() -> ReadCache[TaskBreakdownScope, Breakdown]:
    return ReadCache()


@dataclass(frozen=True, slots=True)
class TaskReadCaches:
    """One cache per polled task read, so that each keeps its own value type."""

    stats: ReadCache[TaskStatsScope, list[StatusCount]] = field(
        default_factory=_stats_cache,
    )
    facets: ReadCache[TaskFacetsScope, Facets] = field(
        default_factory=_facets_cache,
    )
    breakdown: ReadCache[TaskBreakdownScope, Breakdown] = field(
        default_factory=_breakdown_cache,
    )
