"""Typed errors of the monitoring query API.

Every monitoring query returns ``MonitoringResult[T]``. The error side is
``MonitoringQueryError``: a union with one frozen variant per
``MonitoringQueryErrorCode`` member. Every variant has ``code``, ``message``
and ``retryable``. Callers match on the variant.

A monitoring query is a read: no failure changes stored data. An absent
row is not an error: those queries return ``Ok(None)``, so callers branch
on data, not on exceptions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

from horsies._catalog.monitoring import (
    MonitoringQueryErrorCode as MonitoringQueryErrorCode,
)
from horsies._catalog.monitoring_errors import (
    MonitoringQueryCoreError as MonitoringQueryCoreError,
    MonitoringQueryDbOperationFailed as MonitoringQueryDbOperationFailed,
    MonitoringQueryHistoryContractViolation as MonitoringQueryHistoryContractViolation,
)
from horsies.core.types.result import Result


if TYPE_CHECKING:
    from horsies.core.brokers.resources import EventLoopLimit


@dataclass(slots=True, frozen=True)
class MonitoringQueryEventLoopLimit:
    """The broker holds resource sets for its limit of open event loops. Nothing ran.

    ``loop_limit`` names the limit and the threads of the held loops. A
    query from this loop returns the same error until one of the held
    loops closes.
    """

    code: ClassVar[MonitoringQueryErrorCode] = MonitoringQueryErrorCode.EVENT_LOOP_LIMIT
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    loop_limit: EventLoopLimit


type MonitoringQueryError = MonitoringQueryCoreError | MonitoringQueryEventLoopLimit
"""Every failure of a monitoring query: one variant per ``MonitoringQueryErrorCode``."""

type MonitoringResult[T] = Result[T, MonitoringQueryError]
