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

from horsies._catalog.monitoring import (
    MonitoringQueryErrorCode as MonitoringQueryErrorCode,
)
from horsies._catalog.monitoring_errors import (
    MonitoringQueryCoreError as MonitoringQueryCoreError,
    MonitoringQueryDatabaseOperationFailed as MonitoringQueryDatabaseOperationFailed,
    MonitoringQueryHistoryContractViolation as MonitoringQueryHistoryContractViolation,
)
from horsies.core.types.result import Result


type MonitoringQueryError = MonitoringQueryCoreError
"""Every failure of a monitoring query: one variant per ``MonitoringQueryErrorCode``,
each a class of the core's catalog."""

type MonitoringResult[T] = Result[T, MonitoringQueryError]
