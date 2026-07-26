"""Typed error payload for the monitoring query API.

A monitoring read fails only when the database operation fails. An absent
row is not an error: those queries return ``Ok(None)`` so callers branch on
data, not on exceptions.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from horsies.core.types.result import Result


class MonitoringQueryErrorCode(str, Enum):
    """Failure category for a monitoring read."""

    DB_OPERATION_FAILED = 'DB_OPERATION_FAILED'


@dataclass(frozen=True, slots=True)
class MonitoringQueryError:
    """Error payload carried inside ``Err(...)`` for monitoring reads.

    Fields:
        code: which failure category applies
        message: which query failed, plus the driver's description
        retryable: whether the failure is a transient connection error
        exception: the ``SQLAlchemyError`` that caused it
    """

    code: MonitoringQueryErrorCode
    message: str
    retryable: bool
    exception: BaseException


type MonitoringResult[T] = Result[T, MonitoringQueryError]
