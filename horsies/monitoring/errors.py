"""Typed error payload for the monitoring query API.

A monitoring read fails when the database operation fails, or when the
broker already serves its limit of open event loops. An absent
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
    # The broker already holds resource sets for its limit of open event
    # loops, and this call ran on a new one. Nothing ran. It clears when one
    # of those loops closes.
    EVENT_LOOP_LIMIT = 'EVENT_LOOP_LIMIT'


@dataclass(frozen=True, slots=True)
class MonitoringQueryError:
    """Error payload carried inside ``Err(...)`` for monitoring reads.

    Fields:
        code: which failure category applies
        message: which query failed, plus the cause
        retryable: whether the failure is a transient connection error
        exception: the exception that caused it; None for
            ``EVENT_LOOP_LIMIT``, where no query ran
    """

    code: MonitoringQueryErrorCode
    message: str
    retryable: bool
    exception: BaseException | None = None


type MonitoringResult[T] = Result[T, MonitoringQueryError]
