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
from enum import Enum
from typing import TYPE_CHECKING, ClassVar

from horsies.core.types.result import Result


if TYPE_CHECKING:
    from horsies.core.brokers.resources import EventLoopLimit


class MonitoringQueryErrorCode(str, Enum):
    """Failure category for a monitoring read.

    Each member names one variant of ``MonitoringQueryError``.
    """

    DB_OPERATION_FAILED = 'DB_OPERATION_FAILED'
    # The broker already holds resource sets for its limit of open event
    # loops, and this call ran on a new one. Nothing ran. It clears when one
    # of those loops closes.
    EVENT_LOOP_LIMIT = 'EVENT_LOOP_LIMIT'
    # Stored history data does not decode. A repeat gives the same result
    # until the stored row or the server changes.
    HISTORY_CONTRACT_VIOLATION = 'HISTORY_CONTRACT_VIOLATION'


@dataclass(slots=True, frozen=True)
class MonitoringQueryDbOperationFailed:
    """The query failed with a database or driver error. Nothing changed.

    ``retryable`` is the driver classification of the cause: ``True`` when
    the condition may clear, for example a lost connection. ``exception``
    is the ``SQLAlchemyError``.
    """

    code: ClassVar[MonitoringQueryErrorCode] = MonitoringQueryErrorCode.DB_OPERATION_FAILED
    message: str = field(repr=False)
    retryable: bool
    exception: BaseException = field(repr=False)


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


@dataclass(slots=True, frozen=True)
class MonitoringQueryHistoryContractViolation:
    """Stored history data does not decode. Nothing changed.

    ``exception`` is the ``HistoryContractError``: for example a history
    status that is not a ``TaskStatus`` member, an attempt snapshot that
    does not decode, or an EXPLAIN payload that the row estimate cannot
    read. A repeat gives the same result until the stored row or the
    server changes.
    """

    code: ClassVar[MonitoringQueryErrorCode] = (
        MonitoringQueryErrorCode.HISTORY_CONTRACT_VIOLATION
    )
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    exception: BaseException = field(repr=False)


type MonitoringQueryError = (
    MonitoringQueryDbOperationFailed
    | MonitoringQueryEventLoopLimit
    | MonitoringQueryHistoryContractViolation
)
"""Every failure of a monitoring query: one variant per ``MonitoringQueryErrorCode``."""

type MonitoringResult[T] = Result[T, MonitoringQueryError]
