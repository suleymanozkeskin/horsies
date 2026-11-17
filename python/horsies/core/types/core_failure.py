"""The exception that stands for a failure record of the core.

The core reports an expected failure of an operation as a record (a message
with its subject and cause, and a retryable flag), not as an exception. Where
the Python facade must give an exception, it builds a ``CoreOperationFailed``
from the record:

- the ``exception`` field of a broker error, which holds a ``BaseException``;
- the raise of a facade whose Python caller raised the driver's exception
  (the worker's claim release).

``CoreLockNotAvailable`` is the failure record of a lock refused at once
(SQLSTATE 55P03), where a caller reads that code.

The core exception classes of #796 (a database failure with its SQLSTATE, a
connection failure without one) replace both when the core reports them.
"""

from __future__ import annotations

from typing import Final

from horsies._core import (
    CoreCallTimedOut,
    CoreError,
    CoreStartFailed,
    CoreStopped,
    ForkedAfterCoreStart,
    TooManyPendingCalls,
)


def core_error_retryable(exc: CoreError) -> bool:
    """Whether the condition of a core boundary error may clear in this process. Pure.

    - ``TooManyPendingCalls``: yes, the calls in flight end.
    - ``CoreCallTimedOut``: yes, a later call can end within its bound.
    - ``CoreStopped``, ``CoreStartFailed``, ``ForkedAfterCoreStart``: no, the
      state stays for the life of the process.
    """
    match exc:
        case TooManyPendingCalls() | CoreCallTimedOut():
            return True
        case CoreStopped() | CoreStartFailed() | ForkedAfterCoreStart():
            return False
        case _:
            return False


class CoreOperationFailed(Exception):
    """A core operation reported a failure record.

    - ``subject``: the operation, for example
      ``'PostgresBroker.requeue_stale_claimed'``.
    - ``message``: the message of the record, with the subject and the cause
      that the core gave.
    - ``retryable``: the core's classification of the cause: ``True`` when
      the condition may clear, for example a lost connection.

    ``args`` is ``(subject, message, retryable)``.
    """

    subject: str
    message: str
    retryable: bool

    def __init__(self, subject: str, message: str, retryable: bool) -> None:
        super().__init__(subject, message, retryable)
        self.subject = subject
        self.message = message
        self.retryable = retryable

    def __str__(self) -> str:
        return f'{self.subject}: {self.message}'


class CoreLockNotAvailable(CoreOperationFailed):
    """A core operation was refused at once: a lock that it takes without
    waiting was held by another session (SQLSTATE 55P03). Nothing was
    written.

    ``sqlstate`` is ``'55P03'``, as on the driver exception of the statement
    that Python sent before the core ran the operation, so a reader of the
    SQLSTATE of a broker error's cause keeps its answer.
    """

    sqlstate: Final = '55P03'
