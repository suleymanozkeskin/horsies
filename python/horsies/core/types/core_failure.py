"""The retry rule of a core boundary error.

A core operation that ran and failed answers a failure record whose
``exception`` is the core's class of the cause (``horsies._core``:
``CoreServerError``, ``CoreConnectionError``, ``CoreContractViolation``,
``CorePreconditionFailed``), with its own ``retryable``. A ``CoreError``
raised at the call says that the core of this process could not run the
call; ``core_error_retryable`` classifies it.
"""

from __future__ import annotations

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
