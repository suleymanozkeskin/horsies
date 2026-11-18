"""The rules of a core boundary error: which errors a call answers, and
whether they may clear.

A core operation that ran and failed answers a failure record whose
``exception`` is the core's class of the cause (``horsies._core``:
``CoreServerError``, ``CoreConnectionError``, ``CoreContractViolation``,
``CorePreconditionFailed``), with its own ``retryable``. A ``CoreError``
raised at the call says that the core of this process could not run the
call:

- ``ForkedAfterCoreStart`` and ``CoreStopped`` are states of the process.
  Every core call raises them (``call_refusal_or_raise``); no call answers
  them as a result.
- ``TooManyPendingCalls``, ``CoreCallTimedOut`` and ``CoreStartFailed``
  (``CoreCallRefusal``) are the failure of one call: the call answers its
  failure code with the error as ``exception``. ``core_error_retryable``
  classifies them.
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

type CoreCallRefusal = TooManyPendingCalls | CoreCallTimedOut | CoreStartFailed
"""A core boundary error that one call answers as its own failure: the
event loop has too many core calls in flight, a sync call did not end
within its bound, or the core runtime could not start."""


def call_refusal_or_raise(exc: CoreError) -> CoreCallRefusal:
    """The core boundary error ``exc`` as the failure of one call. Every
    ``except CoreError`` of a core call passes its error through this.

    - ``TooManyPendingCalls``, ``CoreCallTimedOut``, ``CoreStartFailed``:
      returned; the caller answers its failure code with it.
    - ``ForkedAfterCoreStart`` (the process was forked without exec after
      the core started) and ``CoreStopped`` (a core background task
      panicked): raised unchanged. They are states of the process, not of
      one call: the core refuses every later call in the process, so no
      call answers them as a result.

    Raises:
        ForkedAfterCoreStart, CoreStopped: ``exc`` itself.
        AssertionError: ``exc`` is a ``CoreError`` class that the core does
            not define (a defect).
    """
    match exc:
        case TooManyPendingCalls() | CoreCallTimedOut() | CoreStartFailed():
            return exc
        case ForkedAfterCoreStart() | CoreStopped():
            raise exc
        case _:
            raise AssertionError(
                f'a core boundary error of a class the core does not define: '
                f'{type(exc).__name__}: {exc}',
            ) from exc


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
