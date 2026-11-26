"""Typed error types for PostgresBroker operations.

Result propagation policy
-------------------------
The library uses two distinct error systems:

1. ``ReadResult[T, TaskError]`` -- user-facing task reads. Returned by
   ``get_result_async`` / ``get_result`` and the handles. Its outcome
   (``Succeeded`` / ``Failed``) carries domain error codes
   (``BuiltInTaskCode`` families) and user-defined error codes; a read
   failure is a ``ReadError`` class.

2. ``BrokerResult[T]`` (this module) -- infrastructure operation outcomes.
   Returned by broker methods (``get_task_info_async``, ``close_async``,
   etc.).  The error side is ``BrokerOperationError``: a union with one
   frozen variant per state. Every variant has ``code`` (a
   ``BrokerErrorCode`` member), ``message`` and ``retryable``. Callers
   match on the variant. A method with fewer failures returns a subset:
   ``enqueue_async`` returns ``Result[UUID, BrokerEnqueueError]``,
   ``enqueue`` adds ``BrokerSyncCallError``, and
   ``ensure_schema_initialized`` returns
   ``Result[None, BrokerOperationFailed]``.

Where Result stops and exceptions take over (the "stop line"):

* **Broker layer** -- returns ``BrokerResult``.  Never raises for
  operational failures.  ``ForkedAfterCoreStart`` and ``CoreStopped``
  (states of the process, ``call_refusal_or_raise``),
  ``asyncio.CancelledError`` and programmer defects propagate, from the
  sync facades as from the async methods.

* **Internal call sites** -- handle ``BrokerResult`` with real decisions:
  retry on transient errors, save partial progress, track consecutive
  failures, branch on ``retryable``.

* **Process boundaries** (CLI startup, scheduler startup) -- convert
  ``Err`` to an exception and let the process crash.  These are fatal
  startup failures where no further recovery is possible within the
  process.  This is intentional, not a gap: the orchestration layer
  (CLI) already wraps these in retry loops
  (``_ensure_schema_with_retry``) *before* the service starts.
  ``broker_error_cause()`` gives the exception to raise ``from``, or
  ``NoCause()``.

* **Workflow start** (``start_workflow_async``, ``start_workflow``) --
  returns ``WorkflowStartResult`` (see ``start_types.py``).  A schema
  init ``BrokerOperationFailed`` becomes ``WorkflowStartEnqueueFailed``
  with the ``retryable`` flag and the cause of the broker error; any
  other exception propagates.

* **User-facing API** (``TaskHandle.info`` / ``info_async``) -- returns
  ``BrokerResult`` directly.  The user decides how to handle
  infrastructure errors for their use case.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar, assert_never
from uuid import UUID

from horsies._catalog.broker import BrokerErrorCode as BrokerErrorCode
from horsies._catalog.broker_errors import (
    BrokerAsyncContext as BrokerAsyncContext,
    BrokerCoreEnqueueError as BrokerCoreEnqueueError,
    BrokerCoreError as BrokerCoreError,
    BrokerEnqueueFailed as BrokerEnqueueFailed,
    BrokerIdempotencyKeyConflict as BrokerIdempotencyKeyConflict,
    BrokerInvalidInput as BrokerInvalidInput,
    BrokerInvalidJsonPayload as BrokerInvalidJsonPayload,
    BrokerInvalidTaskOptions as BrokerInvalidTaskOptions,
    BrokerListenerDisabled as BrokerListenerDisabled,
    BrokerOperationFailed as BrokerOperationFailed,
    BrokerOperationFailedCode as BrokerOperationFailedCode,
    BrokerPayloadMismatch as BrokerPayloadMismatch,
    BrokerWaitInsideTask as BrokerWaitInsideTask,
)
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Result


if TYPE_CHECKING:
    from horsies.core.codec.typed import Json
    from horsies.core.types.status import TaskStatus


@dataclass(slots=True, frozen=True)
class BrokerNotAttached:
    """The task handle has no broker, so it cannot read task info. Nothing ran."""

    code: ClassVar[BrokerErrorCode] = BrokerErrorCode.NO_BROKER
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)


@dataclass(slots=True, frozen=True)
class BrokerNoTypeAvailable:
    """A typed result was requested, but this process has no registered type for the task.

    The stored result is unchanged. The read succeeds in a process that
    registers the task.
    """

    code: ClassVar[BrokerErrorCode] = BrokerErrorCode.NO_TYPE_AVAILABLE
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)


type BrokerOperationError = (
    BrokerCoreError
    | BrokerNotAttached
    | BrokerNoTypeAvailable
)
"""Every failure of a broker operation: one variant per state."""


type BrokerResult[T] = Result[T, BrokerOperationError]


type BrokerEnqueueError = BrokerCoreEnqueueError
"""Every failure of ``PostgresBroker.enqueue_async``."""


type BrokerSyncCallError = BrokerAsyncContext
"""The failure that a sync facade adds to its async method: it was called
inside a running event loop. Nothing ran."""


def broker_error_cause(error: BrokerOperationError) -> BaseException | NoCause:
    """The exception that caused ``error``, or ``NoCause()`` when no exception did.

    Use it before ``raise ... from``, an ``exc_info=`` or an error tracker
    call. It does not change ``error``.
    """
    match error:
        case BrokerOperationFailed(exception=cause):
            return cause
        case (
            BrokerEnqueueFailed(exception=cause)
            | BrokerInvalidJsonPayload(exception=cause)
            | BrokerInvalidTaskOptions(exception=cause)
            | BrokerInvalidInput(exception=cause)
        ):
            return cause
        case (
            BrokerPayloadMismatch()
            | BrokerIdempotencyKeyConflict()
            | BrokerNotAttached()
            | BrokerNoTypeAvailable()
            | BrokerAsyncContext()
            | BrokerListenerDisabled()
            | BrokerWaitInsideTask()
        ):
            return NoCause()
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(slots=True, frozen=True)
class RawResultRecord:
    """Raw broker-level fetch of a task's stored result.

    Returned by ``PostgresBroker.get_raw_result_record_async`` and its
    sync wrapper. The broker stays infrastructure: no typed decoding, no
    user-task imports. Callers (typed user APIs at the handle/app layer)
    compose typed decode on top by deriving ``ok_type`` from
    ``record.task_name`` and calling
    ``decode_task_result(record.raw_result, ok_type)``.

    Fields:
        task_id: Task identifier.
        task_name: Registered task name; used by app/handle to look up
            ``ok_type`` from the local task catalog.
        status: Current row status. Lets callers map non-terminal
            outcomes (``CANCELLED``, ``WAIT_TIMEOUT``) into the
            ``ReadResult`` shape they expose.
        raw_result: The decoded JSON envelope from the result column,
            or ``None`` for rows that haven't reached a terminal state
            yet, were cancelled before completion, or were terminal but
            stored no payload.
    """

    task_id: UUID
    task_name: str
    status: 'TaskStatus'
    raw_result: 'dict[str, Json] | None'
