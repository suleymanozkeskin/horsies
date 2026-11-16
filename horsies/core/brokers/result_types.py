"""Typed error types for PostgresBroker operations.

Result propagation policy
-------------------------
The library uses two distinct error systems:

1. ``TaskResult[T, TaskError]`` -- user-facing task execution outcomes.
   Returned by ``get_result_async`` / ``get_result``.  Carries domain
   error codes (``BuiltInTaskCode`` families) and user-defined error codes.

2. ``BrokerResult[T]`` (this module) -- infrastructure operation outcomes.
   Returned by broker methods (``get_task_info_async``, ``close_async``,
   etc.).  The error side is ``BrokerOperationError``: a union with one
   frozen variant per state. Every variant has ``code`` (a
   ``BrokerErrorCode`` member), ``message`` and ``retryable``. Callers
   match on the variant. A method with fewer failures returns a subset:
   ``enqueue_async`` returns ``Result[str, BrokerEnqueueError]``,
   ``enqueue`` adds ``BrokerSyncCallError``, and
   ``ensure_schema_initialized`` returns
   ``Result[None, BrokerOperationFailed | BrokerEventLoopLimit]``.

Where Result stops and exceptions take over (the "stop line"):

* **Broker layer** -- returns ``BrokerResult``.  Never raises for
  operational failures.  ``asyncio.CancelledError`` and programmer
  defects propagate, from the sync facades as from the async methods.

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
  with the ``retryable`` flag and the cause of the broker error; a
  ``BrokerEventLoopLimit`` becomes ``WorkflowStartEventLoopLimit``.  A
  sync runner failure maps to ``WorkflowStartLoopRunnerFailed``; any other
  exception propagates.

* **User-facing API** (``TaskHandle.info`` / ``info_async``) -- returns
  ``BrokerResult`` directly.  The user decides how to handle
  infrastructure errors for their use case.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar, Literal, assert_never

from horsies._catalog.broker import BrokerErrorCode as BrokerErrorCode
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Result


if TYPE_CHECKING:
    from horsies.core.brokers.resources import EventLoopLimit
    from horsies.core.codec.typed import Json
    from horsies.core.types.status import TaskStatus


type BrokerOperationFailedCode = Literal[
    BrokerErrorCode.SCHEMA_INIT_FAILED,
    BrokerErrorCode.TASK_INFO_QUERY_FAILED,
    BrokerErrorCode.MONITORING_QUERY_FAILED,
    BrokerErrorCode.DB_PING_FAILED,
    BrokerErrorCode.WORKER_PING_FAILED,
    BrokerErrorCode.CLEANUP_FAILED,
    BrokerErrorCode.CLOSE_FAILED,
    BrokerErrorCode.LISTENER_START_FAILED,
    BrokerErrorCode.LISTENER_SUBSCRIBE_FAILED,
]
"""The operation codes of ``BrokerOperationFailed``."""


@dataclass(slots=True, frozen=True)
class BrokerOperationFailed:
    """A broker operation failed with a database, driver or connection error.

    ``code`` names the operation. ``retryable`` is the driver classification
    of the cause: ``True`` when the condition may clear, for example a lost
    connection. A read changed nothing. A cleanup may have finished part of
    its rows; no write of the library's own data is left half done.
    """

    code: BrokerOperationFailedCode
    message: str = field(repr=False)
    retryable: bool
    exception: BaseException = field(repr=False)


@dataclass(slots=True, frozen=True)
class BrokerEnqueueFailed:
    """The enqueue write failed or could not be confirmed. The row may or may not exist.

    ``retryable`` is the driver classification of the cause. An enqueue
    again with the same task id and payload is safe: the broker returns the
    existing task for a duplicate. ``exception`` is ``NoCause()`` when the
    broker found the failure by a data check (the conflicting row was gone).
    """

    code: ClassVar[BrokerErrorCode] = BrokerErrorCode.ENQUEUE_FAILED
    message: str = field(repr=False)
    retryable: bool
    exception: BaseException | NoCause = field(repr=False)


@dataclass(slots=True, frozen=True)
class BrokerPayloadMismatch:
    """The task id holds a different command or ``sent_at``, or its live row a
    different idempotency key. Nothing was written. The same enqueue fails
    again.
    """

    code: ClassVar[BrokerErrorCode] = BrokerErrorCode.PAYLOAD_MISMATCH
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)


@dataclass(slots=True, frozen=True)
class BrokerIdempotencyKeyConflict:
    """The idempotency key is reserved by another task with a different command.

    Nothing was written. ``owner_task_id`` names the task that holds the
    key. While the owner task is live, the reservation stays. After the
    owner task finishes, the reservation ends when the reservation window
    has passed (24 h by default, at most 30 days); an enqueue with the same
    key is then accepted.
    """

    code: ClassVar[BrokerErrorCode] = BrokerErrorCode.IDEMPOTENCY_KEY_CONFLICT
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    owner_task_id: str


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


@dataclass(slots=True, frozen=True)
class BrokerInvalidJsonPayload:
    """Stored JSON failed strict validation. The stored data is unchanged.

    The same read fails again. ``exception`` is ``NoCause()`` when a shape
    check found the failure without a Python exception.
    """

    code: ClassVar[BrokerErrorCode] = BrokerErrorCode.INVALID_JSON_PAYLOAD
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    exception: BaseException | NoCause = field(repr=False)


@dataclass(slots=True, frozen=True)
class BrokerInvalidTaskOptions:
    """The task options are not valid JSON or carry a retry limit that cannot be stored.

    Nothing was written. The same options are refused again. ``exception``
    is ``NoCause()`` when a check refused the options without a Python
    exception.
    """

    code: ClassVar[BrokerErrorCode] = BrokerErrorCode.INVALID_TASK_OPTIONS
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    exception: BaseException | NoCause = field(repr=False)


@dataclass(slots=True, frozen=True)
class BrokerInvalidInput:
    """An argument of the call was refused before anything ran. Nothing was written.

    The message names the argument and the rule. The same arguments are
    refused again. ``exception`` is ``NoCause()`` when a check refused the
    argument without a Python exception.
    """

    code: ClassVar[BrokerErrorCode] = BrokerErrorCode.INVALID_INPUT
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    exception: BaseException | NoCause = field(repr=False)


@dataclass(slots=True, frozen=True)
class BrokerEventLoopLimit:
    """The broker holds resource sets for its limit of open event loops. Nothing ran.

    ``loop_limit`` names the limit and the threads of the held loops. A call
    from this loop returns the same error until one of the held loops closes.
    """

    code: ClassVar[BrokerErrorCode] = BrokerErrorCode.EVENT_LOOP_LIMIT
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    loop_limit: EventLoopLimit


@dataclass(slots=True, frozen=True)
class BrokerAsyncContext:
    """A sync facade was called on a thread that runs an event loop. Nothing ran.

    A sync call on that thread returns the same error; call the
    ``*_async`` form.
    """

    code: ClassVar[BrokerErrorCode] = BrokerErrorCode.ASYNC_CONTEXT
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)


@dataclass(slots=True, frozen=True)
class BrokerLoopRunnerFailed:
    """A sync facade could not run the call on the broker's sync runner. Nothing ran.

    ``exception`` is the ``LoopRunnerError``. A stopped runner does not start
    again; call the ``*_async`` form or use a new broker.
    """

    code: ClassVar[BrokerErrorCode] = BrokerErrorCode.LOOP_RUNNER_FAILED
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    exception: BaseException = field(repr=False)


@dataclass(slots=True, frozen=True)
class BrokerListenerDisabled:
    """The broker has no listener, so the operation cannot run. Nothing ran.

    A broker built with ``assume_initialized=True`` has no listener. Every
    call that needs one returns this error for that broker.
    """

    code: ClassVar[BrokerErrorCode] = BrokerErrorCode.LISTENER_DISABLED
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)


@dataclass(slots=True, frozen=True)
class BrokerWaitInsideTask:
    """A result wait was called in a process that runs task bodies. Nothing ran.

    Every result wait in that process returns this error. Compose the tasks
    with a workflow, or send the other task and return.
    """

    code: ClassVar[BrokerErrorCode] = BrokerErrorCode.WAIT_INSIDE_TASK
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)


type BrokerOperationError = (
    BrokerOperationFailed
    | BrokerEnqueueFailed
    | BrokerPayloadMismatch
    | BrokerIdempotencyKeyConflict
    | BrokerNotAttached
    | BrokerNoTypeAvailable
    | BrokerInvalidJsonPayload
    | BrokerInvalidTaskOptions
    | BrokerInvalidInput
    | BrokerEventLoopLimit
    | BrokerAsyncContext
    | BrokerLoopRunnerFailed
    | BrokerListenerDisabled
    | BrokerWaitInsideTask
)
"""Every failure of a broker operation: one variant per state."""


type BrokerResult[T] = Result[T, BrokerOperationError]


type BrokerEnqueueError = (
    BrokerEnqueueFailed
    | BrokerPayloadMismatch
    | BrokerIdempotencyKeyConflict
    | BrokerInvalidTaskOptions
    | BrokerInvalidInput
    | BrokerEventLoopLimit
)
"""Every failure of ``PostgresBroker.enqueue_async``."""


type BrokerSyncCallError = BrokerAsyncContext | BrokerLoopRunnerFailed
"""The failures that a sync facade adds to its async method. Nothing ran."""


def broker_error_cause(error: BrokerOperationError) -> BaseException | NoCause:
    """The exception that caused ``error``, or ``NoCause()`` when no exception did.

    Use it before ``raise ... from``, an ``exc_info=`` or an error tracker
    call. It does not change ``error``.
    """
    match error:
        case BrokerOperationFailed(exception=cause) | BrokerLoopRunnerFailed(
            exception=cause,
        ):
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
            | BrokerEventLoopLimit()
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
            ``TaskResult`` / ``Result`` shape they expose.
        raw_result: The decoded JSON envelope from the result column,
            or ``None`` for rows that haven't reached a terminal state
            yet, were cancelled before completion, or were terminal but
            stored no payload.
    """

    task_id: str
    task_name: str
    status: 'TaskStatus'
    raw_result: 'dict[str, Json] | None'
