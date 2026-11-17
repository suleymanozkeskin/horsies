"""Typed errors of task send and schedule operations.

``send()``, ``send_async()``, ``schedule()`` and ``schedule_async()`` return
``TaskSendResult[TaskHandle[T]]``. The error side is ``TaskSendError``: a
union with one frozen variant per ``TaskSendErrorCode`` member. Each variant
holds only the fields of its failure, and its ``code`` class variable names
its member. Callers match on the variant.

The retry methods take a ``TaskSendRetryable`` and send its stored
``TaskSendPayload`` again; they need no user arguments.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from horsies._catalog.task_send import TaskSendErrorCode as TaskSendErrorCode
from horsies.core.history.ddl.classes import DEFAULT_RETENTION_CLASS_KEY
from horsies.core.history.periods import RetentionClassKey
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Result


@dataclass(slots=True, frozen=True)
class TaskSendPayload:
    """Serialized envelope for idempotent retry.

    All value fields are pre-serialized to avoid round-trip issues.
    The retry methods extract this payload directly — no user args needed.
    """

    task_name: str
    queue_name: str
    priority: int
    # The task input as JSON text: the keyword arguments as one object, or
    # None for a send with none (stored as ``{}``).
    input_json: str | None
    sent_at: datetime
    good_until: datetime | None
    enqueue_delay_seconds: int | None
    task_options: str | None
    # The caller's idempotency key, distinct from task identity. Riding the
    # payload keeps the uncertain-commit resend keyed: a replayed send
    # claims with the same key it first sent.
    idempotency_key: str | None = None
    # The retention class snapshotted at enqueue. ``None`` means forever,
    # so the field defaults to the immutable 30-day class rather than to
    # ``None`` — a payload that never had the field set must not read as
    # a forever request on replay.
    retention_class_key: RetentionClassKey | None = DEFAULT_RETENTION_CLASS_KEY


@dataclass(slots=True, frozen=True)
class TaskSendSuppressed:
    """Sends are suppressed in this process. Nothing was serialized or written.

    Suppression is active during the worker and scheduler import phase,
    during ``check()``, and under ``HORSIES_SUPPRESS_SENDS=1``. The retry
    methods refuse this error.
    """

    code: ClassVar[TaskSendErrorCode] = TaskSendErrorCode.SEND_SUPPRESSED
    message: str = field(repr=False)


@dataclass(slots=True, frozen=True)
class TaskSendAsyncContext:
    """A sync send or schedule ran inside a running event loop. Nothing was written.

    The payload is prepared. ``retry_send_async(err)`` or
    ``retry_schedule_async(err)`` completes the same dispatch with the same
    task id. A sync retry from the same loop returns this error again.
    """

    code: ClassVar[TaskSendErrorCode] = TaskSendErrorCode.ASYNC_CONTEXT
    task_id: UUID
    payload: TaskSendPayload = field(repr=False)
    message: str = field(repr=False)


@dataclass(slots=True, frozen=True)
class TaskSendValidationFailed:
    """The input was refused before anything was written.

    ``exception`` is the cause, or ``NoCause()`` when a check refused the
    input without a Python exception. The retry methods refuse this error.
    """

    code: ClassVar[TaskSendErrorCode] = TaskSendErrorCode.VALIDATION_FAILED
    message: str = field(repr=False)
    exception: BaseException | NoCause = field(repr=False)


@dataclass(slots=True, frozen=True)
class TaskSendEnqueueFailed:
    """The broker or the database failed during enqueue. The row may or may not exist.

    ``retryable`` is the driver classification of the cause. ``retry_send(err)``
    sends the same task id and payload again; the broker returns the existing
    task for a duplicate. ``exception`` is ``NoCause()`` when the broker found
    the failure by a data check.
    """

    code: ClassVar[TaskSendErrorCode] = TaskSendErrorCode.ENQUEUE_FAILED
    task_id: UUID
    payload: TaskSendPayload = field(repr=False)
    message: str = field(repr=False)
    retryable: bool
    exception: BaseException | NoCause = field(repr=False)


@dataclass(slots=True, frozen=True)
class TaskSendPayloadMismatch:
    """The task id exists with a different command or idempotency key. Nothing was written.

    The message names which one differs. The retry methods refuse this error.
    """

    code: ClassVar[TaskSendErrorCode] = TaskSendErrorCode.PAYLOAD_MISMATCH
    task_id: UUID
    payload: TaskSendPayload = field(repr=False)
    message: str = field(repr=False)


@dataclass(slots=True, frozen=True)
class TaskSendPayloadTooLarge:
    """The serialized kwargs exceed ``payload.reject_bytes``. Nothing was written.

    ``size_bytes`` is the length of the serialized kwargs and ``limit_bytes``
    the configured limit. The retry methods refuse this error.
    """

    code: ClassVar[TaskSendErrorCode] = TaskSendErrorCode.PAYLOAD_TOO_LARGE
    message: str = field(repr=False)
    size_bytes: int
    limit_bytes: int


@dataclass(slots=True, frozen=True)
class TaskSendIdempotencyKeyConflict:
    """The idempotency key is reserved by another task with a different command.

    Nothing was written. ``owner_task_id`` names the task that holds the key.
    The retry methods accept this error: a retry after the reservation ends
    is accepted, and a retry before that returns the same error.
    """

    code: ClassVar[TaskSendErrorCode] = TaskSendErrorCode.IDEMPOTENCY_KEY_CONFLICT
    task_id: UUID
    payload: TaskSendPayload = field(repr=False)
    message: str = field(repr=False)
    owner_task_id: UUID


@dataclass(slots=True, frozen=True)
class TaskSendEventLoopLimit:
    """The broker holds resource sets for its limit of open event loops. Nothing was written.

    The send ran on one more loop. A send from this loop returns the same
    error until one of the held loops closes. The message names the threads
    of the held loops. The retry methods refuse this error.
    """

    code: ClassVar[TaskSendErrorCode] = TaskSendErrorCode.EVENT_LOOP_LIMIT
    message: str = field(repr=False)


@dataclass(slots=True, frozen=True)
class TaskSendLoopRunnerFailed:
    """A sync send or schedule could not run on the broker's sync runner. Nothing ran.

    Nothing was written. A stopped runner does not start again, so a sync
    retry returns this error again. ``retry_send_async(err)`` or
    ``retry_schedule_async(err)`` completes the same dispatch with the same
    task id. ``exception`` is the ``LoopRunnerError``.
    """

    code: ClassVar[TaskSendErrorCode] = TaskSendErrorCode.LOOP_RUNNER_FAILED
    task_id: UUID
    payload: TaskSendPayload = field(repr=False)
    message: str = field(repr=False)
    exception: BaseException = field(repr=False)


type TaskSendError = (
    TaskSendSuppressed
    | TaskSendAsyncContext
    | TaskSendValidationFailed
    | TaskSendEnqueueFailed
    | TaskSendPayloadMismatch
    | TaskSendPayloadTooLarge
    | TaskSendIdempotencyKeyConflict
    | TaskSendEventLoopLimit
    | TaskSendLoopRunnerFailed
)
"""Every failure of a send or schedule: one variant per ``TaskSendErrorCode``."""

type TaskSendRetryable = (
    TaskSendEnqueueFailed
    | TaskSendAsyncContext
    | TaskSendIdempotencyKeyConflict
    | TaskSendLoopRunnerFailed
)
"""The variants that the retry methods accept. Each carries its task id and payload."""

type TaskSendResult[T] = Result[T, TaskSendError]
