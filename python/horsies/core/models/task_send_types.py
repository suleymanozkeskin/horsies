"""Typed errors of task send and schedule operations.

``send()``, ``send_async()``, ``schedule()`` and ``schedule_async()`` return
``TaskSendResult[TaskHandle[T]]``. The error side is ``TaskSendError``: a
union with one frozen variant per ``TaskSendErrorCode`` member. Each variant
holds only the fields of its failure, and its ``code`` class variable names
its member. Callers match on the variant.

The retry methods take a ``TaskSendRetryable`` and send its stored
``TaskSendPayload`` again; they need no user arguments.

The classes of the core's send catalog and ``TaskSendPayload`` are generated
from horsies-core into ``horsies._catalog.task_send_errors`` and re-exported
here. The classes that only this client produces are defined here, and
``TaskSendError`` is the generated union plus them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar
from uuid import UUID

from horsies._catalog.task_send import TaskSendErrorCode as TaskSendErrorCode
from horsies._catalog.task_send_errors import (
    TaskSendAsyncContext as TaskSendAsyncContext,
    TaskSendCoreError as TaskSendCoreError,
    TaskSendEnqueueFailed as TaskSendEnqueueFailed,
    TaskSendIdempotencyKeyConflict as TaskSendIdempotencyKeyConflict,
    TaskSendPayload as TaskSendPayload,
    TaskSendPayloadMismatch as TaskSendPayloadMismatch,
    TaskSendPayloadTooLarge as TaskSendPayloadTooLarge,
    TaskSendSuppressed as TaskSendSuppressed,
    TaskSendValidationFailed as TaskSendValidationFailed,
)
from horsies.core.types.result import Result


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


type TaskSendError = TaskSendCoreError | TaskSendEventLoopLimit | TaskSendLoopRunnerFailed
"""Every failure of a send or schedule: one variant per ``TaskSendErrorCode``.

``TaskSendCoreError`` holds the classes of the core's catalog; the event
loop limit and the loop runner failure are produced by this client only."""

type TaskSendRetryable = (
    TaskSendEnqueueFailed
    | TaskSendAsyncContext
    | TaskSendIdempotencyKeyConflict
    | TaskSendLoopRunnerFailed
)
"""The variants that the retry methods accept. Each carries its task id and payload."""

type TaskSendResult[T] = Result[T, TaskSendError]
