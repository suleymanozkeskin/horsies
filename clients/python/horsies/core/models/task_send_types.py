"""Typed errors of task send operations.

``send()`` and ``send_async()``, with or without ``with_options(...)``, return
``TaskSendResult[TaskHandle[T]]``. The error side is ``TaskSendError``: a
union with one frozen variant per ``TaskSendErrorCode`` member. Each variant
holds only the fields of its failure, and its ``code`` class variable names
its member. Callers match on the variant.

The resend methods take a ``TaskSendResendable`` and send its stored
``TaskSendPayload`` again; they need no user arguments.

The classes of the core's send catalog and ``TaskSendPayload`` are generated
from horsies-core into ``horsies._catalog.task_send_errors`` and re-exported
here. ``TaskSendError`` is the generated union.
"""

from __future__ import annotations

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


type TaskSendError = TaskSendCoreError
"""Every failure of a send: one variant per ``TaskSendErrorCode``,
each a class of the core's catalog."""

type TaskSendResendable = (
    TaskSendEnqueueFailed
    | TaskSendAsyncContext
    | TaskSendIdempotencyKeyConflict
)
"""The variants that the resend methods accept. Each carries its task id and payload."""

type TaskSendResult[T] = Result[T, TaskSendError]
