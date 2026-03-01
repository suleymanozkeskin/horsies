"""Typed error types for task send/schedule operations.

Follows the same pattern as ``WorkflowStartError`` in
``horsies/core/workflows/start_types.py`` and ``BrokerOperationError``
in ``horsies/core/brokers/result_types.py``.

``send()`` / ``send_async()`` / ``schedule()`` return
``TaskSendResult[TaskHandle[T]]``.  The ``Ok`` side is a
``TaskHandle[T]``; callers decide how to handle each
``TaskSendErrorCode`` (retry, log, propagate).

Retry methods (``retry_send``, ``retry_send_async``, ``retry_schedule``)
extract the stored ``TaskSendPayload`` from the error — no user args needed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from horsies.core.types.result import Result


class TaskSendErrorCode(str, Enum):
    """Categorized task send failure codes.

    Follows the same pattern as BrokerErrorCode and WorkflowStartErrorCode.
    """

    SEND_SUPPRESSED = 'SEND_SUPPRESSED'
    VALIDATION_FAILED = 'VALIDATION_FAILED'
    ENQUEUE_FAILED = 'ENQUEUE_FAILED'
    PAYLOAD_MISMATCH = 'PAYLOAD_MISMATCH'


@dataclass(slots=True, frozen=True)
class TaskSendPayload:
    """Serialized envelope for idempotent retry.

    All value fields are pre-serialized to avoid round-trip issues.
    The retry methods extract this payload directly — no user args needed.
    """

    task_name: str
    queue_name: str
    priority: int
    args_json: str | None
    kwargs_json: str | None
    sent_at: datetime
    good_until: datetime | None
    enqueue_delay_seconds: int | None
    task_options: str | None
    enqueue_sha: str  # SHA-256 hex digest


@dataclass(slots=True, frozen=True)
class TaskSendError:
    """Error from task send/schedule operations.

    Fields:
        code: which failure category
        message: human-readable description
        retryable: whether the caller can retry with the same task_id
        task_id: generated task ID (None for SEND_SUPPRESSED, VALIDATION_FAILED)
        payload: serialized envelope for replay (None when no serialization happened)
        exception: the original cause (if any)
    """

    code: TaskSendErrorCode
    message: str
    retryable: bool
    task_id: str | None = None
    payload: TaskSendPayload | None = None
    exception: BaseException | None = None

    def __repr__(self) -> str:
        """Redacted repr — never leak args/kwargs in logs."""
        return (
            f"TaskSendError(code={self.code!r}, retryable={self.retryable}, "
            f"task_id={self.task_id!r})"
        )

    def __str__(self) -> str:
        return self.__repr__()


type TaskSendResult[T] = Result[T, TaskSendError]
