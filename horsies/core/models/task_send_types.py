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

from horsies.core.history.ddl.classes import DEFAULT_RETENTION_CLASS_KEY
from horsies.core.types.result import Result


class TaskSendErrorCode(str, Enum):
    """Categorized task send failure codes.

    Follows the same pattern as BrokerErrorCode and WorkflowStartErrorCode.
    """

    SEND_SUPPRESSED = 'SEND_SUPPRESSED'
    """Send called while suppression is active (worker/scheduler import phase,
    ``check()``, or ``TASKLIB_SUPPRESS_SENDS=1``). Non-retryable."""

    ASYNC_CONTEXT = 'ASYNC_CONTEXT'
    """Sync ``send()``/``schedule()`` (or their sync retry variants) called
    from inside a running event loop; the blocking enqueue round trip would
    stall the loop. The error carries ``task_id`` and payload — complete the
    dispatch with ``retry_send_async`` / ``retry_schedule_async``, or call
    the matching ``*_async`` entry point with the original args.
    ``retryable=False`` (not a transient broker fault; do not spin a sync
    retry loop)."""

    VALIDATION_FAILED = 'VALIDATION_FAILED'
    ENQUEUE_FAILED = 'ENQUEUE_FAILED'
    PAYLOAD_MISMATCH = 'PAYLOAD_MISMATCH'

    PAYLOAD_TOO_LARGE = 'PAYLOAD_TOO_LARGE'
    """Serialized kwargs exceeded ``payload.reject_bytes``; nothing was
    written. Non-retryable — shrink the payload (pass a reference to
    external storage) or raise the configured limit."""


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
    # The caller's idempotency key, distinct from task identity and from
    # enqueue_sha. Riding the payload keeps the uncertain-commit resend
    # keyed: a replayed send claims with the same key it first sent.
    idempotency_key: str | None = None
    # The retention class snapshotted at enqueue. ``None`` means forever,
    # so the field defaults to the immutable 30-day class rather than to
    # ``None`` — a payload that never had the field set must not read as
    # a forever request on replay.
    retention_class_key: str | None = DEFAULT_RETENTION_CLASS_KEY


@dataclass(slots=True, frozen=True)
class TaskSendError:
    """Error from task send/schedule operations.

    Fields:
        code: which failure category
        message: human-readable description
        retryable: whether the caller can retry with the same task_id
        task_id: generated task ID (None for SEND_SUPPRESSED; may or may not
            be set for VALIDATION_FAILED depending on when the failure occurs)
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
