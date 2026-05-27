"""Typed error types for PostgresBroker operations.

Result propagation policy
-------------------------
The library uses two distinct error systems:

1. ``TaskResult[T, TaskError]`` -- user-facing task execution outcomes.
   Returned by ``get_result_async`` / ``get_result``.  Carries domain
   error codes (``BuiltInTaskCode`` families) and user-defined error codes.

2. ``BrokerResult[T]`` (this module) -- infrastructure operation outcomes.
   Returned by broker methods (``enqueue_async``, ``get_task_info_async``,
   ``ensure_schema_initialized``, etc.).  Carries ``BrokerOperationError``
   with a ``retryable`` flag and ``BrokerErrorCode``.

Where Result stops and exceptions take over (the "stop line"):

* **Broker layer** -- returns ``BrokerResult``.  Never raises for
  operational failures (only for ``asyncio.CancelledError``).

* **Internal call sites** -- handle ``BrokerResult`` with real decisions:
  retry on transient errors, save partial progress, track consecutive
  failures, branch on ``retryable``.

* **Process boundaries** (CLI startup, scheduler startup) -- convert
  ``Err`` to an exception and let the process crash.  These are fatal
  startup failures where no further recovery is possible within the
  process.  This is intentional, not a gap: the orchestration layer
  (CLI) already wraps these in retry loops
  (``_ensure_schema_with_retry``) *before* the service starts.

* **Workflow start** (``start_workflow_async``, ``start_workflow``) --
  returns ``WorkflowStartResult`` (see ``start_types.py``).  Schema
  init ``Err`` is converted to ``WorkflowStartError(SCHEMA_INIT_FAILED)``
  preserving the ``retryable`` flag from the broker error.  Sync bridge
  infrastructure failures map to ``LOOP_RUNNER_FAILED``; unexpected
  sync-path exceptions map to ``INTERNAL_FAILED``.

* **User-facing API** (``TaskHandle.info`` / ``info_async``) -- returns
  ``BrokerResult`` directly.  The user decides how to handle
  infrastructure errors for their use case.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from horsies.core.types.result import Result


if TYPE_CHECKING:
    from horsies.core.codec.typed import Json
    from horsies.core.types.status import TaskStatus


class BrokerErrorCode(str, Enum):
    """Categorized broker operation failure codes."""

    SCHEMA_INIT_FAILED = 'SCHEMA_INIT_FAILED'
    ENQUEUE_FAILED = 'ENQUEUE_FAILED'
    PAYLOAD_MISMATCH = 'PAYLOAD_MISMATCH'
    TASK_INFO_QUERY_FAILED = 'TASK_INFO_QUERY_FAILED'
    MONITORING_QUERY_FAILED = 'MONITORING_QUERY_FAILED'
    CLEANUP_FAILED = 'CLEANUP_FAILED'
    CLOSE_FAILED = 'CLOSE_FAILED'
    LISTENER_START_FAILED = 'LISTENER_START_FAILED'
    LISTENER_SUBSCRIBE_FAILED = 'LISTENER_SUBSCRIBE_FAILED'
    NO_BROKER = 'NO_BROKER'
    # Strict-serde additions (design §8). NO_TYPE_AVAILABLE fires when a
    # typed-result decode is requested but the local task catalog can't
    # supply the OkT (task not registered in the calling process).
    # INVALID_JSON_PAYLOAD fires when raw wire JSON fails strict
    # validation (malformed syntax, NaN / Infinity, envelope shape
    # violations); distinct from existing TaskError-domain codes because
    # this surfaces at the Result-returning app/broker layer.
    NO_TYPE_AVAILABLE = 'NO_TYPE_AVAILABLE'
    INVALID_JSON_PAYLOAD = 'INVALID_JSON_PAYLOAD'


@dataclass(slots=True, frozen=True)
class BrokerOperationError:
    """Error payload carried inside Err(...) for broker operations.

    Fields:
        code: which operation category failed
        message: human-readable description
        retryable: whether the caller can retry this operation
        exception: the original cause (if any)
    """

    code: BrokerErrorCode
    message: str
    retryable: bool
    exception: BaseException | None = None


type BrokerResult[T] = Result[T, BrokerOperationError]


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
