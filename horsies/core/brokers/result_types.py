"""Typed error types for PostgresBroker operations.

Result propagation policy
-------------------------
The library uses two distinct error systems:

1. ``TaskResult[T, TaskError]`` -- user-facing task execution outcomes.
   Returned by ``get_result_async`` / ``get_result``.  Carries domain
   error codes (``LibraryErrorCode``) and user-defined error codes.

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

from horsies.core.types.result import Result


class BrokerErrorCode(str, Enum):
    """Categorized broker operation failure codes."""

    SCHEMA_INIT_FAILED = 'SCHEMA_INIT_FAILED'
    ENQUEUE_FAILED = 'ENQUEUE_FAILED'
    TASK_INFO_QUERY_FAILED = 'TASK_INFO_QUERY_FAILED'
    MONITORING_QUERY_FAILED = 'MONITORING_QUERY_FAILED'
    CLEANUP_FAILED = 'CLEANUP_FAILED'
    CLOSE_FAILED = 'CLOSE_FAILED'


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
