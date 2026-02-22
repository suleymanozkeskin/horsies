"""Typed error types for workflow start operations.

Follows the same pattern as ``BrokerOperationError`` in
``horsies/core/brokers/result_types.py``.

Where Result stops and exceptions take over:

* ``start_workflow_async`` / ``start_workflow`` return
  ``WorkflowStartResult[OutT]``.  The ``Ok`` side is always a
  ``WorkflowHandle[T]``; callers decide how to handle each
  ``WorkflowStartErrorCode`` (retry, log, propagate).

* ``WorkflowSpec.start`` / ``start_async`` also return
  ``WorkflowStartResult``.  ``BROKER_NOT_CONFIGURED`` is added at
  the spec layer when no broker is attached.

* Sync bridge classification: ``LOOP_RUNNER_FAILED`` means
  loop-runner infrastructure failure (``LoopRunnerError``).
  ``INTERNAL_FAILED`` means an unexpected sync-path exception
  outside known bridge failures.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from horsies.core.types.result import Result


class WorkflowStartErrorCode(str, Enum):
    """Categorized workflow start failure codes."""

    BROKER_NOT_CONFIGURED = 'BROKER_NOT_CONFIGURED'
    VALIDATION_FAILED = 'VALIDATION_FAILED'
    SERIALIZATION_FAILED = 'SERIALIZATION_FAILED'
    SCHEMA_INIT_FAILED = 'SCHEMA_INIT_FAILED'
    DB_OPERATION_FAILED = 'DB_OPERATION_FAILED'
    LOOP_RUNNER_FAILED = 'LOOP_RUNNER_FAILED'
    INTERNAL_FAILED = 'INTERNAL_FAILED'


class WorkflowStartStage(str, Enum):
    """Coarse pipeline stage where the failure occurred."""

    PREVALIDATE = 'PREVALIDATE'
    ENSURE_SCHEMA = 'ENSURE_SCHEMA'
    DB_TRANSACTION = 'DB_TRANSACTION'
    SYNC_BRIDGE = 'SYNC_BRIDGE'


@dataclass(slots=True, frozen=True)
class WorkflowStartError:
    """Error payload carried inside ``Err(...)`` for workflow start operations.

    Fields:
        code: which failure category
        message: human-readable description
        retryable: whether the caller can safely retry
        stage: coarse pipeline stage where the failure occurred
        workflow_name: workflow spec name (always available)
        workflow_id: generated workflow id (always populated before DB work)
        exception: the original cause (if any)
        details: optional structured metadata
    """

    code: WorkflowStartErrorCode
    message: str
    retryable: bool
    stage: WorkflowStartStage
    workflow_name: str
    workflow_id: str
    exception: BaseException | None = None
    details: dict[str, Any] | None = None


type WorkflowStartResult[T] = Result[T, WorkflowStartError]
