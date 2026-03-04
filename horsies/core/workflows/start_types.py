"""Typed error types for workflow start operations.

Follows the same pattern as ``TaskSendError`` in
``horsies/core/task_decorator.py``.

Where Result stops and exceptions take over:

* ``start_workflow_async`` / ``start_workflow`` return
  ``WorkflowStartResult[OutT]``.  The ``Ok`` side is always a
  ``WorkflowHandle[T]``; callers decide how to handle each
  ``WorkflowStartErrorCode`` (retry, log, propagate).

* ``WorkflowSpec.start`` / ``start_async`` also return
  ``WorkflowStartResult``.  ``BROKER_NOT_CONFIGURED`` is added at
  the spec layer when no broker is attached.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from horsies.core.types.result import Result


class WorkflowStartErrorCode(str, Enum):
    """Categorized workflow start failure codes.

    Mirrors ``TaskSendErrorCode`` shape:
    - ``BROKER_NOT_CONFIGURED``: config error (no broker attached)
    - ``VALIDATION_FAILED``: programmer error (bad DAG, bad args, serialization)
    - ``ENQUEUE_FAILED``: infra error (schema init, DB transaction), conditionally retryable
    - ``INTERNAL_FAILED``: bug / catch-all
    """

    BROKER_NOT_CONFIGURED = 'BROKER_NOT_CONFIGURED'
    VALIDATION_FAILED = 'VALIDATION_FAILED'
    ENQUEUE_FAILED = 'ENQUEUE_FAILED'
    INTERNAL_FAILED = 'INTERNAL_FAILED'


@dataclass(slots=True, frozen=True)
class WorkflowStartError:
    """Error payload carried inside ``Err(...)`` for workflow start operations.

    Fields:
        code: which failure category
        message: human-readable description
        retryable: whether the caller can safely retry
        workflow_name: workflow spec name (always available)
        workflow_id: generated workflow id (always populated before DB work)
        exception: the original cause (if any)
    """

    code: WorkflowStartErrorCode
    message: str
    retryable: bool
    workflow_name: str
    workflow_id: str
    exception: BaseException | None = None


type WorkflowStartResult[T] = Result[T, WorkflowStartError]
