"""Typed error types for workflow lifecycle operations (pause, resume).

Follows the same pattern as ``WorkflowStartError`` in
``horsies/core/workflows/start_types.py`` and ``HandleOperationError``
in ``horsies/core/models/workflow/handle_types.py``.

Where Result stops and exceptions take over:

* ``pause_workflow`` / ``resume_workflow`` return
  ``LifecycleResult[bool]``.  ``Ok(True)`` means the state transition
  succeeded; ``Ok(False)`` means the workflow exists but was not in the
  expected state (no-op).

* ``Err(WORKFLOW_NOT_FOUND)`` replaces the previous ``None`` sentinel.

* ``Err(DB_OPERATION_FAILED)`` catches infrastructure failures that
  previously leaked as raw ``SQLAlchemyError``.

* Sync wrappers add ``LOOP_RUNNER_FAILED`` and ``INTERNAL_FAILED``
  following the ``start_workflow`` sync bridge pattern.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from horsies.core.types.result import Result


class LifecycleErrorCode(str, Enum):
    """Categorized lifecycle operation failure codes."""

    WORKFLOW_NOT_FOUND = 'WORKFLOW_NOT_FOUND'
    DB_OPERATION_FAILED = 'DB_OPERATION_FAILED'
    LOOP_RUNNER_FAILED = 'LOOP_RUNNER_FAILED'
    INTERNAL_FAILED = 'INTERNAL_FAILED'


@dataclass(slots=True, frozen=True)
class LifecycleOperationError:
    """Error payload carried inside ``Err(...)`` for lifecycle operations.

    Fields:
        code: which failure category
        message: human-readable description
        retryable: whether the caller can safely retry
        operation: lifecycle method name ('pause' or 'resume')
        stage: coarse location within the operation
        workflow_id: the workflow this operation targeted
        exception: the original cause (if any)
    """

    code: LifecycleErrorCode
    message: str
    retryable: bool
    operation: str
    stage: str
    workflow_id: str
    exception: BaseException | None = None


type LifecycleResult[T] = Result[T, LifecycleOperationError]
