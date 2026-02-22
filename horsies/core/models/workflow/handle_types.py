"""Typed error types for WorkflowHandle operations.

Follows the same pattern as ``BrokerOperationError`` and ``WorkflowStartError``.

Two error strategies by return type:

1. **Wrap strategy** -- methods that previously returned non-TaskResult types
   (status, cancel, pause, resume, results, tasks) now return
   ``HandleResult[T]``.  Infrastructure errors are ``Err(HandleOperationError)``.

2. **Fold strategy** -- methods that already returned ``TaskResult``
   (get, result_for, _get_result, _get_error) keep their signature.
   Infrastructure errors fold into ``TaskResult(err=TaskError(BROKER_ERROR, ...))``.

``asyncio.CancelledError`` is always re-raised, never caught.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from horsies.core.types.result import Result


class HandleErrorCode(str, Enum):
    """Categorized handle operation failure codes."""

    WORKFLOW_NOT_FOUND = 'WORKFLOW_NOT_FOUND'
    DB_OPERATION_FAILED = 'DB_OPERATION_FAILED'
    LOOP_RUNNER_FAILED = 'LOOP_RUNNER_FAILED'
    INTERNAL_FAILED = 'INTERNAL_FAILED'


@dataclass(slots=True, frozen=True)
class HandleOperationError:
    """Error payload carried inside ``Err(...)`` for handle operations.

    Fields:
        code: which failure category
        message: human-readable description
        retryable: whether the caller can safely retry
        operation: handle method name (status, cancel, pause, resume, results, tasks)
        stage: coarse location within the operation (status_lookup, query, commit, loop_runner)
        workflow_id: the workflow this handle refers to
        exception: the original cause (if any)
    """

    code: HandleErrorCode
    message: str
    retryable: bool
    operation: str
    stage: str
    workflow_id: str
    exception: BaseException | None = None


type HandleResult[T] = Result[T, HandleOperationError]
