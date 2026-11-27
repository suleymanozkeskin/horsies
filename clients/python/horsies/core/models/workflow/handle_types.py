"""Typed errors of WorkflowHandle operations.

Two error strategies by return type:

1. **Wrap strategy**: ``status``, ``results``, ``tasks``, ``cancel``,
   ``pause``, ``resume`` and their ``*_async`` forms, and ``pause_workflow``,
   ``resume_workflow`` and their ``*_async`` forms, return ``HandleResult[T]``. The
   error side is ``WorkflowHandleError``: a union with one frozen variant
   per ``WorkflowHandleErrorCode`` member other than ``INVALID_WORKFLOW_ID``.
   Every variant has ``code``, ``message``, ``retryable`` and
   ``workflow_id``. Callers match on the variant.

2. **Read strategy**: ``get``, ``result_for`` and their ``*_async`` forms
   return ``ReadResult``. A read failure is ``Err(ReadError)``, one class
   per code (``ReadWaitTimeout``, ``ReadWorkflowNotFound``, ``ReadDatabaseError``, ...),
   apart from the outcome of the workflow or the node.

``Horsies.workflow_handle`` returns ``WorkflowHandleInvalidWorkflowId``
(``INVALID_WORKFLOW_ID``) for a workflow id text that is not a UUID.

``asyncio.CancelledError`` and programmer defects propagate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from horsies._catalog.workflow_handle import (
    WorkflowHandleErrorCode as WorkflowHandleErrorCode,
)
from horsies._catalog.workflow_handle_errors import (
    HandleCoreError as HandleCoreError,
    WorkflowHandleAsyncContext as WorkflowHandleAsyncContext,
    WorkflowHandleDatabaseOperationFailed as WorkflowHandleDatabaseOperationFailed,
    WorkflowHandleNotFound as WorkflowHandleNotFound,
    WorkflowHandleResumeRecoveryFailed as WorkflowHandleResumeRecoveryFailed,
    WorkflowHandleStoredDataInvalid as WorkflowHandleStoredDataInvalid,
)
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Result


type WorkflowHandleError = HandleCoreError
"""Every failure of a handle operation: one variant per ``WorkflowHandleErrorCode``
member other than ``INVALID_WORKFLOW_ID``, each a class of the core's catalog."""


@dataclass(slots=True, frozen=True)
class WorkflowHandleInvalidWorkflowId:
    """The workflow id text of ``Horsies.workflow_handle`` is not a UUID. No handle was built.

    ``workflow_id_text`` is the text as given; ``cause`` is the
    ``ValueError`` of ``uuid.UUID``. The same text gives this error again.
    It is not a handle operation failure: ``WorkflowHandleError`` does not
    hold it.
    """

    code: ClassVar[WorkflowHandleErrorCode] = WorkflowHandleErrorCode.INVALID_WORKFLOW_ID
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    workflow_id_text: str
    cause: ValueError = field(repr=False)

    def failure_cause(self) -> BaseException | NoCause:
        """The exception of the cause, or ``NoCause()`` when no exception caused it.

        Use it before ``raise ... from``, an ``exc_info=`` or an error tracker
        call. It does not change the error.
        """
        return self.cause


type HandleResult[T] = Result[T, WorkflowHandleError]
