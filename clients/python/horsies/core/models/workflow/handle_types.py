"""Typed errors of WorkflowHandle operations.

Two error strategies by return type:

1. **Wrap strategy**: ``status``, ``results``, ``tasks``, ``cancel``,
   ``pause``, ``resume`` and their ``*_async`` forms, and ``pause_workflow``,
   ``resume_workflow`` and their ``*_async`` forms, return ``HandleResult[T]``. The
   error side is ``HandleOperationError``: a union with one frozen variant
   per ``HandleErrorCode`` member. Every variant has ``code``, ``message``,
   ``retryable`` and ``workflow_id``. Callers match on the variant.

2. **Read strategy**: ``get``, ``result_for`` and their ``*_async`` forms
   return ``ReadResult``. A read failure is ``Err(ReadError)``, one class
   per code (``WaitTimeout``, ``WorkflowNotFound``, ``BrokerError``, ...),
   apart from the outcome of the workflow or the node.

``Horsies.workflow_handle`` returns ``HandleInvalidWorkflowId`` for a workflow
id text that is not a UUID.

``asyncio.CancelledError`` and programmer defects propagate.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from horsies._catalog.workflow_handle import (
    HandleErrorCode as HandleErrorCode,
)
from horsies._catalog.workflow_handle_errors import (
    HandleAsyncContext as HandleAsyncContext,
    HandleCoreError as HandleCoreError,
    HandleDbOperationFailed as HandleDbOperationFailed,
    HandleResumeRecoveryFailed as HandleResumeRecoveryFailed,
    HandleStoredDataInvalid as HandleStoredDataInvalid,
    HandleWorkflowNotFound as HandleWorkflowNotFound,
)
from horsies.core.types.result import Result


type HandleOperationError = HandleCoreError
"""Every failure of a handle operation: one variant per ``HandleErrorCode``,
each a class of the core's catalog."""


@dataclass(slots=True, frozen=True)
class HandleInvalidWorkflowId:
    """The workflow id text of ``Horsies.workflow_handle`` is not a UUID. No handle was built.

    ``workflow_id_text`` is the text as given; ``exception`` is the
    ``ValueError`` of ``uuid.UUID``. The same text gives this error again.
    It is not a handle operation failure: no ``HandleErrorCode`` names it,
    and ``HandleOperationError`` does not hold it.
    """

    message: str = field(repr=False)
    workflow_id_text: str
    exception: ValueError = field(repr=False)


type HandleResult[T] = Result[T, HandleOperationError]
