"""Typed errors of WorkflowHandle operations.

Two error strategies by return type:

1. **Wrap strategy**: ``status``, ``results``, ``tasks``, ``cancel``,
   ``pause``, ``resume`` and their ``*_async`` forms, and ``pause_workflow``,
   ``resume_workflow`` and their sync forms, return ``HandleResult[T]``. The
   error side is ``HandleOperationError``: a union with one frozen variant
   per ``HandleErrorCode`` member. Every variant has ``code``, ``message``,
   ``retryable`` and ``workflow_id``. Callers match on the variant.

2. **Fold strategy**: ``get``, ``result_for`` and their ``*_async`` forms
   return ``TaskResult``. A failure folds into
   ``TaskResult(err=TaskError(...))`` with a ``RetrievalCode`` or
   ``BROKER_ERROR``.

``WorkflowHandle.for_id`` returns ``HandleInvalidWorkflowId`` for a workflow
id text that is not a UUID.

``asyncio.CancelledError`` and programmer defects propagate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

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


if TYPE_CHECKING:
    from horsies.core.brokers.resources import EventLoopLimit


@dataclass(slots=True, frozen=True)
class HandleLoopRunnerFailed:
    """A sync method could not run the call on the broker's sync runner. Nothing ran.

    ``exception`` is the ``LoopRunnerError``. A stopped runner does not start
    again (``close()`` and ``close_async()`` stop it); call the ``*_async``
    form.
    """

    code: ClassVar[HandleErrorCode] = HandleErrorCode.LOOP_RUNNER_FAILED
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    workflow_id: str
    exception: BaseException = field(repr=False)


@dataclass(slots=True, frozen=True)
class HandleEventLoopLimit:
    """The broker holds resource sets for its limit of open event loops. Nothing ran.

    The call ran on one more loop. ``loop_limit`` names the limit and the
    threads of the held loops. A call from this loop returns the same error
    until one of the held loops closes.
    """

    code: ClassVar[HandleErrorCode] = HandleErrorCode.EVENT_LOOP_LIMIT
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    workflow_id: str
    loop_limit: EventLoopLimit


type HandleOperationError = HandleCoreError | HandleLoopRunnerFailed | HandleEventLoopLimit
"""Every failure of a handle operation: one variant per ``HandleErrorCode``."""


@dataclass(slots=True, frozen=True)
class HandleInvalidWorkflowId:
    """The workflow id text of ``WorkflowHandle.for_id`` is not a UUID. No handle was built.

    ``workflow_id_text`` is the text as given; ``exception`` is the
    ``ValueError`` of ``uuid.UUID``. The same text gives this error again.
    It is not a handle operation failure: no ``HandleErrorCode`` names it,
    and ``HandleOperationError`` does not hold it.
    """

    message: str = field(repr=False)
    workflow_id_text: str
    exception: ValueError = field(repr=False)


type HandleResult[T] = Result[T, HandleOperationError]
