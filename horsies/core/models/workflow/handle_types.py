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

``asyncio.CancelledError`` and programmer defects propagate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, ClassVar

from horsies.core.types.result import Result


if TYPE_CHECKING:
    from horsies.core.brokers.resources import EventLoopLimit


class HandleErrorCode(str, Enum):
    """Categorized handle operation failure codes.

    Each member names one variant of ``HandleOperationError``.
    """

    WORKFLOW_NOT_FOUND = 'WORKFLOW_NOT_FOUND'
    DB_OPERATION_FAILED = 'DB_OPERATION_FAILED'
    # A stored value of the workflow does not parse: an unknown workflow
    # status or a subworkflow summary that is not valid. Nothing changed, and
    # the same call returns the same error.
    STORED_DATA_INVALID = 'STORED_DATA_INVALID'
    # ``resume`` committed the resume, and the recovery pass for the
    # workflow tree, which runs after that commit, failed with a database
    # error. The workflow is RUNNING.
    RESUME_RECOVERY_FAILED = 'RESUME_RECOVERY_FAILED'
    LOOP_RUNNER_FAILED = 'LOOP_RUNNER_FAILED'
    # The broker already holds resource sets for its limit of open event
    # loops, and this call ran on a new one. Nothing ran. It clears when one
    # of those loops closes.
    EVENT_LOOP_LIMIT = 'EVENT_LOOP_LIMIT'
    # A sync handle method was called on a thread that runs an event loop.
    # Its wait would stall that loop, so nothing ran. Call the ``*_async``
    # form.
    ASYNC_CONTEXT = 'ASYNC_CONTEXT'


@dataclass(slots=True, frozen=True)
class HandleWorkflowNotFound:
    """No workflow has this id. Nothing changed.

    The same call returns this error while no workflow has this id.
    """

    code: ClassVar[HandleErrorCode] = HandleErrorCode.WORKFLOW_NOT_FOUND
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    workflow_id: str


@dataclass(slots=True, frozen=True)
class HandleDbOperationFailed:
    """The operation failed with a database or driver error.

    ``retryable`` is the driver classification of the cause: ``True`` when
    the condition may clear, for example a lost connection. A read changed
    nothing. A write (``cancel``, ``pause``, or ``resume`` before its commit)
    rolled back, or its commit outcome is unknown.
    """

    code: ClassVar[HandleErrorCode] = HandleErrorCode.DB_OPERATION_FAILED
    message: str = field(repr=False)
    retryable: bool
    workflow_id: str
    exception: BaseException = field(repr=False)


@dataclass(slots=True, frozen=True)
class HandleStoredDataInvalid:
    """A stored value of the workflow does not parse. Nothing changed.

    The value is a workflow status that is not a ``WorkflowStatus``
    (``status``, ``cancel``), or a subworkflow summary that is not valid
    (``tasks``). ``exception`` is the parse error. The same call returns this
    error until the stored value changes.
    """

    code: ClassVar[HandleErrorCode] = HandleErrorCode.STORED_DATA_INVALID
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    workflow_id: str
    exception: BaseException = field(repr=False)


@dataclass(slots=True, frozen=True)
class HandleResumeRecoveryFailed:
    """``resume`` committed, and the recovery pass for the workflow tree failed.

    The workflow is ``RUNNING`` and the resume enqueued its ready nodes. The
    recovery pass runs after the commit, in a second transaction. It
    completes the propagation for a task that finished while the resume
    transaction ran, when the completion callback saw the workflow as
    ``PAUSED``. That pass rolled back, or its commit outcome is unknown.
    ``exception`` is the database or driver error. The worker reaper runs
    the same pass over all workflows. A repeated ``resume`` returns
    ``Ok(False)``.
    """

    code: ClassVar[HandleErrorCode] = HandleErrorCode.RESUME_RECOVERY_FAILED
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    workflow_id: str
    exception: BaseException = field(repr=False)


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


@dataclass(slots=True, frozen=True)
class HandleAsyncContext:
    """A sync method was called on a thread that runs an event loop. Nothing ran.

    Its wait would stall that loop. Call the ``*_async`` form; a sync call on
    that thread returns this error again.
    """

    code: ClassVar[HandleErrorCode] = HandleErrorCode.ASYNC_CONTEXT
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    workflow_id: str


type HandleOperationError = (
    HandleWorkflowNotFound
    | HandleDbOperationFailed
    | HandleStoredDataInvalid
    | HandleResumeRecoveryFailed
    | HandleLoopRunnerFailed
    | HandleEventLoopLimit
    | HandleAsyncContext
)
"""Every failure of a handle operation: one variant per ``HandleErrorCode``."""

type HandleResult[T] = Result[T, HandleOperationError]
