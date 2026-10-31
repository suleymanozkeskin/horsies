"""Typed errors of workflow start operations.

``start_workflow_async`` / ``start_workflow`` and ``WorkflowSpec.start`` /
``start_async`` return ``WorkflowStartResult[WorkflowHandle[T]]``. The error
side is ``WorkflowStartError``: a union with one frozen variant per
``WorkflowStartErrorCode`` member. Every variant has ``code``, ``message``,
``retryable``, ``workflow_name`` and ``workflow_id``. Callers match on the
variant.

``workflow_id`` is the id that this start used: the caller's id, or the id
minted for the start when the caller gave none.

The retry methods ``WorkflowSpec.retry_start`` / ``retry_start_async`` take a
``WorkflowStartRetryable`` and start the workflow again with its id.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, ClassVar

from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Result


if TYPE_CHECKING:
    from horsies.core.brokers.resources import EventLoopLimit


class WorkflowStartErrorCode(str, Enum):
    """Categorized workflow start failure codes.

    Each member names one variant of ``WorkflowStartError``.
    """

    BROKER_NOT_CONFIGURED = 'BROKER_NOT_CONFIGURED'
    VALIDATION_FAILED = 'VALIDATION_FAILED'
    # The workflow id exists for another workflow name; nothing was written.
    WORKFLOW_ID_CONFLICT = 'WORKFLOW_ID_CONFLICT'
    ENQUEUE_FAILED = 'ENQUEUE_FAILED'
    # Sync ``start()`` could not run the call on the broker's sync runner
    # (the runner is stopped, its thread did not start, or scheduling
    # failed); nothing ran. A stopped runner does not start again
    # (``broker.close()`` and ``close_async()`` stop it).
    LOOP_RUNNER_FAILED = 'LOOP_RUNNER_FAILED'
    # The broker already holds resource sets for its limit of open event
    # loops, and this call ran on a new one; nothing was written. It clears
    # when one of those loops closes.
    EVENT_LOOP_LIMIT = 'EVENT_LOOP_LIMIT'
    # Sync ``start()`` was called on a thread that runs an event loop; its
    # wait would stall that loop, so nothing was written.
    ASYNC_CONTEXT = 'ASYNC_CONTEXT'


@dataclass(slots=True, frozen=True)
class WorkflowStartBrokerNotConfigured:
    """The spec has no broker, so the start cannot run. Nothing ran.

    Build the spec with ``app.workflow()`` or set its broker. The retry
    methods refuse this error.
    """

    code: ClassVar[WorkflowStartErrorCode] = WorkflowStartErrorCode.BROKER_NOT_CONFIGURED
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    workflow_name: str
    workflow_id: str


@dataclass(slots=True, frozen=True)
class WorkflowStartValidationFailed:
    """The spec, its args or the retry input was refused. Nothing was written.

    The same start is refused again. ``exception`` is ``NoCause()`` when a
    check refused the input without a Python exception. The retry methods
    refuse this error.
    """

    code: ClassVar[WorkflowStartErrorCode] = WorkflowStartErrorCode.VALIDATION_FAILED
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    workflow_name: str
    workflow_id: str
    exception: BaseException | NoCause = field(repr=False)


@dataclass(slots=True, frozen=True)
class WorkflowStartIdConflict:
    """The workflow id exists for another workflow name. Nothing was written.

    ``existing_workflow_name`` names the workflow that holds the id. A start
    with the same id is refused again; use another id. The retry methods
    refuse this error.
    """

    code: ClassVar[WorkflowStartErrorCode] = WorkflowStartErrorCode.WORKFLOW_ID_CONFLICT
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    workflow_name: str
    workflow_id: str
    existing_workflow_name: str


@dataclass(slots=True, frozen=True)
class WorkflowStartEnqueueFailed:
    """Schema initialization or the start transaction failed with a database or driver error.

    ``retryable`` is the driver classification of the cause: ``True`` when
    the condition may clear, for example a lost connection. The transaction
    rolled back, or its commit outcome is unknown. ``retry_start(err)``
    starts again with the same id; a workflow that the failed attempt
    committed comes back as the existing handle.
    """

    code: ClassVar[WorkflowStartErrorCode] = WorkflowStartErrorCode.ENQUEUE_FAILED
    message: str = field(repr=False)
    retryable: bool
    workflow_name: str
    workflow_id: str
    exception: BaseException = field(repr=False)


@dataclass(slots=True, frozen=True)
class WorkflowStartLoopRunnerFailed:
    """Sync ``start()`` could not run the start on the broker's sync runner. Nothing ran.

    ``exception`` is the ``LoopRunnerError``. A stopped runner does not start
    again (``close()`` and ``close_async()`` stop it). ``retry_start_async(err)``
    starts the workflow with the same id; the sync ``retry_start`` on a
    stopped runner returns this error again.
    """

    code: ClassVar[WorkflowStartErrorCode] = WorkflowStartErrorCode.LOOP_RUNNER_FAILED
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    workflow_name: str
    workflow_id: str
    exception: BaseException = field(repr=False)


@dataclass(slots=True, frozen=True)
class WorkflowStartEventLoopLimit:
    """The broker holds resource sets for its limit of open event loops. Nothing was written.

    The start ran on one more loop. ``loop_limit`` names the limit and the
    threads of the held loops. A start from this loop returns the same error
    until one of the held loops closes. The retry methods refuse this error.
    """

    code: ClassVar[WorkflowStartErrorCode] = WorkflowStartErrorCode.EVENT_LOOP_LIMIT
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    workflow_name: str
    workflow_id: str
    loop_limit: EventLoopLimit


@dataclass(slots=True, frozen=True)
class WorkflowStartAsyncContext:
    """Sync ``start()`` or ``retry_start()`` was called on a thread that runs an event loop.

    Nothing was written: the wait would stall that loop. ``retry_start_async(err)``
    starts the workflow with the same id; a sync call on that thread returns
    this error again.
    """

    code: ClassVar[WorkflowStartErrorCode] = WorkflowStartErrorCode.ASYNC_CONTEXT
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    workflow_name: str
    workflow_id: str


type WorkflowStartError = (
    WorkflowStartBrokerNotConfigured
    | WorkflowStartValidationFailed
    | WorkflowStartIdConflict
    | WorkflowStartEnqueueFailed
    | WorkflowStartLoopRunnerFailed
    | WorkflowStartEventLoopLimit
    | WorkflowStartAsyncContext
)
"""Every failure of a workflow start: one variant per ``WorkflowStartErrorCode``."""

type WorkflowStartRetryable = (
    WorkflowStartEnqueueFailed | WorkflowStartAsyncContext | WorkflowStartLoopRunnerFailed
)
"""The variants that the retry methods accept. Each carries the id of its start."""

type WorkflowStartResult[T] = Result[T, WorkflowStartError]
