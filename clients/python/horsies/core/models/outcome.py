"""The outcome of a task, and the result of a read of it.

A task body returns ``Result[T, TaskError]``: ``Ok(value)`` or
``Err(TaskError(...))``. A read of a finished task returns a ``ReadResult``.
The result says whether the read worked. The outcome inside it says how the
task ended:

- ``Ok(Succeeded(value))``: the task returned this value.
- ``Ok(Failed(task_error))``: the task returned this error, or the library
  recorded it (``TASK_CANCELLED``, ``TASK_TIMEOUT``, ``WORKER_CRASHED``, ...).
- ``Err(read_error)``: the read gave no outcome. ``read_error`` is one class
  of ``ReadError``, named after its catalog code. A later read can give an
  outcome for some of them (``WaitTimeout``, ``TaskNotFound``,
  ``BrokerError``).

A workflow node input that takes an upstream result (``args_from``) is a
``TaskOutcome``, and so is the result of ``WorkflowContext.result_for``.

``ReadError`` is a closed union of frozen classes, so a ``match`` on it is
exhaustive under a type checker. Every class has ``code`` (its catalog
member) and ``message`` (the subject and the cause, as text).
``BrokerError`` also has ``retryable`` and ``exception``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar, Literal

from horsies._catalog.task_codes import (
    ContractCode,
    OperationalErrorCode,
    RetrievalCode,
)
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok

__all__ = [
    'AsyncContext',
    'BrokerError',
    'Failed',
    'InvalidInput',
    'NoTypeAvailable',
    'ReadError',
    'ReadResult',
    'ResultDeserializationError',
    'ResultNotAvailable',
    'ResultNotReady',
    'Succeeded',
    'TaskNotFound',
    'TaskOutcome',
    'WaitInsideTask',
    'WaitTimeout',
    'WorkflowCtxMissingId',
    'WorkflowNotFound',
]


@dataclass(frozen=True, slots=True)
class Succeeded[T]:
    """The task returned ``value``."""

    value: T


@dataclass(frozen=True, slots=True)
class Failed[E]:
    """The task returned ``error``, or the library recorded it for the task."""

    error: E


type TaskOutcome[T, E] = Succeeded[T] | Failed[E]
"""How a task ended: its value, or its error."""


@dataclass(slots=True, frozen=True)
class WaitTimeout:
    """The task or the workflow had not ended when the wait limit passed.

    It may still run. A later read may find it ended. Not kept by the handle.
    """

    code: ClassVar[Literal[RetrievalCode.WAIT_TIMEOUT]] = RetrievalCode.WAIT_TIMEOUT
    message: str


@dataclass(slots=True, frozen=True)
class TaskNotFound:
    """Neither the live table nor the task history holds the task.

    A handle built by id can race the commit of its send, so a later read
    may find it. Not kept by the handle.
    """

    code: ClassVar[Literal[RetrievalCode.TASK_NOT_FOUND]] = RetrievalCode.TASK_NOT_FOUND
    message: str


@dataclass(slots=True, frozen=True)
class WorkflowNotFound:
    """No workflow has the id of the handle."""

    code: ClassVar[Literal[RetrievalCode.WORKFLOW_NOT_FOUND]] = (
        RetrievalCode.WORKFLOW_NOT_FOUND
    )
    message: str


@dataclass(slots=True, frozen=True)
class ResultNotAvailable:
    """The task reached a terminal status and stored no result, or a handle
    with no broker holds no result. Kept by the handle."""

    code: ClassVar[Literal[RetrievalCode.RESULT_NOT_AVAILABLE]] = (
        RetrievalCode.RESULT_NOT_AVAILABLE
    )
    message: str


@dataclass(slots=True, frozen=True)
class ResultNotReady:
    """The workflow node stores no result yet. A later read may find one."""

    code: ClassVar[Literal[RetrievalCode.RESULT_NOT_READY]] = (
        RetrievalCode.RESULT_NOT_READY
    )
    message: str


@dataclass(slots=True, frozen=True)
class WaitInsideTask:
    """A result wait in a process that runs task bodies. Nothing was read.

    Compose the tasks with a workflow there. Not kept by the handle.
    """

    code: ClassVar[Literal[RetrievalCode.WAIT_INSIDE_TASK]] = (
        RetrievalCode.WAIT_INSIDE_TASK
    )
    message: str


@dataclass(slots=True, frozen=True)
class AsyncContext:
    """A sync read on a thread that runs an event loop. Nothing was read.

    Its wait would stall that loop. Call the ``*_async`` form there. Not kept
    by the handle.
    """

    code: ClassVar[Literal[RetrievalCode.ASYNC_CONTEXT]] = RetrievalCode.ASYNC_CONTEXT
    message: str


@dataclass(slots=True, frozen=True)
class InvalidInput:
    """The wait limit is not ``None`` or an int from 0 to
    ``MAX_TASK_TIMEOUT_MS``. Nothing was read. Not kept by the handle."""

    code: ClassVar[Literal[RetrievalCode.INVALID_INPUT]] = RetrievalCode.INVALID_INPUT
    message: str


@dataclass(slots=True, frozen=True)
class ResultDeserializationError:
    """The stored result does not read as the result of the declared types.

    The same read gives the same error until the stored value changes. Kept
    by the handle.
    """

    code: ClassVar[Literal[OperationalErrorCode.RESULT_DESERIALIZATION_ERROR]] = (
        OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
    )
    message: str


@dataclass(slots=True, frozen=True)
class BrokerError:
    """The database, the connection or the core call failed.

    ``message`` names the code of the failed operation and its cause.
    ``retryable`` is the classification of the cause: ``True`` when the
    condition may clear, so that a later read may succeed. ``exception`` is
    the cause, or ``NoCause()``. Not kept by the handle.
    """

    code: ClassVar[Literal[OperationalErrorCode.BROKER_ERROR]] = (
        OperationalErrorCode.BROKER_ERROR
    )
    message: str
    retryable: bool
    exception: BaseException | NoCause = field(repr=False)


@dataclass(slots=True, frozen=True)
class NoTypeAvailable:
    """The stored value succeeded, and this process has no declared type to
    read it with: the handle was built by id, or the task or workflow is not
    registered here. Read the raw result, or register the task. Kept by the
    handle."""

    code: ClassVar[Literal[ContractCode.NO_TYPE_AVAILABLE]] = ContractCode.NO_TYPE_AVAILABLE
    message: str


@dataclass(slots=True, frozen=True)
class WorkflowCtxMissingId:
    """``result_for`` got a node with no ``node_id``. Nothing was read."""

    code: ClassVar[Literal[ContractCode.WORKFLOW_CTX_MISSING_ID]] = (
        ContractCode.WORKFLOW_CTX_MISSING_ID
    )
    message: str


type ReadError = (
    WaitTimeout
    | TaskNotFound
    | WorkflowNotFound
    | ResultNotAvailable
    | ResultNotReady
    | WaitInsideTask
    | AsyncContext
    | InvalidInput
    | ResultDeserializationError
    | BrokerError
    | NoTypeAvailable
    | WorkflowCtxMissingId
)
"""Why a read gave no outcome: one class per catalog code."""


type ReadResult[T, E] = Ok[Succeeded[T]] | Ok[Failed[E]] | Err[ReadError]
"""The result of a read of a task or a workflow.

The three members are listed apart, not as ``Result[TaskOutcome[T, E],
ReadError]``: a type checker narrows ``case Ok(Succeeded(...))`` and
``case Ok(Failed(...))`` only on separate members.
"""
