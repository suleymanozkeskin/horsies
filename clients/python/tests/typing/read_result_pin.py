"""Type pin: the result of a body, the result of a read, and the read failures.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing calls its functions. Each
line that pyright must refuse carries an ignore comment for its rule. The
config makes an unneeded ignore comment an error, so a ``ReadResult`` whose
members a match cannot tell apart, a ``ReadError`` that is not a closed
union, or a read result that has ``ok_value`` on every member fails the
check.
"""

from __future__ import annotations

from typing import assert_never

from horsies import (
    AsyncContext,
    BrokerError,
    Err,
    Failed,
    InvalidInput,
    NoTypeAvailable,
    Ok,
    ReadError,
    ReadResult,
    Result,
    ResultDeserializationError,
    ResultNotAvailable,
    ResultNotReady,
    Succeeded,
    TaskError,
    TaskNotFound,
    TaskOutcome,
    WaitInsideTask,
    WaitTimeout,
    WorkflowCtxMissingId,
    WorkflowNotFound,
)


def body(*, email: str) -> Result[str, TaskError]:
    if '@' not in email:
        return Err(TaskError(error_code='INVALID_EMAIL', message='the address has no @'))
    return Ok(email)


def every_arm(read: ReadResult[str, TaskError]) -> str:
    match read:
        case Ok(Succeeded(value)):
            return value
        case Ok(Failed(task_error)):
            return str(task_error.error_code)
        case Err(read_error):
            return str(read_error.code)
        case _ as unreachable:
            assert_never(unreachable)


def failed_arm_left_out(read: ReadResult[str, TaskError]) -> str:
    match read:  # pyright: ignore[reportMatchNotExhaustive]
        case Ok(Succeeded(value)):
            return value
        case Err(read_error):
            return read_error.message
    return ''


def takes_outcome(outcome: TaskOutcome[str, TaskError]) -> str:
    match outcome:
        case Succeeded(value):
            return value
        case Failed(task_error):
            return task_error.message or ''


def outcome_passed_on(read: ReadResult[str, TaskError]) -> str:
    match read:
        case Ok(outcome):
            return takes_outcome(outcome)
        case Err(read_error):
            return read_error.message


def no_ok_value_on_every_member(read: ReadResult[str, TaskError]) -> object:
    if read.is_ok():
        return read.ok_value  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]
    return None


def every_read_error(error: ReadError) -> bool:
    """Whether a later read may give an outcome: one arm per class."""
    match error:
        case WaitTimeout() | TaskNotFound() | ResultNotReady() | BrokerError():
            return True
        case (
            WorkflowNotFound()
            | ResultNotAvailable()
            | WaitInsideTask()
            | AsyncContext()
            | InvalidInput()
            | ResultDeserializationError()
            | NoTypeAvailable()
            | WorkflowCtxMissingId()
        ):
            return False
        case _ as unreachable:
            assert_never(unreachable)


def read_error_left_out(error: ReadError) -> bool:
    match error:  # pyright: ignore[reportMatchNotExhaustive]
        case WaitTimeout() | TaskNotFound() | ResultNotReady() | BrokerError():
            return True
        case (
            WorkflowNotFound()
            | ResultNotAvailable()
            | WaitInsideTask()
            | AsyncContext()
            | InvalidInput()
            | ResultDeserializationError()
            | NoTypeAvailable()
        ):
            return False
    return False


def task_error_is_not_a_read_error(task_error: TaskError) -> ReadResult[str, TaskError]:
    return Err(task_error)  # pyright: ignore[reportReturnType]
