"""Type pin: typed custom error codes, from the declared Enum to the read.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing calls its functions. Each
line that pyright must refuse carries an ignore comment for its rule. The
config makes an unneeded ignore comment an error, so a code type that the
handle does not carry, or a match that names every member without an
error for a member left out, fails the check.
"""

from __future__ import annotations

from enum import Enum
from typing import assert_never, assert_type

from horsies import (
    ContractCode,
    Err,
    Failed,
    Horsies,
    Ok,
    OperationalCode,
    OutcomeCode,
    ReadResult,
    Result,
    RetrievalCode,
    RetryPolicy,
    Succeeded,
    TaskError,
    TaskHandle,
    TaskNode,
    TaskOutcome,
    WorkflowContext,
    WorkflowHandle,
)


class WelcomeCode(str, Enum):
    INVALID_EMAIL = 'INVALID_EMAIL'
    MAILBOX_FULL = 'MAILBOX_FULL'


def register(app: Horsies) -> None:
    @app.task('send_welcome_email', error_codes=WelcomeCode)
    def send_welcome_email(*, email: str) -> Result[str, TaskError[WelcomeCode]]:
        if '@' not in email:
            return Err(TaskError(error_code=WelcomeCode.INVALID_EMAIL, message=email))
        return Ok(email)

    match send_welcome_email.send(email='a@b'):
        case Ok(ok_value=handle):
            assert_type(handle, TaskHandle[str, WelcomeCode])
            assert_type(handle.get(), ReadResult[str, TaskError[WelcomeCode]])
        case Err():
            pass

    node = send_welcome_email.node()(email='a@b')
    assert_type(node, TaskNode[str, WelcomeCode])


def node_reads(
    node: TaskNode[str, WelcomeCode],
    ctx: WorkflowContext,
    workflow: WorkflowHandle[str],
) -> None:
    assert_type(ctx.result_for(node), TaskOutcome[str, TaskError[WelcomeCode]])
    assert_type(workflow.result_for(node), ReadResult[str, TaskError[WelcomeCode]])
    assert_type(workflow.result_for(node.key()), ReadResult[str, TaskError[WelcomeCode]])


def context_code_left_out(node: TaskNode[str, WelcomeCode], ctx: WorkflowContext) -> str:
    match ctx.result_for(node):
        case Succeeded(value):
            return value
        case Failed(task_error):
            match task_error.error_code:  # pyright: ignore[reportMatchNotExhaustive]
                case WelcomeCode.INVALID_EMAIL:
                    return 'invalid'
                case OperationalCode() | ContractCode() | RetrievalCode() | OutcomeCode():
                    return 'library'
    return ''


def every_code(task_error: TaskError[WelcomeCode]) -> str:
    match task_error.error_code:
        case WelcomeCode.INVALID_EMAIL:
            return 'invalid'
        case WelcomeCode.MAILBOX_FULL:
            return 'full'
        case OperationalCode() | ContractCode() | RetrievalCode() | OutcomeCode():
            return 'library'
        case _ as unreachable:
            assert_never(unreachable)


def code_left_out(task_error: TaskError[WelcomeCode]) -> str:
    match task_error.error_code:  # pyright: ignore[reportMatchNotExhaustive]
        case WelcomeCode.INVALID_EMAIL:
            return 'invalid'
        case OperationalCode() | ContractCode() | RetrievalCode() | OutcomeCode():
            return 'library'
    return ''


def text_codes_need_a_default_arm(task_error: TaskError) -> str:
    match task_error.error_code:  # pyright: ignore[reportMatchNotExhaustive]
        case 'INVALID_EMAIL':
            return 'invalid'
        case OperationalCode() | ContractCode() | RetrievalCode() | OutcomeCode():
            return 'library'
    return ''


def typed_read(read: ReadResult[str, TaskError[WelcomeCode]]) -> str:
    match read:
        case Ok(Succeeded(value)):
            return value
        case Ok(Failed(task_error)):
            return every_code(task_error)
        case Err(read_error):
            return read_error.message


def retry_codes() -> None:
    """`auto_retry_for` takes code members: a built-in member, or a member
    of the declared `Enum` class. A plain `str` is refused."""
    _ = RetryPolicy.fixed(
        [1, 2],
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED, WelcomeCode.MAILBOX_FULL],
    )
    _ = RetryPolicy.exponential(1, max_retries=2, auto_retry_for=[OutcomeCode.TASK_TIMEOUT])
    _ = RetryPolicy.fixed([1], auto_retry_for=['MAILBOX_FULL'])  # pyright: ignore[reportArgumentType]
    _ = RetryPolicy.exponential(1, max_retries=2, auto_retry_for=['WORKER_CRASHED'])  # pyright: ignore[reportArgumentType]
    _ = RetryPolicy(max_retries=1, intervals=[1], auto_retry_for=['MAILBOX_FULL'])  # pyright: ignore[reportArgumentType]
