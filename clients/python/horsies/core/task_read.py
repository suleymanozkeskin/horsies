"""The read of a task's stored result as a ``ReadResult``.

``TaskHandle.get`` and ``Horsies.get_result`` read the broker's raw result
record of a task; this module turns that record into the ``ReadResult`` that
both return. It does no I/O.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, assert_never
from uuid import UUID

from horsies.core.brokers.result_types import (
    DatabaseAsyncContext,
    DatabaseEnqueueFailed,
    DatabaseIdempotencyKeyConflict,
    DatabaseInvalidInput,
    DatabaseInvalidJsonPayload,
    DatabaseInvalidTaskOptions,
    DatabaseListenerDisabled,
    DatabaseNoTypeAvailable,
    DatabaseNotAttached,
    DatabaseOperationError,
    DatabaseOperationFailed,
    DatabasePayloadMismatch,
    DatabaseResult,
    DatabaseWaitInsideTask,
    StoredResult,
)
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.typed import (
    Json,
    TypeAnnotation,
    decode_task_error,
    decode_task_result,
    validate_task_result_envelope,
)
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.models.outcome import (
    Failed,
    ReadAsyncContext,
    ReadDatabaseError,
    ReadError,
    ReadInvalidInput,
    ReadNoTypeAvailable,
    ReadResult,
    ReadResultDeserializationError,
    ReadResultNotAvailable,
    ReadResultNotReady,
    ReadTaskNotFound,
    ReadUnknownCustomCode,
    ReadWaitInsideTask,
    ReadWaitTimeout,
    ReadWorkflowContextMissingId,
    ReadWorkflowNotFound,
    Succeeded,
    TaskOutcome,
)
from horsies.core.models.tasks import (
    BUILTIN_FAMILIES,
    CustomCodeType,
    EnumCodes,
    OutcomeCode,
    TaskError,
    TextCodes,
)
from horsies.core.types.result import Err, Ok, Result
from horsies.core.types.status import TaskStatus

__all__ = [
    'is_final_read',
    'ok_of_outcome',
    'read_of_record',
    'typed_outcome',
]


_TERMINAL_WITH_RESULT = (
    TaskStatus.COMPLETED,
    TaskStatus.FAILED,
    TaskStatus.EXPIRED,
)
"""The terminal statuses whose row stores a result."""


def is_final_read(read: ReadResult[Any, TaskError]) -> bool:
    """Whether a handle keeps ``read``: a fact of the stored data, not of the
    moment of the read. Pure.

    An outcome, ``ReadResultNotAvailable``, ``ReadResultDeserializationError`` and
    ``ReadNoTypeAvailable`` are final. A wait that ran out, a task not found, a
    refused wait and a broker failure are not: a later read may succeed.
    """
    match read:
        case Ok():
            return True
        case Err(err_value=error):
            return _is_final_error(error)


def _is_final_error(error: ReadError) -> bool:
    """``is_final_read`` for a read failure. Pure."""
    match error:
        case (
            ReadResultNotAvailable()
            | ReadResultDeserializationError()
            | ReadUnknownCustomCode()
            | ReadNoTypeAvailable()
        ):
            return True
        case (
            ReadWaitTimeout()
            | ReadTaskNotFound()
            | ReadWorkflowNotFound()
            | ReadResultNotReady()
            | ReadWaitInsideTask()
            | ReadAsyncContext()
            | ReadInvalidInput()
            | ReadDatabaseError()
            | ReadWorkflowContextMissingId()
        ):
            return False
        case _ as unreachable:
            assert_never(unreachable)


def _read_error_of_broker(task_id: UUID, error: DatabaseOperationError) -> ReadError:
    """The read failure of a failed broker result read. Pure.

    A stored payload that does not read and a missing type are facts of the
    stored data. ``ASYNC_CONTEXT`` is a defect of the caller: both readers
    refuse a running event loop before a sync broker call, and the async
    broker call does not return it.
    """
    match error:
        case DatabaseNoTypeAvailable(message=message):
            return ReadNoTypeAvailable(message=message)
        case DatabaseInvalidJsonPayload(message=message):
            return ReadResultDeserializationError(message=message)
        case DatabaseWaitInsideTask(message=message):
            return ReadWaitInsideTask(message=message)
        case DatabaseAsyncContext():
            raise AssertionError(
                f'the result read of task {task_id} got ASYNC_CONTEXT; the '
                'reader refuses a running event loop before a sync broker call',
            )
        case (
            DatabaseOperationFailed()
            | DatabaseEnqueueFailed()
            | DatabasePayloadMismatch()
            | DatabaseIdempotencyKeyConflict()
            | DatabaseNotAttached()
            | DatabaseInvalidTaskOptions()
            | DatabaseInvalidInput()
            | DatabaseListenerDisabled()
        ):
            return ReadDatabaseError(
                message=(
                    f'the result read of task {task_id} failed '
                    f'({error.code.value}): {error.message}'
                ),
                retryable=error.retryable,
                cause=error.failure_cause(),
            )
        case _ as unreachable:
            assert_never(unreachable)


def read_of_record(
    task_id: UUID,
    broker_result: DatabaseResult[StoredResult | None],
    ok_type: TypeAnnotation | None,
    wait_ms: int | None,
    code_type: CustomCodeType,
) -> ReadResult[Any, TaskError[Any]]:
    """The read of a task from the broker's raw result record. Pure.

    Outcomes:
        - A failed broker read: its read failure (``_read_error_of_broker``).
        - No row: ``ReadTaskNotFound``.
        - A ``CANCELLED`` row: ``Ok(Failed(TASK_CANCELLED))``.
        - No stored result: ``ReadResultNotAvailable`` for a terminal status,
          ``ReadWaitTimeout`` for any other (the wait ran out first).
        - A stored result: the outcome of ``_read_of_envelope``.

    ``ok_type`` is the declared value type of the task, or ``None`` when the
    reader has none. ``code_type`` is the type of its custom codes: with
    ``EnumCodes(E)``, a stored custom code reads as the member of ``E``, and
    a code that ``E`` does not name is ``ReadUnknownCustomCode``.
    """
    match broker_result:
        case Err(err_value=broker_error):
            return Err(_read_error_of_broker(task_id, broker_error))
        case Ok(ok_value=found):
            match found:
                case None:
                    return Err(ReadTaskNotFound(
                        message=f'Task {task_id} not found in database',
                    ))
                case StoredResult():
                    return _read_of_row(task_id, found, ok_type, wait_ms, code_type)


def _read_of_row(
    task_id: UUID,
    record: StoredResult,
    ok_type: TypeAnnotation | None,
    wait_ms: int | None,
    code_type: CustomCodeType,
) -> ReadResult[Any, TaskError[Any]]:
    """``read_of_record`` for a row that the broker found. Pure."""
    if record.status == TaskStatus.CANCELLED:
        return Ok(Failed(TaskError(
            code=OutcomeCode.TASK_CANCELLED,
            message=f'Task {task_id} was cancelled before completion',
            data={'task_id': str(task_id)},
        )))
    match record.raw_result:
        case None if record.status in _TERMINAL_WITH_RESULT:
            return Err(ReadResultNotAvailable(message=(
                f'Task {task_id} reached terminal status '
                f'{record.status.value} but stored no result payload'
            )))
        case None:
            return Err(ReadWaitTimeout(message=(
                f'Timed out waiting for task {task_id} after {wait_ms}ms; '
                f'its status is {record.status.value} and it may still run'
            )))
        case raw:
            match _read_of_envelope(task_id, raw, ok_type):
                case Ok(ok_value=outcome):
                    return _typed_read(task_id, outcome, code_type)
                case Err() as failed:
                    return failed


def _read_of_envelope(
    task_id: UUID,
    raw: Json,
    ok_type: TypeAnnotation | None,
) -> ReadResult[Any, TaskError]:
    """The read of a stored result envelope. Pure.

    The envelope shape is checked before either slot is read, so a malformed
    envelope (no marker, both slots set, other keys) fails closed. The err
    slot reads without ``ok_type``: ``TaskError`` has a fixed schema. The ok
    slot needs ``ok_type``: ``ReadNoTypeAvailable`` without it.
    """
    try:
        envelope = validate_task_result_envelope(raw)
    except StrictJsonError as exc:
        return Err(ReadResultDeserializationError(
            message=f'TaskResult envelope invalid for task {task_id}: {exc}',
        ))
    err_slot = envelope.get('err')
    if err_slot is not None:
        try:
            return Ok(Failed(decode_task_error(err_slot)))
        except (StrictJsonError, ValueValidationError) as exc:
            return Err(ReadResultDeserializationError(
                message=f'TaskError decode failed for task {task_id}: {exc}',
            ))
    match ok_type:
        case None:
            return Err(ReadNoTypeAvailable(message=(
                f'Task {task_id} ok-result decode requires a declared ok type; '
                'the reader has none (task not registered in this process). '
                'Read the raw result instead.'
            )))
        case declared:
            try:
                return ok_of_outcome(decode_task_result(raw, declared))
            except (StrictJsonError, ValueValidationError) as exc:
                return Err(ReadResultDeserializationError(
                    message=f'decode_task_result failed for task {task_id}: {exc}',
                ))


def _typed_read(
    task_id: UUID,
    outcome: TaskOutcome[Any, TaskError],
    code_type: CustomCodeType,
) -> ReadResult[Any, TaskError[Any]]:
    """The read of a decoded outcome with the custom codes of ``code_type``. Pure."""
    match typed_outcome(outcome, code_type):
        case Ok(ok_value=typed):
            return ok_of_outcome(typed)
        case Err(err_value=unknown):
            return Err(ReadUnknownCustomCode(
                message=f'the stored error of task {task_id}: {unknown.message}',
                custom_code=unknown.custom_code,
            ))


def typed_outcome(
    outcome: TaskOutcome[Any, TaskError],
    code_type: CustomCodeType,
) -> Result[TaskOutcome[Any, TaskError[Any]], ReadUnknownCustomCode]:
    """``outcome`` with its custom code as a member of the declared ``Enum``. Pure.

    A decode gives a custom code as its stored text. With ``TextCodes`` the
    outcome stays as it is. With ``EnumCodes(E)``, the text of a failed
    outcome becomes the member of ``E`` that has it as its value; a text
    that no member has is ``ReadUnknownCustomCode``. A built-in code and a
    succeeded outcome stay as they are.
    """
    match outcome:
        case Succeeded():
            return Ok(outcome)
        case Failed(error=error):
            match code_type:
                case TextCodes():
                    return Ok(outcome)
                case EnumCodes(code_type=enum_class):
                    return _typed_failed(error, enum_class)


def _typed_failed(
    error: TaskError, enum_class: type[Enum],
) -> Result[TaskOutcome[Any, TaskError[Any]], ReadUnknownCustomCode]:
    """``typed_outcome`` of a failed outcome with the codes of ``enum_class``. Pure."""
    match error.code:
        case code if isinstance(code, BUILTIN_FAMILIES) or isinstance(code, enum_class):
            return Ok(Failed(error))
        case str() as text:
            pass
        case _ as unreachable:
            assert_never(unreachable)
    members = {member.value: member for member in enum_class}
    match members.get(text):
        case None:
            return Err(ReadUnknownCustomCode(
                message=f'custom code {text!r} is not a member of {enum_class.__name__}',
                custom_code=text,
            ))
        case member:
            return Ok(Failed(error.model_copy(update={'code': member})))


def ok_of_outcome[T, E](outcome: TaskOutcome[T, E]) -> ReadResult[T, E]:
    """``Ok`` of an outcome, typed as one member of ``ReadResult``. Pure."""
    match outcome:
        case Succeeded():
            return Ok(outcome)
        case Failed():
            return Ok(outcome)
