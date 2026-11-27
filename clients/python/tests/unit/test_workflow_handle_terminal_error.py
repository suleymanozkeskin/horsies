"""``get()`` of a workflow whose wait ends in a terminal status.

The core reads the status, the stored result and the stored error in one
statement (``CoreHandle.wait_workflow_result_async``) and answers with the
stored text: ``WorkflowCompletedRead`` for ``COMPLETED``, ``WorkflowEndedRead``
for ``FAILED``, ``CANCELLED`` and ``EXPIRED``. The handle decodes the text.
The row cannot change between a status read and a result read: there is one
read.
"""

from __future__ import annotations

import uuid

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import horsies._core as core
from horsies.core.models.outcome import Failed, ReadResult, ReadResultDeserializationError
from horsies.core.models.tasks import (
    OutcomeCode,
    TaskError,
)
from horsies.core.models.workflow.enums import WorkflowStatus
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.types.result import Err, Ok

pytestmark = [pytest.mark.unit]

WORKFLOW_ID = '0b6f3c1e-9d2a-4e7b-8c5f-1a2b3c4d5e6f'
LEGACY_EXPIRY_TEXT = 'paused_workflow_cancel_after: 1 day, 0:00:00'

TERMINAL_ERROR_STATUSES = [
    WorkflowStatus.FAILED,
    WorkflowStatus.CANCELLED,
    WorkflowStatus.EXPIRED,
]


def _handle(status: WorkflowStatus, error_row: Any) -> WorkflowHandle[Any]:
    """A handle whose core wait ends with ``status`` and the stored text of
    ``error_row`` (``result`` for ``COMPLETED``, else ``error``)."""
    match status:
        case WorkflowStatus.COMPLETED:
            record = MagicMock(spec=core.WorkflowCompletedRead)
            record.result_json = error_row.result
        case _:
            record = MagicMock(spec=core.WorkflowEndedRead)
            record.status = status.value
            record.error_json = error_row.error
    broker = MagicMock()
    broker._core_handle.wait_workflow_result_async = AsyncMock(return_value=record)
    return WorkflowHandle(workflow_id=uuid.UUID(WORKFLOW_ID), broker=broker)


def _failed_error(result: ReadResult[Any, TaskError]) -> TaskError:
    """The task error of a read that gave a failed outcome."""
    match result:
        case Ok(Failed(task_error)):
            return task_error
        case _:
            pytest.fail(f'expected a failed outcome, got {result!r}')


def _unreadable_message(result: ReadResult[Any, TaskError]) -> str:
    """The message of a read that gave ``ReadResultDeserializationError``."""
    match result:
        case Err(ReadResultDeserializationError(message=message)):
            return message
        case _:
            pytest.fail(f'expected ReadResultDeserializationError, got {result!r}')


class TestNoStoredError:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('status', 'code'),
        [
            (WorkflowStatus.FAILED, OutcomeCode.WORKFLOW_FAILED),
            (WorkflowStatus.CANCELLED, OutcomeCode.WORKFLOW_CANCELLED),
            (WorkflowStatus.EXPIRED, OutcomeCode.WORKFLOW_EXPIRED),
        ],
        ids=['FAILED', 'CANCELLED', 'EXPIRED'],
    )
    async def test_the_code_is_the_outcome_code_of_the_read_status(
        self,
        status: WorkflowStatus,
        code: OutcomeCode,
    ) -> None:
        # The error row has no status field: the error read does not read
        # the status again.
        handle = _handle(status, error_row=SimpleNamespace(error=None))

        result = await handle.get_async(wait_ms=0)

        error = _failed_error(result)
        assert error.code is code
        assert error.message == f'Workflow {status.value.lower()}'


class TestCompletedWithoutStoredResult:
    @pytest.mark.asyncio
    async def test_a_null_result_is_a_deserialization_error(self) -> None:
        # The library stores a result in the statement that sets COMPLETED,
        # so a NULL result is stored data that does not parse.
        handle = _handle(
            WorkflowStatus.COMPLETED, error_row=SimpleNamespace(result=None),
        )

        result = await handle.get_async(wait_ms=0)

        assert _unreadable_message(result) == (
            f'Workflow {WORKFLOW_ID} is COMPLETED and its stored result is NULL'
        )

    @pytest.mark.asyncio
    async def test_an_empty_result_is_a_deserialization_error(self) -> None:
        # No writer stores an empty result: it is a text that does not read.
        handle = _handle(
            WorkflowStatus.COMPLETED, error_row=SimpleNamespace(result=''),
        )

        result = await handle.get_async(wait_ms=0)

        assert _unreadable_message(result) == (
            'Workflow result JSON corrupt: JSON parse failed: the text is empty'
        )


class TestEmptyStoredError:
    @pytest.mark.asyncio
    @pytest.mark.parametrize('status', TERMINAL_ERROR_STATUSES, ids=str)
    async def test_it_is_a_deserialization_error(
        self, status: WorkflowStatus,
    ) -> None:
        # No library writer stores an empty error. Only NULL is "no stored
        # error" (TestNoStoredError).
        handle = _handle(status, error_row=SimpleNamespace(error=''))

        result = await handle.get_async(wait_ms=0)

        assert _unreadable_message(result) == (
            'Workflow error payload corrupt: JSON parse failed: the text is empty'
        )


class TestLegacyExpiryText:
    @pytest.mark.asyncio
    async def test_the_read_status_decides_the_legacy_expiry_text(self) -> None:
        handle = _handle(
            WorkflowStatus.EXPIRED,
            error_row=SimpleNamespace(error=LEGACY_EXPIRY_TEXT),
        )

        result = await handle.get_async(wait_ms=0)

        error = _failed_error(result)
        assert error.code is OutcomeCode.WORKFLOW_EXPIRED
        assert error.message == LEGACY_EXPIRY_TEXT


class TestStoredErrorThatIsNotAnObject:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('stored', 'type_name'),
        [
            ('"workflow failed"', 'str'),
            ('[1, 2]', 'list'),
            ('5', 'int'),
            ('true', 'bool'),
            ('null', 'NoneType'),
        ],
        ids=['string', 'array', 'number', 'boolean', 'null'],
    )
    async def test_it_is_a_deserialization_error_with_the_cause(
        self,
        stored: str,
        type_name: str,
    ) -> None:
        handle = _handle(
            WorkflowStatus.FAILED, error_row=SimpleNamespace(error=stored),
        )

        result = await handle.get_async(wait_ms=0)

        assert _unreadable_message(result) == (
            f'Workflow error payload is not a JSON object: got {type_name}'
        )


class TestStoredErrorObjectThatDoesNotDecode:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'stored',
        [
            # StrictJsonError: an error_code object that is not the
            # built-in code shape.
            '{"error_code": {"__builtin_task_code__": "X", "extra": 1}}',
            # ValueValidationError: message is not a string.
            '{"message": 5}',
        ],
        ids=['strict-json-refusal', 'validation-refusal'],
    )
    async def test_a_codec_refusal_is_a_deserialization_error(
        self,
        stored: str,
    ) -> None:
        handle = _handle(
            WorkflowStatus.FAILED, error_row=SimpleNamespace(error=stored),
        )

        result = await handle.get_async(wait_ms=0)

        assert _unreadable_message(result).startswith(
            'Workflow error payload validation failed: ',
        )

    @pytest.mark.asyncio
    async def test_a_defect_in_the_decode_propagates(self) -> None:
        handle = _handle(
            WorkflowStatus.FAILED,
            error_row=SimpleNamespace(error='{"message": "boom"}'),
        )

        with patch(
            'horsies.core.models.workflow.handle.decode_task_error',
            autospec=True,
            side_effect=RuntimeError('decode defect'),
        ):
            with pytest.raises(RuntimeError, match='decode defect'):
                await handle.get_async(wait_ms=0)
