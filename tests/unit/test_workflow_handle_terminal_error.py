"""``get()`` of a workflow whose wait ends in a terminal status.

The core reads the status, the stored result and the stored error in one
statement (``CoreHandle.wait_workflow_result_async``) and answers with the
stored text: ``WorkflowCompletedRead`` for ``COMPLETED``, ``WorkflowEndedRead``
for ``FAILED``, ``CANCELLED`` and ``EXPIRED``. The handle decodes the text.
The row cannot change between a status read and a result read: there is one
read.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import horsies._core as core
from horsies.core.models.tasks import (
    OperationalErrorCode,
    OutcomeCode,
)
from horsies.core.models.workflow.enums import WorkflowStatus
from horsies.core.models.workflow.handle import WorkflowHandle

pytestmark = [pytest.mark.unit]

WORKFLOW_ID = '0b6f3c1e-9d2a-4e7b-8c5f-1a2b3c4d5e6f'
LEGACY_EXPIRY_TEXT = 'paused_workflow_auto_cancel_after: 1 day, 0:00:00'

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
    return WorkflowHandle(workflow_id=WORKFLOW_ID, broker=broker)


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

        result = await handle.get_async(timeout_ms=0)

        error = result.unwrap_err()
        assert error.error_code is code
        assert error.message == f'Workflow {status.value.lower()}'


class TestCompletedWithoutStoredResult:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('stored', 'stored_form'),
        [(None, 'NULL'), ('', 'empty')],
        ids=['null', 'empty'],
    )
    async def test_it_is_a_deserialization_error(
        self,
        stored: str | None,
        stored_form: str,
    ) -> None:
        # The library stores a result in the statement that sets COMPLETED,
        # so a NULL or empty result is stored data that does not parse.
        handle = _handle(
            WorkflowStatus.COMPLETED, error_row=SimpleNamespace(result=stored),
        )

        result = await handle.get_async(timeout_ms=0)

        error = result.unwrap_err()
        assert error.error_code is OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        assert error.message == (
            f'Workflow {WORKFLOW_ID} is COMPLETED and its stored result is '
            f'{stored_form}'
        )
        assert error.data == {'workflow_id': WORKFLOW_ID}


class TestEmptyStoredError:
    @pytest.mark.asyncio
    @pytest.mark.parametrize('status', TERMINAL_ERROR_STATUSES, ids=str)
    async def test_it_is_a_deserialization_error(
        self, status: WorkflowStatus,
    ) -> None:
        # No library writer stores an empty error. Only NULL is "no stored
        # error" (TestNoStoredError).
        handle = _handle(status, error_row=SimpleNamespace(error=''))

        result = await handle.get_async(timeout_ms=0)

        error = result.unwrap_err()
        assert error.error_code is OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        assert error.message == 'Workflow error payload is empty'
        assert error.data == {'workflow_id': WORKFLOW_ID}


class TestLegacyExpiryText:
    @pytest.mark.asyncio
    async def test_the_read_status_decides_the_legacy_expiry_text(self) -> None:
        handle = _handle(
            WorkflowStatus.EXPIRED,
            error_row=SimpleNamespace(error=LEGACY_EXPIRY_TEXT),
        )

        result = await handle.get_async(timeout_ms=0)

        error = result.unwrap_err()
        assert error.error_code is OutcomeCode.WORKFLOW_EXPIRED
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

        result = await handle.get_async(timeout_ms=0)

        error = result.unwrap_err()
        assert error.error_code is OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        assert error.message == (
            f'Workflow error payload is not a JSON object: got {type_name}'
        )
        assert error.data == {'workflow_id': WORKFLOW_ID}


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

        result = await handle.get_async(timeout_ms=0)

        error = result.unwrap_err()
        assert error.error_code is OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        assert (error.message or '').startswith(
            'Workflow error payload validation failed: ',
        )
        assert error.data == {'workflow_id': WORKFLOW_ID}

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
                await handle.get_async(timeout_ms=0)
