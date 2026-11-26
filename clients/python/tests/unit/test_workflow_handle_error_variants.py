# pyright: reportPrivateUsage=false
"""The handle error variants: one per code, redacted, and the sites that map them.

``HandleOperationError`` is a union with one frozen variant per
``HandleErrorCode`` member. A variant's repr hides the message and the
cause. Every variant carries ``workflow_id``. Stored data that does not
parse is ``HandleStoredDataInvalid`` with its parse error. A failure that
the core answers is its generated class with the core's message, returned
as it is. ``get()`` returns a failed wait as a read failure (``ReadError``),
and the web routes map each variant onto one response.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import horsies._core as core
from horsies.core.codec.json_io import EmptyJsonTextError, SerializationError
from horsies.core.models.outcome import (
    BrokerError,
    Failed,
    ReadError,
    ResultDeserializationError,
    WorkflowNotFound,
)
from horsies.core.models.tasks import OperationalErrorCode
from horsies.core.models.workflow.enums import WorkflowStatus
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.models.workflow.handle_types import (
    HandleAsyncContext,
    HandleDbOperationFailed,
    HandleErrorCode,
    HandleOperationError,
    HandleResumeRecoveryFailed,
    HandleStoredDataInvalid,
    HandleWorkflowNotFound,
)
from horsies.core.types.result import Err, Ok, is_err, is_ok
from tests.union_members import union_members

pytestmark = [pytest.mark.unit]

MESSAGE_MARKER = 'message-marker-7d2e'
CAUSE_MARKER = 'cause-marker-7d2e'
WORKFLOW_ID = '3c9e1f4a-6b2d-4c58-8e71-0a1b2c3d4e5f'
UNKNOWN_STATUS = 'NOT_A_WORKFLOW_STATUS'
_UNKNOWN_STATUS_CAUSE = core.CoreContractViolation(
    'workflow_status', f'invalid status: {UNKNOWN_STATUS}',
)
"""The exception of a stored workflow status that does not parse."""


def _every_variant(cause: BaseException) -> list[HandleOperationError]:
    """One value of each variant; each cause field holds ``cause``."""
    message = f'failed: {MESSAGE_MARKER}'
    return [
        HandleWorkflowNotFound(message=message, workflow_id=WORKFLOW_ID),
        HandleDbOperationFailed(
            message=message, retryable=True, workflow_id=WORKFLOW_ID, exception=cause,
        ),
        HandleStoredDataInvalid(
            message=message, workflow_id=WORKFLOW_ID, exception=cause,
        ),
        HandleResumeRecoveryFailed(
            message=message, workflow_id=WORKFLOW_ID, exception=cause,
        ),
        HandleAsyncContext(message=message, workflow_id=WORKFLOW_ID),
    ]


def _variant_id(error: HandleOperationError) -> str:
    return type(error).__name__


class TestVariantCatalog:
    def test_each_code_names_exactly_one_variant(self) -> None:
        codes = [variant.code for variant in union_members(HandleOperationError)]

        assert len(codes) == len(set(codes))
        assert set(codes) == set(HandleErrorCode)

    def test_the_fixture_builds_every_variant(self) -> None:
        built = {type(error) for error in _every_variant(RuntimeError('x'))}

        assert built == set(union_members(HandleOperationError))

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError(CAUSE_MARKER)), ids=_variant_id,
    )
    def test_repr_hides_message_and_cause(self, error: HandleOperationError) -> None:
        text = repr(error)

        assert MESSAGE_MARKER not in text
        assert CAUSE_MARKER not in text
        assert type(error).__name__ in text
        assert WORKFLOW_ID in text

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('x')), ids=_variant_id,
    )
    def test_every_variant_has_code_retryable_and_the_workflow_id(
        self,
        error: HandleOperationError,
    ) -> None:
        assert isinstance(error.code, HandleErrorCode)
        assert isinstance(error.retryable, bool)
        assert error.workflow_id == WORKFLOW_ID

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('x')), ids=_variant_id,
    )
    def test_only_a_database_failure_can_be_retryable(
        self,
        error: HandleOperationError,
    ) -> None:
        match error:
            case HandleDbOperationFailed():
                assert error.retryable is True
            case _:
                assert error.retryable is False


# =============================================================================
# Sites that read the stored workflow status
# =============================================================================


def _core_record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a record of ``horsies._core``: a class pattern matches it."""
    record = MagicMock(spec=record_class)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _handle_answering(method: str, answer: object) -> WorkflowHandle[Any]:
    """A handle whose core handle answers ``method`` with ``answer``."""
    broker = MagicMock()
    setattr(broker._core_handle, method, AsyncMock(return_value=answer))
    return WorkflowHandle(workflow_id=uuid.UUID(WORKFLOW_ID), broker=broker)


class TestStoredWorkflowStatus:
    @pytest.mark.asyncio
    async def test_a_known_status_parses(self) -> None:
        handle = _handle_answering(
            'workflow_status_async',
            _core_record(core.WorkflowStatusRead, status='RUNNING'),
        )

        result = await handle.status_async()

        assert is_ok(result)
        assert result.ok_value is WorkflowStatus.RUNNING

    @pytest.mark.asyncio
    async def test_an_unknown_status_is_stored_data_invalid(self) -> None:
        invalid = HandleStoredDataInvalid(
            message=(
                f'Status failed for workflow {WORKFLOW_ID}: invalid status: {UNKNOWN_STATUS}'
            ),
            workflow_id=WORKFLOW_ID,
            exception=_UNKNOWN_STATUS_CAUSE,
        )
        handle = _handle_answering('workflow_status_async', invalid)

        result = await handle.status_async()

        assert is_err(result)
        err = result.err_value
        assert err is invalid
        assert isinstance(err, HandleStoredDataInvalid)
        assert err.exception is _UNKNOWN_STATUS_CAUSE
        assert err.workflow_id == WORKFLOW_ID
        assert UNKNOWN_STATUS in err.message


# =============================================================================
# cancel: the outcome of horsies_workflow_cancel
# =============================================================================


def _cancel_record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a record of ``horsies._core``: a class pattern matches it."""
    record = MagicMock(spec=record_class)
    record.workflow_id = uuid.UUID(WORKFLOW_ID)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _core_broker(answer: object = None, *, raises: BaseException | None = None) -> MagicMock:
    """A broker whose core handle answers the cancel with ``answer``, or raises."""
    broker = MagicMock()
    match raises:
        case None:
            broker._core_handle.cancel_workflow_async = AsyncMock(return_value=answer)
        case _:
            broker._core_handle.cancel_workflow_async = AsyncMock(side_effect=raises)
    return broker


def _handle_with_core_answer(
    answer: object = None, *, raises: BaseException | None = None,
) -> WorkflowHandle[Any]:
    """A handle on ``_core_broker``."""
    return WorkflowHandle(workflow_id=uuid.UUID(WORKFLOW_ID), broker=_core_broker(answer, raises=raises))


class TestCancelOutcome:
    """cancel_async runs one horsies_workflow_cancel in the core; its record decides."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'record_class', [core.WorkflowCancelled, core.WorkflowNotCancellable],
    )
    async def test_a_cancelled_or_terminal_workflow_is_ok(self, record_class: type) -> None:
        broker = _core_broker(_cancel_record(record_class))
        handle: WorkflowHandle[Any] = WorkflowHandle(workflow_id=uuid.UUID(WORKFLOW_ID), broker=broker)

        result = await handle.cancel_async()

        assert result == Ok(None)
        broker._core_handle.cancel_workflow_async.assert_awaited_once_with(
            uuid.UUID(WORKFLOW_ID),
        )

    @pytest.mark.asyncio
    async def test_an_unknown_workflow_is_not_found(self) -> None:
        handle = _handle_with_core_answer(_cancel_record(core.WorkflowAbsent))

        result = await handle.cancel_async()

        assert is_err(result)
        assert isinstance(result.err_value, HandleWorkflowNotFound)
        assert result.err_value.workflow_id == WORKFLOW_ID

    @pytest.mark.asyncio
    async def test_a_failure_of_the_core_is_its_class_as_it_is(self) -> None:
        cause = core.CoreConnectionError('cancel_workflow', CAUSE_MARKER, True)
        failed = HandleDbOperationFailed(
            message=f'Cancel failed for workflow {WORKFLOW_ID}: {CAUSE_MARKER}',
            retryable=True,
            workflow_id=WORKFLOW_ID,
            exception=cause,
        )
        handle = _handle_with_core_answer(failed)

        result = await handle.cancel_async()

        assert is_err(result)
        err = result.err_value
        assert err is failed
        assert isinstance(err, HandleDbOperationFailed)
        assert err.exception is cause
        assert err.retryable is True
        assert CAUSE_MARKER in err.message

    @pytest.mark.asyncio
    async def test_a_core_boundary_error_is_db_operation_failed(self) -> None:
        not_started = core.CoreStartFailed('the core runtime could not start')
        handle = _handle_with_core_answer(raises=not_started)

        result = await handle.cancel_async()

        assert is_err(result)
        assert isinstance(result.err_value, HandleDbOperationFailed)
        assert result.err_value.exception is not_started
        assert result.err_value.retryable is False


# =============================================================================
# get() returns a failed wait as a read failure
# =============================================================================


def _wait_failures() -> list[tuple[Any, ReadError]]:
    """Each failure answer of the core wait, with its read failure: a failure
    of the core keeps the core's message, its classification and its cause."""
    failed_message = f'Get failed for workflow {WORKFLOW_ID}: {CAUSE_MARKER}'
    connection_lost = core.CoreConnectionError('wait_workflow_result', CAUSE_MARKER, True)
    contract_violated = core.CoreContractViolation('wait_workflow_result', CAUSE_MARKER)
    return [
        (
            _core_record(core.WorkflowAbsent),
            WorkflowNotFound(message=f'Workflow {WORKFLOW_ID} not found'),
        ),
        (
            HandleDbOperationFailed(
                message=failed_message,
                retryable=True,
                workflow_id=WORKFLOW_ID,
                exception=connection_lost,
            ),
            BrokerError(message=failed_message, retryable=True, exception=connection_lost),
        ),
        (
            HandleStoredDataInvalid(
                message=failed_message,
                workflow_id=WORKFLOW_ID,
                exception=contract_violated,
            ),
            BrokerError(message=failed_message, retryable=False, exception=contract_violated),
        ),
    ]


@pytest.mark.parametrize(
    ('answer', 'read_error'),
    _wait_failures(),
    ids=['absent', 'db_operation_failed', 'stored_data_invalid'],
)
def test_get_returns_each_wait_failure_as_a_read_error(
    answer: Any, read_error: ReadError,
) -> None:
    handle = _handle_answering('wait_workflow_result_async', answer)

    result = asyncio.run(handle.get_async(timeout_ms=0))

    assert result == Err(read_error)


# =============================================================================
# tasks(): the stored subworkflow summary
# =============================================================================


def _task_row(summary: str | None, result_json: str | None = None) -> MagicMock:
    return _core_record(
        core.StoredNodeRecord,
        node_id='parent:0',
        task_index=0,
        task_name='child_wf',
        status='COMPLETED',
        result_json=result_json,
        started_at=None,
        completed_at=None,
        sub_workflow_id=uuid.UUID('00000000-0000-0000-0000-0000000000c1'),
        sub_workflow_summary_json=summary,
        is_subworkflow=True,
        sub_definition_key=None,
    )


def _tasks_handle(
    summary: str | None, result_json: str | None = None,
) -> WorkflowHandle[Any]:
    return _handle_answering(
        'workflow_nodes_async',
        _core_record(core.WorkflowNodesRead, nodes=[_task_row(summary, result_json)]),
    )


class TestStoredSubworkflowSummary:
    @pytest.mark.asyncio
    async def test_a_valid_summary_parses(self) -> None:
        handle = _tasks_handle(json.dumps({
            'status': 'COMPLETED',
            'total_tasks': 1,
            'completed_tasks': 1,
            'failed_tasks': 0,
            'skipped_tasks': 0,
        }))

        result = await handle.tasks_async()

        assert is_ok(result)
        summary = result.ok_value[0].sub_workflow_summary
        assert summary is not None
        assert summary.status is WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_a_stored_null_is_no_summary(self) -> None:
        handle = _tasks_handle(None)

        result = await handle.tasks_async()

        assert is_ok(result)
        assert result.ok_value[0].sub_workflow_summary is None

    @pytest.mark.asyncio
    async def test_an_empty_summary_is_stored_data_invalid(self) -> None:
        handle = _tasks_handle('')

        result = await handle.tasks_async()

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleStoredDataInvalid)
        assert err.code is HandleErrorCode.STORED_DATA_INVALID
        assert not err.retryable
        assert isinstance(err.exception, EmptyJsonTextError)
        assert err.message == (
            'sub_workflow_summary JSON corrupt for node parent:0: '
            'JSON parse failed: the text is empty'
        )

    @pytest.mark.asyncio
    async def test_corrupt_summary_json_is_stored_data_invalid(self) -> None:
        handle = _tasks_handle('{not valid json')

        result = await handle.tasks_async()

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleStoredDataInvalid)
        assert isinstance(err.exception, SerializationError)
        assert 'sub_workflow_summary JSON corrupt for node parent:0' in err.message

    @pytest.mark.asyncio
    async def test_a_summary_past_the_depth_bound_is_stored_data_invalid(self) -> None:
        handle = _tasks_handle('{"status":' + '[' * 1_100 + ']' * 1_100 + '}')

        result = await handle.tasks_async()

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleStoredDataInvalid)
        assert isinstance(err.exception, SerializationError)
        assert 'nested deeper than' in err.message

    @pytest.mark.asyncio
    async def test_an_invalid_summary_is_stored_data_invalid(self) -> None:
        handle = _tasks_handle('{"status": "NOT_A_STATUS", "total_tasks": 1}')

        result = await handle.tasks_async()

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleStoredDataInvalid)
        assert isinstance(err.exception, ValueError)
        assert not isinstance(err.exception, SerializationError)
        assert 'sub_workflow_summary invalid for node parent:0' in err.message
        assert 'NOT_A_STATUS' in str(err.exception)


class TestStoredNodeResult:
    """tasks(): NULL is a node with no result; an empty text is corrupt data."""

    @pytest.mark.asyncio
    async def test_a_stored_null_is_no_result(self) -> None:
        handle = _tasks_handle(None, result_json=None)

        result = await handle.tasks_async()

        assert is_ok(result)
        assert result.ok_value[0].result is None

    @pytest.mark.asyncio
    async def test_an_empty_result_is_a_deserialization_error(self) -> None:
        handle = _tasks_handle(None, result_json='')

        result = await handle.tasks_async()

        assert is_ok(result)
        node_result = result.ok_value[0].result
        assert isinstance(node_result, Failed)
        error = node_result.error
        assert error.code is OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        assert error.message == (
            'Result JSON corrupt for node parent:0: '
            'JSON parse failed: the text is empty'
        )

    @pytest.mark.asyncio
    async def test_a_result_that_is_not_json_names_the_node(self) -> None:
        handle = _tasks_handle(None, result_json='{not valid json')

        result = await handle.tasks_async()

        assert is_ok(result)
        node_result = result.ok_value[0].result
        assert isinstance(node_result, Failed)
        error = node_result.error
        assert error.code is OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        assert error.message is not None
        assert error.message.startswith(
            'Result JSON corrupt for node parent:0: JSON parse failed: ',
        )


# =============================================================================
# Web: each variant maps to one response
# =============================================================================


class TestStoredWorkflowResult:
    @pytest.mark.asyncio
    async def test_a_result_past_the_depth_bound_is_a_deserialization_error(
        self,
    ) -> None:
        """Before, `RecursionError` left `get()`."""
        stored = (
            '{"__h_task_result__":true,"ok":'
            + '[' * 1_100 + ']' * 1_100
            + ',"err":null}'
        )
        broker = MagicMock()
        handle: WorkflowHandle[Any] = WorkflowHandle(workflow_id=uuid.UUID(WORKFLOW_ID), broker=broker)
        handle.out_type = int

        result = handle._completed_result(stored)

        match result:
            case Err(ResultDeserializationError(message=message)):
                assert 'nested deeper than' in message
            case _:
                pytest.fail(f'expected Err(ResultDeserializationError), got {result!r}')


