"""The client's part of the workflow start's resend and of its failure answer.

The core resends a start after a transient failure when the caller asks for
it (``PostgresBroker::start_client_workflow``, tested in
``crates/horsies-core/src/broker/workflow_start.rs``). The client passes the
``resend_on_transient_err`` choice into the ``WorkflowStartRequest``
unchanged, and maps the core's answer: a failure record keeps its retry rule
and its cause, an exception of the core call that is not a ``CoreError``
propagates, and a refused prevalidation makes no core call. The core handle
is faked.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import horsies._core as core
from horsies.core.types.result import Ok, is_err, is_ok
from horsies.core.workflows.lifecycle import start_workflow, start_workflow_async
from horsies.core.workflows.start_types import (
    WorkflowStartEnqueueFailed,
    WorkflowStartErrorCode,
    WorkflowStartValidationFailed,
)

WORKFLOW_ID = '9f1b7a2c-0d4e-4f61-8a23-5c6d7e8f9a0b'
"""A caller-supplied id; workflow identity is a UUID."""


def _make_spec(name: str = 'test-wf') -> Any:
    """A minimal mock spec that passes prevalidation."""
    spec = MagicMock()
    spec.name = name
    spec.tasks = []
    spec.workflow_def_cls = None
    spec.output = None
    spec.success_policy = None
    spec.on_error.value = 'FAIL_WORKFLOW'
    spec.definition_key = None
    return spec


def _broker(answer: object = None, *, raises: BaseException | None = None) -> Any:
    """A broker whose core handle answers the start with ``answer``, or
    raises ``raises``."""
    broker = MagicMock()
    match raises:
        case None:
            broker._core_handle.start_workflow_async = AsyncMock(return_value=answer)
        case _:
            broker._core_handle.start_workflow_async = AsyncMock(side_effect=raises)
    return broker


def _failure_record(cause: BaseException, *, retryable: bool) -> MagicMock:
    """A stand-in for the core's ``WorkflowStartFailed``: a class pattern matches it."""
    record = MagicMock(spec=core.WorkflowStartFailed)
    record.workflow_id = uuid.UUID(WORKFLOW_ID)
    record.message = f'start failed: {cause}'
    record.retryable = retryable
    record.exception = cause
    return record


async def _start_with_spec_data(broker: Any, **options: Any) -> Any:
    """``start_workflow_async`` with the spec data faked: the mock spec has
    no node rows to encode."""
    with (
        patch('horsies.core.workflows.start_tree.top_node_rows', return_value=()),
        patch(
            'horsies.core.workflows.spec_data.workflow_spec_json',
            return_value=Ok('{}'),
        ),
        patch('horsies._core.WorkflowStartRequest'),
    ):
        return await start_workflow_async(_make_spec(), broker, WORKFLOW_ID, **options)


# =============================================================================
# The failure answer of the core
# =============================================================================


@pytest.mark.unit
class TestTransactionCatch:
    """A database failure of the start is an enqueue failure with its retry
    rule; any other exception of the core call is a defect."""

    @pytest.mark.asyncio
    async def test_a_driver_error_is_an_enqueue_failure_with_its_classification(
        self,
    ) -> None:
        cause = core.CoreConnectionError('start_workflow', 'server closed the connection', True)
        broker = _broker(_failure_record(cause, retryable=True))

        result = await _start_with_spec_data(broker)

        assert is_err(result)
        assert isinstance(result.err_value, WorkflowStartEnqueueFailed)
        assert result.err_value.exception is cause
        assert result.err_value.retryable is True

    @pytest.mark.asyncio
    async def test_a_sqlalchemy_error_is_an_enqueue_failure(self) -> None:
        """A database error that cannot clear (a unique violation) keeps
        ``retryable=False``."""
        cause = core.CoreServerError('start_workflow', '23505', 'duplicate key', False)
        broker = _broker(_failure_record(cause, retryable=False))

        result = await _start_with_spec_data(broker)

        assert is_err(result)
        assert isinstance(result.err_value, WorkflowStartEnqueueFailed)
        assert result.err_value.exception is cause
        assert result.err_value.retryable is False

    @pytest.mark.asyncio
    async def test_a_defect_in_the_transaction_propagates(self) -> None:
        defect = TypeError('unsupported operand type')
        broker = _broker(raises=defect)

        with pytest.raises(TypeError) as exc_info:
            await _start_with_spec_data(broker, resend_on_transient_err=True)

        assert exc_info.value is defect

    @pytest.mark.asyncio
    async def test_validation_error_never_retried(self) -> None:
        """A refused prevalidation is never sent: the core makes no start, so
        it cannot resend one."""
        spec = _make_spec()
        bad_task = MagicMock()
        bad_task.queue = None
        bad_task.priority = 100
        bad_task.name = 'bad-task'
        spec.tasks = [bad_task]
        broker = _broker()

        result = await start_workflow_async(
            spec, broker, WORKFLOW_ID, resend_on_transient_err=True,
        )

        assert is_err(result)
        assert isinstance(result.err_value, WorkflowStartValidationFailed)
        assert result.err_value.code == WorkflowStartErrorCode.VALIDATION_FAILED
        assert result.err_value.retryable is False
        broker._core_handle.start_workflow_async.assert_not_awaited()


# =============================================================================
# The resend choice reaches the start request unchanged
# =============================================================================


@pytest.mark.unit
class TestAsyncForwarding:
    """start_workflow_async passes resend_on_transient_err into the start
    request of its core call (``CoreHandle.start_workflow_async``)."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize('resend', [True, False])
    async def test_async_forwards_flag_to_the_start_request(self, resend: bool) -> None:
        broker = _broker(MagicMock(spec=core.WorkflowStarted))

        with (
            patch('horsies.core.workflows.start_tree.top_node_rows', return_value=()),
            patch(
                'horsies.core.workflows.spec_data.workflow_spec_json',
                return_value=Ok('{}'),
            ),
            patch('horsies._core.WorkflowStartRequest') as request_class,
        ):
            result = await start_workflow_async(
                _make_spec(), broker, WORKFLOW_ID, resend_on_transient_err=resend,
            )

        assert is_ok(result)
        assert request_class.call_args.kwargs['resend_on_transient_err'] is resend
        broker._core_handle.start_workflow_async.assert_awaited_once_with(
            request_class.return_value,
        )


@pytest.mark.unit
class TestSyncWrapperForwarding:
    """start_workflow forwards resend_on_transient_err to the start request of
    its blocking core call (``CoreHandle.start_workflow``)."""

    @pytest.mark.parametrize('resend', [True, False])
    def test_sync_forwards_flag_to_the_start_request(self, resend: bool) -> None:
        """start_workflow passes resend_on_transient_err into the request."""
        spec = _make_spec()
        broker = MagicMock()
        broker._core_handle.start_workflow.return_value = MagicMock(spec=core.WorkflowStarted)

        with (
            patch('horsies.core.workflows.start_tree.top_node_rows', return_value=[]),
            patch(
                'horsies.core.workflows.spec_data.workflow_spec_json',
                return_value=Ok('{}'),
            ),
            patch('horsies._core.WorkflowStartRequest') as request_class,
        ):
            result = start_workflow(
                spec, broker, WORKFLOW_ID, resend_on_transient_err=resend,
            )

        assert is_ok(result)
        assert request_class.call_args.kwargs['resend_on_transient_err'] is resend
        broker._core_handle.start_workflow.assert_called_once_with(
            request_class.return_value,
        )
        broker._core_handle.start_workflow_async.assert_not_called()
