"""The error classes generated from horsies-core keep the public shape.

`horsies-codegen` renders the classes of the broker, workflow handle,
monitoring query and task action catalogs into `horsies._catalog` from the
core's records (`horsies_core::error_catalog`). The old modules re-export
them. This test pins, for each class, that the public name is the
generated class, and that its fields (with their `repr` flags), its `code`
class value and its `retryable` class value are the ones of the
hand-written class that it replaced.
"""

from __future__ import annotations

import dataclasses
from typing import Final, NamedTuple

import pytest

from horsies._catalog import broker_errors, monitoring_errors, workflow_handle_errors
from horsies._catalog.error_detail import NoCause as GeneratedNoCause
from horsies.core.brokers import result_types
from horsies.core.models.workflow import handle_types
from horsies.core.types import error_detail
from horsies.monitoring import errors, task_actions

pytestmark = [pytest.mark.unit]


class ClassShape(NamedTuple):
    public: type
    generated: type
    #: (field name, shown by repr), in order.
    fields: tuple[tuple[str, bool], ...]
    #: The member name of the `code` class value; None for a code field.
    code_member: str | None
    never_retryable: bool


SHAPES: Final[tuple[ClassShape, ...]] = (
    ClassShape(result_types.BrokerOperationFailed, broker_errors.BrokerOperationFailed, (('code', True), ('message', False), ('retryable', True), ('exception', False),), None, never_retryable=False),
    ClassShape(result_types.BrokerEnqueueFailed, broker_errors.BrokerEnqueueFailed, (('message', False), ('retryable', True), ('exception', False),), 'ENQUEUE_FAILED', never_retryable=False),
    ClassShape(result_types.BrokerPayloadMismatch, broker_errors.BrokerPayloadMismatch, (('message', False),), 'PAYLOAD_MISMATCH', never_retryable=True),
    ClassShape(result_types.BrokerIdempotencyKeyConflict, broker_errors.BrokerIdempotencyKeyConflict, (('message', False), ('owner_task_id', True),), 'IDEMPOTENCY_KEY_CONFLICT', never_retryable=True),
    ClassShape(result_types.BrokerInvalidJsonPayload, broker_errors.BrokerInvalidJsonPayload, (('message', False), ('exception', False),), 'INVALID_JSON_PAYLOAD', never_retryable=True),
    ClassShape(result_types.BrokerInvalidTaskOptions, broker_errors.BrokerInvalidTaskOptions, (('message', False), ('exception', False),), 'INVALID_TASK_OPTIONS', never_retryable=True),
    ClassShape(result_types.BrokerInvalidInput, broker_errors.BrokerInvalidInput, (('message', False), ('exception', False),), 'INVALID_INPUT', never_retryable=True),
    ClassShape(result_types.BrokerAsyncContext, broker_errors.BrokerAsyncContext, (('message', False),), 'ASYNC_CONTEXT', never_retryable=True),
    ClassShape(result_types.BrokerListenerDisabled, broker_errors.BrokerListenerDisabled, (('message', False),), 'LISTENER_DISABLED', never_retryable=True),
    ClassShape(result_types.BrokerWaitInsideTask, broker_errors.BrokerWaitInsideTask, (('message', False),), 'WAIT_INSIDE_TASK', never_retryable=True),
    ClassShape(handle_types.HandleWorkflowNotFound, workflow_handle_errors.HandleWorkflowNotFound, (('message', False), ('workflow_id', True),), 'WORKFLOW_NOT_FOUND', never_retryable=True),
    ClassShape(handle_types.HandleDbOperationFailed, workflow_handle_errors.HandleDbOperationFailed, (('message', False), ('retryable', True), ('workflow_id', True), ('exception', False),), 'DB_OPERATION_FAILED', never_retryable=False),
    ClassShape(handle_types.HandleStoredDataInvalid, workflow_handle_errors.HandleStoredDataInvalid, (('message', False), ('workflow_id', True), ('exception', False),), 'STORED_DATA_INVALID', never_retryable=True),
    ClassShape(handle_types.HandleResumeRecoveryFailed, workflow_handle_errors.HandleResumeRecoveryFailed, (('message', False), ('workflow_id', True), ('exception', False),), 'RESUME_RECOVERY_FAILED', never_retryable=True),
    ClassShape(handle_types.HandleAsyncContext, workflow_handle_errors.HandleAsyncContext, (('message', False), ('workflow_id', True),), 'ASYNC_CONTEXT', never_retryable=True),
    ClassShape(errors.MonitoringQueryDbOperationFailed, monitoring_errors.MonitoringQueryDbOperationFailed, (('message', False), ('retryable', True), ('exception', False),), 'DB_OPERATION_FAILED', never_retryable=False),
    ClassShape(errors.MonitoringQueryHistoryContractViolation, monitoring_errors.MonitoringQueryHistoryContractViolation, (('message', False), ('exception', False),), 'HISTORY_CONTRACT_VIOLATION', never_retryable=True),
    ClassShape(task_actions.TaskActionNotFound, monitoring_errors.TaskActionNotFound, (('message', False), ('task_id', True),), 'TASK_NOT_FOUND', never_retryable=True),
    ClassShape(task_actions.TaskActionNotCancellable, monitoring_errors.TaskActionNotCancellable, (('message', False), ('task_id', True), ('current_status', True),), 'TASK_NOT_CANCELLABLE', never_retryable=True),
    ClassShape(task_actions.TaskActionWorkflowTask, monitoring_errors.TaskActionWorkflowTask, (('message', False), ('task_id', True), ('current_status', True),), 'TASK_IS_WORKFLOW_TASK', never_retryable=True),
    ClassShape(task_actions.TaskActionDbOperationFailed, monitoring_errors.TaskActionDbOperationFailed, (('message', False), ('retryable', True), ('task_id', True), ('exception', False),), 'DB_OPERATION_FAILED', never_retryable=False),
    ClassShape(task_actions.TaskActionStoredDataInvalid, monitoring_errors.TaskActionStoredDataInvalid, (('message', False), ('task_id', True), ('exception', False),), 'STORED_DATA_INVALID', never_retryable=True),
)


@pytest.mark.parametrize('shape', SHAPES, ids=lambda shape: shape.generated.__name__)
class TestGeneratedErrorClass:
    def test_the_public_name_is_the_generated_class(self, shape: ClassShape) -> None:
        assert shape.public is shape.generated

    def test_the_fields_and_their_repr_flags_are_the_old_ones(self, shape: ClassShape) -> None:
        fields = tuple((item.name, item.repr) for item in dataclasses.fields(shape.generated))

        assert fields == shape.fields

    def test_the_class_values_are_the_old_ones(self, shape: ClassShape) -> None:
        match shape.code_member:
            case None:
                assert 'code' in {item.name for item in dataclasses.fields(shape.generated)}
            case member:
                assert getattr(shape.generated, 'code').name == member
        match shape.never_retryable:
            case True:
                assert getattr(shape.generated, 'retryable') is False
            case False:
                assert 'retryable' in {item.name for item in dataclasses.fields(shape.generated)}

    def test_the_class_is_a_frozen_dataclass_with_slots(self, shape: ClassShape) -> None:
        params = getattr(shape.generated, '__dataclass_params__')

        assert params.frozen is True
        assert '__slots__' in vars(shape.generated)


def test_no_cause_is_the_generated_class_without_fields() -> None:
    assert error_detail.NoCause is GeneratedNoCause
    assert dataclasses.fields(GeneratedNoCause) == ()
    assert GeneratedNoCause() == GeneratedNoCause()


def test_each_public_union_is_the_generated_union_plus_the_client_only_classes() -> None:
    assert result_types.BrokerOperationError.__value__ == (
        broker_errors.BrokerCoreError
        | result_types.BrokerNotAttached
        | result_types.BrokerNoTypeAvailable
        | result_types.BrokerEventLoopLimit
        | result_types.BrokerLoopRunnerFailed
    )
    assert result_types.BrokerEnqueueError.__value__ == (
        broker_errors.BrokerCoreEnqueueError | result_types.BrokerEventLoopLimit
    )
    assert handle_types.HandleOperationError.__value__ == (
        workflow_handle_errors.HandleCoreError
        | handle_types.HandleLoopRunnerFailed
        | handle_types.HandleEventLoopLimit
    )
    assert errors.MonitoringQueryError.__value__ == (
        monitoring_errors.MonitoringQueryCoreError | errors.MonitoringQueryEventLoopLimit
    )
    assert task_actions.TaskActionError.__value__ == (
        monitoring_errors.TaskActionCoreError | task_actions.TaskActionEventLoopLimit
    )
