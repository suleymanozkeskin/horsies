"""The error classes generated from horsies-core keep the public shape.

`horsies-codegen` renders the classes of the broker, workflow handle,
monitoring query, task action, task send and workflow start catalogs into
`horsies._catalog` from the core's records (`horsies_core::error_catalog`),
with the payload record of a send and the retention class key. The old
modules re-export them. This test pins, for each class, that the public
name is the generated class, and that its fields (with their `repr` flags),
its `code` class value and its `retryable` class value are the ones of the
hand-written class that it replaced. It also pins the generated exception
classes ``OutcomeDecodeError`` and ``HistoryContractError`` to the shape of
the classes that they replaced.
"""

from __future__ import annotations

import dataclasses
from enum import Enum
from typing import Final, NamedTuple

import pytest

import horsies
from horsies._catalog import (
    broker_errors,
    history_errors,
    lifecycle_errors,
    monitoring_errors,
    retention_class,
    task_send_errors,
    workflow_handle_errors,
    workflow_start_errors,
)
from horsies._catalog.error_detail import NoCause as GeneratedNoCause
from horsies.core.brokers import result_types
from horsies.core.history import errors as history_error_module
from horsies.core.history import periods
from horsies.core.history.ddl import classes
from horsies.core.lifecycle import outcomes
from horsies.core.models import task_send_types
from horsies.core.models.workflow import handle_types
from horsies.core.types import error_detail
from horsies.core.workflows import start_types
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
    ClassShape(start_types.WorkflowStartValidationFailed, workflow_start_errors.WorkflowStartValidationFailed, (('message', False), ('workflow_name', True), ('workflow_id', True), ('exception', False),), 'VALIDATION_FAILED', never_retryable=True),
    ClassShape(start_types.WorkflowStartIdConflict, workflow_start_errors.WorkflowStartIdConflict, (('message', False), ('workflow_name', True), ('workflow_id', True), ('existing_workflow_name', True),), 'WORKFLOW_ID_CONFLICT', never_retryable=True),
    ClassShape(start_types.WorkflowStartCommandConflict, workflow_start_errors.WorkflowStartCommandConflict, (('message', False), ('workflow_name', True), ('workflow_id', True),), 'WORKFLOW_COMMAND_CONFLICT', never_retryable=True),
    ClassShape(start_types.WorkflowStartEnqueueFailed, workflow_start_errors.WorkflowStartEnqueueFailed, (('message', False), ('retryable', True), ('workflow_name', True), ('workflow_id', True), ('exception', False),), 'ENQUEUE_FAILED', never_retryable=False),
    ClassShape(start_types.WorkflowStartAsyncContext, workflow_start_errors.WorkflowStartAsyncContext, (('message', False), ('workflow_name', True), ('workflow_id', True),), 'ASYNC_CONTEXT', never_retryable=True),
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


class TestOutcomeDecodeError:
    """The class that the lifecycle decoders raise and that
    ``TaskActionStoredDataInvalid.exception`` holds: a plain ``Exception``
    subclass whose one argument is its message."""

    def test_the_public_name_is_the_generated_class(self) -> None:
        assert outcomes.OutcomeDecodeError is lifecycle_errors.OutcomeDecodeError

    def test_it_is_an_exception_subclass_and_no_dataclass(self) -> None:
        generated = lifecycle_errors.OutcomeDecodeError

        assert generated.__bases__ == (Exception,)
        assert not dataclasses.is_dataclass(generated)

    def test_its_text_and_its_args_are_its_message(self) -> None:
        error = lifecycle_errors.OutcomeDecodeError('row 3 has no kind')

        assert str(error) == 'row 3 has no kind'
        assert error.args == ('row 3 has no kind',)

    def test_it_keeps_python_s_doc(self) -> None:
        doc = lifecycle_errors.OutcomeDecodeError.__doc__

        assert doc is not None
        assert doc.startswith('The returned row does not satisfy the wire contract.')


class TestHistoryContractError:
    """The class of ``MonitoringQueryHistoryContractViolation.exception``:
    a plain ``Exception`` subclass whose one argument is its message."""

    def test_the_public_name_is_the_generated_class(self) -> None:
        assert history_error_module.HistoryContractError is history_errors.HistoryContractError

    def test_it_is_an_exception_subclass_and_no_dataclass(self) -> None:
        generated = history_errors.HistoryContractError

        assert generated.__bases__ == (Exception,)
        assert not dataclasses.is_dataclass(generated)

    def test_its_text_and_its_args_are_its_message(self) -> None:
        error = history_errors.HistoryContractError('history row has status BOGUS')

        assert str(error) == 'history row has status BOGUS'
        assert error.args == ('history row has status BOGUS',)

    def test_it_keeps_python_s_doc(self) -> None:
        assert history_errors.HistoryContractError.__doc__ == (
            'The database state violates an invariant this subsystem relies on.'
        )


def test_no_cause_is_the_generated_class_without_fields() -> None:
    assert error_detail.NoCause is GeneratedNoCause
    assert dataclasses.fields(GeneratedNoCause) == ()
    assert GeneratedNoCause() == GeneratedNoCause()


def test_each_public_union_is_the_generated_union_and_its_client_only_classes() -> None:
    assert result_types.BrokerOperationError.__value__ == (
        broker_errors.BrokerCoreError
        | result_types.BrokerNotAttached
        | result_types.BrokerNoTypeAvailable
    )
    assert result_types.BrokerEnqueueError.__value__ is broker_errors.BrokerCoreEnqueueError
    assert handle_types.HandleOperationError.__value__ is workflow_handle_errors.HandleCoreError
    assert errors.MonitoringQueryError.__value__ is monitoring_errors.MonitoringQueryCoreError
    assert task_actions.TaskActionError.__value__ is monitoring_errors.TaskActionCoreError
    assert start_types.WorkflowStartError.__value__ == (
        workflow_start_errors.WorkflowStartCoreError
        | start_types.WorkflowStartBrokerNotConfigured
    )


class SendRetry(Enum):
    """Where a send class has its ``retryable``."""

    #: A ``retryable`` field of the value.
    FIELD = 'field'
    #: The class value ``retryable: ClassVar[bool] = True``.
    ALWAYS = 'always'
    #: No ``retryable``.
    ABSENT = 'absent'


class SendClassShape(NamedTuple):
    public: type
    generated: type
    #: (field name, shown by repr), in order.
    fields: tuple[tuple[str, bool], ...]
    #: The member name of the `code` class value.
    code_member: str
    retry: SendRetry


SEND_SHAPES: Final[tuple[SendClassShape, ...]] = (
    SendClassShape(task_send_types.TaskSendSuppressed, task_send_errors.TaskSendSuppressed, (('message', False),), 'SEND_SUPPRESSED', SendRetry.ABSENT),
    SendClassShape(task_send_types.TaskSendAsyncContext, task_send_errors.TaskSendAsyncContext, (('task_id', True), ('payload', False), ('message', False)), 'ASYNC_CONTEXT', SendRetry.ABSENT),
    SendClassShape(task_send_types.TaskSendValidationFailed, task_send_errors.TaskSendValidationFailed, (('message', False), ('exception', False)), 'VALIDATION_FAILED', SendRetry.ABSENT),
    SendClassShape(task_send_types.TaskSendEnqueueFailed, task_send_errors.TaskSendEnqueueFailed, (('task_id', True), ('payload', False), ('message', False), ('retryable', True), ('exception', False)), 'ENQUEUE_FAILED', SendRetry.FIELD),
    SendClassShape(task_send_types.TaskSendPayloadMismatch, task_send_errors.TaskSendPayloadMismatch, (('task_id', True), ('payload', False), ('message', False)), 'PAYLOAD_MISMATCH', SendRetry.ABSENT),
    SendClassShape(task_send_types.TaskSendPayloadTooLarge, task_send_errors.TaskSendPayloadTooLarge, (('message', False), ('size_bytes', True), ('limit_bytes', True)), 'PAYLOAD_TOO_LARGE', SendRetry.ABSENT),
    SendClassShape(task_send_types.TaskSendIdempotencyKeyConflict, task_send_errors.TaskSendIdempotencyKeyConflict, (('task_id', True), ('payload', False), ('message', False), ('owner_task_id', True)), 'IDEMPOTENCY_KEY_CONFLICT', SendRetry.ALWAYS),
)


@pytest.mark.parametrize('shape', SEND_SHAPES, ids=lambda shape: shape.generated.__name__)
class TestGeneratedSendClass:
    """``TaskSendEnqueueFailed`` has a ``retryable`` field, and
    ``TaskSendIdempotencyKeyConflict`` the class value ``retryable = True``.
    The other send classes have no ``retryable``."""

    def test_the_public_names_are_the_generated_class(self, shape: SendClassShape) -> None:
        assert shape.public is shape.generated
        assert getattr(horsies, shape.generated.__name__) is shape.generated

    def test_the_fields_and_their_repr_flags_are_the_old_ones(
        self,
        shape: SendClassShape,
    ) -> None:
        fields = tuple((item.name, item.repr) for item in dataclasses.fields(shape.generated))

        assert fields == shape.fields

    def test_the_code_is_the_old_one_and_the_retryable_is_in_its_place(
        self,
        shape: SendClassShape,
    ) -> None:
        field_names = {item.name for item in dataclasses.fields(shape.generated)}

        assert getattr(shape.generated, 'code').name == shape.code_member
        match shape.retry:
            case SendRetry.FIELD:
                assert 'retryable' in field_names
            case SendRetry.ALWAYS:
                assert 'retryable' not in field_names
                assert getattr(shape.generated, 'retryable') is True
            case SendRetry.ABSENT:
                assert not hasattr(shape.generated, 'retryable')

    def test_the_class_is_a_frozen_dataclass_with_slots(self, shape: SendClassShape) -> None:
        params = getattr(shape.generated, '__dataclass_params__')

        assert params.frozen is True
        assert '__slots__' in vars(shape.generated)


def test_the_send_payload_is_the_generated_record_with_the_old_fields() -> None:
    payload = task_send_errors.TaskSendPayload
    assert task_send_types.TaskSendPayload is payload
    assert horsies.TaskSendPayload is payload
    assert getattr(payload, '__dataclass_params__').frozen is True
    assert '__slots__' in vars(payload)
    assert [(item.name, item.repr) for item in dataclasses.fields(payload)] == [
        ('task_name', True),
        ('queue_name', True),
        ('priority', True),
        ('input_json', True),
        ('sent_at', True),
        ('good_until', True),
        ('enqueue_delay_seconds', True),
        ('task_options', True),
        ('idempotency_key', True),
        ('retention_class_key', True),
    ]
    defaults = {
        item.name: item.default
        for item in dataclasses.fields(payload)
        if item.default is not dataclasses.MISSING
    }
    assert defaults == {
        'idempotency_key': None,
        'retention_class_key': 'standard_30d',
    }


def test_the_retention_class_key_and_its_default_are_the_generated_ones() -> None:
    assert periods.RetentionClassKey is retention_class.RetentionClassKey
    assert classes.DEFAULT_RETENTION_CLASS_KEY is retention_class.DEFAULT_RETENTION_CLASS_KEY
    assert retention_class.DEFAULT_RETENTION_CLASS_KEY == 'standard_30d'
    assert (
        periods.period_class_key(classes.DEFAULT_RETENTION_DAYS)
        == retention_class.DEFAULT_RETENTION_CLASS_KEY
    )


def test_the_send_union_is_the_generated_union() -> None:
    assert task_send_types.TaskSendError.__value__ is task_send_errors.TaskSendCoreError
