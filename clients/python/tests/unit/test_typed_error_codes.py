# pyright: reportPrivateUsage=false
"""Typed custom error codes: one test per outcome.

A task that declares `error_codes=E` and returns `Result[T, TaskError[E]]`
passes members of `E`, keeps the member value as the stored text, and its
readers get members back. A task that returns `TaskError` keeps `str` codes.
"""

from __future__ import annotations

import json
import uuid
from enum import Enum
from typing import Any

import pytest
from pydantic import SecretStr

from horsies import (
    ContractCode,
    Err,
    Failed,
    Ok,
    OperationalCode,
    OutcomeCode,
    ReadUnknownCustomCode,
    Result,
    RetrievalCode,
    Succeeded,
    TaskError,
    TaskOutcome,
)
from horsies.core.app import Horsies
from horsies.core.brokers.result_types import DatabaseResult, StoredResult
from horsies.core.codec.typed import encode_returned_result
from horsies.core.errors import HorsiesErrorCode, TaskDefinitionError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import EnumCodes, TextCodes
from horsies.core.models.workflow.nodes import TaskNode
from horsies.core.models.workflow.spec import _args_from_code_type_error
from horsies.core.task_read import is_final_read, read_of_record
from horsies.core.task_runtime import TaskRuntime
from horsies.core.types.status import TaskStatus
from horsies.core.worker import child_runner
from horsies.core.worker import current as current_module

pytestmark = [pytest.mark.unit]

UNREACHABLE_URL = 'postgresql://u:p@127.0.0.1:1/db'
TASK_ID = uuid.UUID('0b6f2f0e-6c1d-7f7a-9a52-3d2b1c0e9f83')


class WelcomeCode(str, Enum):
    INVALID_EMAIL = 'INVALID_EMAIL'
    MAILBOX_FULL = 'MAILBOX_FULL'


class OtherCode(Enum):
    INVALID_EMAIL = 'INVALID_EMAIL'


def _app(mapper: dict[type[BaseException], str] | None = None) -> Horsies:
    return Horsies(AppConfig(
        database=PostgresConfig(database_url=SecretStr(UNREACHABLE_URL)),
        exception_mapper=mapper or {},
    ))


def _stored_error(code: str) -> DatabaseResult[StoredResult | None]:
    return Ok(StoredResult(
        task_id=TASK_ID,
        task_name='send_welcome_email',
        status=TaskStatus.FAILED,
        raw_result={
            '__h_task_result__': True,
            'ok': None,
            'err': {'error_code': code, 'message': 'no', 'data': None, 'exception': None},
        },
    ))


# ---------------------------------------------------------------------------
# The body and the decorator
# ---------------------------------------------------------------------------


def test_body_passes_a_member_and_stores_its_value() -> None:
    app = _app()

    @app.task('send_welcome_email', error_codes=WelcomeCode)
    def send_welcome_email(*, email: str) -> Result[str, TaskError[WelcomeCode]]:
        return Err(TaskError(code=WelcomeCode.INVALID_EMAIL, message=email))

    returned = send_welcome_email(email='a')

    match returned:
        case Err(err_value=TaskError(code=WelcomeCode.INVALID_EMAIL)):
            pass
        case other:
            pytest.fail(f'expected Err(INVALID_EMAIL), got {other!r}')
    envelope = encode_returned_result(returned, str)
    err_slot = envelope['err']
    assert isinstance(err_slot, dict)
    assert err_slot['error_code'] == 'INVALID_EMAIL'
    assert send_welcome_email.task_code_type == EnumCodes(code_type=WelcomeCode)


def test_text_code_in_a_typed_task_is_return_type_mismatch() -> None:
    app = _app()

    @app.task('send_welcome_email', error_codes=WelcomeCode)
    def send_welcome_email(*, email: str) -> Result[str, TaskError[WelcomeCode]]:
        return Err(TaskError(code='INVALID_EMAIL', message=email))  # pyright: ignore

    match send_welcome_email(email='a'):
        case Err(err_value=TaskError(code=ContractCode.RETURN_TYPE_MISMATCH, data=data)):
            assert isinstance(data, dict) and data['variant'] == 'err'
        case other:
            pytest.fail(f'expected RETURN_TYPE_MISMATCH, got {other!r}')


def test_untyped_task_keeps_text_codes() -> None:
    app = _app()

    @app.task('send_receipt', error_codes=WelcomeCode)
    def send_receipt(*, email: str) -> Result[str, TaskError]:
        return Err(TaskError(code='MAILBOX_FULL', message=email))

    assert send_receipt.task_code_type == TextCodes()
    match send_receipt(email='a'):
        case Err(err_value=TaskError(code='MAILBOX_FULL')):
            pass
        case other:
            pytest.fail(f'expected Err(MAILBOX_FULL), got {other!r}')


def test_typed_error_without_its_declaration_is_refused() -> None:
    app = _app()

    with pytest.raises(TaskDefinitionError) as refused:
        @app.task('send_welcome_email', error_codes=('INVALID_EMAIL',))
        def send_welcome_email(*, email: str) -> Result[str, TaskError[WelcomeCode]]:  # pyright: ignore[reportUnusedFunction]
            return Ok(email)

    assert refused.value.code == HorsiesErrorCode.TASK_STRICT_SIGNATURE_VIOLATION


def test_mapper_code_outside_the_enum_is_refused() -> None:
    app = _app({KeyError: 'UNKNOWN_KEY'})

    with pytest.raises(TaskDefinitionError) as refused:
        @app.task('send_welcome_email', error_codes=WelcomeCode)
        def send_welcome_email(*, email: str) -> Result[str, TaskError[WelcomeCode]]:  # pyright: ignore[reportUnusedFunction]
            return Ok(email)

    assert refused.value.code == HorsiesErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER


def test_mapped_exception_gives_the_member() -> None:
    app = _app({KeyError: 'MAILBOX_FULL'})

    @app.task('send_welcome_email', error_codes=WelcomeCode)
    def send_welcome_email(*, email: str) -> Result[str, TaskError[WelcomeCode]]:
        raise KeyError(email)

    match send_welcome_email(email='a'):
        case Err(err_value=TaskError(code=code)):
            assert code is WelcomeCode.MAILBOX_FULL
        case other:
            pytest.fail(f'expected Err(MAILBOX_FULL), got {other!r}')


# ---------------------------------------------------------------------------
# The read
# ---------------------------------------------------------------------------


def test_typed_read_gives_the_member() -> None:
    read = read_of_record(
        TASK_ID, _stored_error('INVALID_EMAIL'), str, None, EnumCodes(code_type=WelcomeCode),
    )
    match read:
        case Ok(Failed(TaskError(code=code))):
            assert code is WelcomeCode.INVALID_EMAIL
        case other:
            pytest.fail(f'expected Ok(Failed(member)), got {other!r}')


def test_untyped_read_gives_the_text() -> None:
    read = read_of_record(TASK_ID, _stored_error('INVALID_EMAIL'), str, None, TextCodes())
    match read:
        case Ok(Failed(TaskError(code=code))):
            assert code == 'INVALID_EMAIL' and not isinstance(code, Enum)
        case other:
            pytest.fail(f'expected Ok(Failed(text)), got {other!r}')


def test_stored_code_that_the_enum_does_not_name_is_unknown_custom_code() -> None:
    read = read_of_record(
        TASK_ID, _stored_error('RATE_LIMITED'), str, None, EnumCodes(code_type=WelcomeCode),
    )
    match read:
        case Err(err_value=ReadUnknownCustomCode(custom_code='RATE_LIMITED') as unknown):
            assert unknown.code is RetrievalCode.UNKNOWN_CUSTOM_CODE
        case other:
            pytest.fail(f'expected Err(ReadUnknownCustomCode), got {other!r}')
    assert is_final_read(read)


def test_built_in_code_reads_unchanged_with_typed_codes() -> None:
    stored: DatabaseResult[StoredResult | None] = Ok(StoredResult(
        task_id=TASK_ID, task_name='send_welcome_email', status=TaskStatus.CANCELLED, raw_result=None,
    ))
    match read_of_record(TASK_ID, stored, str, None, EnumCodes(code_type=WelcomeCode)):
        case Ok(Failed(TaskError(code=OutcomeCode.TASK_CANCELLED))):
            pass
        case other:
            pytest.fail(f'expected Ok(Failed(TASK_CANCELLED)), got {other!r}')


# ---------------------------------------------------------------------------
# Node inputs
# ---------------------------------------------------------------------------


@pytest.fixture
def worker_app(monkeypatch: pytest.MonkeyPatch) -> Horsies:
    """A typed source task and a typed receiver, bound as the app of this
    process for one test."""
    app = _app()

    @app.task('send_welcome_email', error_codes=WelcomeCode)
    def send_welcome_email(*, email: str) -> Result[str, TaskError[WelcomeCode]]:  # pyright: ignore[reportUnusedFunction]
        return Ok(email)

    @app.task('report', error_codes=WelcomeCode)
    def report(*, sent: TaskOutcome[str, TaskError[WelcomeCode]]) -> Result[str, TaskError[WelcomeCode]]:  # pyright: ignore[reportUnusedFunction]
        match sent:
            case Succeeded(value):
                return Ok(value)
            case Failed(error):
                return Err(error)

    monkeypatch.setattr(current_module, '_current_app', app)
    return app


def _report_input(code: str) -> str:
    return json.dumps({
        'sent': {
            '__h_taskresult_envelope__': True,
            'source_task_name': 'send_welcome_email',
            'source_definition_key': None,
            'inner': {
                '__h_task_result__': True,
                'ok': None,
                'err': {'error_code': code, 'message': 'no', 'data': None, 'exception': None},
            },
        },
    })


def _prepared(input_json: str) -> child_runner._PreparedCall | child_runner.EncodedResult:
    return child_runner._prepare_task_call(
        'report', input_json, child_runner.OnSharedThreadSeat(), TaskRuntime.for_test(task_id=TASK_ID),
    )


@pytest.mark.usefixtures('worker_app')
def test_typed_node_input_gives_the_member() -> None:
    prepared = _prepared(_report_input('MAILBOX_FULL'))
    assert isinstance(prepared, child_runner._PreparedCall), prepared
    match prepared.kwargs['sent']:
        case Failed(TaskError(code=code)):
            assert code is WelcomeCode.MAILBOX_FULL
        case other:
            pytest.fail(f'expected Failed(member), got {other!r}')


@pytest.mark.usefixtures('worker_app')
def test_typed_node_input_with_an_unknown_code_is_worker_serialization_error() -> None:
    prepared = _prepared(_report_input('RATE_LIMITED'))
    assert isinstance(prepared, child_runner.EncodedResult), prepared
    stored: dict[str, Any] = json.loads(prepared.envelope.text)
    assert stored['err']['error_code'] == {'__builtin_task_code__': 'WORKER_SERIALIZATION_ERROR'}
    assert 'RATE_LIMITED' in stored['err']['message']


@pytest.fixture
def text_receiver_app(monkeypatch: pytest.MonkeyPatch) -> Horsies:
    """A receiver that declares `TaskError` (text codes), a source that
    declares `WelcomeCode`, and a source that declares no codes."""
    app = _app()

    @app.task('send_welcome_email', error_codes=WelcomeCode)
    def send_welcome_email(*, email: str) -> Result[str, TaskError[WelcomeCode]]:  # pyright: ignore[reportUnusedFunction]
        return Ok(email)

    @app.task('send_plain')
    def send_plain(*, email: str) -> Result[str, TaskError]:  # pyright: ignore[reportUnusedFunction]
        return Ok(email)

    @app.task('report_text')
    def report_text(*, sent: TaskOutcome[str, TaskError]) -> Result[str, TaskError]:  # pyright: ignore[reportUnusedFunction]
        match sent:
            case Succeeded(value):
                return Ok(value)
            case Failed(error):
                return Err(error)

    monkeypatch.setattr(current_module, '_current_app', app)
    return app


def _report_text_input(source_task_name: str, code: str) -> str:
    return json.dumps({
        'sent': {
            '__h_taskresult_envelope__': True,
            'source_task_name': source_task_name,
            'source_definition_key': None,
            'inner': {
                '__h_task_result__': True,
                'ok': None,
                'err': {'error_code': code, 'message': 'no', 'data': None, 'exception': None},
            },
        },
    })


def _prepared_text(input_json: str) -> child_runner._PreparedCall | child_runner.EncodedResult:
    return child_runner._prepare_task_call(
        'report_text', input_json, child_runner.OnSharedThreadSeat(), TaskRuntime.for_test(task_id=TASK_ID),
    )


@pytest.mark.usefixtures('text_receiver_app')
def test_upstream_code_that_the_upstream_task_declares_reads() -> None:
    prepared = _prepared_text(_report_text_input('send_welcome_email', 'MAILBOX_FULL'))
    assert isinstance(prepared, child_runner._PreparedCall), prepared
    match prepared.kwargs['sent']:
        case Failed(TaskError(code='MAILBOX_FULL')):
            pass
        case other:
            pytest.fail(f'expected Failed(MAILBOX_FULL), got {other!r}')


@pytest.mark.usefixtures('text_receiver_app')
def test_upstream_code_that_the_upstream_task_does_not_declare_is_worker_serialization_error() -> None:
    prepared = _prepared_text(_report_text_input('send_welcome_email', 'RATE_LIMITED'))
    assert isinstance(prepared, child_runner.EncodedResult), prepared
    stored: dict[str, Any] = json.loads(prepared.envelope.text)
    assert stored['err']['error_code'] == {'__builtin_task_code__': 'WORKER_SERIALIZATION_ERROR'}
    assert 'RATE_LIMITED' in stored['err']['message']


@pytest.mark.usefixtures('text_receiver_app')
def test_upstream_task_without_declared_codes_reads_any_code() -> None:
    prepared = _prepared_text(_report_text_input('send_plain', 'RATE_LIMITED'))
    assert isinstance(prepared, child_runner._PreparedCall), prepared
    match prepared.kwargs['sent']:
        case Failed(TaskError(code='RATE_LIMITED')):
            pass
        case other:
            pytest.fail(f'expected Failed(RATE_LIMITED), got {other!r}')


def test_args_from_source_with_other_codes_is_refused() -> None:
    app = _app()

    @app.task('produce', error_codes=OtherCode)
    def produce() -> Result[str, TaskError[OtherCode]]:
        return Ok('x')

    refusal = _args_from_code_type_error(
        'report', 'sent', TaskNode(function=produce), TaskOutcome[str, TaskError[WelcomeCode]],
    )

    assert refusal is not None
    assert refusal.code == HorsiesErrorCode.WORKFLOW_ARGS_FROM_TYPE_MISMATCH


def test_args_from_source_with_the_same_codes_is_accepted() -> None:
    app = _app()

    @app.task('produce', error_codes=WelcomeCode)
    def produce() -> Result[str, TaskError[WelcomeCode]]:
        return Ok('x')

    refusal = _args_from_code_type_error(
        'report', 'sent', TaskNode(function=produce), TaskOutcome[str, TaskError[WelcomeCode]],
    )

    assert refusal is None


# ---------------------------------------------------------------------------
# Workflow reads: result_for, results/tasks, the context
# ---------------------------------------------------------------------------


def _typed_app() -> Horsies:
    """An app with a typed task and an untyped task."""
    app = _app()

    @app.task('send_welcome_email', error_codes=WelcomeCode)
    def send_welcome_email(*, email: str) -> Result[str, TaskError[WelcomeCode]]:  # pyright: ignore[reportUnusedFunction]
        return Ok(email)

    @app.task('send_receipt')
    def send_receipt(*, email: str) -> Result[str, TaskError]:  # pyright: ignore[reportUnusedFunction]
        return Ok(email)

    return app


def _stored_error_text(code: str) -> str:
    return json.dumps({
        '__h_task_result__': True,
        'ok': None,
        'err': {'error_code': code, 'message': 'no', 'data': None, 'exception': None},
    })


def _node_decode(app: Horsies, task_name: str, code: str) -> Any:
    from horsies.core.models.workflow.handle import _stored_node_decode

    return _stored_node_decode(
        _stored_error_text(code), app=app, task_name=task_name, node_id='n', sub_definition_key=None,
    )


def test_result_for_a_typed_node_gives_the_member() -> None:
    from horsies.core.models.workflow.handle import _node_read

    match _node_read(_node_decode(_typed_app(), 'send_welcome_email', 'MAILBOX_FULL')):
        case Ok(Failed(TaskError(code=code))):
            assert code is WelcomeCode.MAILBOX_FULL
        case other:
            pytest.fail(f'expected Ok(Failed(member)), got {other!r}')


def test_result_for_an_unknown_code_is_unknown_custom_code() -> None:
    from horsies.core.models.workflow.handle import _node_read

    match _node_read(_node_decode(_typed_app(), 'send_welcome_email', 'RATE_LIMITED')):
        case Err(err_value=ReadUnknownCustomCode(custom_code='RATE_LIMITED')):
            pass
        case other:
            pytest.fail(f'expected Err(ReadUnknownCustomCode), got {other!r}')


def test_result_for_an_untyped_node_gives_the_text() -> None:
    from horsies.core.models.workflow.handle import _node_read

    match _node_read(_node_decode(_typed_app(), 'send_receipt', 'MAILBOX_FULL')):
        case Ok(Failed(TaskError(code=code))):
            assert code == 'MAILBOX_FULL' and not isinstance(code, Enum)
        case other:
            pytest.fail(f'expected Ok(Failed(text)), got {other!r}')


def test_results_map_folds_an_unknown_code_into_the_entry() -> None:
    from horsies.core.models.workflow.handle import _folded

    match _folded('n', _node_decode(_typed_app(), 'send_welcome_email', 'RATE_LIMITED')):
        case Failed(TaskError(code=OperationalCode.RESULT_DESERIALIZATION_ERROR, data=data)):
            assert isinstance(data, dict) and data['custom_code'] == 'RATE_LIMITED'
        case other:
            pytest.fail(f'expected Failed(RESULT_DESERIALIZATION_ERROR), got {other!r}')


def test_context_entry_gives_the_member() -> None:
    match child_runner._context_entry(
        'n', Failed(TaskError(code='MAILBOX_FULL')), EnumCodes(code_type=WelcomeCode),
    ):
        case Failed(TaskError(code=code)):
            assert code is WelcomeCode.MAILBOX_FULL
        case other:
            pytest.fail(f'expected Failed(member), got {other!r}')


def test_context_entry_with_an_unknown_code_is_result_deserialization_error() -> None:
    match child_runner._context_entry(
        'n', Failed(TaskError(code='RATE_LIMITED')), EnumCodes(code_type=WelcomeCode),
    ):
        case Failed(TaskError(code=OperationalCode.RESULT_DESERIALIZATION_ERROR, data=data)):
            assert isinstance(data, dict) and data['custom_code'] == 'RATE_LIMITED'
        case other:
            pytest.fail(f'expected Failed(RESULT_DESERIALIZATION_ERROR), got {other!r}')
