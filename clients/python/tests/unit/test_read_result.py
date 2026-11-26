# pyright: reportPrivateUsage=false
"""The read model: one test per outcome of a read and of a node input.

``read_of_record`` is the pure read of ``TaskHandle.get`` and
``Horsies.get_result``: each test gives it one broker answer and checks the
one ``ReadResult`` member that comes out. The workflow handle tests call
its pure parts. The handle tests run on an app whose broker cannot connect;
each refusal comes before any core call. The node input tests run the
argument decode of a worker child on one stored input each.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Callable, Generator
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr

from horsies import (
    Failed,
    ReadAsyncContext,
    ReadDatabaseError,
    ReadInvalidInput,
    ReadNoTypeAvailable,
    ReadResult,
    ReadResultDeserializationError,
    ReadResultNotAvailable,
    ReadTaskNotFound,
    ReadWaitInsideTask,
    ReadWaitTimeout,
    ReadWorkflowContextMissingId,
    Succeeded,
)
from horsies.core import process_role as process_role_module
from horsies.core.app import Horsies
from horsies.core.brokers.result_types import (
    BrokerAsyncContext,
    BrokerErrorCode,
    BrokerInvalidInput,
    BrokerInvalidJsonPayload,
    BrokerNoTypeAvailable,
    BrokerOperationFailed,
    BrokerResult,
    BrokerWaitInsideTask,
    StoredResult,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.outcome import TaskOutcome
from horsies.core.models.tasks import OutcomeCode, TaskError
from horsies.core.models.workflow.handle import (
    WorkflowHandle,
    _completed_without_stored_result,
    _decode_workflow_envelope,
    _ended_workflow_error,
)
from horsies.core.models.workflow.enums import WorkflowStatus
from horsies.core.models.workflow.nodes import NodeKey
from horsies.core.process_role import ProcessRole
from horsies.core.task_decorator import TaskHandle
from horsies.core.models.tasks import TextCodes
from horsies.core.task_read import is_final_read, read_of_record
from horsies.core.task_runtime import TaskRuntime
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok, Result
from horsies.core.types.status import TaskStatus
from horsies.core.worker import child_runner
from horsies.core.worker import current as current_module

pytestmark = [pytest.mark.unit]

UNREACHABLE_URL = 'postgresql://u:p@127.0.0.1:1/db'
TASK_ID = uuid.UUID('0b6f2f0e-6c1d-7f7a-9a52-3d2b1c0e9f82')
WORKFLOW_ID = uuid.UUID('0b6f2f0e-6c1d-4f7a-9a52-3d2b1c0e9f81')
WAIT_ABOVE_LIMIT_MS = 2**32
"""One more than the largest wait, 4,294,967,295 ms: every read refuses it."""


def _record(status: TaskStatus, raw_result: dict[str, Any] | None) -> BrokerResult[StoredResult | None]:
    return Ok(StoredResult(
        task_id=TASK_ID, task_name='produce', status=status, raw_result=raw_result,
    ))


def _ok_envelope(value: object) -> dict[str, Any]:
    return {'__h_task_result__': True, 'ok': value, 'err': None}


def _err_envelope(code: str) -> dict[str, Any]:
    return {
        '__h_task_result__': True,
        'ok': None,
        'err': {'error_code': code, 'message': 'stored failure', 'data': None, 'exception': None},
    }


def _read(
    answer: BrokerResult[StoredResult | None], ok_type: object | None = int,
) -> ReadResult[Any, TaskError]:
    return read_of_record(TASK_ID, answer, ok_type, 1_000, TextCodes())


def _read_error(read: ReadResult[Any, TaskError]) -> object:
    match read:
        case Err(err_value=error):
            return error
        case Ok():
            pytest.fail(f'expected a read failure, got {read!r}')


# ---------------------------------------------------------------------------
# read_of_record: one test per outcome
# ---------------------------------------------------------------------------


def test_stored_value_reads_as_succeeded() -> None:
    assert _read(_record(TaskStatus.COMPLETED, _ok_envelope(7))) == Ok(Succeeded(7))


def test_stored_error_reads_as_failed() -> None:
    match _read(_record(TaskStatus.FAILED, _err_envelope('CARD_DECLINED'))):
        case Ok(Failed(TaskError(error_code='CARD_DECLINED', message='stored failure'))):
            pass
        case other:
            pytest.fail(f'expected Ok(Failed(CARD_DECLINED)), got {other!r}')


def test_cancelled_row_reads_as_failed_task_cancelled() -> None:
    match _read(_record(TaskStatus.CANCELLED, None)):
        case Ok(Failed(TaskError(error_code=OutcomeCode.TASK_CANCELLED))):
            pass
        case other:
            pytest.fail(f'expected Ok(Failed(TASK_CANCELLED)), got {other!r}')


def test_no_row_is_task_not_found() -> None:
    assert isinstance(_read_error(_read(Ok(None))), ReadTaskNotFound)


def test_unfinished_row_after_the_wait_is_wait_timeout() -> None:
    assert isinstance(_read_error(_read(_record(TaskStatus.RUNNING, None))), ReadWaitTimeout)


def test_terminal_row_without_result_is_result_not_available() -> None:
    error = _read_error(_read(_record(TaskStatus.COMPLETED, None)))
    assert isinstance(error, ReadResultNotAvailable)


def test_malformed_envelope_is_result_deserialization_error() -> None:
    malformed = {'__h_task_result__': True, 'ok': 1, 'err': {'error_code': 'X'}}
    error = _read_error(_read(_record(TaskStatus.COMPLETED, malformed)))
    assert isinstance(error, ReadResultDeserializationError)


def test_value_of_another_type_is_result_deserialization_error() -> None:
    error = _read_error(_read(_record(TaskStatus.COMPLETED, _ok_envelope('seven'))))
    assert isinstance(error, ReadResultDeserializationError)


def test_stored_value_without_a_declared_type_is_no_type_available() -> None:
    error = _read_error(_read(_record(TaskStatus.COMPLETED, _ok_envelope(7)), ok_type=None))
    assert isinstance(error, ReadNoTypeAvailable)


def test_stored_error_reads_without_a_declared_type() -> None:
    read = _read(_record(TaskStatus.FAILED, _err_envelope('CARD_DECLINED')), ok_type=None)
    assert isinstance(read, Ok) and isinstance(read.ok_value, Failed)


def test_broker_failure_is_broker_error_with_its_cause_and_class() -> None:
    cause = ConnectionError('lost')
    answer: BrokerResult[StoredResult | None] = Err(BrokerOperationFailed(
        code=BrokerErrorCode.TASK_INFO_QUERY_FAILED,
        message='read failed',
        retryable=True,
        exception=cause,
    ))
    error = _read_error(_read(answer))
    assert isinstance(error, ReadDatabaseError)
    assert error.exception is cause
    assert error.retryable is True
    assert 'TASK_INFO_QUERY_FAILED' in error.message


def test_broker_payload_refusal_is_result_deserialization_error() -> None:
    answer: BrokerResult[StoredResult | None] = Err(BrokerInvalidJsonPayload(
        message='not JSON', exception=NoCause(),
    ))
    assert isinstance(_read_error(_read(answer)), ReadResultDeserializationError)


def test_broker_missing_type_is_no_type_available() -> None:
    answer: BrokerResult[StoredResult | None] = Err(BrokerNoTypeAvailable(message='no type'))
    assert isinstance(_read_error(_read(answer)), ReadNoTypeAvailable)


def test_broker_wait_inside_task_is_wait_inside_task() -> None:
    answer: BrokerResult[StoredResult | None] = Err(BrokerWaitInsideTask(message='inside'))
    assert isinstance(_read_error(_read(answer)), ReadWaitInsideTask)


def test_broker_async_context_is_a_defect() -> None:
    answer: BrokerResult[StoredResult | None] = Err(BrokerAsyncContext(message='loop'))
    with pytest.raises(AssertionError, match='ASYNC_CONTEXT'):
        _ = _read(answer)


@pytest.mark.parametrize(
    ('read', 'final'),
    [
        (Ok(Succeeded(1)), True),
        (Ok(Failed(TaskError(error_code='X'))), True),
        (Err(ReadResultNotAvailable(message='m')), True),
        (Err(ReadResultDeserializationError(message='m')), True),
        (Err(ReadNoTypeAvailable(message='m')), True),
        (Err(ReadWaitTimeout(message='m')), False),
        (Err(ReadTaskNotFound(message='m')), False),
        (Err(ReadDatabaseError(message='m', retryable=True, exception=NoCause())), False),
        (Err(ReadWaitInsideTask(message='m')), False),
        (Err(ReadInvalidInput(message='m')), False),
    ],
)
def test_a_handle_keeps_only_final_reads(read: ReadResult[Any, TaskError], final: bool) -> None:
    assert is_final_read(read) is final


# ---------------------------------------------------------------------------
# The checks before a read, and their order
# ---------------------------------------------------------------------------


@pytest.fixture
def app() -> Generator[Horsies, None, None]:
    """An app whose broker cannot connect; its core handle records each call."""
    app = Horsies(AppConfig(broker=PostgresConfig(database_url=SecretStr(UNREACHABLE_URL))))
    broker = app.get_broker()
    broker._core_handle = MagicMock(wraps=broker._core_handle)
    yield app


@pytest.fixture
def task_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give this process the role of a worker child for one test."""
    monkeypatch.setattr(process_role_module, '_role', ProcessRole.RUNS_TASK_BODIES)


def _nothing_ran(app: Horsies) -> bool:
    core_handle = app.get_broker()._core_handle
    assert isinstance(core_handle, MagicMock)
    return core_handle.mock_calls == []


def _workflow_handle(app: Horsies) -> WorkflowHandle[int]:
    return WorkflowHandle(workflow_id=WORKFLOW_ID, broker=app.get_broker())


def _task_handle(app: Horsies) -> TaskHandle[int]:
    return TaskHandle(TASK_ID, app=app, broker_mode=True, ok_type=int)


READS: dict[str, Callable[[Horsies, int | None], ReadResult[Any, TaskError]]] = {
    'TaskHandle.get': lambda app, wait: _task_handle(app).get(wait_ms=wait),
    'TaskHandle.get_async': lambda app, wait: asyncio.run(_task_handle(app).get_async(wait_ms=wait)),
    'WorkflowHandle.get': lambda app, wait: _workflow_handle(app).get(wait_ms=wait),
    'WorkflowHandle.get_async': lambda app, wait: asyncio.run(
        _workflow_handle(app).get_async(wait_ms=wait),
    ),
    'app.get_result': lambda app, wait: app.get_result(TASK_ID, wait),
    'app.get_result_async': lambda app, wait: asyncio.run(app.get_result_async(TASK_ID, wait)),
}


@pytest.mark.parametrize('read_name', sorted(READS))
def test_wait_above_the_limit_is_invalid_input(app: Horsies, read_name: str) -> None:
    error = _read_error(READS[read_name](app, WAIT_ABOVE_LIMIT_MS))
    assert isinstance(error, ReadInvalidInput)
    assert _nothing_ran(app)


@pytest.mark.parametrize('read_name', sorted(READS))
@pytest.mark.usefixtures('task_process')
def test_bad_wait_inside_a_task_is_invalid_input(app: Horsies, read_name: str) -> None:
    """The wait limit is the first check, before the refusal of a wait in a
    process that runs task bodies: the same input gives the same code in
    every client."""
    error = _read_error(READS[read_name](app, WAIT_ABOVE_LIMIT_MS))
    assert isinstance(error, ReadInvalidInput)
    assert _nothing_ran(app)


@pytest.mark.parametrize('read_name', sorted(READS))
@pytest.mark.usefixtures('task_process')
def test_valid_wait_inside_a_task_is_wait_inside_task(app: Horsies, read_name: str) -> None:
    error = _read_error(READS[read_name](app, 0))
    assert isinstance(error, ReadWaitInsideTask)
    assert _nothing_ran(app)


@pytest.mark.parametrize('read_name', ['get_raw_result', 'get_raw_result_async'])
@pytest.mark.usefixtures('task_process')
def test_bad_raw_wait_inside_a_task_is_invalid_input(app: Horsies, read_name: str) -> None:
    read: Callable[[Horsies], BrokerResult[Any]] = {
        'get_raw_result': lambda a: a.get_raw_result(TASK_ID, WAIT_ABOVE_LIMIT_MS),
        'get_raw_result_async': lambda a: asyncio.run(
            a.get_raw_result_async(TASK_ID, WAIT_ABOVE_LIMIT_MS),
        ),
    }[read_name]
    answer = read(app)
    assert isinstance(answer, Err) and isinstance(answer.err_value, BrokerInvalidInput)
    assert _nothing_ran(app)


def test_sync_reads_on_an_event_loop_are_async_context(app: Horsies) -> None:
    async def sync_reads() -> list[ReadResult[Any, TaskError]]:
        return [
            _task_handle(app).get(wait_ms=0),
            _workflow_handle(app).get(wait_ms=0),
            _workflow_handle(app).result_for(NodeKey[int]('n')),
            app.get_result(TASK_ID, 0),
        ]

    for read in asyncio.run(sync_reads()):
        assert isinstance(_read_error(read), ReadAsyncContext)
    assert _nothing_ran(app)


def test_handle_with_no_broker_and_no_result_is_result_not_available() -> None:
    handle: TaskHandle[int] = TaskHandle(TASK_ID)
    assert isinstance(_read_error(handle.get()), ReadResultNotAvailable)


def test_handle_returns_its_immediate_result() -> None:
    handle: TaskHandle[int] = TaskHandle(TASK_ID)
    handle.set_immediate_result(Ok(5))
    assert handle.get() == Ok(Succeeded(5))


def test_result_for_a_node_with_no_id_is_workflow_ctx_missing_id(app: Horsies) -> None:
    match _workflow_handle(app)._node_result_key(NodeKey[int](node_id=None)):  # pyright: ignore[reportArgumentType]
        case Err(err_value=ReadWorkflowContextMissingId()):
            pass
        case other:
            pytest.fail(f'expected Err(ReadWorkflowContextMissingId), got {other!r}')


# ---------------------------------------------------------------------------
# The workflow read: outcomes of its pure parts
# ---------------------------------------------------------------------------


def test_failed_workflow_without_stored_error_is_failed_workflow_failed() -> None:
    match _ended_workflow_error(str(WORKFLOW_ID), WorkflowStatus.FAILED, None):
        case Ok(Failed(TaskError(error_code=OutcomeCode.WORKFLOW_FAILED))):
            pass
        case other:
            pytest.fail(f'expected Ok(Failed(WORKFLOW_FAILED)), got {other!r}')


def test_stored_workflow_error_that_does_not_read_is_result_deserialization_error() -> None:
    read = _ended_workflow_error(str(WORKFLOW_ID), WorkflowStatus.FAILED, 'not json')
    assert isinstance(_read_error(read), ReadResultDeserializationError)


def test_completed_workflow_without_stored_result_is_result_deserialization_error() -> None:
    read = _completed_without_stored_result(str(WORKFLOW_ID))
    assert isinstance(_read_error(read), ReadResultDeserializationError)


def test_workflow_value_without_a_declared_type_is_no_type_available() -> None:
    read = _decode_workflow_envelope(_ok_envelope(7), None, app=None, workflow_id=str(WORKFLOW_ID))
    assert isinstance(_read_error(read), ReadNoTypeAvailable)


def test_workflow_value_reads_as_succeeded() -> None:
    read = _decode_workflow_envelope(_ok_envelope(7), int, app=None, workflow_id=str(WORKFLOW_ID))
    assert read == Ok(Succeeded(7))


# ---------------------------------------------------------------------------
# Node inputs: the argument decode of a worker child
# ---------------------------------------------------------------------------


@pytest.fixture
def worker_app(monkeypatch: pytest.MonkeyPatch) -> Horsies:
    """An app with a source task and a receiver task, bound as the app of
    this process for one test."""
    app = Horsies(AppConfig(broker=PostgresConfig(database_url=SecretStr(UNREACHABLE_URL))))

    @app.task('produce')
    def produce() -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
        return Ok(7)

    @app.task('receive')
    def receive(*, dep: TaskOutcome[int, TaskError]) -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
        match dep:
            case Succeeded(value):
                return Ok(value)
            case Failed(error):
                return Err(error)

    monkeypatch.setattr(current_module, '_current_app', app)
    return app


def _node_input(inner: dict[str, Any]) -> str:
    return json.dumps({
        'dep': {
            '__h_taskresult_envelope__': True,
            'source_task_name': 'produce',
            'source_definition_key': None,
            'inner': inner,
        },
    })


def _prepared_dep(input_json: str) -> object:
    prepared = child_runner._prepare_task_call(
        'receive',
        input_json,
        child_runner.OnSharedThreadSeat(),
        TaskRuntime.for_test(task_id=TASK_ID),
    )
    assert isinstance(prepared, child_runner._PreparedCall), prepared
    return prepared.kwargs['dep']


@pytest.mark.usefixtures('worker_app')
def test_node_input_of_a_succeeded_source_is_succeeded() -> None:
    assert _prepared_dep(_node_input(_ok_envelope(7))) == Succeeded(7)


@pytest.mark.usefixtures('worker_app')
def test_node_input_of_a_failed_source_is_failed() -> None:
    match _prepared_dep(_node_input(_err_envelope('UPSTREAM_BAD'))):
        case Failed(TaskError(error_code='UPSTREAM_BAD', message='stored failure')):
            pass
        case other:
            pytest.fail(f'expected Failed(UPSTREAM_BAD), got {other!r}')
