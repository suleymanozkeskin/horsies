"""Python's side of the error data vectors (``stored_forms/error_data.json``).

Each vector is a library error with ``data``, stored as a result envelope
(``serialize_error_payload``) or as the error object of the workflow error
column (``dumps_json(encode_task_error(...))``). The writers that are their
own functions are called with the vector's inputs: ``timeout_error_data``,
``crash_result_json``, ``orphaned_workflow_error``, ``recovery_limit_error``.
The core and the Rust client test the same file.
"""

from __future__ import annotations

from datetime import datetime
from typing import Final, assert_never, cast

import pytest

from horsies.core.brokers.stale_running import (
    StaleCandidate,
    StaleThresholds,
    crash_result_json,
)
from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.codec.json_io import dumps_json, loads_json
from horsies.core.codec.typed import (
    decode_task_error,
    decode_task_result,
    encode_task_error,
)
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import Err, Ok
from horsies.core.worker.shared_stop import CancelEnd, NoCancelSignal, timeout_error_data
from horsies.core.workflows.recovery import orphaned_workflow_error
from horsies.core.workflows.recovery_limit import (
    RecoveryCase,
    RecoveryRaise,
    recovery_limit_error,
)
from tests.stored_form_vectors import load_form, rows, task_error_of, text_of

_FORM: Final = load_form('error_data.json')
_VECTORS: Final = rows(_FORM, 'vectors')


def _vector(name: str) -> dict[str, object]:
    found = [vector for vector in _VECTORS if vector['name'] == name]
    assert len(found) == 1, f'error_data.json has no single vector {name}'
    return found[0]


def _inputs(vector: dict[str, object]) -> dict[str, object]:
    inputs = vector['inputs']
    assert isinstance(inputs, dict)
    return cast('dict[str, object]', inputs)


def _stored_text(vector: dict[str, object], error: TaskError) -> str:
    match text_of(vector, 'stored_as'):
        case 'envelope':
            return serialize_error_payload(TaskResult(err=error))
        case 'error':
            match dumps_json(encode_task_error(error)):
                case Ok(ok_value=text):
                    return text
                case Err(err_value=refused):
                    raise AssertionError(f'the error object does not encode: {refused}')
                case _ as unreachable:
                    assert_never(unreachable)
        case other:
            raise AssertionError(f'unknown stored form {other!r}')


def _read(vector: dict[str, object]) -> TaskError:
    match loads_json(text_of(vector, 'text')):
        case Ok(ok_value=raw):
            pass
        case Err(err_value=refused):
            raise AssertionError(f'the text does not load: {refused}')
        case _ as unreachable:
            assert_never(unreachable)
    match text_of(vector, 'stored_as'):
        case 'envelope':
            result = decode_task_result(raw, type(None))
            assert result.is_err()
            return result.err_value
        case 'error':
            return decode_task_error(raw)
        case other:
            raise AssertionError(f'unknown stored form {other!r}')


@pytest.mark.unit
@pytest.mark.parametrize('vector', _VECTORS, ids=lambda vector: text_of(vector, 'name'))
def test_each_error_encodes_to_its_text(vector: dict[str, object]) -> None:
    assert _stored_text(vector, task_error_of(vector['error'])) == text_of(vector, 'text')


@pytest.mark.unit
@pytest.mark.parametrize('vector', _VECTORS, ids=lambda vector: text_of(vector, 'name'))
def test_each_text_reads_back_to_its_error(vector: dict[str, object]) -> None:
    assert _read(vector) == task_error_of(vector['error'])


@pytest.mark.unit
@pytest.mark.parametrize(
    'name',
    [
        'task_timeout_returned_after_signal',
        'task_timeout_dropped_after_grace',
        'task_timeout_killed_with_child',
        'task_timeout_no_cancel_signal',
    ],
)
def test_the_timeout_data_is_the_writer_s_in_its_order(name: str) -> None:
    vector = _vector(name)
    inputs = _inputs(vector)
    match inputs['cancel_end']:
        case None:
            cancel: CancelEnd | NoCancelSignal = NoCancelSignal()
        case str() as end:
            cancel = CancelEnd(end)
        case other:
            raise AssertionError(f'cancel_end is {other!r}')
    task_id = inputs['task_id']
    timeout_ms = inputs['timeout_ms']
    assert isinstance(task_id, str) and isinstance(timeout_ms, int)
    data = timeout_error_data(task_id, timeout_ms, cancel)
    error = cast('dict[str, object]', vector['error'])
    assert list(data.items()) == list(cast('dict[str, object]', error['data']).items())


@pytest.mark.unit
def test_the_stale_crash_result_is_the_writer_s_text() -> None:
    vector = _vector('worker_crashed_stale')
    inputs = _inputs(vector)

    def text(key: str) -> str:
        value = inputs[key]
        assert isinstance(value, str)
        return value

    stale_after_ms = inputs['stale_after_ms']
    worker_pid = inputs['worker_pid']
    assert isinstance(stale_after_ms, int) and isinstance(worker_pid, int)
    candidate = StaleCandidate(
        task_id='0192f3b4-5c6d-7e8f-9a0b-1c2d3e4f5a6b',
        retry_count=0,
        max_retries=0,
        task_options=None,
        good_until=None,
        claimed_by_worker_id=text('claimed_by_worker_id'),
        worker_pid=worker_pid,
        worker_hostname=text('worker_hostname'),
        worker_process_name='runner-0',
        started_at=datetime.fromisoformat(text('started_at')),
        db_now=datetime.fromisoformat(text('db_now')),
    )
    thresholds = StaleThresholds(stale_after_ms=stale_after_ms, finalizing_stale_after_ms=60_000)
    assert crash_result_json(candidate, thresholds) == Ok(text_of(vector, 'text'))


@pytest.mark.unit
def test_the_orphaned_workflow_error_is_the_writer_s_text() -> None:
    vector = _vector('workflow_start_incomplete')
    error = cast('dict[str, object]', vector['error'])
    data = cast('dict[str, object]', error['data'])
    workflow_id = data['workflow_id']
    assert isinstance(workflow_id, str)
    assert orphaned_workflow_error(workflow_id, 'checkout') == text_of(vector, 'text')


@pytest.mark.unit
def test_the_recovery_limit_error_is_the_writer_s_error() -> None:
    vector = _vector('workflow_recovery_limit_reached')
    error = cast('dict[str, object]', vector['error'])
    data = cast('dict[str, object]', error['data'])
    workflow_id = data['workflow_id']
    assert isinstance(workflow_id, str)
    failure = RecoveryRaise(
        workflow_id=workflow_id,
        task_index=3,
        case=RecoveryCase.READY_NOT_ENQUEUED,
        exception_type='OperationalError',
        detail='connection reset',
    )
    written = serialize_error_payload(TaskResult(err=recovery_limit_error(failure, 3)))
    assert written == text_of(vector, 'text')
