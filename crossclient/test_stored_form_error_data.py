"""Python's side of the error data vectors (``stored_forms/error_data.json``).

Each vector is a library error with ``data``, stored as a result envelope
(``serialize_error_payload``) or as the error object of the workflow error
column (``dumps_json(encode_task_error(...))``). The writers that are their
own functions are called with the vector's inputs: ``timeout_error_data``,
``crash_result_json``, ``orphaned_workflow_error``, ``recovery_limit_error``.
The core and the Rust client test the same file.
"""

from __future__ import annotations

from typing import Final, assert_never

import pytest

from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.codec.json_io import dumps_json, loads_json
from horsies.core.codec.typed import (
    decode_task_error,
    decode_task_result,
    encode_task_error,
)
from horsies.core.models.outcome import Failed, Succeeded
from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Err, Ok
from crossclient.stored_form_vectors import load_form, rows, task_error_of, text_of

_FORM: Final = load_form('error_data.json')
_VECTORS: Final = rows(_FORM, 'vectors')


def _stored_text(vector: dict[str, object], error: TaskError) -> str:
    match text_of(vector, 'stored_as'):
        case 'envelope':
            return serialize_error_payload(error)
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
            match decode_task_result(raw, type(None)):
                case Failed(error=error):
                    return error
                case Succeeded() as succeeded:
                    raise AssertionError(f'the envelope reads as a value: {succeeded!r}')
                case _ as unreachable:
                    assert_never(unreachable)
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
