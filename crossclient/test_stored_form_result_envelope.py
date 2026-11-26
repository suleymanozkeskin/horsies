"""Python's side of the result envelope vectors (``stored_forms/result_envelope.json``).

A task body's return is encoded by ``encode_returned_result`` and
``dumps_json``; a read decodes the text with ``loads_json`` and
``decode_task_result``. The core and the Rust client test the same file.
"""

from __future__ import annotations

from typing import Any, Final, assert_never, cast

import pytest

from horsies.core.codec.json_io import dumps_json, loads_json
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.typed import decode_task_result, encode_returned_result
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.models.outcome import Failed, Succeeded, TaskOutcome
from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Err, Ok, Result
from crossclient.stored_form_vectors import load_form, rows, task_error_of, text_of, typed

_FORM: Final = load_form('result_envelope.json')


def _value_and_type(vector: dict[str, object]) -> tuple[Result[Any, TaskError], object]:
    """The task result of a vector and the ok type that encodes it."""
    match vector:
        case {'ok': ok}:
            value, annotation = typed(cast('dict[str, object]', ok))
            return Ok(value), annotation
        case {'err': notation}:
            return Err(task_error_of(notation)), type(None)
        case _:
            raise AssertionError(f'{text_of(vector, "name")}: a vector has an ok or an err value')


def _stored_error(vector: dict[str, object]) -> TaskError:
    """The error that a read gives back: the notation's, with the data that
    the writer stores in place of a refused one."""
    error = task_error_of(vector['err'])
    match vector.get('stored_data'):
        case None:
            return error
        case stored:
            return error.model_copy(update={'data': stored})


def _read(text: str, annotation: object) -> TaskOutcome[Any, TaskError]:
    match loads_json(text):
        case Ok(ok_value=raw):
            return decode_task_result(raw, annotation)
        case Err(err_value=error):
            raise StrictJsonError(str(error))
        case _ as unreachable:
            assert_never(unreachable)


@pytest.mark.unit
@pytest.mark.parametrize(
    'vector', rows(_FORM, 'vectors'), ids=lambda vector: text_of(vector, 'name'),
)
def test_each_result_encodes_to_its_text(vector: dict[str, object]) -> None:
    result, annotation = _value_and_type(vector)
    match dumps_json(encode_returned_result(result, annotation)):
        case Ok(ok_value=text):
            assert text == text_of(vector, 'text')
        case Err(err_value=error):
            raise AssertionError(f'dumps_json refused an encoded result: {error}')
        case _ as unreachable:
            assert_never(unreachable)


@pytest.mark.unit
@pytest.mark.parametrize(
    'vector', rows(_FORM, 'vectors'), ids=lambda vector: text_of(vector, 'name'),
)
def test_each_text_decodes_to_its_result(vector: dict[str, object]) -> None:
    result, annotation = _value_and_type(vector)
    read = _read(text_of(vector, 'text'), annotation)
    match result:
        case Ok(ok_value=value):
            assert read == Succeeded(value)
        case Err():
            assert read == Failed(_stored_error(vector))
        case _ as unreachable:
            assert_never(unreachable)


@pytest.mark.unit
@pytest.mark.parametrize(
    'refused', rows(_FORM, 'refused_texts'), ids=lambda refused: text_of(refused, 'name'),
)
def test_each_refused_text_is_refused_by_the_read(refused: dict[str, object]) -> None:
    with pytest.raises((StrictJsonError, ValueValidationError)):
        _read(text_of(refused, 'text'), int)
