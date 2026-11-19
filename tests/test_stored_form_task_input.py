"""Python's side of the task input vectors (``stored_forms/task_input.json``).

A vector's value is a set of keyword arguments with declared types. Python's
send encodes them with ``encode_kwargs_read_back`` and ``dumps_json``; its
child reads the text with ``loads_json`` and ``decode_kwargs``
(``clients/python/horsies/core/worker/child_runner.py:1300-1329``). The core and the
Rust client test the same file.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, Final, assert_never, cast

import pytest

from horsies.core.codec.json_io import dumps_json, loads_json
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.kwargs import decode_kwargs, encode_kwargs_read_back
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.types.result import Err, Ok
from tests.stored_form_vectors import (
    annotation_of,
    load_form,
    rows,
    text_of,
    value_of,
)

_FORM: Final = load_form('task_input.json')


def _parameters(row: dict[str, object]) -> list[dict[str, object]]:
    return rows(row, 'parameters')


def _task_fn(parameters: list[dict[str, object]]) -> Callable[..., None]:
    """A function with one keyword-only parameter per vector parameter, in
    order, annotated with the parameter's type."""

    def task(**kwargs: object) -> None:
        del kwargs

    annotations: dict[str, object] = {
        text_of(parameter, 'name'): annotation_of(text_of(parameter, 'type'))
        for parameter in parameters
    }
    task.__signature__ = inspect.Signature(  # pyright: ignore[reportFunctionMemberAccess]
        [
            inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, annotation=annotation)
            for name, annotation in annotations.items()
        ],
        return_annotation=None,
    )
    task.__annotations__ = {**annotations, 'return': None}
    return task


def _kwargs(parameters: list[dict[str, object]]) -> dict[str, object]:
    return {
        text_of(parameter, 'name'): value_of(text_of(parameter, 'type'), parameter['value'])
        for parameter in parameters
    }


def _encoded_text(parameters: list[dict[str, object]]) -> str:
    """The send's text of the vector's keyword arguments."""
    encoded = encode_kwargs_read_back(_task_fn(parameters), _kwargs(parameters))
    match dumps_json(encoded):
        case Ok(ok_value=text):
            return text
        case Err(err_value=error):
            raise AssertionError(f'dumps_json refused an encoded input: {error}')
        case _ as unreachable:
            assert_never(unreachable)


def _read_input(parameters: list[dict[str, object]], text: str) -> dict[str, Any]:
    """The child's read of a stored input: strict JSON, an object, then each
    member decoded with its parameter's type. Raises on a refusal."""
    match loads_json(text):
        case Ok(ok_value=dict() as raw):
            return decode_kwargs(_task_fn(parameters), cast('dict[str, Any]', raw))
        case Ok(ok_value=other):
            raise StrictJsonError(f'task input is a JSON {type(other).__name__}, not an object')
        case Err(err_value=error):
            raise StrictJsonError(str(error))
        case _ as unreachable:
            assert_never(unreachable)


@pytest.mark.unit
@pytest.mark.parametrize(
    'vector', rows(_FORM, 'vectors'), ids=lambda vector: text_of(vector, 'name'),
)
def test_each_value_encodes_to_its_text(vector: dict[str, object]) -> None:
    assert _encoded_text(_parameters(vector)) == text_of(vector, 'text')


@pytest.mark.unit
@pytest.mark.parametrize(
    'vector', rows(_FORM, 'vectors'), ids=lambda vector: text_of(vector, 'name'),
)
def test_each_text_decodes_to_its_value(vector: dict[str, object]) -> None:
    parameters = _parameters(vector)
    assert _read_input(parameters, text_of(vector, 'text')) == _kwargs(parameters)


@pytest.mark.unit
@pytest.mark.parametrize(
    'vector', rows(_FORM, 'refused_values'), ids=lambda vector: text_of(vector, 'name'),
)
def test_each_refused_value_is_refused_at_the_send(vector: dict[str, object]) -> None:
    with pytest.raises((StrictJsonError, ValueValidationError)):
        _encoded_text(_parameters(vector))


@pytest.mark.unit
@pytest.mark.parametrize(
    'refused', rows(_FORM, 'refused_texts'), ids=lambda refused: text_of(refused, 'name'),
)
def test_each_refused_text_is_refused_by_the_read(refused: dict[str, object]) -> None:
    with pytest.raises(StrictJsonError):
        _read_input([], text_of(refused, 'text'))


@pytest.mark.unit
@pytest.mark.parametrize(
    'refused',
    [row for row in rows(_FORM, 'refused_texts') if 'refusal_text' in row],
    ids=lambda refused: text_of(refused, 'name'),
)
def test_each_refusal_text_is_the_text_of_the_read(refused: dict[str, object]) -> None:
    """Python's reader words these refusals itself; the core writes the same
    text (``StrictJsonError``)."""
    match loads_json(text_of(refused, 'text')):
        case Err(err_value=error):
            assert str(error) == f'JSON parse failed: {text_of(refused, "refusal_text")}'
        case Ok(ok_value=value):
            raise AssertionError(f'the read accepted a refused text: {value!r}')
        case _ as unreachable:
            assert_never(unreachable)


@pytest.mark.unit
@pytest.mark.parametrize(
    'refused',
    rows(_FORM, 'typed_refused_texts'),
    ids=lambda refused: text_of(refused, 'name'),
)
def test_each_typed_refused_text_is_refused_by_the_decode(refused: dict[str, object]) -> None:
    with pytest.raises((StrictJsonError, ValueValidationError)):
        _read_input(_parameters(refused), text_of(refused, 'text'))
