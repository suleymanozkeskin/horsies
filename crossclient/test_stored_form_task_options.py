"""Python's side of the task options vectors (``stored_forms/task_options.json``).

A task stores ``serialize_task_options`` of its ``TaskOptions``; a send with
a deadline stores the send's options with ``good_until``
(``clients/python/horsies/core/task_decorator.py:2067-2087``); a workflow node stores
its task's options with its deadline merged as ``good_until``
(``clients/python/horsies/core/workflows/child_build.py``). The core reads the text
at enqueue. Python reads it back with the public ``RetryPolicy`` model and
``stored_execution`` (the class read of a child). The core and the Rust
client test the same file.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Final, assert_never, cast

import pytest

from horsies.core.codec.json_io import Json, dumps_json, loads_json
from horsies.core.codec.task_options import (
    serialize_task_options,
    stored_execution,
)
from horsies.core.models.tasks import Execution, RetryPolicy, TaskOptions
from horsies.core.types.result import Err, Ok, Result
from crossclient.stored_form_vectors import load_form, rows, text_of

_FORM: Final = load_form('task_options.json')


def _options(vector: dict[str, object]) -> dict[str, Any]:
    options = vector['options']
    assert isinstance(options, dict)
    return cast('dict[str, Any]', options)


def _deadline(vector: dict[str, object]) -> datetime | None:
    match vector['deadline']:
        case None:
            return None
        case str() as text:
            return datetime.fromisoformat(text)
        case other:
            raise AssertionError(f'deadline is {other!r}')


def _retry_policy(vector: dict[str, object]) -> RetryPolicy | None:
    fields: object = _options(vector)['retry_policy']
    if fields is None:
        return None
    assert isinstance(fields, dict), f'retry_policy is {fields!r}'
    return RetryPolicy(**cast('dict[str, Any]', fields))


def _task_options(vector: dict[str, object], good_until: datetime | None) -> TaskOptions:
    options = _options(vector)
    return TaskOptions(
        task_name='stored_form_vector',
        retry_policy=_retry_policy(vector),
        timeout_ms=options['timeout_ms'],
        execution=Execution(options['execution']),
        cancel_grace_ms=options['cancel_grace_ms'],
        good_until=good_until,
    )


def _ok[T](result: Result[T, Any]) -> T:
    match result:
        case Ok(ok_value=value):
            return value
        case Err(err_value=error):
            raise AssertionError(f'refused: {error}')
        case _ as unreachable:
            assert_never(unreachable)


def _stored_text(vector: dict[str, object]) -> str:
    match text_of(vector, 'kind'):
        case 'task' | 'send':
            return _ok(serialize_task_options(_task_options(vector, _deadline(vector))))
        case 'node':
            deadline = _deadline(vector)
            assert deadline is not None, 'a node vector has a deadline'
            base = _ok(loads_json(_ok(serialize_task_options(_task_options(vector, None)))))
            assert isinstance(base, dict)
            merged = cast('dict[str, Json]', base)
            merged['good_until'] = deadline.isoformat()
            return _ok(dumps_json(merged))
        case other:
            raise AssertionError(f'unknown kind {other!r}')


@pytest.mark.unit
@pytest.mark.parametrize(
    'vector', rows(_FORM, 'vectors'), ids=lambda vector: text_of(vector, 'name'),
)
def test_each_options_set_encodes_to_its_text(vector: dict[str, object]) -> None:
    assert _stored_text(vector) == text_of(vector, 'text')


@pytest.mark.unit
@pytest.mark.parametrize(
    'vector', rows(_FORM, 'vectors'), ids=lambda vector: text_of(vector, 'name'),
)
def test_each_text_reads_back_to_its_options(vector: dict[str, object]) -> None:
    stored = _ok(loads_json(text_of(vector, 'text')))
    assert isinstance(stored, dict)
    options = cast('dict[str, Json]', stored)
    match _retry_policy(vector):
        case None:
            assert options.get('retry_policy') is None
        case policy:
            assert RetryPolicy.model_validate(options['retry_policy'], strict=True) == policy
    expected = _options(vector)
    assert stored_execution(stored) == Ok(Execution(expected['execution']))
    assert options['timeout_ms'] == expected['timeout_ms']
    assert options['cancel_grace_ms'] == expected['cancel_grace_ms']
    match options['good_until']:
        case None:
            assert _deadline(vector) is None
        case str() as text:
            assert datetime.fromisoformat(text) == _deadline(vector)
        case other:
            raise AssertionError(f'good_until is {other!r}')
