"""A value that the workflow engine built and that does not serialize raises.

The engine serializes only values that it built in the process: kwargs,
results, errors and summaries. It builds some of them from values that it
parsed from stored rows, and a parsed JSON value serializes again. So a
serialization failure is a defect. Each writer raises ``SerializationError``
with the subject in the message, before the statement that would store the
value: no ``'null'`` text and no NULL goes into a result or error column.

The child workflow start also encodes the kwargs of each child node. Spec
construction in ``build_with()`` encoded and decoded every one of them, so
an encode refusal there is a defect too, and raises in the same way.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import uuid
from enum import Enum
from typing import Any

import pytest
from pydantic import BaseModel, SecretStr

from horsies.core.app import Horsies
from horsies.core.codec.json_value import JsonValue
from horsies.core.errors import ErrorCode, WorkflowValidationError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import (
    TaskError,
)
from horsies.core.models.workflow import (
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
)
from horsies.core.task_decorator import TaskFunction
from horsies.core.types.result import Ok, Result


pytestmark = [pytest.mark.unit]

LONE_SURROGATE_TEXT = f'name{chr(0xdc80)}'
DATABASE_URL = 'postgresql://u:p@localhost:5432/db'


def _real_app() -> Horsies:
    return Horsies(AppConfig(
        broker=PostgresConfig(database_url=SecretStr(DATABASE_URL)),
    ))


def _register_note_task(app: Horsies) -> TaskFunction[..., int]:
    @app.task('surrogate.note')
    def note(*, text: str, labels: dict[str, int]) -> Result[int, TaskError]:
        return Ok(len(text) + len(labels))
    return note


def _key(label: str) -> str:
    return f'tests.unit.surrogate.{label}.{uuid.uuid4().hex}'


class TestSpecConstructionRefusesALoneSurrogate:
    """Spec construction encodes every kwarg, so it refuses a lone surrogate.

    The value does not reach the workflow start transaction or a child start,
    where a failure to serialize is a defect.
    """

    @pytest.mark.parametrize(
        ('kwargs', 'subject'),
        [
            ({'text': LONE_SURROGATE_TEXT, 'labels': {}}, 'a string'),
            ({'text': 'ok', 'labels': {LONE_SURROGATE_TEXT: 1}}, 'a dict key'),
        ],
        ids=['string', 'key'],
    )
    def test_a_task_node_kwarg_is_hrs_031(
        self,
        kwargs: dict[str, Any],
        subject: str,
    ) -> None:
        app = _real_app()
        note = _register_note_task(app)

        with pytest.raises(WorkflowValidationError) as raised:
            _ = app.workflow(
                name='surrogate-task-node',
                tasks=[TaskNode(function=note, kwargs=kwargs)],
                definition_key=_key('task_node'),
            )

        assert raised.value.code is ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE
        assert raised.value.code.value == 'HRS-031'
        assert (
            f'{subject} holds a lone surrogate' in ' '.join(raised.value.notes)
        )

    def test_an_any_or_bytes_build_with_parameter_is_hrs_031(self) -> None:
        app = _real_app()

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'surrogate-child'
            definition_key = _key('child')

            @classmethod
            def build_with(
                cls, app: Horsies, *args: Any, text: str = '', **params: Any,
            ) -> Any:
                raise AssertionError('spec construction refuses before this')

        with pytest.raises(WorkflowValidationError) as raised:
            _ = app.workflow(
                name='surrogate-sub-node',
                tasks=[SubWorkflowNode(
                    workflow_def=ChildWorkflow,
                    kwargs={'text': LONE_SURROGATE_TEXT},
                )],
                definition_key=_key('sub_node'),
            )

        assert raised.value.code is ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE


class _ListValued(Enum):
    MEMBER = [1]


class TestDictKeyWithoutAJsonForm:
    """Registration refuses these types in a task signature, so `.send()` and
    a schedule cannot reach this. It does not check a `build_with()` parameter."""

    @pytest.mark.parametrize(
        ('value', 'type_name'),
        [
            ({_ListValued.MEMBER: 1}, 'list'),
            ({frozenset({1}): 1}, 'frozenset'),
        ],
        ids=['enum-list-value', 'frozenset'],
    )
    def test_an_any_or_bytes_build_with_parameter_is_hrs_031(
        self,
        value: dict[Any, int],
        type_name: str,
    ) -> None:
        """Before, the raw `TypeError` left `app.workflow()`."""
        app = _real_app()

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'no-json-key-child'
            definition_key = _key('no_json_key_child')

            @classmethod
            def build_with(
                cls, app: Horsies, *args: Any, labels: Any = None, **params: Any,
            ) -> Any:
                raise AssertionError('spec construction refuses before this')

        with pytest.raises(WorkflowValidationError) as raised:
            _ = app.workflow(
                name='no-json-key',
                tasks=[SubWorkflowNode(
                    workflow_def=ChildWorkflow, kwargs={'labels': value},
                )],
                definition_key=_key('no_json_key'),
            )

        # A `build_with` parameter cannot be `Any` any more, so the allow-list
        # refuses this before the dump can.
        assert raised.value.code is ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE
        assert 'banned type `Any`' in ' '.join(raised.value.notes)


class _Linked(BaseModel):
    """A self-referencing model: registration accepts it in a task signature."""

    next: _Linked | None = None


class TestValueThatHoldsItself:
    """Before, the raw `ValueError` left `app.workflow()`."""

    def test_a_task_node_kwarg_is_hrs_031(self) -> None:
        app = _real_app()

        @app.task('circular.linked')
        def linked(*, x: _Linked) -> Result[int, TaskError]:
            return Ok(1)

        node = _Linked()
        node.next = node

        with pytest.raises(WorkflowValidationError) as raised:
            _ = app.workflow(
                name='circular-task-node',
                tasks=[TaskNode(function=linked, kwargs={'x': node})],
                definition_key=_key('circular_task_node'),
            )

        assert raised.value.code is ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE
        assert 'Circular reference detected (id repeated)' in ' '.join(
            raised.value.notes,
        )

    def test_an_any_or_bytes_build_with_parameter_is_hrs_031(self) -> None:
        """Registration does not check a `build_with()` parameter, so `Any`
        is possible there."""
        app = _real_app()

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'circular-child'
            definition_key = _key('circular_child')

            @classmethod
            def build_with(
                cls, app: Horsies, *args: Any, labels: Any = None, **params: Any,
            ) -> Any:
                raise AssertionError('spec construction refuses before this')

        value: list[Any] = []
        value.append(value)

        with pytest.raises(WorkflowValidationError) as raised:
            _ = app.workflow(
                name='circular-sub-node',
                tasks=[SubWorkflowNode(
                    workflow_def=ChildWorkflow, kwargs={'labels': value},
                )],
                definition_key=_key('circular_sub_node'),
            )

        assert raised.value.code is ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE
        assert 'banned type `Any`' in ' '.join(raised.value.notes)


class TestValueThatIsTooDeep:
    def test_a_task_node_kwarg_is_hrs_031(self) -> None:
        """Before, the raw `ValueError` left `app.workflow()`."""
        app = _real_app()

        @app.task('deep.linked')
        def linked(*, x: _Linked) -> Result[int, TaskError]:
            return Ok(1)

        node = _Linked()
        for _ in range(299):
            node = _Linked(next=node)

        with pytest.raises(WorkflowValidationError) as raised:
            _ = app.workflow(
                name='deep-task-node',
                tasks=[TaskNode(function=linked, kwargs={'x': node})],
                definition_key=_key('deep_task_node'),
            )

        assert raised.value.code is ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE
        assert 'Circular reference detected (depth exceeded)' in ' '.join(
            raised.value.notes,
        )


class TestValuePastTheDepthBound:
    def test_a_task_node_kwarg_is_hrs_031(self) -> None:
        """Before, `RecursionError` left `app.workflow()`."""
        app = _real_app()

        @app.task('cycle.json_value')
        def holds_json(*, x: JsonValue) -> Result[int, TaskError]:
            return Ok(1)

        value: list[object] = []
        value.append(value)

        with pytest.raises(WorkflowValidationError) as raised:
            _ = app.workflow(
                name='cycle-task-node',
                tasks=[TaskNode(function=holds_json, kwargs={'x': value})],
                definition_key=_key('cycle_task_node'),
            )

        assert raised.value.code is ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE
        assert 'nested deeper than' in ' '.join(raised.value.notes)


class TestBytesThatAreNotUtf8:
    """Registration refuses `bytes` in a task signature, so `.send()` and a
    schedule cannot reach this. It does not check a `build_with()` parameter."""

    def test_an_any_or_bytes_build_with_parameter_is_hrs_031(self) -> None:
        """Before, the raw `UnicodeDecodeError` left `app.workflow()`."""
        app = _real_app()

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'not-utf8-child'
            definition_key = _key('not_utf8_child')

            @classmethod
            def build_with(
                cls, app: Horsies, *args: Any, payload: bytes = b'', **params: Any,
            ) -> Any:
                raise AssertionError('spec construction refuses before this')

        with pytest.raises(WorkflowValidationError) as raised:
            _ = app.workflow(
                name='not-utf8-sub-node',
                tasks=[SubWorkflowNode(
                    workflow_def=ChildWorkflow, kwargs={'payload': b'\xff'},
                )],
                definition_key=_key('not_utf8_sub_node'),
            )

        assert raised.value.code is ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE
        assert 'banned type `bytes`' in ' '.join(raised.value.notes)


