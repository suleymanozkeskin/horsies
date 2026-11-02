"""The wire form of node kwargs: what a workflow start stores."""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest
from pydantic import BaseModel

from horsies import AppConfig, Horsies, JsonValue, PostgresConfig, TaskError, TaskResult
from horsies.core.codec.json_value import MAX_TYPED_JSON_DEPTH
from horsies.core.errors import ErrorCode, WorkflowValidationError
from horsies.core.models.workflow import (
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)
from horsies.core.models.workflow.nodes import stored_kwargs


class _Item(BaseModel):
    n: int


@dataclasses.dataclass
class _Point:
    x: int


_ROWS: list[tuple[str, Any, Any, Any]] = [
    ('list_int', list[int], [1, 2], [1, 2]),
    ('dict_int', dict[str, int], {'a': 1}, {'a': 1}),
    ('json_list', JsonValue, [1, 'a', None], [1, 'a', None]),
    ('json_dict', JsonValue, {'a': [1]}, {'a': [1]}),
    ('list_model', list[_Item], [_Item(n=1)], [{'n': 1}]),
    ('list_dataclass', list[_Point], [_Point(x=1)], [{'x': 1}]),
    ('dict_of_list', dict[str, list[int]], {'a': [1]}, {'a': [1]}),
    ('tuple', tuple[int, str], (1, 'a'), [1, 'a']),
]


def _app() -> Horsies:
    return Horsies(
        AppConfig(
            broker=PostgresConfig(database_url='postgresql+psycopg://u:p@localhost/x'),
        ),
    )


def _task(app: Horsies, name: str, annotation: Any) -> Any:
    def take(*, value: Any) -> TaskResult[int, TaskError]:
        return TaskResult(ok=1)

    take.__annotations__ = {
        'value': annotation,
        'return': TaskResult[int, TaskError],
    }
    return app.task(task_name=name)(take)


def _child(name: str, build_with: Any) -> Any:
    return type(
        name,
        (WorkflowDefinition,),
        {
            'name': name,
            'definition_key': f'tests.{name}',
            'build_with': classmethod(build_with),
        },
    )


def _with_hint(annotation: Any) -> Any:
    def build_with(cls: Any, app: Any, value: Any) -> Any:
        raise AssertionError('the start of the parent does not build the child')

    build_with.__annotations__ = {'cls': Any, 'app': Any, 'value': annotation}
    return build_with


def _with_opaque_params(cls, app, **params):  # type: ignore[no-untyped-def]  # noqa: ANN001, ANN003, ANN202
    raise AssertionError('the start of the parent does not build the child')


def _with_no_hint(cls, app, value):  # type: ignore[no-untyped-def]  # noqa: ANN001, ANN202
    raise AssertionError('the start of the parent does not build the child')


@pytest.mark.unit
class TestWireForm:
    @pytest.mark.parametrize(
        ('label', 'annotation', 'value', 'wire'),
        _ROWS,
        ids=[row[0] for row in _ROWS],
    )
    def test_a_task_node_stores_its_wire_form(
        self, label: str, annotation: Any, value: Any, wire: Any,
    ) -> None:
        app = _app()
        node = TaskNode(fn=_task(app, label, annotation), kwargs={'value': value})
        spec = app.workflow(name=label, tasks=[node], definition_key=f'd.{label}')

        assert stored_kwargs(spec.tasks[0]) == {'value': wire}

    @pytest.mark.parametrize(
        ('label', 'annotation', 'value', 'wire'),
        _ROWS,
        ids=[row[0] for row in _ROWS],
    )
    def test_a_sub_workflow_node_stores_its_wire_form(
        self, label: str, annotation: Any, value: Any, wire: Any,
    ) -> None:
        app = _app()
        node = SubWorkflowNode(
            workflow_def=_child(f'child_{label}', _with_hint(annotation)),
            kwargs={'value': value},
        )
        spec = app.workflow(name=label, tasks=[node], definition_key=f'd.{label}')

        assert spec.tasks[0].wire_kwargs == {'value': wire}

    def test_a_node_of_an_earlier_spec_keeps_its_wire_form(self) -> None:
        app = _app()
        node = TaskNode(fn=_task(app, 'reused', list[int]), kwargs={'value': [1, 2]})
        first = app.workflow(name='first', tasks=[node], definition_key='d.first')
        second = app.workflow(
            name='second',
            tasks=list(first.tasks),
            definition_key='d.second',
        )

        assert second.tasks[0].kwargs['value'] == (1, 2)
        assert stored_kwargs(second.tasks[0]) == {'value': [1, 2]}

    def test_a_later_change_to_the_source_does_not_reach_the_wire(self) -> None:
        app = _app()
        values = [1, 2]
        node = TaskNode(fn=_task(app, 'detached', list[int]), kwargs={'value': values})
        spec = app.workflow(name='detached', tasks=[node], definition_key='d.detached')

        values.append(3)

        assert stored_kwargs(spec.tasks[0]) == {'value': [1, 2]}

    def test_a_node_that_no_spec_took_is_a_defect(self) -> None:
        app = _app()
        node = TaskNode(fn=_task(app, 'no_spec', list[int]), kwargs={'value': [1]})

        with pytest.raises(AssertionError, match='did not pass through a WorkflowSpec'):
            stored_kwargs(node)

    def test_a_task_node_with_no_hints_keeps_its_values(self) -> None:
        """Only test scaffolding has such a node."""
        def scaffold(value):  # type: ignore[no-untyped-def]  # noqa: ANN001, ANN202
            return value

        values = [1, 2]
        node = TaskNode(fn=scaffold, kwargs={'value': values})
        spec = WorkflowSpec(name='s', tasks=[node])

        values.append(3)

        assert stored_kwargs(spec.tasks[0]) == {'value': [1, 2]}

    def test_the_wire_form_stays_out_of_the_repr_and_the_compare(self) -> None:
        app = _app()
        task = _task(app, 'compared', list[int])
        spec = app.workflow(
            name='compared',
            tasks=[TaskNode(fn=task, kwargs={'value': [1]})],
            definition_key='d.compared',
        )

        assert 'wire_kwargs' not in repr(spec.tasks[0])


@pytest.mark.unit
class TestBuildWithWithoutHints:
    def test_opaque_params_store_a_container(self) -> None:
        app = _app()
        node = SubWorkflowNode(
            workflow_def=_child('opaque_params', _with_opaque_params),
            kwargs={'values': [1, 2], 'shape': {'a': [1]}},
        )
        spec = app.workflow(name='o', tasks=[node], definition_key='d.o')

        assert spec.tasks[0].wire_kwargs == {'values': [1, 2], 'shape': {'a': [1]}}

    def test_a_value_that_is_not_json_is_refused_at_the_spec(self) -> None:
        app = _app()
        node = SubWorkflowNode(
            workflow_def=_child('opaque_params_object', _with_opaque_params),
            kwargs={'values': object()},
        )

        with pytest.raises(WorkflowValidationError) as caught:
            app.workflow(name='r', tasks=[node], definition_key='d.r')

        assert caught.value.code == ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE

    def test_a_value_deeper_than_the_read_limit_is_refused_at_the_spec(self) -> None:
        """The encoder allows this depth, and the read of a worker does not."""
        deep: list[object] = []
        for _ in range(MAX_TYPED_JSON_DEPTH + 50):
            deep = [deep]
        node = SubWorkflowNode(
            workflow_def=_child('opaque_params_deep', _with_opaque_params),
            kwargs={'values': deep},
        )

        with pytest.raises(WorkflowValidationError) as caught:
            app = _app()
            app.workflow(name='d', tasks=[node], definition_key='d.d')

        assert caught.value.code == ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE

    def test_a_named_parameter_with_no_hint_is_refused_at_the_spec(self) -> None:
        app = _app()
        node = SubWorkflowNode(
            workflow_def=_child('no_hint', _with_no_hint),
            kwargs={'value': 1},
        )

        with pytest.raises(WorkflowValidationError) as caught:
            app.workflow(name='h', tasks=[node], definition_key='d.h')

        assert caught.value.code == ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE
