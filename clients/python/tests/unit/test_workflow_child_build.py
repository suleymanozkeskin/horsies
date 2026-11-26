"""The child build of a sub-workflow node.

``build_child_within_depth`` builds the child spec of a sub-workflow node at
the depth that the core checked; each failure is a ``ChildBuildFailed`` with
the error that the node stores. The child spec source answers the core's
``ChildSpecRequest`` with it.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies._core import ChildSpecBuilt, ChildSpecRefused, ChildSpecRequest
from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.outcome import Succeeded, TaskOutcome
from horsies.core.models.tasks import OperationalErrorCode, TaskError
from horsies.core.models.workflow import (
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)
from horsies.core.workflows.child_build import (
    ChildBuildFailed,
    ChildBuilt,
    ChildOfNode,
    TaskNodeKind,
    build_child_within_depth,
)
from horsies.core.workflows.child_spec_source import build_child_spec
from horsies.core.workflows.registry import register_workflow_definition
from horsies.core.types.result import Ok, Result

pytestmark = [pytest.mark.unit]

APP = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(database_url=SecretStr('postgresql://u@127.0.0.1:1/unused')),
    ),
)


@APP.task(task_name='child_build.value')
def value_task(*, value: int) -> Result[int, TaskError]:
    return Ok(value)


class ValueChild(WorkflowDefinition[int]):
    """A child whose build takes one int and gives a root and its dependent."""

    name = 'child_build.value_child'
    definition_key = 'child_build.value_child.v1'

    @classmethod
    def build_with(  # pyright: ignore[reportIncompatibleMethodOverride]
        cls, app: Horsies, value: int = 7,
    ) -> WorkflowSpec[int]:
        root = TaskNode(fn=value_task, kwargs={'value': value})
        dependent = TaskNode(fn=value_task, kwargs={'value': value + 1}, waits_for=[root])
        return app.workflow(
            cls.name, [root, dependent], output=dependent, definition_key=cls.definition_key,
        )


class ResultChild(WorkflowDefinition[int]):
    """A child whose build takes the result of a dependency."""

    name = 'child_build.result_child'
    definition_key = 'child_build.result_child.v1'

    @classmethod
    def build_with(  # pyright: ignore[reportIncompatibleMethodOverride]
        cls, app: Horsies, source: TaskOutcome[int, TaskError],
    ) -> WorkflowSpec[int]:
        assert isinstance(source, Succeeded), f'the tests give a succeeded source, got {source!r}'
        node = TaskNode(fn=value_task, kwargs={'value': source.value})
        return app.workflow(cls.name, [node], output=node, definition_key=cls.definition_key)


class SurrogateNameChild(WorkflowDefinition[int]):
    """A child whose built spec has a name with a lone surrogate."""

    name = 'child_build.surrogate_name_child'
    definition_key = 'child_build.surrogate_name_child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> WorkflowSpec[int]:
        _ = (args, params)
        node = TaskNode(fn=value_task, kwargs={'value': 1})
        return app.workflow(
            'child_build.\ud800', [node], output=node, definition_key=cls.definition_key,
        )


class BrokenChild(WorkflowDefinition[int]):
    """A child whose build raises."""

    name = 'child_build.broken_child'
    definition_key = 'child_build.broken_child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> WorkflowSpec[int]:
        _ = (app, args, params)
        raise RuntimeError('cannot build')


@pytest.fixture(autouse=True)
def registered_definitions() -> None:
    """The definitions of this module, registered again: other tests clear
    the definition registry."""
    for definition in (ValueChild, ResultChild, BrokenChild, SurrogateNameChild):
        register_workflow_definition(definition)


def _broker() -> MagicMock:
    broker = MagicMock()
    broker.app = APP
    return broker


def _node(**values: Any) -> ChildOfNode:
    defaults: dict[str, Any] = {
        'workflow_id': 'parent-wf',
        'workflow_name': 'parent',
        'task_index': 3,
        'parent_depth': 0,
        'sub_definition_key': ValueChild.definition_key,
        'node_input': '{}',
        'args_from': None,
    }
    return ChildOfNode(**{**defaults, **values})


def _build(
    broker: MagicMock,
    node: ChildOfNode,
    dependency_results: Mapping[int, TaskOutcome[Any, TaskError]],
) -> ChildBuilt | ChildBuildFailed:
    """The build that the child spec source runs: the child one level below the
    node's workflow, at a depth that the core checked against the limit."""
    return build_child_within_depth(broker, node, dependency_results, node.parent_depth + 1)


def _failure(built: object) -> TaskError:
    assert isinstance(built, ChildBuildFailed), built
    return built.error


class TestBuildChild:
    def test_a_child_that_builds_gives_its_spec_depth_and_node_rows(self) -> None:
        built = _build(_broker(), _node(node_input='{"value": 4}'), {})

        assert isinstance(built, ChildBuilt)
        assert built.depth == 1
        assert [row.dependencies for row in built.nodes] == [(), (0,)]
        assert [json.loads(row.input_json) for row in built.nodes] == [
            {'value': 4}, {'value': 5},
        ]
        assert all(isinstance(row.kind, TaskNodeKind) for row in built.nodes)

    def test_a_definition_that_does_not_load_fails_the_node(self) -> None:
        error = _failure(_build(_broker(), _node(sub_definition_key='unknown.v1'), {}))

        assert error.error_code is OperationalErrorCode.SUBWORKFLOW_LOAD_FAILED
        assert error.message == 'Failed to load subworkflow definition for parent:3'
        assert error.data == {'definition_key': 'unknown.v1'}

    def test_a_stored_input_that_is_not_json_fails_the_node(self) -> None:
        error = _failure(_build(_broker(), _node(node_input='{not json'), {}))

        assert error.error_code is OperationalErrorCode.WORKER_SERIALIZATION_ERROR
        assert error.message == 'Corrupt kwargs JSON for subworkflow task 3'

    def test_an_empty_stored_input_fails_the_node(self) -> None:
        # A node without params stores '{}'; an empty text is corrupt data.
        error = _failure(_build(_broker(), _node(node_input=''), {}))

        assert error.error_code is OperationalErrorCode.WORKER_SERIALIZATION_ERROR
        assert error.message == 'Corrupt kwargs JSON for subworkflow task 3'
        assert error.data == {'workflow_id': 'parent-wf', 'task_index': 3}

    def test_an_input_that_does_not_decode_to_build_with_fails_the_node(self) -> None:
        error = _failure(_build(_broker(), _node(node_input='{"value": "x"}'), {}))

        assert error.error_code is OperationalErrorCode.WORKER_SERIALIZATION_ERROR
        assert (error.message or '').startswith('Typed decode of subworkflow kwargs failed for task 3: ')

    def test_an_args_from_text_that_is_not_json_fails_the_node(self) -> None:
        error = _failure(_build(_broker(), _node(args_from='{not json'), {}))

        assert error.error_code is OperationalErrorCode.WORKER_SERIALIZATION_ERROR
        assert error.message == 'Corrupt args_from JSON for subworkflow task 3'

    def test_an_empty_args_from_text_fails_the_node(self) -> None:
        # A node without args_from stores NULL; an empty text is corrupt data.
        error = _failure(_build(_broker(), _node(args_from=''), {}))

        assert error.error_code is OperationalErrorCode.WORKER_SERIALIZATION_ERROR
        assert error.message == 'Corrupt args_from JSON for subworkflow task 3'
        assert error.data == {'workflow_id': 'parent-wf', 'task_index': 3}

    def test_an_args_from_index_that_is_not_an_int_fails_the_node(self) -> None:
        error = _failure(_build(_broker(), _node(args_from='{"value": "0"}'), {}))

        assert error.error_code is OperationalErrorCode.WORKER_SERIALIZATION_ERROR
        assert error.message == 'args_from has non-int values for subworkflow task 3'

    @pytest.mark.parametrize('flag', ['true', 'false'])
    def test_an_args_from_index_that_is_a_bool_fails_the_node(self, flag: str) -> None:
        # bool is a subclass of int: true and false are not the indexes 1 and 0.
        error = _failure(
            _build(
                _broker(),
                _node(args_from=f'{{"value": {flag}}}'),
                {0: Succeeded(4), 1: Succeeded(5)},
            ),
        )

        assert error.error_code is OperationalErrorCode.WORKER_SERIALIZATION_ERROR
        assert error.message == 'args_from has non-int values for subworkflow task 3'
        assert error.data == {'workflow_id': 'parent-wf', 'task_index': 3}

    def test_an_args_from_key_that_names_a_static_kwarg_fails_the_node(self) -> None:
        error = _failure(
            _build(_broker(), _node(node_input='{"value": 1}', args_from='{"value": 0}'), {}),
        )

        assert error.error_code is OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED
        assert error.message == (
            "args_from key 'value' conflicts with static kwarg (subworkflow task 3)"
        )

    def test_a_dependency_result_reaches_build_with(self) -> None:
        built = _build(
            _broker(),
            _node(sub_definition_key=ResultChild.definition_key, args_from='{"source": 0}'),
            {0: Succeeded(9)},
        )

        assert isinstance(built, ChildBuilt)
        assert json.loads(built.nodes[0].input_json) == {'value': 9}

    def test_a_dependency_result_that_does_not_fit_build_with_fails_the_node(self) -> None:
        error = _failure(
            _build(
                _broker(),
                _node(sub_definition_key=ResultChild.definition_key, args_from='{"source": 0}'),
                {0: Succeeded('not an int')},
            ),
        )

        assert error.error_code is OperationalErrorCode.WORKER_SERIALIZATION_ERROR
        assert (error.message or '').startswith(
            "args_from value for 'source' does not fit the type that build_with declares "
            '(subworkflow task 3): ',
        )

    def test_a_build_that_raises_fails_the_node(self) -> None:
        error = _failure(
            _build(_broker(), _node(sub_definition_key=BrokenChild.definition_key), {}),
        )

        assert error.error_code is OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED
        assert error.message == (
            'Subworkflow build failed: RuntimeError: cannot build (workflow parent:3)'
        )

    def test_a_child_node_with_positional_args_fails_the_node(self) -> None:
        with patch(
            'horsies.core.workflows.lifecycle.guard_no_positional_args',
            side_effect=ValueError('positional args'),
        ):
            error = _failure(_build(_broker(), _node(), {}))

        assert error.error_code is OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED
        assert error.message == 'Invalid child task node args for child_build.value: positional args'

    def test_a_child_task_whose_queue_the_app_refuses_fails_the_node(self) -> None:
        custom = MagicMock()
        custom.app = MagicMock(wraps=APP)
        custom.app.config.queue_mode.name = 'CUSTOM'
        custom.app.get_valid_queue_names.return_value = ['other']
        with patch('horsies.core.workflows.child_build._built_spec') as built_spec:
            built_spec.return_value = ValueChild.build_with(APP)
            error = _failure(_build(custom, _node(), {}))

        assert error.error_code is OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED
        assert (error.message or '').startswith('Invalid queue/priority for child task child_build.value: ')


def _request(**values: Any) -> ChildSpecRequest:
    """A stand-in for the core's ``ChildSpecRequest``: the source reads its attributes."""
    defaults: dict[str, Any] = {
        'parent_workflow_id': uuid.UUID('6f9d4c1e-2b7a-4e3f-9c8d-1a2b3c4d5e6f'),
        'parent_workflow_name': 'parent',
        'task_index': 3,
        'child_depth': 1,
        'definition_key': ValueChild.definition_key,
        'params': 'GIVEN',
        'node_input': '{"value": 4}',
        'args_from': None,
        'sources': [],
    }
    return cast(ChildSpecRequest, SimpleNamespace(**{**defaults, **values}))


class TestBuildChildSpec:
    """The child spec source of the core: ``build_child_within_depth`` at the
    depth of the request."""

    def test_a_child_that_builds_answers_its_spec_data(self) -> None:
        answer = build_child_spec(cast(Any, _broker()), _request())

        assert isinstance(answer, ChildSpecBuilt)
        data = json.loads(answer.spec_json)
        assert data['name'] == ValueChild.name
        assert data['definition_key'] == {'kind': 'KEYED', 'key': ValueChild.definition_key}
        assert [node['dependencies'] for node in data['nodes']] == [[], [0]]
        assert [json.loads(node['input_json']) for node in data['nodes']] == [
            {'value': 4}, {'value': 5},
        ]
        assert data['output'] == {'kind': 'NODE', 'index': 1}
        assert data['queue_classes'] == {'default': 'standard_30d'}

    def test_a_definition_that_does_not_load_answers_the_stored_error(self) -> None:
        answer = build_child_spec(
            cast(Any, _broker()), _request(definition_key='child_build.missing.v1'),
        )

        assert isinstance(answer, ChildSpecRefused)
        assert answer.message == 'Failed to load subworkflow definition for parent:3'
        envelope = json.loads(answer.failure_result)
        assert envelope['err']['error_code'] == {
            '__builtin_task_code__': OperationalErrorCode.SUBWORKFLOW_LOAD_FAILED.value,
        }
        assert envelope['err']['data'] == {'definition_key': 'child_build.missing.v1'}

    @pytest.mark.parametrize(
        ('changes', 'message'),
        [
            pytest.param(
                {'node_input': ''}, 'Corrupt kwargs JSON for subworkflow task 3', id='input',
            ),
            pytest.param(
                {'args_from': ''}, 'Corrupt args_from JSON for subworkflow task 3', id='args_from',
            ),
        ],
    )
    def test_an_empty_stored_text_answers_the_node_failure(
        self, changes: dict[str, Any], message: str,
    ) -> None:
        answer = build_child_spec(cast(Any, _broker()), _request(**changes))

        assert isinstance(answer, ChildSpecRefused)
        assert answer.message == message
        envelope = json.loads(answer.failure_result)
        assert envelope['err']['error_code'] == {
            '__builtin_task_code__': OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value,
        }

    def test_a_build_that_raises_answers_the_node_failure(self) -> None:
        """The node gives the child no params: ``BrokenChild.build_with``
        declares none, so a node input with a member would be refused by
        the typed decode before the build runs."""
        answer = build_child_spec(
            cast(Any, _broker()),
            _request(definition_key=BrokenChild.definition_key, node_input='{}'),
        )

        assert isinstance(answer, ChildSpecRefused)
        assert answer.message.startswith('Subworkflow build failed: RuntimeError: cannot build')

    def test_a_child_whose_spec_data_does_not_encode_answers_the_node_failure(self) -> None:
        answer = build_child_spec(
            cast(Any, _broker()),
            _request(definition_key=SurrogateNameChild.definition_key, node_input='{}'),
        )

        assert isinstance(answer, ChildSpecRefused)
        assert 'does not encode as spec data' in answer.message
        envelope = json.loads(answer.failure_result)
        assert envelope['err']['error_code'] == {
            '__builtin_task_code__': OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED.value,
        }
        assert envelope['err']['data'] == {
            'workflow_id': str(_request().parent_workflow_id),
            'task_index': 3,
        }

    def test_a_source_row_reaches_build_with_as_the_dependency_result(self) -> None:
        source = SimpleNamespace(
            task_index=0,
            node_id='source',
            status='COMPLETED',
            task_name='child_build.value',
            definition_key=None,
            result='{"__h_task_result__":true,"ok":11,"err":null}',
        )
        answer = build_child_spec(
            cast(Any, _broker()),
            _request(
                definition_key=ResultChild.definition_key,
                node_input='{}',
                args_from='{"source": 0}',
                sources=[source],
            ),
        )

        assert isinstance(answer, ChildSpecBuilt)
        data = json.loads(answer.spec_json)
        assert json.loads(data['nodes'][0]['input_json']) == {'value': 11}
