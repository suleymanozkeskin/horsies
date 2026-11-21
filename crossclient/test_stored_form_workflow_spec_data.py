"""Python's side of the workflow spec data vectors (``stored_forms/workflow_spec_data.json``).

A vector's value is the spec data document; Python builds the generated
dataclasses of ``horsies._catalog.workflow_spec`` from it and
``dump_workflow_spec_data`` writes the text that crosses to the core
(``clients/python/horsies/core/workflows/spec_data.py``, ``workflow_spec_json``) with
``dumps_json``, the encoder of every stored form. The core reads the text
with ``parse_workflow_spec_data``; the core and the Rust client test the same
file.
"""

from __future__ import annotations

import dataclasses
import json
from typing import Any, Final, cast

import pytest

from horsies._catalog.workflow_spec import (
    SpecDefinitionKey,
    SpecDefinitionKeyKeyed,
    SpecDefinitionKeyUnkeyed,
    SpecJoin,
    SpecJoinAll,
    SpecJoinAny,
    SpecJoinQuorum,
    SpecNode,
    SpecNodeBody,
    SpecNodeBodySubWorkflow,
    SpecNodeBodyTask,
    SpecOutput,
    SpecOutputNode,
    SpecOutputNoOutput,
    SpecSuccessCase,
    SpecSuccessPolicy,
    SpecSuccessPolicyCases,
    SpecSuccessPolicyNoPolicy,
    SpecTaskOptions,
    SpecTaskOptionsEncoded,
    SpecTaskOptionsNoOptions,
    WorkflowSpecData,
    dump_workflow_spec_data,
)
from horsies.core.codec.json_io import SerializationError
from horsies.core.types.result import Err, Ok
from crossclient.stored_form_vectors import load_form, rows, text_of

_FORM: Final = load_form('workflow_spec_data.json')

type Document = dict[str, Any]


def _definition_key(document: Document) -> SpecDefinitionKey:
    match document:
        case {'kind': 'KEYED', 'key': str() as key}:
            return SpecDefinitionKeyKeyed(key=key)
        case {'kind': 'UNKEYED'}:
            return SpecDefinitionKeyUnkeyed()
        case other:
            raise AssertionError(f'definition_key {other!r}')


def _output(document: Document) -> SpecOutput:
    match document:
        case {'kind': 'NODE', 'index': int() as index}:
            return SpecOutputNode(index=index)
        case {'kind': 'NO_OUTPUT'}:
            return SpecOutputNoOutput()
        case other:
            raise AssertionError(f'output {other!r}')


def _success_policy(document: Document) -> SpecSuccessPolicy:
    match document['kind']:
        case 'CASES':
            cases = cast('list[Document]', document['cases'])
            return SpecSuccessPolicyCases(
                cases=tuple(SpecSuccessCase(required=tuple(case['required'])) for case in cases),
                optional=tuple(cast('list[int]', document['optional'])),
            )
        case 'NO_POLICY':
            return SpecSuccessPolicyNoPolicy()
        case other:
            raise AssertionError(f'success_policy {other!r}')


def _join(document: Document) -> SpecJoin:
    match document:
        case {'kind': 'ALL'}:
            return SpecJoinAll()
        case {'kind': 'ANY'}:
            return SpecJoinAny()
        case {'kind': 'QUORUM', 'min_success': int() as min_success}:
            return SpecJoinQuorum(min_success=min_success)
        case other:
            raise AssertionError(f'join {other!r}')


def _task_options(document: Document) -> SpecTaskOptions:
    match document:
        case {'kind': 'ENCODED', 'json': str() as text}:
            return SpecTaskOptionsEncoded(json=text)
        case {'kind': 'NO_OPTIONS'}:
            return SpecTaskOptionsNoOptions()
        case other:
            raise AssertionError(f'task_options {other!r}')


def _body(document: Document) -> SpecNodeBody:
    match document:
        case {'kind': 'TASK', 'queue': str() as queue, 'priority': int() as priority}:
            return SpecNodeBodyTask(
                queue=queue,
                priority=priority,
                task_options=_task_options(document['task_options']),
            )
        case {'kind': 'SUB_WORKFLOW', 'definition_key': str() as key, 'params': 'GIVEN' | 'NOT_GIVEN'}:
            return SpecNodeBodySubWorkflow(definition_key=key, params=document['params'])
        case other:
            raise AssertionError(f'body {other!r}')


def _node(document: Document) -> SpecNode:
    return SpecNode(
        index=document['index'],
        node_id=document['node_id'],
        task_name=document['task_name'],
        input_json=document['input_json'],
        dependencies=tuple(document['dependencies']),
        args_from=dict(document['args_from']),
        workflow_ctx_from=tuple(document['workflow_ctx_from']),
        allow_failed_deps=document['allow_failed_deps'],
        join=_join(document['join']),
        body=_body(document['body']),
    )


def spec_data_of(document: Document) -> WorkflowSpecData:
    """The generated dataclasses of a spec data document."""
    return WorkflowSpecData(
        data_version=document['data_version'],
        name=document['name'],
        definition_key=_definition_key(document['definition_key']),
        on_error=document['on_error'],
        output=_output(document['output']),
        success_policy=_success_policy(document['success_policy']),
        queue_classes=dict(document['queue_classes']),
        nodes=tuple(_node(node) for node in document['nodes']),
    )


def _document(vector: dict[str, object]) -> Document:
    document = vector['document']
    assert isinstance(document, dict)
    return cast('Document', document)


@pytest.mark.unit
@pytest.mark.parametrize(
    'vector', rows(_FORM, 'vectors'), ids=lambda vector: text_of(vector, 'name'),
)
def test_each_document_encodes_to_its_text(vector: dict[str, object]) -> None:
    assert dump_workflow_spec_data(spec_data_of(_document(vector))) == Ok(text_of(vector, 'text'))


@pytest.mark.unit
def test_a_text_with_a_lone_surrogate_is_refused_by_the_encoder() -> None:
    vector = rows(_FORM, 'vectors')[0]
    data = dataclasses.replace(spec_data_of(_document(vector)), name='checkout\ud800')
    match dump_workflow_spec_data(data):
        case Err(err_value=refused):
            assert isinstance(refused, SerializationError)
        case Ok(ok_value=text):
            raise AssertionError(f'the encoder wrote {text!r}')


@pytest.mark.unit
@pytest.mark.parametrize(
    'vector', rows(_FORM, 'vectors'), ids=lambda vector: text_of(vector, 'name'),
)
def test_each_text_is_its_document_in_member_order(vector: dict[str, object]) -> None:
    def pairs(value: object) -> object:
        match value:
            case dict():
                return [(key, pairs(item)) for key, item in cast('Document', value).items()]
            case list():
                return [pairs(item) for item in cast('list[object]', value)]
            case _:
                return value

    assert pairs(json.loads(text_of(vector, 'text'))) == pairs(_document(vector))
