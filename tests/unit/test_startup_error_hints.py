"""Every startup error names a fix.

`horsies check` reports a problem the way a compiler does, so an AI coding
agent or a person can act on it. The first class pins that each site that
builds a startup error gives a code and `help_text`. The second class
triggers the sites and reads the report.
"""

from __future__ import annotations

import ast
import dataclasses
import types
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import BaseModel, SecretStr

import horsies
from horsies import (
    AppConfig,
    Horsies,
    PostgresConfig,
    SubWorkflowNode,
    TaskError,
    TaskNode,
    TaskResult,
    WorkflowDefinition,
    WorkflowSpec,
)
from horsies.core.errors import (
    ErrorCode,
    HorsiesError,
    TaskDefinitionError,
    WorkflowValidationError,
)

_ERROR_BUILDERS = frozenset(
    {
        'ConfigurationError',
        'HorsiesError',
        'RegistryError',
        'TaskDefinitionError',
        'WorkflowValidationError',
        'task_definition_error',
        'workflow_validation_error',
    },
)
"""The classes and helpers that build a startup error."""

_HELPERS_FILE = Path('core/errors.py')
"""The one file that the walk skips. Other files named errors.py stay in it."""

_NOT_BUILT_AT_A_SITE = frozenset({'MultipleValidationErrors'})
"""Wraps errors that are built elsewhere, so it adds no message of its own."""

_SITES_FLOOR = 180
"""A scan that finds fewer sites has lost them, and would pass on nothing."""

_DOCS_URL = 'https://suleymanozkeskin.github.io/horsies/internals/serialization/'


@dataclasses.dataclass(frozen=True)
class _Site:
    """One call that builds a startup error."""

    where: str
    line: int
    gives_code: bool
    gives_help: bool


def _called_name(call: ast.Call) -> str | None:
    match call.func:
        case ast.Name(id=name):
            return name
        case ast.Attribute(attr=name):
            return name
        case _:
            return None


def _gives(call: ast.Call, argument: str) -> bool:
    """Whether the call passes the argument with a value that a report shows.

    A literal None or an empty string prints nothing, because the report
    tests truthiness.
    """
    for keyword in call.keywords:
        match keyword:
            case ast.keyword(arg=name, value=ast.Constant(value=None | '')) if name == argument:
                return False
            case ast.keyword(arg=name) if name == argument:
                return True
            case _:
                continue
    return False


def _sites_in(source: str, *, where: str) -> list[_Site]:
    """Walk the syntax tree, so that no spelling of a call hides a site."""
    return [
        _Site(
            where=where,
            line=node.lineno,
            gives_code=_gives(node, 'code'),
            gives_help=_gives(node, 'help_text'),
        )
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call) and _called_name(node) in _ERROR_BUILDERS
    ]


def _library_sites() -> list[_Site]:
    root = Path(horsies.__file__).parent
    sites: list[_Site] = []
    for path in sorted(root.rglob('*.py')):
        where = path.relative_to(root)
        if where == _HELPERS_FILE:
            continue  # The helpers forward `code` and `help_text`.
        sites.extend(_sites_in(path.read_text(), where=str(where)))
    return sites


@pytest.mark.unit
class TestEverySiteGivesACodeAndHelp:
    def test_the_scan_finds_a_site_with_no_help(self) -> None:
        sites = _sites_in(
            "raise WorkflowValidationError('positional', code=ErrorCode.X)",
            where='known',
        )

        assert [site.gives_help for site in sites] == [False]

    def test_the_scan_finds_a_site_with_no_code(self) -> None:
        sites = _sites_in(
            "raise WorkflowValidationError('positional', help_text='fix')",
            where='known',
        )

        assert [site.gives_code for site in sites] == [False]

    def test_the_scan_refuses_an_explicit_none(self) -> None:
        sites = _sites_in(
            "raise ConfigurationError(message='m', help_text=None)",
            where='known',
        )

        assert [site.gives_help for site in sites] == [False]

    def test_the_scan_refuses_an_empty_string(self) -> None:
        sites = _sites_in(
            "raise ConfigurationError(message='m', help_text='')",
            where='known',
        )

        assert [site.gives_help for site in sites] == [False]

    def test_the_scan_knows_each_error_class(self) -> None:
        library_classes = {
            subclass.__name__
            for subclass in HorsiesError.__subclasses__()
            if subclass.__module__.startswith('horsies.')
        }

        assert library_classes - _ERROR_BUILDERS - _NOT_BUILT_AT_A_SITE == set()

    def test_the_scan_reads_a_call_through_a_module(self) -> None:
        sites = _sites_in(
            "raise errors.HorsiesError(message='m', help_text='fix')",
            where='known',
        )

        assert [site.gives_help for site in sites] == [True]

    def test_the_library_has_the_sites_that_the_scan_counts(self) -> None:
        assert len(_library_sites()) >= _SITES_FLOOR

    def test_each_site_in_the_library_gives_help(self) -> None:
        missing = [
            f'{site.where}:{site.line}'
            for site in _library_sites()
            if not site.gives_help
        ]

        assert missing == []

    def test_each_site_in_the_library_gives_a_code(self) -> None:
        missing = [
            f'{site.where}:{site.line}'
            for site in _library_sites()
            if not site.gives_code
        ]

        assert missing == []


def _new_app() -> Horsies:
    return Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr(
                    'postgresql+psycopg://user:password@localhost/unused',
                ),
            ),
        ),
    )


_app = _new_app()


class _Note(BaseModel):
    n: int


class _SubNote(_Note):
    secret: str


@_app.task('hints_one')
def _one() -> TaskResult[int, TaskError]:
    return TaskResult(ok=1)


@_app.task('hints_takes_note')
def _takes_note(*, note: _Note) -> TaskResult[int, TaskError]:
    return TaskResult(ok=note.n)


def _key(label: str) -> str:
    return f'tests.hints.{label}.{uuid.uuid4().hex}'


def _quorum(*, min_success: int | None, join: Any = 'quorum') -> None:
    first = TaskNode(fn=_one, node_id='first')
    second = TaskNode(
        fn=_one,
        node_id='second',
        waits_for=[first],
        join=join,
        min_success=min_success,
    )
    WorkflowSpec(name='hints_join', tasks=[first, second])


def _quorum_with_no_dependencies(*, min_success: int | None) -> None:
    WorkflowSpec(
        name='hints_join_empty',
        tasks=[
            TaskNode(
                fn=_one,
                node_id='only',
                join='quorum',
                min_success=min_success,
            ),
        ],
    )


def _no_tasks() -> None:
    WorkflowSpec(name='hints_empty', tasks=[])


def _blank_node_id() -> None:
    WorkflowSpec(name='hints_blank', tasks=[TaskNode(fn=_one, node_id=' ')])


def _skip_index_assignment(_spec: object) -> None:
    return None


def _index_not_set() -> None:
    with patch.object(WorkflowSpec, '_assign_indices', _skip_index_assignment):
        WorkflowSpec(name='hints_index', tasks=[TaskNode(fn=_one)])


def _task_key() -> None:
    TaskNode(fn=_one).key()


def _subworkflow_key() -> None:
    class Child(WorkflowDefinition[int]):
        name = 'hints_key_child'
        definition_key = _key('key_child')
        only = TaskNode(fn=_one)

    SubWorkflowNode(workflow_def=Child).key()


def _no_name() -> None:
    class NoName(WorkflowDefinition[int]):
        definition_key = _key('no_name')
        only = TaskNode(fn=_one)

    NoName.build(_app)


def _no_nodes() -> None:
    class NoNodes(WorkflowDefinition[int]):
        name = 'hints_no_nodes'
        definition_key = _key('no_nodes')

    NoNodes.build(_app)


def _no_definition_key() -> None:
    class NoKey(WorkflowDefinition[int]):
        name = 'hints_no_key'
        only = TaskNode(fn=_one)

    NoKey.build(_app)


def _duplicate_definition_key() -> None:
    shared = _key('duplicate')

    class First(WorkflowDefinition[int]):
        name = 'hints_first'
        definition_key = shared
        only = TaskNode(fn=_one)

    class Second(WorkflowDefinition[int]):
        name = 'hints_second'
        definition_key = shared
        only = TaskNode(fn=_one)


def _cached_spec() -> None:
    prebuilt: WorkflowSpec[Any] = WorkflowSpec(
        name='hints_cached',
        tasks=[TaskNode(fn=_one)],
    )

    class Cached(WorkflowDefinition[Any]):
        name = 'hints_cached'
        definition_key = _key('cached')
        only = TaskNode(fn=_one)

        @classmethod
        def build_with(cls, app: Any, **params: Any) -> WorkflowSpec[Any]:
            _ = params
            return prebuilt

    Cached.build_with(_app)


def _value_built_past_validation() -> None:
    WorkflowSpec(
        name='hints_read_back',
        tasks=[
            TaskNode(fn=_takes_note, kwargs={'note': _Note.model_construct(n='7')}),
        ],
    )


def _value_with_a_field_the_type_lacks() -> None:
    WorkflowSpec(
        name='hints_fields',
        tasks=[
            TaskNode(fn=_takes_note, kwargs={'note': _SubNote(n=1, secret='kept')}),
        ],
    )


def _broker_that_fails() -> None:
    with patch('horsies.core.app.PostgresBroker', side_effect=RuntimeError('boom')):
        _new_app().get_broker()


@pytest.mark.unit
@pytest.mark.parametrize(
    ('trigger', 'code', 'hint'),
    [
        (_no_tasks, ErrorCode.WORKFLOW_NO_NODES, 'add at least one TaskNode'),
        (_blank_node_id, ErrorCode.WORKFLOW_INVALID_NODE_ID, 'non-empty string'),
        (_index_not_set, ErrorCode.WORKFLOW_INVALID_NODE_ID, 'report this bug'),
        (
            lambda: _quorum(min_success=None),
            ErrorCode.WORKFLOW_INVALID_JOIN,
            'number of dependencies that must succeed',
        ),
        (
            lambda: _quorum(min_success=0),
            ErrorCode.WORKFLOW_INVALID_JOIN,
            '1 or more',
        ),
        (
            lambda: _quorum(min_success=5),
            ErrorCode.WORKFLOW_INVALID_JOIN,
            'at most their number',
        ),
        (
            lambda: _quorum_with_no_dependencies(min_success=1),
            ErrorCode.WORKFLOW_INVALID_JOIN,
            'add nodes to waits_for',
        ),
        (
            lambda: _quorum(min_success=1, join='all'),
            ErrorCode.WORKFLOW_INVALID_JOIN,
            "use join='quorum'",
        ),
        (_task_key, ErrorCode.WORKFLOW_INVALID_NODE_ID, 'set node_id on the node'),
        (
            _subworkflow_key,
            ErrorCode.WORKFLOW_INVALID_NODE_ID,
            'set node_id on the node',
        ),
        (_no_name, ErrorCode.WORKFLOW_NO_NAME, "name = 'my_workflow'"),
        (_no_nodes, ErrorCode.WORKFLOW_NO_NODES, 'in place of build()'),
        (_no_definition_key, ErrorCode.WORKFLOW_NO_DEFINITION_KEY, 'definition_key ='),
        (
            _duplicate_definition_key,
            ErrorCode.WORKFLOW_DUPLICATE_DEFINITION_KEY,
            'its own definition_key',
        ),
        (
            _cached_spec,
            ErrorCode.WORKFLOW_CHECK_BUILDER_EXCEPTION,
            'new WorkflowSpec inside build_with()',
        ),
        (
            _value_built_past_validation,
            ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE,
            'decodes back to its declared type',
        ),
        (
            _value_with_a_field_the_type_lacks,
            ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE,
            'declare the field on that type',
        ),
        (_broker_that_fails, ErrorCode.BROKER_INIT_FAILED, 'check database_url'),
    ],
    ids=[
        'no_tasks',
        'blank_node_id',
        'index_not_set',
        'quorum_without_min_success',
        'min_success_below_one',
        'min_success_above_dependencies',
        'min_success_with_no_dependencies',
        'min_success_without_quorum',
        'task_node_key',
        'subworkflow_node_key',
        'definition_no_name',
        'definition_no_nodes',
        'definition_no_key',
        'definition_duplicate_key',
        'build_with_cached_spec',
        'kwarg_read_back',
        'kwarg_fields_lost',
        'broker_init',
    ],
)
def test_the_report_names_the_fix(
    trigger: Callable[[], None],
    code: ErrorCode,
    hint: str,
) -> None:
    with pytest.raises(HorsiesError) as refused:
        trigger()

    assert refused.value.code is code
    assert refused.value.help_text is not None
    assert hint in refused.value.help_text
    assert 'help' in str(refused.value)


class _RefusedChild(WorkflowDefinition[int]):
    name = 'hints_refused_child'
    definition_key = _key('refused_child')

    @classmethod
    def build_with(cls, app: Any, *, value: Any = None) -> Any:
        raise AssertionError('not built')


@pytest.mark.unit
class TestARefusedSignatureSplitsItsReport:
    """The refusal holds its parts, so the report has one line for each."""

    def _assert_parts(self, error: HorsiesError) -> None:
        assert 'Any is not a concrete type' in error.notes
        assert error.help_text is not None
        assert error.help_text.startswith('use a concrete type')
        assert _DOCS_URL in error.help_text

    def test_at_the_task_decorator(self) -> None:
        with pytest.raises(TaskDefinitionError) as refused:

            @_app.task('hints_banned')
            def banned(*, value: Any) -> TaskResult[int, TaskError]:
                return TaskResult(ok=1)

        assert refused.value.code is ErrorCode.TASK_STRICT_SIGNATURE_VIOLATION
        assert 'Fix:' not in refused.value.message
        assert "parameter 'value'" in refused.value.message
        self._assert_parts(refused.value)

    def test_at_the_spec(self) -> None:
        with pytest.raises(WorkflowValidationError) as refused:
            WorkflowSpec(
                name='hints_parent',
                tasks=[SubWorkflowNode(workflow_def=_RefusedChild)],
            )

        assert refused.value.code is ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE
        self._assert_parts(refused.value)

    def test_at_the_boot_check(self) -> None:
        class Parent:
            child = SubWorkflowNode(workflow_def=_RefusedChild)

        with (
            patch.object(_app, '_discovered_task_modules', ['boot_module']),
            patch.object(
                _app,
                '_resolve_imported_module',
                return_value=types.ModuleType('boot_module'),
            ),
            patch.object(
                _app,
                '_collect_module_workflow_definitions',
                return_value=[Parent],
            ),
            patch.object(Parent, '_require_definition_key', create=True),
        ):
            errors = _app._check_workflow_definitions()

        assert [error.code for error in errors] == [
            ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE,
        ]
        self._assert_parts(errors[0])
