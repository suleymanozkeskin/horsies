"""Unit tests for the public API snapshot tool (tests/public_api/snapshot.py).

The scope scan, the lookup, and the description of each kind of object. The
classes below set ``__module__`` to a ``horsies.`` module, so the tool treats
them as package classes. The module has no ``from __future__ import
annotations``: the annotations are objects, as the tool resolves them.
"""

import dataclasses
import enum
import sys
import types
import typing
from pathlib import Path

import pydantic
import pytest

from tests.public_api import snapshot
from tests.public_api.snapshot import (
    BoundExceeded,
    Describer,
    Err,
    FileTooLarge,
    FileUnreadable,
    Found,
    ImportedModule,
    ImportedName,
    ModuleNotImportable,
    NameNotFound,
    NoExportList,
    Ok,
    RootMissing,
    ScopeFailure,
    ScopeFileMalformed,
    SkippedStatement,
    SourceUnparsable,
    TooManyFiles,
    describe_scope_failure,
    exports_of,
    imports_of_page,
    imports_of_source,
    look_up,
    public_name_index,
    read_scope_file,
    scan_scope,
    scope_files,
    scope_of_snapshot,
)

pytestmark = pytest.mark.unit

PACKAGE_MODULE = 'horsies.example'


class Color(enum.Enum):
    __module__ = PACKAGE_MODULE
    RED = 'red'
    BLUE = 'blue'

    @property
    def label(self) -> str:
        return self.value


@dataclasses.dataclass(frozen=True, slots=True)
class Point:
    __module__ = PACKAGE_MODULE
    x: int
    tags: list[str] = dataclasses.field(default_factory=list[str])
    label: str = 'origin'


class Settings(pydantic.BaseModel):
    __module__ = PACKAGE_MODULE
    model_config = pydantic.ConfigDict(frozen=True, extra='forbid')
    name: str = pydantic.Field(alias='Name')
    size: int = pydantic.Field(default=1, ge=1)


class Pair(typing.NamedTuple):
    left: int
    right: int = 0


Pair.__module__ = PACKAGE_MODULE


class Options(typing.TypedDict, total=False):
    timeout: float


Options.__module__ = PACKAGE_MODULE


class Hidden:
    """A package class that no scope path exports."""

    __module__ = PACKAGE_MODULE


class Client:
    __module__ = PACKAGE_MODULE
    retries: int
    _secret: str

    def __init__(self, name: str, *, retries: int = 3) -> None:
        self.retries = retries

    def send(self, payload: bytes, hidden: Hidden | None = None) -> bool:
        return bool(payload)

    async def send_async(self, payload: bytes) -> bool:
        return bool(payload)

    @staticmethod
    def version() -> str:
        return '1'

    @classmethod
    def create(cls, name: str) -> 'Client':
        return cls(name)

    @property
    def name(self) -> str:
        return 'client'

    def __enter__(self) -> 'Client':
        return self

    def _private(self) -> None:
        return None


type Alias = int | str


def _describer(public: dict[str, object] | None = None) -> Describer:
    return Describer(public_name_index(public or {}))


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding='utf-8')
    return path


class TestScopeFiles:
    def test_pages_and_sources_are_named_from_the_parent_of_their_root(self, tmp_path: Path) -> None:
        root = tmp_path / 'docs'
        _write(root / 'b.md', '')
        _write(root / 'a' / 'c.mdx', '')
        _write(root / 'd.py', '')
        _write(root / 'e.txt', '')
        match scope_files([root]):
            case Ok(files):
                assert [str(item.shown) for item in files] == ['docs/a/c.mdx', 'docs/b.md', 'docs/d.py']
            case Err(failure):
                raise AssertionError(failure)

    def test_a_missing_root(self, tmp_path: Path) -> None:
        assert scope_files([tmp_path / 'none']) == Err(RootMissing(tmp_path / 'none'))

    def test_more_files_than_the_bound(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(snapshot, 'MAX_SCANNED_FILES', 1)
        _write(tmp_path / 'a.md', '')
        _write(tmp_path / 'b.md', '')
        assert scope_files([tmp_path]) == Err(TooManyFiles(tmp_path, 1))


class TestReadScopeFile:
    def test_a_file_larger_than_the_bound(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(snapshot, 'MAX_FILE_BYTES', 2)
        path = _write(tmp_path / 'a.md', 'abc')
        assert read_scope_file(path) == Err(FileTooLarge(path, 3, 2))

    def test_a_file_that_is_not_utf8(self, tmp_path: Path) -> None:
        path = tmp_path / 'a.md'
        path.write_bytes(b'\xff')
        match read_scope_file(path):
            case Err(FileUnreadable(failed_path, cause)):
                assert failed_path == path
                assert cause.startswith('UnicodeDecodeError')
            case other:
                raise AssertionError(other)


class TestImports:
    def test_a_source_gives_absolute_package_imports_only(self) -> None:
        source = (
            'import horsies.core.x\nimport json\n'
            'from horsies.core.models import a, b as c\nfrom horsies.web import *\n'
            'from . import local\nfrom pydantic import BaseModel\n'
        )
        match imports_of_source(Path('a.py'), source):
            case Ok(found):
                assert set(found) == {
                    ImportedModule('horsies.core.x'),
                    ImportedName('horsies.core.models', 'a'),
                    ImportedName('horsies.core.models', 'b'),
                    ImportedModule('horsies.web'),
                }
            case Err(failure):
                raise AssertionError(failure)

    def test_a_source_that_does_not_parse(self) -> None:
        assert imports_of_source(Path('a.py'), 'def (') == Err(
            SourceUnparsable(Path('a.py'), 1, 'invalid syntax')
        )

    def test_a_page_statement_ends_where_the_parser_says(self) -> None:
        page = (
            'Text.\n\n```python title="x"\n'
            'from horsies import (\n'
            '    Horsies,  # the app (see below)\n'
            '    AppConfig,\n'
            ')\n'
            'def f():\n'
            '    from horsies.core.models.schedule import CronSchedule\n'
            '```\n\n```text\nfrom horsies import Ignored\n```\n'
        )
        found, skipped = imports_of_page(Path('docs/a.md'), page)
        assert set(found) == {
            ImportedName('horsies', 'Horsies'),
            ImportedName('horsies', 'AppConfig'),
            ImportedName('horsies.core.models.schedule', 'CronSchedule'),
        }
        assert skipped == []

    def test_a_page_statement_that_never_parses_is_skipped(self) -> None:
        found, skipped = imports_of_page(Path('docs/a.md'), '```py\nx = 1\nfrom horsies import ...\n```\n')
        assert found == []
        assert skipped == [SkippedStatement('docs/a.md', 3, 'from horsies import ...')]

    def test_scan_collects_pages_and_sources(self, tmp_path: Path) -> None:
        _write(tmp_path / 'docs' / 'a.md', '```python\nfrom horsies import A\n```\n')
        _write(tmp_path / 'app' / 'b.py', 'from horsies.core.errors import B\n')
        match scan_scope([tmp_path / 'docs', tmp_path / 'app']):
            case Ok(result):
                assert result.imports == frozenset(
                    {ImportedName('horsies', 'A'), ImportedName('horsies.core.errors', 'B')}
                )
            case Err(failure):
                raise AssertionError(failure)


class TestScopeOfSnapshot:
    def test_the_recorded_names_and_modules(self, tmp_path: Path) -> None:
        path = _write(
            tmp_path / 's.json',
            '{"scope": {"imported_names": ["horsies.core.errors:ErrorCode"], "imported_modules": ["horsies.x"]}}',
        )
        assert scope_of_snapshot(path) == Ok(
            frozenset({ImportedName('horsies.core.errors', 'ErrorCode'), ImportedModule('horsies.x')})
        )

    def test_a_file_that_is_not_json(self, tmp_path: Path) -> None:
        match scope_of_snapshot(_write(tmp_path / 's.json', '{')):
            case Err(ScopeFileMalformed(_, cause)):
                assert cause.startswith('not JSON')
            case other:
                raise AssertionError(other)

    def test_a_file_without_a_scope(self, tmp_path: Path) -> None:
        path = _write(tmp_path / 's.json', '{}')
        assert scope_of_snapshot(path) == Err(
            ScopeFileMalformed(path, 'no scope.imported_names and scope.imported_modules lists')
        )

    def test_a_name_that_is_not_module_and_name(self, tmp_path: Path) -> None:
        path = _write(tmp_path / 's.json', '{"scope": {"imported_names": ["x"], "imported_modules": []}}')
        assert scope_of_snapshot(path) == Err(ScopeFileMalformed(path, 'imported name \'x\' is not "module:name"'))


class TestLookUp:
    def test_a_name_of_a_module(self) -> None:
        assert look_up(ImportedName('json', 'dumps')) == Found(sys.modules['json'].dumps)

    def test_a_submodule_imported_by_name(self) -> None:
        match look_up(ImportedName('email', 'charset')):
            case Found(types.ModuleType() as module):
                assert module.__name__ == 'email.charset'
            case other:
                raise AssertionError(other)

    def test_a_missing_name(self) -> None:
        assert look_up(ImportedName('json', 'no_such_name')) == NameNotFound('json', 'no_such_name')

    def test_a_module_that_does_not_import(self) -> None:
        match look_up(ImportedModule('horsies_no_such_module')):
            case ModuleNotImportable(module, cause):
                assert module == 'horsies_no_such_module'
                assert cause.startswith('ModuleNotFoundError')
            case other:
                raise AssertionError(other)

    def test_exports_of_a_module_without_an_export_list(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, 'horsies_no_exports', types.ModuleType('horsies_no_exports'))
        assert exports_of('horsies_no_exports') == NoExportList('horsies_no_exports')


class TestTypeText:
    def test_union_members_are_sorted(self) -> None:
        assert _describer().type_text(str | int | None) == 'None | int | str'

    def test_literal_annotated_and_callable(self) -> None:
        describer = _describer()
        assert describer.type_text(typing.Literal['b', 'a']) == "Literal['a', 'b']"
        assert describer.type_text(typing.Annotated[int, 'unit']) == "Annotated[int, 'unit']"
        callable_type = typing.Callable[[int], str]
        assert describer.type_text(callable_type) == 'collections.abc.Callable[[int], str]'

    def test_type_variables_aliases_and_unresolved_text(self) -> None:
        describer = _describer()
        assert describer.type_text(typing.TypeVar('T')) == '~T'
        assert describer.type_text(Alias) == f'{__name__}.Alias'
        assert describer.type_text('Missing') == "unresolved 'Missing'"

    def test_a_class_has_its_shortest_public_path(self) -> None:
        describer = _describer({'horsies.core.x:Hidden': Hidden, 'horsies:Hidden': Hidden})
        assert describer.type_text(Hidden) == 'horsies.Hidden'
        assert describer.reachable() == {}

    def test_a_package_class_without_a_public_path_is_reachable(self) -> None:
        describer = _describer()
        assert describer.type_text(list[Hidden]) == f'list[{PACKAGE_MODULE}.Hidden]'
        assert list(describer.reachable()) == [f'{PACKAGE_MODULE}.Hidden']

    def test_a_type_that_nests_past_the_bound(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(snapshot, 'MAX_TYPE_DEPTH', 1)
        with pytest.raises(BoundExceeded):
            _describer().type_text(list[list[list[int]]])


class TestValueText:
    def test_a_set_is_sorted_and_an_enum_member_is_named(self) -> None:
        text = _describer({'horsies:Color': Color}).value_text(frozenset({Color.RED, Color.BLUE}))
        assert text == 'frozenset({horsies.Color.BLUE, horsies.Color.RED})'

    def test_addresses_are_scrubbed(self) -> None:
        assert _describer().value_text(object()) == '<object object at 0x…>'

    def test_a_value_that_nests_past_the_bound(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(snapshot, 'MAX_VALUE_DEPTH', 1)
        with pytest.raises(BoundExceeded):
            _describer().value_text([[[1]]])


class TestDescribeClass:
    def test_an_enum_records_its_members_and_methods(self) -> None:
        described = _describer().describe('Color', Color)
        assert described['shape'] == {'kind': 'enum', 'members': {'RED': "'red'", 'BLUE': "'blue'"}}
        assert described['members'] == {'label': {'kind': 'property', 'type': 'str', 'settable': False}}

    def test_a_dataclass_records_its_fields(self) -> None:
        shape = _describer().describe('Point', Point)['shape']
        assert shape == {
            'kind': 'dataclass',
            'frozen': True,
            'slots': True,
            'fields': [
                {'name': 'x', 'type': 'int', 'default': 'required', 'init': True, 'kw_only': False},
                {'name': 'tags', 'type': 'list[str]', 'default': 'factory list', 'init': True, 'kw_only': False},
                {'name': 'label', 'type': 'str', 'default': "'origin'", 'init': True, 'kw_only': False},
            ],
        }

    def test_a_pydantic_model_records_its_fields_and_config(self) -> None:
        shape = _describer().describe('Settings', Settings)['shape']
        assert shape == {
            'kind': 'pydantic',
            'config': {'frozen': 'True', 'extra': "'forbid'", 'strict': 'unset'},
            'fields': [
                {'name': 'name', 'type': 'str', 'default': 'required', 'alias': 'Name', 'metadata': []},
                {
                    'name': 'size',
                    'type': 'int',
                    'default': '1',
                    'alias': None,
                    'metadata': ['Ge(ge=1)'],
                },
            ],
        }

    def test_a_named_tuple_and_a_typed_dict(self) -> None:
        describer = _describer()
        assert describer.describe('Pair', Pair)['shape'] == {
            'kind': 'namedtuple',
            'fields': [
                {'name': 'left', 'type': 'int', 'default': 'required'},
                {'name': 'right', 'type': 'int', 'default': '0'},
            ],
        }
        assert describer.describe('Options', Options)['shape'] == {
            'kind': 'typeddict',
            'fields': [{'name': 'timeout', 'type': 'float', 'required': False}],
        }

    def test_a_plain_class_records_its_public_members_and_attributes(self) -> None:
        described = _describer({'horsies:Client': Client}).describe('Client', Client)
        assert described['shape'] == {'kind': 'plain', 'attributes': {'retries': 'int'}}
        members = described['members']
        assert isinstance(members, dict)
        assert sorted(members) == ['__enter__', 'create', 'name', 'send', 'send_async', 'version']
        assert members['create'] == {
            'kind': 'classmethod',
            'signature': {
                'async': 'sync',
                'parameters': [{'name': 'name', 'kind': 'POSITIONAL_OR_KEYWORD', 'default': 'required', 'annotation': 'str'}],
                'returns': 'horsies.Client',
            },
        }
        send_async = members['send_async']
        assert isinstance(send_async, dict)
        signature = send_async['signature']
        assert isinstance(signature, dict)
        assert signature['async'] == 'coroutine'

    def test_the_constructor_signature(self) -> None:
        constructor = _describer().describe('Client', Client)['constructor']
        assert constructor == {
            'async': 'sync',
            'parameters': [
                {'name': 'name', 'kind': 'POSITIONAL_OR_KEYWORD', 'default': 'required', 'annotation': 'str'},
                {'name': 'retries', 'kind': 'KEYWORD_ONLY', 'default': '3', 'annotation': 'int'},
            ],
            'returns': 'None',
        }

    def test_a_signature_that_inspect_cannot_read(self) -> None:
        assert _describer().signature_of(dict) == {
            'unavailable': "ValueError: no signature found for builtin type <class 'dict'>"
        }


class TestDescribeOtherKinds:
    def test_a_module_a_type_alias_and_a_type_form(self) -> None:
        describer = _describer()
        assert describer.describe('json', sys.modules['json']) == {'kind': 'module'}
        assert describer.describe('Alias', Alias) == {
            'kind': 'type alias',
            'defined_as': f'{__name__}.Alias',
            'type_params': [],
            'value': 'int | str',
        }
        assert describer.describe('Form', int | None) == {'kind': 'type form', 'text': 'None | int'}

    def test_the_version_is_recorded_without_its_value(self) -> None:
        assert _describer().describe('__version__', '0.7.0') == {'kind': 'value', 'type': 'str'}

    def test_a_constant_records_its_type_and_value(self) -> None:
        assert _describer().describe('LIMIT', 5) == {'kind': 'value', 'type': 'int', 'value': '5'}


@pytest.mark.parametrize(
    ('failure', 'message'),
    [
        (RootMissing(Path('d')), 'scope folder d does not exist'),
        (TooManyFiles(Path('d'), 1), 'scope folder d: more than 1 pages and .py files'),
        (FileTooLarge(Path('f'), 3, 2), 'scope file f: 3 bytes, more than 2'),
        (FileUnreadable(Path('f'), 'cause'), 'scope file f cannot be read: cause'),
        (SourceUnparsable(Path('f'), 2, 'cause'), 'scope file f:2 does not parse: cause'),
        (ScopeFileMalformed(Path('s'), 'cause'), 'snapshot s: cause'),
    ],
)
def test_each_scope_failure_names_its_subject(failure: ScopeFailure, message: str) -> None:
    assert describe_scope_failure(failure) == message
