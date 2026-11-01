"""Frozen inventory of JSON parsing and serializing in the runtime package.

``horsies/core/codec/json_io.py`` is the JSON boundary. ``loads_json`` carries
the strict hooks, so a parse anywhere else lets ``NaN``, ``Infinity`` or
``1e9999`` into the system. This inventory is the tripwire: an added parse
fails here, and an added, removed, or moved ``json.dumps`` fails here and must
be reviewed before the inventory changes.

Scope: every ``.py`` file under ``horsies/``. The scan resolves import aliases
and also counts a reference that is passed without a call, such as
``json_deserializer=json.loads``.

The ``json.dumps`` sites listed here serialize values the library builds, and
stay outside ``dumps_json`` because that returns a Result each site cannot
fail on, and runs the strict walker on the claim path. Each one sets
``allow_nan=False``. ``enqueue_fingerprint`` also stays outside
``dumps_canonical_json``: see ``tests/unit/test_enqueue_fingerprint.py``.

Known limits:

- The database driver parses JSONB columns, and this scan cannot see that.
  ``test_jsonb_columns_are_frozen`` lists the columns, which hold library-built
  config. A new JSONB column with task input must go through review here. Task
  input and results are ``Text`` columns, so they reach ``loads_json``. Only
  columns declared through ``mapped_column`` are listed.
- A dynamic access such as ``getattr(json, 'loads')`` is not seen.
"""

from __future__ import annotations

import ast
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pytest

pytestmark = [pytest.mark.unit]

_REPO_ROOT: Final = Path(__file__).resolve().parents[2]
_RUNTIME_ROOT: Final = _REPO_ROOT / 'horsies'
_CODEC_MODULE: Final = 'horsies/core/codec/json_io.py'
_MODULE_SCOPE: Final = '<module>'

_JSON_MODULES: Final = frozenset({'json', 'json.decoder', 'json.encoder'})
_JSON_NAMES: Final = frozenset(
    {'load', 'loads', 'dump', 'dumps', 'JSONDecoder', 'JSONEncoder'}
)
_PYDANTIC_PARSE_METHODS: Final = frozenset(
    {'validate_json', 'model_validate_json'}
)

_FROZEN_DUMPS: Final = Counter(
    {
        (_CODEC_MODULE, 'dumps_json'): 1,
        (_CODEC_MODULE, 'dumps_canonical_json'): 1,
        ('horsies/core/history/maintenance/coverage.py', '_expected_registrations'): 1,
        ('horsies/core/utils/fingerprint.py', 'enqueue_fingerprint'): 1,
        ('horsies/core/worker/claiming.py', 'ClaimMixin._claim_and_dispatch_all'): 4,
        ('horsies/core/workflows/recovery.py', 'recover_stuck_workflows'): 1,
        ('horsies/core/workflows/recovery.py', '_recover_global_orphan_candidate'): 1,
        ('horsies/web/app.py', '_inject'): 1,
        ('horsies/web/routes/events.py', 'build_router.stream'): 2,
    }
)

_FROZEN_JSONB_COLUMNS: Final = frozenset(
    {
        ('horsies/core/models/task_pg.py', 'queue_priorities'),
        ('horsies/core/models/task_pg.py', 'queue_max_concurrency'),
        ('horsies/core/models/task_pg.py', 'recovery_config'),
        ('horsies/core/models/workflow_pg.py', 'success_policy'),
        ('horsies/core/models/workflow_pg.py', 'args_from'),
    }
)


@dataclass(frozen=True, slots=True)
class JsonUse:
    """One use of a JSON parser or serializer in a source file."""

    module: str
    scope: str
    name: str
    allow_nan_false: bool


def _root_name(node: ast.expr) -> str | None:
    match node:
        case ast.Attribute(value=value):
            return _root_name(value)
        case ast.Name(id=name):
            return name
        case _:
            return None


def _json_bindings(tree: ast.Module) -> tuple[frozenset[str], dict[str, str]]:
    """The names bound to a json module, and the names imported from one."""
    modules: set[str] = set()
    imported: dict[str, str] = {}
    for node in ast.walk(tree):
        match node:
            case ast.Import(names=aliases):
                for alias in aliases:
                    if alias.name in _JSON_MODULES:
                        modules.add(alias.asname or alias.name.split('.')[0])
            case ast.ImportFrom(module=module, names=aliases) if (
                module in _JSON_MODULES
            ):
                for alias in aliases:
                    if alias.name in _JSON_NAMES:
                        imported[alias.asname or alias.name] = alias.name
            case _:
                pass
    return frozenset(modules), imported


def _sets_allow_nan_false(call: ast.Call) -> bool:
    return any(
        keyword.arg == 'allow_nan'
        and isinstance(keyword.value, ast.Constant)
        and keyword.value.value is False
        for keyword in call.keywords
    )


class _Scanner(ast.NodeVisitor):
    def __init__(
        self,
        module: str,
        json_modules: frozenset[str],
        imported: dict[str, str],
    ) -> None:
        self.module = module
        self.json_modules = json_modules
        self.imported = imported
        self.scope: list[str] = []
        self.uses: list[JsonUse] = []

    def _json_name(self, node: ast.expr) -> str | None:
        match node:
            case ast.Attribute(attr=attr) if attr in _PYDANTIC_PARSE_METHODS:
                return attr
            case ast.Attribute(attr=attr) if attr in _JSON_NAMES:
                return attr if _root_name(node) in self.json_modules else None
            case ast.Name(id=name):
                return self.imported.get(name)
            case _:
                return None

    def _record(self, name: str, *, allow_nan_false: bool) -> None:
        self.uses.append(
            JsonUse(
                module=self.module,
                scope='.'.join(self.scope) or _MODULE_SCOPE,
                name=name,
                allow_nan_false=allow_nan_false,
            )
        )

    def _visit_scope(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef
    ) -> None:
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_scope(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_scope(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._visit_scope(node)

    def visit_Call(self, node: ast.Call) -> None:
        name = self._json_name(node.func)
        if name is None:
            self.generic_visit(node)
            return
        self._record(name, allow_nan_false=_sets_allow_nan_false(node))
        for child in (*node.args, *node.keywords):
            self.visit(child)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        name = self._json_name(node)
        if name is not None:
            self._record(name, allow_nan_false=False)
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        name = self._json_name(node)
        if name is not None:
            self._record(name, allow_nan_false=False)


def scan_source(source: str, module: str = 'sample.py') -> list[JsonUse]:
    tree = ast.parse(source)
    json_modules, imported = _json_bindings(tree)
    scanner = _Scanner(module, json_modules, imported)
    scanner.visit(tree)
    return scanner.uses


def _runtime_sources() -> list[tuple[str, str]]:
    return [
        (path.relative_to(_REPO_ROOT).as_posix(), path.read_text())
        for path in sorted(_RUNTIME_ROOT.rglob('*.py'))
    ]


def _runtime_uses() -> list[JsonUse]:
    return [
        use
        for module, source in _runtime_sources()
        for use in scan_source(source, module)
    ]


def _jsonb_columns(source: str, module: str) -> set[tuple[str, str]]:
    return {
        (module, node.target.id)
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == 'mapped_column'
        and any(
            isinstance(argument, ast.Name) and argument.id == 'JSONB'
            for argument in node.value.args
        )
    }


class TestRuntimeInventory:
    def test_scan_covers_the_runtime_package(self) -> None:
        modules = {module for module, _ in _runtime_sources()}
        assert _CODEC_MODULE in modules
        assert len(modules) > 100

    def test_parsing_stays_in_the_codec(self) -> None:
        parsing = Counter(
            (use.module, use.scope, use.name)
            for use in _runtime_uses()
            if use.name != 'dumps'
        )
        assert parsing == Counter({(_CODEC_MODULE, 'loads_json', 'loads'): 1})

    def test_json_dumps_sites_are_frozen(self) -> None:
        dumps = Counter(
            (use.module, use.scope)
            for use in _runtime_uses()
            if use.name == 'dumps'
        )
        assert dumps == _FROZEN_DUMPS

    def test_every_json_dumps_refuses_non_finite_floats(self) -> None:
        unflagged = [
            use
            for use in _runtime_uses()
            if use.name == 'dumps' and not use.allow_nan_false
        ]
        assert unflagged == []

    def test_jsonb_columns_are_frozen(self) -> None:
        columns = {
            column
            for module, source in _runtime_sources()
            for column in _jsonb_columns(source, module)
        }
        assert columns == _FROZEN_JSONB_COLUMNS


class TestScanner:
    """The scan must see each way to reach a parser, or the inventory is blind."""

    @pytest.mark.parametrize(
        ('source', 'name'),
        [
            ('import json\njson.loads("1")\n', 'loads'),
            ('import json as codec\ncodec.loads("1")\n', 'loads'),
            ('from json import loads\nloads("1")\n', 'loads'),
            ('from json import loads as parse\nparse("1")\n', 'loads'),
            ('import json\njson.load(handle)\n', 'load'),
            ('import json\ndeserialize = json.loads\n', 'loads'),
            ('import json\nconnect(deserializer=json.loads)\n', 'loads'),
            ('from json import loads\nconnect(deserializer=loads)\n', 'loads'),
            ('import json\njson.JSONDecoder().decode("1")\n', 'JSONDecoder'),
            ('from json.decoder import JSONDecoder\nJSONDecoder()\n', 'JSONDecoder'),
            ('import json.decoder\njson.decoder.JSONDecoder()\n', 'JSONDecoder'),
            ('import json\njson.decoder.JSONDecoder()\n', 'JSONDecoder'),
            ('import json.decoder as dec\ndec.JSONDecoder()\n', 'JSONDecoder'),
            ('import json\njson.JSONEncoder().encode(1)\n', 'JSONEncoder'),
            ('adapter.validate_json("1")\n', 'validate_json'),
            ('Model.model_validate_json("1")\n', 'model_validate_json'),
        ],
    )
    def test_finds_the_use(self, source: str, name: str) -> None:
        assert [use.name for use in scan_source(source)] == [name]

    @pytest.mark.parametrize(
        'source',
        [
            'import pickle\npickle.loads(blob)\n',
            'def loads(text):\n    return text\nloads("1")\n',
            'import json\nvalue = json.JSONDecodeError\n',
        ],
    )
    def test_ignores_what_is_not_a_json_parser(self, source: str) -> None:
        assert scan_source(source) == []

    @pytest.mark.parametrize(
        ('call', 'expected'),
        [
            ('json.dumps(1)', False),
            ('json.dumps(1, allow_nan=True)', False),
            ('json.dumps(1, allow_nan=flag)', False),
            ('json.dumps(1, allow_nan=False)', True),
        ],
    )
    def test_reads_the_allow_nan_flag(self, call: str, expected: bool) -> None:
        uses = scan_source(f'import json\n{call}\n')
        assert [use.allow_nan_false for use in uses] == [expected]

    def test_names_the_enclosing_scope(self) -> None:
        source = (
            'import json\n'
            'class Box:\n'
            '    async def send(self):\n'
            '        def inner():\n'
            '            return json.dumps(1)\n'
        )
        assert [use.scope for use in scan_source(source)] == ['Box.send.inner']

    def test_a_nested_call_in_an_argument_is_counted(self) -> None:
        source = 'import json\njson.dumps(json.loads("1"), allow_nan=False)\n'
        assert sorted(use.name for use in scan_source(source)) == ['dumps', 'loads']

    def test_a_jsonb_column_is_found(self) -> None:
        source = 'class Row:\n    config: Mapped[dict] = mapped_column(JSONB)\n'
        assert _jsonb_columns(source, 'row.py') == {('row.py', 'config')}
