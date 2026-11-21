"""The public API snapshot of the installed ``horsies`` package.

Scope:

- the exports (``__all__``) of ``horsies``, ``horsies.monitoring`` and
  ``horsies.web``;
- every ``from horsies... import name`` and ``import horsies...`` in the
  python blocks of the pages and in the ``.py`` files under the ``--docs``
  folders (the docs, the skill files, the showcase), and the imported names
  of an earlier snapshot (``--scope-from``), so a name that a page stops
  importing stays in scope;
- the horsies classes and type aliases that a recorded type hint, base class
  or value refers to and that no scope path exports (``reachable``).

Per name it records the kind and the shape: the signature of a function
(parameter names, kinds, defaults, resolved type hints, sync or async,
overloads); the bases, MRO, type parameters, constructor, members and fields
of a class (dataclass, Pydantic model, NamedTuple, TypedDict); the members of
an enum; the type and value of a constant; the text of a type alias. A class
is named by its shortest scope path when one exports it. ``defined_as`` (the
module and qualified name of the definition) is information only: a move of
a definition alone is not a change of the API. The value of ``__version__``
is not recorded.

Run it with the interpreter of the environment that holds the package, with
``-I`` and from a folder outside the checkout, so the checkout cannot shadow
the installed package::

    python -I tests/public_api/snapshot.py --docs DIR [--docs DIR ...] \\
        [--scope-from SNAPSHOT] --output OUTPUT

Exit codes: 0 the snapshot is written; 2 a scope file or the output cannot
be read, parsed or written, or a bound was hit (the message names the file
or the bound). A module of the scope that does not import is recorded as
``absent`` with its cause, not as a failure. An exception that the package
raises while it is imported, other than ImportError, ends the run with its
traceback: the package is broken.
"""

from __future__ import annotations

import argparse
import ast
import dataclasses
import enum
import functools
import importlib
import importlib.metadata
import inspect
import json
import re
import sys
import textwrap
import types
import typing
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, assert_never

import pydantic

type JsonValue = str | int | float | bool | None | list[JsonValue] | dict[str, JsonValue]
type JsonObject = dict[str, JsonValue]

FORMAT_VERSION: Final = 1
PACKAGE: Final = 'horsies'
EXPORT_MODULES: Final = ('horsies', 'horsies.monitoring', 'horsies.web')
# Recorded with their type only: the value changes with every release.
UNCOMPARED_VALUES: Final = frozenset({'__version__'})
PAGE_SUFFIXES: Final = frozenset({'.md', '.mdx'})
SOURCE_SUFFIX: Final = '.py'
PYTHON_FENCE_LANGUAGES: Final = frozenset({'python', 'py'})
# Dunder members that define how a user calls or uses an instance.
PROTOCOL_DUNDERS: Final = frozenset(
    {
        '__call__',
        '__enter__',
        '__exit__',
        '__aenter__',
        '__aexit__',
        '__iter__',
        '__aiter__',
        '__next__',
        '__anext__',
        '__await__',
        '__len__',
        '__contains__',
        '__getitem__',
        '__bool__',
    }
)
MAX_SCANNED_FILES: Final = 5_000
MAX_FILE_BYTES: Final = 4 * 1024 * 1024
MAX_REACHABLE: Final = 2_000
MAX_TYPE_DEPTH: Final = 32
MAX_VALUE_DEPTH: Final = 8
MAX_STATEMENT_LINES: Final = 200

EXIT_WRITTEN: Final = 0
EXIT_FAILED: Final = 2

_ADDRESS: Final = re.compile(r' at 0x[0-9a-fA-F]+')
# The first line of a horsies import statement in a python block.
_STATEMENT_START: Final = re.compile(r'[ \t]*(?:from[ \t]+horsies[\w.]*[ \t]+import\b|import[ \t]+horsies\b)')


@dataclass(frozen=True, slots=True)
class Ok[T]:
    value: T


@dataclass(frozen=True, slots=True)
class Err[E]:
    error: E


type Result[T, E] = Ok[T] | Err[E]


# ---- Scope ----


@dataclass(frozen=True, slots=True, order=True)
class ImportedName:
    """``from <module> import <name>``."""

    module: str
    name: str

    def key(self) -> str:
        return f'{self.module}:{self.name}'


@dataclass(frozen=True, slots=True, order=True)
class ImportedModule:
    """``import <module>``, or ``from <module> import *``."""

    module: str


type Imported = ImportedName | ImportedModule


@dataclass(frozen=True, slots=True)
class SkippedStatement:
    """An import statement of a page that does not parse (an elided block)."""

    page: str
    line: int
    text: str


@dataclass(frozen=True, slots=True)
class ScanResult:
    imports: frozenset[Imported]
    skipped: tuple[SkippedStatement, ...]


@dataclass(frozen=True, slots=True)
class RootMissing:
    root: Path


@dataclass(frozen=True, slots=True)
class TooManyFiles:
    root: Path
    bound: int


@dataclass(frozen=True, slots=True)
class FileTooLarge:
    path: Path
    size: int
    bound: int


@dataclass(frozen=True, slots=True)
class FileUnreadable:
    path: Path
    cause: str


@dataclass(frozen=True, slots=True)
class SourceUnparsable:
    path: Path
    line: int
    cause: str


@dataclass(frozen=True, slots=True)
class ScopeFileMalformed:
    path: Path
    cause: str


type ScopeFailure = (
    RootMissing | TooManyFiles | FileTooLarge | FileUnreadable | SourceUnparsable | ScopeFileMalformed
)


@dataclass(frozen=True, slots=True)
class ScopeFile:
    """One page or ``.py`` file, with its name relative to the parent of its root."""

    path: Path
    shown: Path


def scope_files(roots: Sequence[Path]) -> Result[tuple[ScopeFile, ...], ScopeFailure]:
    """The pages and ``.py`` files under ``roots``, sorted per root. Reads no file."""
    found: list[ScopeFile] = []
    for root in roots:
        if not root.is_dir():
            return Err(RootMissing(root))
        for path in sorted(root.rglob('*')):
            if path.suffix not in PAGE_SUFFIXES and path.suffix != SOURCE_SUFFIX:
                continue
            if not path.is_file():
                continue
            if len(found) >= MAX_SCANNED_FILES:
                return Err(TooManyFiles(root, MAX_SCANNED_FILES))
            found.append(ScopeFile(path, path.relative_to(root.parent)))
    return Ok(tuple(found))


def read_scope_file(path: Path) -> Result[str, ScopeFailure]:
    """The UTF-8 text of one scope file, at most ``MAX_FILE_BYTES``."""
    try:
        size = path.stat().st_size
        if size > MAX_FILE_BYTES:
            return Err(FileTooLarge(path, size, MAX_FILE_BYTES))
        return Ok(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError) as exc:
        return Err(FileUnreadable(path, f'{type(exc).__name__}: {exc}'))


def imports_in_tree(tree: ast.AST) -> list[Imported]:
    """The absolute horsies imports of a parsed module or statement."""
    found: list[Imported] = []
    for node in ast.walk(tree):
        match node:
            case ast.ImportFrom(module=str(module), level=0) if _is_package_module(module):
                for alias in node.names:
                    match alias.name:
                        case '*':
                            found.append(ImportedModule(module))
                        case name:
                            found.append(ImportedName(module, name))
            case ast.Import():
                found.extend(
                    ImportedModule(alias.name)
                    for alias in node.names
                    if _is_package_module(alias.name)
                )
            case _:
                pass
    return found


def imports_of_source(path: Path, text: str) -> Result[list[Imported], ScopeFailure]:
    """The horsies imports of one ``.py`` file."""
    try:
        tree = ast.parse(text, filename=str(path))
    except SyntaxError as exc:
        return Err(SourceUnparsable(path, exc.lineno or 0, exc.msg))
    return Ok(imports_in_tree(tree))


@dataclass(frozen=True, slots=True)
class PythonBlock:
    """The lines of one python fence, and the page line of its first line."""

    first_line: int
    lines: tuple[str, ...]


def python_blocks(text: str) -> list[PythonBlock]:
    """The python fences of a page."""
    blocks: list[PythonBlock] = []
    lines = text.splitlines()
    index = 0
    while index < len(lines):
        opening = lines[index].strip()
        index += 1
        if not opening.startswith('```'):
            continue
        language = opening.removeprefix('```').strip().split(' ')[0]
        start = index
        while index < len(lines) and not lines[index].strip().startswith('```'):
            index += 1
        if language in PYTHON_FENCE_LANGUAGES:
            blocks.append(PythonBlock(start + 1, tuple(lines[start:index])))
        index += 1
    return blocks


@dataclass(frozen=True, slots=True)
class Unparsed:
    """No run of lines from the statement start parses."""

    first_line: str


def parse_statement(lines: Sequence[str], start: int) -> ast.Module | Unparsed:
    """The shortest run of ``lines`` from ``start`` that parses, at most
    ``MAX_STATEMENT_LINES`` lines. A comment or a bracket inside the
    statement does not end it early, because the parser decides the end."""
    end_bound = min(len(lines), start + MAX_STATEMENT_LINES)
    for end in range(start + 1, end_bound + 1):
        try:
            return ast.parse(textwrap.dedent('\n'.join(lines[start:end])))
        except SyntaxError:
            continue
    return Unparsed(lines[start].strip())


def imports_of_page(page: Path, text: str) -> tuple[list[Imported], list[SkippedStatement]]:
    """The horsies imports of the python blocks of one page.

    Each import statement is parsed alone, so a block that is a fragment
    still gives its imports. A statement that does not parse is returned as
    skipped, with its page and line.
    """
    found: list[Imported] = []
    skipped: list[SkippedStatement] = []
    for block in python_blocks(text):
        for offset, line in enumerate(block.lines):
            if _STATEMENT_START.match(line) is None:
                continue
            match parse_statement(block.lines, offset):
                case Unparsed(first_line):
                    skipped.append(SkippedStatement(str(page), block.first_line + offset, first_line))
                case tree:
                    found.extend(imports_in_tree(tree))
    return found, skipped


def scan_scope(roots: Sequence[Path]) -> Result[ScanResult, ScopeFailure]:
    """The horsies imports of every page and ``.py`` file under ``roots``.

    A page is named relative to the parent of its root, so the names do not
    depend on where the checkout is.
    """
    match scope_files(roots):
        case Err() as failed:
            return failed
        case Ok(files):
            pass
    imports: set[Imported] = set()
    skipped: list[SkippedStatement] = []
    for scope_file in files:
        match read_scope_file(scope_file.path):
            case Err() as failed:
                return failed
            case Ok(text):
                pass
        shown = scope_file.shown
        if shown.suffix == SOURCE_SUFFIX:
            match imports_of_source(shown, text):
                case Err() as failed:
                    return failed
                case Ok(found):
                    imports.update(found)
        else:
            found, page_skipped = imports_of_page(shown, text)
            imports.update(found)
            skipped.extend(page_skipped)
    return Ok(ScanResult(frozenset(imports), tuple(skipped)))


def scope_of_snapshot(path: Path) -> Result[frozenset[Imported], ScopeFailure]:
    """The imported names and modules recorded in the scope of a snapshot file."""
    match read_scope_file(path):
        case Err() as failed:
            return failed
        case Ok(text):
            pass
    try:
        document: JsonValue = json.loads(text)
    except json.JSONDecodeError as exc:
        return Err(ScopeFileMalformed(path, f'not JSON: {exc}'))
    match document:
        case {'scope': {'imported_names': list(names), 'imported_modules': list(modules)}}:
            pass
        case _:
            return Err(ScopeFileMalformed(path, 'no scope.imported_names and scope.imported_modules lists'))
    found: set[Imported] = set()
    for key in names:
        match key:
            case str() if key.count(':') == 1:
                module, name = key.split(':')
                found.add(ImportedName(module, name))
            case _:
                return Err(ScopeFileMalformed(path, f'imported name {key!r} is not "module:name"'))
    for module in modules:
        match module:
            case str():
                found.add(ImportedModule(module))
            case _:
                return Err(ScopeFileMalformed(path, f'imported module {module!r} is not a string'))
    return Ok(frozenset(found))


def _is_package_module(module: str) -> bool:
    return module == PACKAGE or module.startswith(PACKAGE + '.')


# ---- Lookup ----


@dataclass(frozen=True, slots=True)
class Found:
    value: object


@dataclass(frozen=True, slots=True)
class ModuleNotImportable:
    module: str
    cause: str


@dataclass(frozen=True, slots=True)
class NameNotFound:
    module: str
    name: str


@dataclass(frozen=True, slots=True)
class NoExportList:
    module: str


type Lookup = Found | ModuleNotImportable | NameNotFound


def import_module(module: str) -> types.ModuleType | ModuleNotImportable:
    try:
        return importlib.import_module(module)
    except ImportError as exc:
        return ModuleNotImportable(module, f'{type(exc).__name__}: {exc}')


def look_up(imported: Imported) -> Lookup:
    """The object that an import statement binds in the installed package."""
    match imported:
        case ImportedModule(module):
            loaded = import_module(module)
            match loaded:
                case ModuleNotImportable():
                    return loaded
                case _:
                    return Found(loaded)
        case ImportedName(module, name):
            loaded = import_module(module)
            match loaded:
                case ModuleNotImportable():
                    return loaded
                case _:
                    pass
            if name in vars(loaded):
                return Found(vars(loaded)[name])
            # `from package import submodule` binds a submodule.
            submodule = import_module(f'{module}.{name}')
            match submodule:
                case ModuleNotImportable():
                    return NameNotFound(module, name)
                case _:
                    return Found(submodule)
        case _:
            assert_never(imported)


def exports_of(module: str) -> tuple[ImportedName, ...] | ModuleNotImportable | NoExportList:
    """The names of ``__all__`` of one export module."""
    loaded = import_module(module)
    match loaded:
        case ModuleNotImportable():
            return loaded
        case _:
            pass
    names: object = vars(loaded).get('__all__')
    match names:
        case list() | tuple():
            return tuple(ImportedName(module, str(name)) for name in typing.cast(Iterable[object], names))
        case _:
            return NoExportList(module)


# ---- Description ----


class BoundExceeded(Exception):
    """A bound of the walk was hit. The run fails closed with the subject."""


@dataclass(frozen=True, slots=True)
class UnresolvedAnnotation:
    """An annotation string that does not evaluate in its module."""

    text: str


class ClassKind(enum.Enum):
    ENUM = 'enum'
    PYDANTIC = 'pydantic'
    DATACLASS = 'dataclass'
    TYPEDDICT = 'typeddict'
    NAMEDTUPLE = 'namedtuple'
    PLAIN = 'plain'


def is_named_tuple(cls: type[object]) -> bool:
    return '_fields' in vars(cls) and issubclass(cls, tuple)


def class_kind(cls: type[object]) -> ClassKind:
    if issubclass(cls, enum.Enum):
        return ClassKind.ENUM
    if issubclass(cls, pydantic.BaseModel):
        return ClassKind.PYDANTIC
    if dataclasses.is_dataclass(cls):
        return ClassKind.DATACLASS
    if typing.is_typeddict(cls):
        return ClassKind.TYPEDDICT
    if is_named_tuple(cls):
        return ClassKind.NAMEDTUPLE
    return ClassKind.PLAIN


def is_type_form(value: object) -> bool:
    """Whether ``value`` is a typing construct such as ``A | B`` or ``list[int]``."""
    return isinstance(value, types.UnionType) or typing.get_origin(value) is not None


def scrub(text: str) -> str:
    """``text`` without memory addresses."""
    return _ADDRESS.sub(' at 0x…', text)


def is_package_object(value: type | typing.TypeAliasType) -> bool:
    """Whether a class or type alias is defined in the package. A type alias
    with no module is not."""
    match value.__module__:
        case str(module):
            return _is_package_module(module)
        case None:
            return False


def defined_as(value: type | typing.TypeAliasType) -> str:
    """The module and qualified name of a class or type alias definition; the
    name alone for a type alias with no module."""
    match value:
        case type():
            return f'{value.__module__}.{value.__qualname__}'
        case typing.TypeAliasType():
            match value.__module__:
                case str(module):
                    return f'{module}.{value.__name__}'
                case None:
                    return value.__name__


def is_type_checking_test(test: ast.expr) -> bool:
    """Whether an ``if`` test is ``TYPE_CHECKING`` or ``typing.TYPE_CHECKING``."""
    match test:
        case ast.Name(id='TYPE_CHECKING') | ast.Attribute(attr='TYPE_CHECKING'):
            return True
        case _:
            return False


def type_checking_namespace(module: types.ModuleType) -> dict[str, object]:
    """A copy of the globals of ``module`` with the imports of its top-level
    ``if TYPE_CHECKING:`` blocks executed in it.

    An import that fails (an optional dependency that is not installed) is
    left out; an annotation that needs it stays unresolved. A module whose
    source cannot be read gets its globals only.
    """
    namespace = dict(vars(module))
    try:
        source = inspect.getsource(module)
    except (OSError, TypeError):
        return namespace
    filename = getattr(module, '__file__', None) or module.__name__
    for node in ast.parse(source).body:
        match node:
            case ast.If(test=test, body=body) if is_type_checking_test(test):
                pass
            case _:
                continue
        for statement in body:
            match statement:
                case ast.Import() | ast.ImportFrom():
                    code = compile(ast.Module(body=[statement], type_ignores=[]), filename, 'exec')
                    try:
                        exec(code, namespace)  # noqa: S102 - the package's own import statement
                    except ImportError:
                        continue
                case _:
                    continue
    return namespace


def type_parameters(target: object) -> dict[str, object]:
    """The PEP 695 type parameters of a class or function, by name."""
    parameters = typing.cast(tuple[object, ...], getattr(target, '__type_params__', ()))
    return {str(getattr(parameter, '__name__')): parameter for parameter in parameters}


def class_scope(cls: type) -> dict[str, object]:
    """The names that an annotation in the body of ``cls`` sees besides the
    module globals: the class body and its type parameters."""
    scope = dict(vars(cls))
    scope.update(type_parameters(cls))
    return scope


def is_public_member(name: str) -> bool:
    return not name.startswith('_') or name in PROTOCOL_DUNDERS


def async_kind(function: object) -> str:
    if inspect.isasyncgenfunction(function):
        return 'async generator'
    if inspect.iscoroutinefunction(function):
        return 'coroutine'
    return 'sync'


class Describer:
    """Describes the objects of one snapshot.

    It owns the public name of each scope object (``public_names``, keyed by
    object id) and the queue of the package classes and type aliases that a
    description refers to and no scope path exports. ``reachable()``
    describes the queue until it is empty.
    """

    def __init__(self, public_names: dict[int, str]) -> None:
        self._public_names = public_names
        self._queued: dict[int, type | typing.TypeAliasType] = {}
        self._pending: list[type | typing.TypeAliasType] = []
        self._namespaces: dict[str, dict[str, object]] = {}

    # -- names --

    def name_of(self, value: type | typing.TypeAliasType) -> str:
        public = self._public_names.get(id(value))
        if public is not None:
            return public
        if value.__module__ == 'builtins':
            return defined_as(value).removeprefix('builtins.')
        if is_package_object(value) and id(value) not in self._queued:
            if len(self._queued) >= MAX_REACHABLE:
                raise BoundExceeded(f'more than {MAX_REACHABLE} reachable classes and type aliases')
            self._queued[id(value)] = value
            self._pending.append(value)
        return defined_as(value)

    def reachable(self) -> JsonObject:
        described: JsonObject = {}
        while self._pending:
            value = self._pending.pop()
            name = defined_as(value)
            match value:
                case type():
                    described[name] = self.describe_class(value)
                case typing.TypeAliasType():
                    described[name] = self.describe_type_alias(value)
        return dict(sorted(described.items()))

    # -- types and values --

    def type_text(self, annotation: object, depth: int = 0) -> str:
        if depth > MAX_TYPE_DEPTH:
            raise BoundExceeded(f'a type nests deeper than {MAX_TYPE_DEPTH} levels: {annotation!r}')
        inner = functools.partial(self.type_text, depth=depth + 1)
        match annotation:
            case None | types.NoneType:
                return 'None'
            case types.EllipsisType():
                return '...'
            case UnresolvedAnnotation(text):
                return f'unresolved {text!r}'
            case str():
                return f'unresolved {annotation!r}'
            case typing.ForwardRef():
                return f'unresolved {annotation.__forward_arg__!r}'
            case typing.TypeAliasType():
                return self.name_of(annotation)
            case typing.TypeVar() | typing.ParamSpec() | typing.TypeVarTuple():
                return f'~{annotation.__name__}'
            case list():
                return '[' + ', '.join(inner(item) for item in typing.cast(list[object], annotation)) + ']'
            case _ if is_type_form(annotation):
                return self.type_form_text(annotation, inner)
            case type():
                return self.name_of(annotation)
            case _:
                return scrub(repr(annotation))

    def type_form_text(self, annotation: object, inner: Callable[[object], str]) -> str:
        origin = typing.get_origin(annotation)
        arguments = typing.get_args(annotation)
        if isinstance(annotation, types.UnionType) or origin is typing.Union:
            return ' | '.join(sorted(inner(argument) for argument in arguments))
        if origin is typing.Literal:
            return 'Literal[' + ', '.join(sorted(scrub(repr(argument)) for argument in arguments)) + ']'
        if origin is typing.Annotated:
            metadata = typing.cast(tuple[object, ...], getattr(annotation, '__metadata__'))
            return (
                f'Annotated[{inner(arguments[0])}, '
                + ', '.join(self.value_text(item) for item in metadata)
                + ']'
            )
        match origin:
            case type() | typing.TypeAliasType():
                head = self.name_of(origin)
            case _:
                head = scrub(repr(origin))
        if not arguments:
            return head
        return head + '[' + ', '.join(inner(argument) for argument in arguments) + ']'

    def value_text(self, value: object, depth: int = 0) -> str:
        """``repr`` of a value, with sets sorted and addresses scrubbed."""
        if depth > MAX_VALUE_DEPTH:
            raise BoundExceeded(f'a value nests deeper than {MAX_VALUE_DEPTH} levels: {value!r}')
        inner = functools.partial(self.value_text, depth=depth + 1)
        value_type: type[object] = type(value)
        match value:
            case enum.Enum():
                return f'{self.name_of(value_type)}.{value.name}'
            case set() | frozenset():
                items = sorted(inner(item) for item in typing.cast(Iterable[object], value))
                return f'{value_type.__name__}({{{", ".join(items)}}})'
            case tuple() if value_type is tuple:
                items = [inner(item) for item in typing.cast(tuple[object, ...], value)]
                return '(' + ', '.join(items) + (',)' if len(items) == 1 else ')')
            case list():
                return '[' + ', '.join(inner(item) for item in typing.cast(list[object], value)) + ']'
            case dict():
                pairs = typing.cast(dict[object, object], value).items()
                return '{' + ', '.join(f'{inner(key)}: {inner(item)}' for key, item in pairs) + '}'
            case type() | typing.TypeAliasType():
                return self.name_of(value)
            case _ if is_type_form(value):
                return self.type_text(value)
            case _:
                return scrub(repr(value))

    # -- hints and signatures --

    def namespace_of(self, module_name: str) -> dict[str, object]:
        """The globals of a module plus the imports of its ``if TYPE_CHECKING:``
        blocks, built once per module."""
        cached = self._namespaces.get(module_name)
        if cached is not None:
            return cached
        module = sys.modules.get(module_name)
        namespace = type_checking_namespace(module) if module is not None else {}
        self._namespaces[module_name] = namespace
        return namespace

    def own_hints(self, owner: object, module_name: str, local: dict[str, object]) -> dict[str, object]:
        """The annotations that ``owner`` itself declares, each resolved alone
        with ``typing.get_type_hints`` (quoted names inside an annotation
        resolve too).

        An annotation that does not resolve is kept as ``UnresolvedAnnotation``.
        """
        try:
            raw = inspect.get_annotations(typing.cast(Callable[..., object], owner))
        except TypeError:
            return {}
        namespace = self.namespace_of(module_name)
        resolved: dict[str, object] = {}
        for name, annotation in raw.items():
            # A class holder for a class annotation, where `ClassVar` is valid.
            holder: object = (
                type('Annotations', (), {'__annotations__': {name: annotation}})
                if isinstance(owner, type)
                else types.SimpleNamespace(__annotations__={name: annotation})
            )
            try:
                resolved[name] = typing.get_type_hints(holder, namespace, local, include_extras=True)[name]
            except (NameError, TypeError, AttributeError, SyntaxError):
                resolved[name] = UnresolvedAnnotation(str(annotation))
        return resolved

    def hints_of(self, target: object, owner: type | None = None) -> dict[str, object]:
        """The resolved annotations of a function, or of a class and its bases.

        Each annotation is resolved in the namespace of the module that
        declares it, with the names that its ``if TYPE_CHECKING:`` block
        imports. The names of a class body and the type parameters of a class
        (``owner`` for a method) or of a function are visible too.
        """
        match target:
            case type():
                hints: dict[str, object] = {}
                for base in reversed(target.__mro__):
                    hints.update(self.own_hints(base, base.__module__, class_scope(base)))
                return hints
            case _:
                module_name = getattr(target, '__module__', None)
                local = class_scope(owner) if owner is not None else {}
                local.update(type_parameters(target))
                return self.own_hints(target, module_name if isinstance(module_name, str) else '', local)

    def signature_of(
        self,
        target: object,
        hints: dict[str, object] | None = None,
        owner: type | None = None,
    ) -> JsonObject:
        """The parameters, return type and sync or async kind of a callable.

        ``hints`` replaces the annotations of ``target``; ``owner`` is the
        class that defines a method.
        """
        try:
            signature = inspect.signature(typing.cast(Callable[..., object], target))
        except (ValueError, TypeError) as exc:
            return {'unavailable': f'{type(exc).__name__}: {exc}'}
        resolved = self.hints_of(target, owner) if hints is None else hints
        parameters: list[JsonValue] = []
        for parameter in signature.parameters.values():
            annotation = resolved.get(parameter.name, parameter.annotation)
            parameters.append(
                {
                    'name': parameter.name,
                    'kind': parameter.kind.name,
                    'default': (
                        'required'
                        if parameter.default is inspect.Parameter.empty
                        else self.value_text(parameter.default)
                    ),
                    'annotation': (
                        'unannotated'
                        if annotation is inspect.Parameter.empty
                        else self.type_text(annotation)
                    ),
                }
            )
        returns = resolved.get('return', signature.return_annotation)
        return {
            'async': async_kind(target),
            'parameters': parameters,
            'returns': 'unannotated' if returns is inspect.Signature.empty else self.type_text(returns),
        }

    def describe_function(self, function: object) -> JsonObject:
        overloads: list[JsonValue] = [
            self.signature_of(overload)
            for overload in typing.get_overloads(typing.cast(Callable[..., object], function))
        ]
        qualname = getattr(function, '__qualname__', None)
        return {
            'kind': 'function',
            'defined_as': f'{getattr(function, "__module__", None)}.{qualname}',
            'signature': self.signature_of(function),
            'overloads': overloads,
        }

    # -- classes --

    def describe_class(self, cls: type) -> JsonObject:
        return {
            'kind': 'class',
            'defined_as': defined_as(cls),
            'bases': [self.name_of(base) for base in cls.__bases__],
            'mro': [self.name_of(base) for base in cls.__mro__[1:]],
            'type_params': [
                self.type_text(parameter)
                for parameter in typing.cast(tuple[object, ...], getattr(cls, '__parameters__', ()))
            ],
            'abstract': inspect.isabstract(cls),
            'protocol': typing.is_protocol(cls),
            'shape': self.shape_of(cls),
            'constructor': self.constructor_of(cls),
            'members': self.members_of(cls),
        }

    def constructor_of(self, cls: type) -> JsonObject:
        """``inspect.signature(cls)``, with the hints of ``__init__`` and of the class."""
        hints = self.hints_of(cls)
        hints.update(self.hints_of(cls.__init__))
        return self.signature_of(cls, hints)

    def shape_of(self, cls: type) -> JsonObject:
        kind = class_kind(cls)
        match kind:
            case ClassKind.ENUM:
                members = typing.cast(dict[str, enum.Enum], getattr(cls, '__members__'))
                return {
                    'kind': kind.value,
                    'members': {name: self.value_text(member.value) for name, member in members.items()},
                }
            case ClassKind.PYDANTIC:
                return self.pydantic_shape(typing.cast(type[pydantic.BaseModel], cls))
            case ClassKind.DATACLASS:
                return self.dataclass_shape(cls)
            case ClassKind.TYPEDDICT:
                hints = self.hints_of(cls)
                required = typing.cast(frozenset[str], getattr(cls, '__required_keys__'))
                return {
                    'kind': kind.value,
                    'fields': [
                        {'name': name, 'type': self.type_text(hint), 'required': name in required}
                        for name, hint in hints.items()
                    ],
                }
            case ClassKind.NAMEDTUPLE:
                hints = self.hints_of(cls)
                defaults = typing.cast(dict[str, object], getattr(cls, '_field_defaults'))
                fields = typing.cast(tuple[str, ...], getattr(cls, '_fields'))
                return {
                    'kind': kind.value,
                    'fields': [
                        {
                            'name': name,
                            'type': self.type_text(hints.get(name, inspect.Parameter.empty)),
                            'default': self.value_text(defaults[name]) if name in defaults else 'required',
                        }
                        for name in fields
                    ],
                }
            case ClassKind.PLAIN:
                return {
                    'kind': kind.value,
                    'attributes': {
                        name: self.type_text(hint)
                        for name, hint in self.hints_of(cls).items()
                        if not name.startswith('_')
                    },
                }
            case _:
                assert_never(kind)

    def pydantic_shape(self, cls: type[pydantic.BaseModel]) -> JsonObject:
        config = cls.model_config
        fields: list[JsonValue] = []
        for name, info in cls.model_fields.items():
            if info.is_required():
                default = 'required'
            elif info.default_factory is not None:
                default = f'factory {getattr(info.default_factory, "__qualname__", repr(info.default_factory))}'
            else:
                default = self.value_text(info.default)
            fields.append(
                {
                    'name': name,
                    'type': self.type_text(info.annotation),
                    'default': default,
                    'alias': info.alias,
                    'metadata': [self.value_text(item) for item in info.metadata],
                }
            )
        settings: dict[str, object] = dict(config)
        return {
            'kind': ClassKind.PYDANTIC.value,
            'config': {
                key: self.value_text(settings[key]) if key in settings else 'unset'
                for key in ('frozen', 'extra', 'strict')
            },
            'fields': fields,
        }

    def dataclass_shape(self, cls: type) -> JsonObject:
        hints = self.hints_of(cls)
        params = getattr(cls, '__dataclass_params__')
        fields: list[JsonValue] = []
        for field in dataclasses.fields(cls):
            if field.default is not dataclasses.MISSING:
                default = self.value_text(field.default)
            elif field.default_factory is not dataclasses.MISSING:
                default = f'factory {getattr(field.default_factory, "__qualname__", repr(field.default_factory))}'
            else:
                default = 'required'
            fields.append(
                {
                    'name': field.name,
                    'type': self.type_text(hints.get(field.name, field.type)),
                    'default': default,
                    'init': field.init,
                    'kw_only': field.kw_only is True,
                }
            )
        return {
            'kind': ClassKind.DATACLASS.value,
            'frozen': bool(getattr(params, 'frozen')),
            'slots': '__slots__' in vars(cls),
            'fields': fields,
        }

    def shape_names(self, cls: type) -> frozenset[str]:
        """Names that the shape records, left out of the members."""
        kind = class_kind(cls)
        match kind:
            case ClassKind.ENUM:
                return frozenset(typing.cast(dict[str, object], getattr(cls, '__members__')))
            case ClassKind.PYDANTIC:
                return frozenset({'model_config'})
            case ClassKind.DATACLASS:
                return frozenset(field.name for field in dataclasses.fields(cls))
            case ClassKind.NAMEDTUPLE:
                return frozenset(typing.cast(tuple[str, ...], getattr(cls, '_fields')))
            case ClassKind.TYPEDDICT | ClassKind.PLAIN:
                return frozenset()
            case _:
                assert_never(kind)

    def members_of(self, cls: type) -> JsonObject:
        """The public members that a package class of the MRO defines.

        A name belongs to the first class of the MRO that defines it, so a
        member that a third-party base overrides is not recorded.
        """
        skip = self.shape_names(cls)
        seen: set[str] = set()
        members: JsonObject = {}
        for klass in cls.__mro__:
            for name, raw in vars(klass).items():
                if name in seen:
                    continue
                seen.add(name)
                if name in skip or not is_public_member(name) or not is_package_object(klass):
                    continue
                members[name] = self.describe_member(cls, klass, name, raw)
        return dict(sorted(members.items()))

    def describe_member(self, cls: type, owner: type, name: str, raw: object) -> JsonObject:
        """One member ``name`` of ``cls``, as ``owner`` (a class of the MRO) defines it."""
        match raw:
            # Through the class, a static method is its function and a class
            # method is bound (its signature has no `cls`).
            case staticmethod():
                return {'kind': 'staticmethod', 'signature': self.signature_of(getattr(cls, name), owner=owner)}
            case classmethod():
                return {'kind': 'classmethod', 'signature': self.signature_of(getattr(cls, name), owner=owner)}
            case property(fget=None):
                return {'kind': 'property', 'type': 'no getter', 'settable': raw.fset is not None}
            case property(fget=getter):
                returns = self.hints_of(getter, owner).get('return', inspect.Parameter.empty)
                return {
                    'kind': 'property',
                    'type': 'unannotated' if returns is inspect.Parameter.empty else self.type_text(returns),
                    'settable': raw.fset is not None,
                }
            case functools.cached_property():
                getter = typing.cast(Callable[..., object], raw.func)
                returns = self.hints_of(getter, owner).get('return', inspect.Parameter.empty)
                return {
                    'kind': 'cached property',
                    'type': 'unannotated' if returns is inspect.Parameter.empty else self.type_text(returns),
                }
            case type():
                return {'kind': 'class', 'type': self.name_of(raw)}
            case _ if inspect.isfunction(raw):
                return {
                    'kind': 'method',
                    'signature': self.signature_of(raw, owner=owner),
                    'overloads': [
                        self.signature_of(overload, owner=owner) for overload in typing.get_overloads(raw)
                    ],
                }
            case _ if inspect.ismethoddescriptor(raw) or inspect.isbuiltin(raw):
                return {'kind': 'method', 'signature': self.signature_of(raw, owner=owner), 'overloads': []}
            case _:
                return {'kind': 'attribute', 'type': self.name_of(type(raw)), 'value': self.value_text(raw)}

    # -- other kinds --

    def describe_type_alias(self, alias: typing.TypeAliasType) -> JsonObject:
        return {
            'kind': 'type alias',
            'defined_as': defined_as(alias),
            'type_params': [self.type_text(parameter) for parameter in alias.__type_params__],
            'value': self.type_text(alias.__value__),
        }

    def describe(self, name: str, value: object) -> JsonObject:
        """The description of one scope object."""
        match value:
            case types.ModuleType():
                return {'kind': 'module'}
            case type():
                return self.describe_class(value)
            case typing.TypeAliasType():
                return self.describe_type_alias(value)
            case typing.TypeVar() | typing.ParamSpec() | typing.TypeVarTuple():
                return {'kind': 'type variable', 'text': self.type_text(value)}
            case _ if is_type_form(value):
                return {'kind': 'type form', 'text': self.type_text(value)}
            case _ if inspect.isfunction(value) or inspect.isbuiltin(value):
                return self.describe_function(value)
            case _ if name in UNCOMPARED_VALUES:
                return {'kind': 'value', 'type': self.name_of(type(value))}
            case _:
                return {'kind': 'value', 'type': self.name_of(type(value)), 'value': self.value_text(value)}


# ---- Snapshot ----


def public_name_index(found: dict[str, object]) -> dict[int, str]:
    """The shortest scope path of each class and type alias of the scope.

    ``found`` maps a scope key (``module:name``) to its object. A path with
    fewer module parts wins, then the lower module and name.
    """
    candidates: dict[int, list[tuple[int, str, str]]] = {}
    for key, value in found.items():
        if not isinstance(value, (type, typing.TypeAliasType)):
            continue
        module, _, name = key.partition(':')
        candidates.setdefault(id(value), []).append((module.count('.'), module, name))
    return {
        identity: f'{module}.{name}'
        for identity, options in candidates.items()
        for _, module, name in [min(options)]
    }


def absent_entry(lookup: ModuleNotImportable | NameNotFound | NoExportList) -> JsonObject:
    match lookup:
        case ModuleNotImportable(module, cause):
            return {'kind': 'absent', 'cause': f'module {module} does not import: {cause}'}
        case NameNotFound(module, name):
            return {'kind': 'absent', 'cause': f'module {module} has no name {name}'}
        case NoExportList(module):
            return {'kind': 'absent', 'cause': f'module {module} has no __all__ list'}
        case _:
            assert_never(lookup)


def scope_keys(imports: Iterable[Imported]) -> dict[str, Imported]:
    """Each import under its key: ``module:name`` for a name, ``module`` for a module."""
    keyed: dict[str, Imported] = {}
    for imported in imports:
        match imported:
            case ImportedName():
                keyed[imported.key()] = imported
            case ImportedModule(module):
                keyed[module] = imported
            case _:
                assert_never(imported)
    return dict(sorted(keyed.items()))


def build_snapshot(scan: ScanResult, extra: frozenset[Imported]) -> JsonObject:
    """The snapshot of the installed package over the export modules and ``scan``."""
    entries: JsonObject = {}
    exported: list[Imported] = []
    for module in EXPORT_MODULES:
        names = exports_of(module)
        match names:
            case ModuleNotImportable() | NoExportList():
                entries[module] = absent_entry(names)
            case _:
                exported.extend(names)
    imported = scope_keys(scan.imports | extra)
    keyed = scope_keys([*exported, *imported.values()])
    found: dict[str, object] = {}
    for key, item in keyed.items():
        lookup = look_up(item)
        match lookup:
            case Found(value):
                found[key] = value
            case ModuleNotImportable() | NameNotFound():
                entries[key] = absent_entry(lookup)
    describer = Describer(public_name_index(found))
    for key, value in found.items():
        entries[key] = describer.describe(key.partition(':')[2], value)
    names_in_scope = [key for key, item in imported.items() if isinstance(item, ImportedName)]
    modules_in_scope = [key for key, item in imported.items() if isinstance(item, ImportedModule)]
    return {
        'format': FORMAT_VERSION,
        'package_version': importlib.metadata.version(PACKAGE),
        'scope': {
            'export_modules': list(EXPORT_MODULES),
            'imported_names': list(names_in_scope),
            'imported_modules': list(modules_in_scope),
            'skipped_statements': [
                f'{skipped.page}:{skipped.line}: {skipped.text}' for skipped in scan.skipped
            ],
        },
        'entries': dict(sorted(entries.items())),
        'reachable': describer.reachable(),
    }


def describe_scope_failure(failure: ScopeFailure) -> str:
    match failure:
        case RootMissing(root):
            return f'scope folder {root} does not exist'
        case TooManyFiles(root, bound):
            return f'scope folder {root}: more than {bound} pages and .py files'
        case FileTooLarge(path, size, bound):
            return f'scope file {path}: {size} bytes, more than {bound}'
        case FileUnreadable(path, cause):
            return f'scope file {path} cannot be read: {cause}'
        case SourceUnparsable(path, line, cause):
            return f'scope file {path}:{line} does not parse: {cause}'
        case ScopeFileMalformed(path, cause):
            return f'snapshot {path}: {cause}'
        case _:
            assert_never(failure)


def write_json(path: Path, document: JsonObject) -> Result[None, str]:
    try:
        path.write_text(json.dumps(document, indent=1, sort_keys=True, ensure_ascii=False) + '\n', encoding='utf-8')
    except OSError as exc:
        return Err(f'output {path} cannot be written: {type(exc).__name__}: {exc}')
    return Ok(None)


def main(arguments: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description='Write the public API snapshot of the installed horsies.')
    parser.add_argument('--docs', type=Path, action='append', required=True, help='a folder of pages and .py files')
    parser.add_argument('--scope-from', type=Path, help='an earlier snapshot whose imported names stay in scope')
    parser.add_argument('--output', type=Path, required=True)
    options = parser.parse_args(arguments)
    docs = typing.cast(list[Path], options.docs)
    match scan_scope([root.resolve() for root in docs]):
        case Err(failure):
            print(describe_scope_failure(failure), file=sys.stderr)
            return EXIT_FAILED
        case Ok(scan):
            pass
    scope_from = typing.cast(Path | None, options.scope_from)
    extra: frozenset[Imported] = frozenset()
    if scope_from is not None:
        match scope_of_snapshot(scope_from):
            case Err(failure):
                print(describe_scope_failure(failure), file=sys.stderr)
                return EXIT_FAILED
            case Ok(extra):
                pass
    try:
        snapshot = build_snapshot(scan, extra)
    except BoundExceeded as exc:
        print(f'the walk of the public API hit a bound: {exc}', file=sys.stderr)
        return EXIT_FAILED
    match write_json(typing.cast(Path, options.output), snapshot):
        case Err(message):
            print(message, file=sys.stderr)
            return EXIT_FAILED
        case Ok():
            return EXIT_WRITTEN


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
