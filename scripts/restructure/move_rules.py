"""Path rules of the 0.8.0 repository restructure.

The restructure puts the core crate and each client in its own folder:

    core/             the core crate (was crates/horsies-core), with baseline/,
                      webui-dist/ and derive/ (was crates/horsies-core-derive)
    clients/python/   horsies/ (was python/horsies), bridge/ (was
                      crates/horsies-python), codegen/ (was
                      crates/horsies-codegen), tests/ (was tests/ without the
                      cross-client entries), examples/ (was showcase/),
                      pyproject.toml, uv.lock, the two pyright configs
    clients/rust/     horsies/ (was crates/horsies), macros/ (was
                      crates/horsies-macros), tests/ (was rust/tests),
                      examples/ (was rust/examples, with rust/showcase at
                      examples/showcase/), scripts/ (was rust/scripts)
    tests/            cross-client only: parity/ with the kept snapshot files
                      at parity/snapshot/ (was tests/dualrun), perf/ with its
                      compose file and its unit tests, stored_form_vectors.py
                      with its unit tests, and links to the four Python client
                      test modules that these harnesses import
    webui/ website/ scripts/   unchanged

This module holds the data of the move and the pure text rules. It does no
I/O and runs no git command: ``restructure.py`` moves the files and calls the
rules, and ``check_old_paths.py`` scans the result. Each rule takes the old
path of a file, its new path and its text, and returns the new text and the
sites that a person checks by hand (``Review``). A rule changes a path only
when it can tell what the path names; a site that it cannot tell is a
``Review``, never a guess.

Standard library only; Python 3.13 or later (``PurePosixPath.full_match``).
"""

from __future__ import annotations

import posixpath
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum
from pathlib import PurePosixPath
from typing import Final

# ---------------------------------------------------------------------------
# The move
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Move:
    """A tracked file or folder and its path after the restructure."""

    old: str
    new: str


@dataclass(frozen=True, slots=True)
class Stay:
    """A path under a moved folder that keeps its place, and why."""

    path: str
    reason: str


PERF_UNIT_TESTS: Final = (
    'test_perf_release_patches.py',
    'test_perf_result_artifacts.py',
    'test_perf_run_exit.py',
    'test_perf_seed_ids.py',
    'test_perf_statistics.py',
    'test_perf_worker_process.py',
)
"""The unit tests of the perf harness, in tests/unit/ before the move and next
to the harness, in tests/perf/, after it."""

STORED_FORM_UNIT_TESTS: Final = (
    'test_stored_form_error_data.py',
    'test_stored_form_result_envelope.py',
    'test_stored_form_task_input.py',
    'test_stored_form_task_options.py',
    'test_stored_form_workflow_spec_data.py',
)
"""The unit tests of the stored-form vectors, in tests/unit/ before the move
and next to tests/stored_form_vectors.py, in tests/, after it."""

MOVES: Final = (
    Move('crates/horsies-core', 'core'),
    Move('crates/horsies-core-derive', 'core/derive'),
    Move('crates/horsies-python', 'clients/python/bridge'),
    Move('crates/horsies-codegen', 'clients/python/codegen'),
    Move('python/horsies', 'clients/python/horsies'),
    Move('pyproject.toml', 'clients/python/pyproject.toml'),
    Move('uv.lock', 'clients/python/uv.lock'),
    Move('pyrightconfig.clean.json', 'clients/python/pyrightconfig.clean.json'),
    Move('pyrightconfig.docs.json', 'clients/python/pyrightconfig.docs.json'),
    Move('showcase', 'clients/python/examples'),
    *(Move(f'tests/unit/{name}', f'tests/perf/{name}') for name in PERF_UNIT_TESTS),
    *(Move(f'tests/unit/{name}', f'tests/{name}') for name in STORED_FORM_UNIT_TESTS),
    Move('tests/dualrun', 'tests/parity/snapshot'),
    Move('tests', 'clients/python/tests'),
    Move('crates/horsies', 'clients/rust/horsies'),
    Move('crates/horsies-macros', 'clients/rust/macros'),
    Move('rust/tests', 'clients/rust/tests'),
    Move('rust/examples', 'clients/rust/examples'),
    Move('rust/showcase', 'clients/rust/examples/showcase'),
    Move('rust/scripts', 'clients/rust/scripts'),
    Move('rust/imported-commit-map.txt', 'clients/rust/imported-commit-map.txt'),
)
"""Every move, in the order of the ``git mv`` steps. A move whose new folder
is inside the new folder of another move comes after it:
``crates/horsies-core`` before ``crates/horsies-core-derive``,
``rust/examples`` before ``rust/showcase``. The unit tests of the cross-client
harnesses leave ``tests/unit/``, and the kept snapshot files leave
``tests/dualrun/``, before ``tests`` moves."""

STAYS: Final = (
    Stay('tests/__init__.py', 'the package `tests` of the cross-client harnesses'),
    Stay('tests/parity', 'cross-client: drives the Python and the Rust client'),
    Stay('tests/perf', 'cross-client: the measurement harness'),
    Stay('tests/fixtures/perf', 'cross-client: the servers of the measurement harness'),
    Stay('tests/stored_form_vectors.py', 'cross-client: the stored-form vectors'),
)

OLD_FOLDERS_GONE: Final = ('crates', 'python', 'rust', 'showcase')
"""Top-level folders that hold nothing after the move."""

FOLDER_ALIASES: Final = (('python', 'clients/python'),)
"""A folder that is an import root before the move, and the import root
after it. ``python/`` held only the package, so the folder that holds the
package is ``clients/python/``."""

PYTHON_PROJECT: Final = 'clients/python'
PYTHON_TESTS: Final = 'clients/python/tests'
"""The package ``tests`` of the Python client. The top-level ``tests/`` is the
package ``tests`` of the cross-client harnesses. They are two packages with
one name: no process imports both, and neither extends its package path."""


@dataclass(frozen=True, slots=True)
class OpenImport:
    """A ``tests.`` import under ``folder`` that names a module of the other
    package ``tests``, and the decision that it waits for. The checks report
    it as open, not as a failure; each entry must match an import."""

    folder: str
    module: str
    decision: str


OPEN_IMPORTS: Final[tuple[OpenImport, ...]] = ()
"""Empty: every ``tests.`` import of the moved tree names a module of its own
package. The parity harness imports its snapshot modules from
``tests.parity.snapshot`` (``MODULE_RENAMES``), and the four Python client
test modules that the cross-client harnesses import are links in ``tests/``
(``NEW_LINKS``)."""


@dataclass(frozen=True, slots=True)
class NewFile:
    """A file that the restructure adds, as a copy of a file of the moved tree."""

    path: str
    copy_of: str
    reason: str


@dataclass(frozen=True, slots=True)
class NewLink:
    """A symbolic link that the restructure adds."""

    path: str
    target: str
    reason: str


NEW_FILES: Final = (
    NewFile(
        'clients/python/tests/__init__.py',
        'tests/__init__.py',
        'the package `tests` of the Python client',
    ),
)

CLIENT_TEST_MODULE_LINKS: Final = (
    ('tests/session_database.py', '../clients/python/tests/session_database.py'),
    ('tests/sqlalchemy_url.py', '../clients/python/tests/sqlalchemy_url.py'),
    (
        'tests/attempt_snapshot_support.py',
        '../clients/python/tests/attempt_snapshot_support.py',
    ),
    (
        'tests/history_seeding.py',
        '../clients/python/tests/integration/history_seeding.py',
    ),
)
"""Modules of the Python client's package ``tests`` that the cross-client
harnesses import: the server URL of the test session (parity and snapshot
conftests), the SQLAlchemy URL (parity send, snapshot self-check, perf), and
the history coverage pass of the snapshot self-check with the module it
imports. Each is a link, so each module keeps one definition and is a module
of both packages ``tests``; none imports a ``tests.`` module that the
top-level package does not hold."""

NEW_LINKS: Final = (
    *(
        NewLink(path, target, 'a Python client test module of a cross-client harness')
        for path, target in CLIENT_TEST_MODULE_LINKS
    ),
    NewLink(
        'clients/python/README.md',
        '../../README.md',
        'the readme of the wheel (`project.readme`) stays the root README',
    ),
    NewLink(
        'clients/python/LICENSE',
        '../../LICENSE',
        '`project.license-files` takes no path outside the project folder',
    ),
)


# ---------------------------------------------------------------------------
# Text that keeps an old path
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class KeptFile:
    """Files (new paths, a glob) that no rule changes, and why."""

    pattern: str
    reason: str


@dataclass(frozen=True, slots=True)
class KeptLine:
    """A line of ``path`` (new path) that holds ``text`` keeps its old path.

    Each entry must match at least one line, so that none goes stale.
    """

    path: str
    text: str
    reason: str


HISTORY: Final = 'release history'
FROZEN: Final = (
    'the frozen Rust client at FREEZE_COMMIT, where the crate is at crates/horsies'
)

KEPT_FILES: Final = (
    KeptFile('CHANGELOG.md', HISTORY),
    KeptFile('website/src/content/docs/changelog.md', HISTORY),
    KeptFile('core/CHANGELOG.md', HISTORY),
    KeptFile('clients/rust/horsies/CHANGELOG.md', HISTORY),
    KeptFile(
        'core/baseline/*.sql', 'migration 1000; a changed byte changes its checksum'
    ),
    KeptFile(
        'tests/perf/releases/**',
        'patches and steps for the v0.7.0 checkout, where the package is at horsies/',
    ),
    KeptFile(
        'clients/rust/tests/old-client-test-fates.tsv',
        'paths of the old rust/tests crates, by design',
    ),
)

TOOL_FOLDER: Final = 'scripts/restructure'
"""This tool names the old paths. No rule changes it and the check leaves it
out, when a checkout tracks it."""

KEPT_LINES: Final = (
    KeptLine(
        'clients/python/pyproject.toml',
        'readme = "README.md"',
        'clients/python/README.md links to the root README',
    ),
    KeptLine(
        'clients/python/pyproject.toml',
        'license-files = ["LICENSE"]',
        'clients/python/LICENSE links to the root LICENSE',
    ),
    KeptLine(
        'clients/rust/tests/frozen_fates.py',
        "FROZEN_CRATE: Final = 'crates/horsies'",
        FROZEN,
    ),
    KeptLine(
        'clients/rust/tests/frozen_fates.py',
        'the frozen crate crates/horsies at',
        FROZEN,
    ),
    KeptLine(
        'clients/rust/tests/frozen_fates.py',
        'The frozen crate is `crates/horsies` at FREEZE_COMMIT',
        FROZEN,
    ),
    KeptLine(
        'clients/rust/tests/frozen-crate-test-fates.tsv',
        'the frozen crate crates/horsies at',
        FROZEN,
    ),
)


# ---------------------------------------------------------------------------
# Path arithmetic
# ---------------------------------------------------------------------------


def parts_of(path: str) -> tuple[str, ...]:
    """The components of a repository path; the root ``''`` has none."""
    return tuple(part for part in path.split('/') if part)


def is_under(path: str, prefix: str) -> bool:
    """Whether ``path`` is ``prefix`` or a path inside it (by components)."""
    path_parts, prefix_parts = parts_of(path), parts_of(prefix)
    return path_parts[: len(prefix_parts)] == prefix_parts


def join(*paths: str) -> str:
    """The normalized repository path of ``paths`` joined; ``''`` is the root.

    A result that leaves the repository starts with ``..``.
    """
    present = [path for path in paths if path] or ['.']
    joined = posixpath.normpath(posixpath.join(*present))
    return '' if joined == '.' else joined


def relative_path(target: str, start: str) -> str:
    """``target`` relative to the folder ``start`` (both repository paths)."""
    return posixpath.relpath(target or '.', start or '.')


def dirname(path: str) -> str:
    return posixpath.dirname(path)


def stay_of(path: str) -> Stay | None:
    """The ``Stay`` that keeps ``path`` in place, if one does."""
    for stay in STAYS:
        if is_under(path, stay.path):
            return stay
    return None


def move_of(path: str) -> Move | None:
    """The ``Move`` with the longest old path that holds ``path``."""
    found: Move | None = None
    for move in MOVES:
        if is_under(path, move.old) and (
            found is None or len(parts_of(move.old)) > len(parts_of(found.old))
        ):
            found = move
    return found


def new_path_of(old: str) -> str:
    """Where the file or folder ``old`` is after the move (unmoved: ``old``)."""
    if stay_of(old) is not None:
        return old
    match move_of(old):
        case None:
            return old
        case move:
            rest = parts_of(old)[len(parts_of(move.old)) :]
            return '/'.join((move.new, *rest))


@dataclass(frozen=True, slots=True)
class Tree:
    """The tracked files of one tree, the folders that hold them, and the
    package roots (folders with a ``Cargo.toml`` or a ``pyproject.toml``)."""

    files: frozenset[str]
    folders: frozenset[str]
    package_roots: frozenset[str]

    @staticmethod
    def of(paths: Iterable[str]) -> Tree:
        files = frozenset(paths)
        folders = {''}
        for path in files:
            parts = parts_of(path)
            for depth in range(1, len(parts)):
                folders.add('/'.join(parts[:depth]))
        roots = frozenset(
            dirname(path)
            for path in files
            if posixpath.basename(path) in ('Cargo.toml', 'pyproject.toml')
        )
        return Tree(files=files, folders=frozenset(folders), package_roots=roots)

    def holds(self, path: str) -> bool:
        return path in self.files or path in self.folders

    def package_root_of(self, path: str) -> str:
        """The nearest folder above ``path`` that is a package root; ``''`` at the top."""
        folder = dirname(path)
        for _ in range(len(parts_of(folder)) + 1):
            if folder in self.package_roots:
                return folder
            folder = dirname(folder)
        return ''


@dataclass(frozen=True, slots=True)
class SplitFolder:
    """A folder whose content goes to more than one place."""

    path: str


def new_location_of(old: str, tree: Tree) -> str | SplitFolder:
    """Where a file or folder of the old tree is after the move.

    A folder whose files do not all move by one prefix (``tests``,
    ``tests/fixtures``, ``crates``, ``rust``) is a ``SplitFolder``; an alias
    of ``FOLDER_ALIASES`` maps to its new import root; the root stays the
    root. A path that is not tracked moves with the folder that holds it.
    """
    for alias, target in FOLDER_ALIASES:
        if old == alias:
            return target
    if old == '' or old in tree.files or old not in tree.folders:
        return new_path_of(old) if old else ''
    if stay_of(old) is None and any(
        is_under(stay.path, old) and stay.path != old for stay in STAYS
    ):
        return SplitFolder(old)
    if move_of(old) is None and any(is_under(move.old, old) for move in MOVES):
        return SplitFolder(old)
    return new_path_of(old)


# ---------------------------------------------------------------------------
# Rule results
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Review:
    """A site that a rule did not change and a person checks: where and why."""

    path: str
    line: int
    cause: str


@dataclass(frozen=True, slots=True)
class FileContext:
    """One file of the move: its paths before and after, and the old tree."""

    old_path: str
    new_path: str
    old_tree: Tree


@dataclass(frozen=True, slots=True)
class Rewrite:
    """The text after a rule, and the sites that the rule left for a person."""

    text: str
    reviews: tuple[Review, ...]


def line_of(text: str, offset: int) -> int:
    return text.count('\n', 0, offset) + 1


# ---------------------------------------------------------------------------
# Rule: repository paths cited in text
# ---------------------------------------------------------------------------

CITATION: Final = re.compile(
    r'(?<![A-Za-z0-9_.@/~$*-])(?P<path>[A-Za-z0-9_][A-Za-z0-9_.-]*(?:/[A-Za-z0-9_.*-]*)+)',
)
"""A path that starts at a folder name: a character before it that can be
part of a path, a URL or a variable means that it does not start there."""

TRAILING_PUNCTUATION: Final = '.,:;'


def literal_parts(path: str) -> tuple[str, ...]:
    """The components of ``path`` up to the first one with a glob character."""
    found: list[str] = []
    for part in parts_of(path):
        if '*' in part:
            break
        found.append(part)
    return tuple(found)


def names_local_path(path: str, context: FileContext) -> bool:
    """Whether ``path`` names a tracked path next to the file or in its package."""
    tree = context.old_tree
    clean = '/'.join(literal_parts(path.rstrip(TRAILING_PUNCTUATION)))
    for base in (dirname(context.old_path), tree.package_root_of(context.old_path)):
        if base and tree.holds(join(base, clean)):
            return True
    return False


def cited_path_after_move(path: str, context: FileContext) -> str | None:
    """The new text of a cited repository path; None when it does not move.

    A move of one component (``tests``, ``showcase``) is ambiguous: a crate
    has a ``tests/`` folder of its own. Such a citation moves only when it
    does not name a path next to the file or in its package, and its first
    two components name a tracked path of the root. In a file of the Python
    project, a ``tests/`` path stays relative to the project: pytest node
    ids and the paths of a pytest run are relative to its root.
    """
    clean = path.rstrip(TRAILING_PUNCTUATION)
    suffix = path[len(clean) :]
    if stay_of(clean) is not None:
        return None
    match move_of(clean):
        case None:
            return None
        case move if move.old == 'tests' and is_under(context.new_path, PYTHON_PROJECT):
            return None
        case move if len(parts_of(move.old)) == 1:
            tree = context.old_tree
            head = '/'.join(parts_of(clean)[:2])
            if len(parts_of(clean)) == 1 and isinstance(
                new_location_of(move.old, tree), SplitFolder
            ):
                return None
            if names_local_path(clean, context) or not tree.holds(head):
                return None
            return move.new + clean[len(move.old) :] + suffix
        case move:
            return move.new + clean[len(move.old) :] + suffix


def rewrite_citations(text: str, context: FileContext) -> Rewrite:
    """Move every cited repository path (relative to the root) to its new path."""

    def replace(match: re.Match[str]) -> str:
        moved = cited_path_after_move(match.group('path'), context)
        return match.group(0) if moved is None else moved

    return Rewrite(text=CITATION.sub(replace, text), reviews=())


GITHUB_SOURCE: Final = re.compile(
    r'(?P<prefix>github\.com/suleymanozkeskin/horsies/(?:blob|tree|edit|raw)/[A-Za-z0-9_.-]+/)'
    r'(?P<path>[A-Za-z0-9_.@-]+(?:/[A-Za-z0-9_.@-]+)*)',
)


def rewrite_github_links(text: str, context: FileContext) -> Rewrite:
    """Move the repository path of a GitHub source link."""

    def replace(match: re.Match[str]) -> str:
        match new_location_of(match.group('path'), context.old_tree):
            case SplitFolder():
                return match.group(0)
            case str() as moved:
                return f'{match.group("prefix")}{moved}'

    return Rewrite(text=GITHUB_SOURCE.sub(replace, text), reviews=())


MODULE_RENAMES: Final = (
    (re.compile(r'(?<![A-Za-z0-9_.])showcase\.acme\b'), 'examples.acme'),
    (re.compile(r'(?<![A-Za-z0-9_.])tests\.dualrun\b'), 'tests.parity.snapshot'),
)
"""Python module paths that follow a folder rename: ``showcase/acme`` is
``clients/python/examples/acme``, imported from ``clients/python/``;
``tests/dualrun`` is ``tests/parity/snapshot``."""


def rewrite_module_names(text: str, context: FileContext) -> Rewrite:
    del context
    for pattern, new in MODULE_RENAMES:
        text = pattern.sub(new, text)
    return Rewrite(text=text, reviews=())


# ---------------------------------------------------------------------------
# Rule: paths relative to the file or to its crate
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RelativeForm:
    """Where a relative path is written, and the folder it is relative to."""

    suffixes: tuple[str, ...]
    pattern: re.Pattern[str]
    base: Callable[[str, Tree], str]


def file_folder(path: str, tree: Tree) -> str:
    del tree
    return dirname(path)


def crate_folder(path: str, tree: Tree) -> str:
    return tree.package_root_of(path)


RELATIVE_FORMS: Final = (
    RelativeForm(
        ('Cargo.toml',), re.compile(r'\bpath\s*=\s*"(?P<rel>[^"\n]+)"'), file_folder
    ),
    RelativeForm(
        ('.rs',),
        re.compile(
            r'include_(?:str|bytes)!\(\s*(?:concat!\(\s*)?"(?P<rel>\.\.?/[^"\n]*)"'
        ),
        file_folder,
    ),
    RelativeForm(
        ('.rs',), re.compile(r'#\[path\s*=\s*"(?P<rel>[^"\n]+)"\]'), file_folder
    ),
    RelativeForm(
        ('.rs',),
        re.compile(
            r'env!\("CARGO_MANIFEST_DIR"\)\s*\)\s*\.join\(\s*"(?P<rel>\.\.[^"\n]*)"\s*\)'
        ),
        crate_folder,
    ),
    RelativeForm(
        ('.sh',),
        re.compile(
            r'\$\(dirname\s+"?\$\{?(?:BASH_SOURCE(?:\[0\])?|0)\}?"?\)/(?P<rel>[^"\s)]+)'
        ),
        file_folder,
    ),
    RelativeForm(
        ('.md', '.mdx'),
        re.compile(r'\]\((?P<rel>(?![a-z]+:|#|/)[^)\s#]+)'),
        file_folder,
    ),
    RelativeForm(
        ('.ts', '.mjs', '.js'),
        re.compile(r'(?P<quote>[\'"])(?P<rel>\.\.?/[^\'"\n]*)(?P=quote)'),
        file_folder,
    ),
)


def relative_after_move(
    rel: str, context: FileContext, base_of: Callable[[str, Tree], str]
) -> str | Review | None:
    """The new text of a path relative to a folder of the file; None when unchanged.

    The path must name a tracked file or folder of the old tree; a path that
    names nothing tracked is left as it is.
    """
    tree = context.old_tree
    old_base = base_of(context.old_path, tree)
    target = join(old_base, rel.rstrip('/'))
    if target.startswith('..') or not tree.holds(target):
        return None
    new_base = (
        dirname(context.new_path) if base_of is file_folder else new_path_of(old_base)
    )
    match new_location_of(target, tree):
        case SplitFolder(path=split):
            return Review(
                context.new_path,
                0,
                f'relative path {rel!r} names the split folder {split}/',
            )
        case str() as new_target:
            new_rel = relative_path(new_target, new_base)
            if rel.startswith('./') and not new_rel.startswith('.'):
                new_rel = f'./{new_rel}'
            if rel.endswith('/') and not new_rel.endswith('/'):
                new_rel = f'{new_rel}/'
            return None if new_rel == rel else new_rel


def rewrite_relative_paths(text: str, context: FileContext) -> Rewrite:
    """Re-point every path that is relative to the file or to its crate."""
    reviews: list[Review] = []
    for form in RELATIVE_FORMS:
        if not context.new_path.endswith(form.suffixes):
            continue

        def replace(match: re.Match[str], form: RelativeForm = form) -> str:
            rel = match.group('rel')
            match relative_after_move(rel, context, form.base):
                case None:
                    return match.group(0)
                case Review(cause=cause):
                    reviews.append(
                        Review(context.new_path, line_of(text, match.start()), cause)
                    )
                    return match.group(0)
                case str() as new_rel:
                    start, end = match.span('rel')
                    return (
                        match.group(0)[: start - match.start()]
                        + new_rel
                        + match.group(0)[end - match.start() :]
                    )

        text = form.pattern.sub(replace, text)
    return Rewrite(text=text, reviews=tuple(reviews))


# ---------------------------------------------------------------------------
# Rule: Python paths built from __file__
# ---------------------------------------------------------------------------

FILE_BASE: Final = re.compile(
    r'Path\(\s*__file__\s*\)(?P<resolve>\.resolve\(\))?'
    r'(?:\.parents\[(?P<index>\d+)\]|(?P<parents>(?:\.parent(?![A-Za-z0-9_]))+))'
    r'(?P<chain>(?:\s*/\s*(?:\'[^\'\n]*\'|"[^"\n]*"))*)',
)
"""``Path(__file__)``, optionally resolved, then ``.parents[i]`` or a chain of
``.parent``, then the string literals joined to it with ``/``."""

CHAIN_LITERAL: Final = re.compile(
    r'(?P<gap>\s*/\s*)(?P<quote>[\'"])(?P<text>[^\'"\n]*)(?P=quote)'
)

ROOT_ASSIGNMENT: Final = re.compile(
    r'^\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?:\s*:\s*Final)?\s*=\s*', re.MULTILINE
)


@dataclass(frozen=True, slots=True)
class FileBase:
    """A ``Path(__file__)`` expression: how many folders up, and the literals joined to it."""

    levels: int
    literals: tuple[str, ...]


def file_base_of(match: re.Match[str]) -> FileBase:
    match (match.group('index'), match.group('parents')):
        case (str() as index, _):
            levels = int(index) + 1
        case (None, str() as parents):
            levels = parents.count('.parent')
        case _:
            raise AssertionError(f'FILE_BASE matched neither form: {match.group(0)!r}')
    literals = tuple(
        literal.group('text')
        for literal in CHAIN_LITERAL.finditer(match.group('chain'))
    )
    return FileBase(levels=levels, literals=literals)


def folder_up(path: str, levels: int) -> str | None:
    """The folder ``levels`` steps above the file ``path``; None above the root."""
    parts = parts_of(path)
    if levels > len(parts):
        return None
    return '/'.join(parts[: len(parts) - levels])


def common_folder(first: str, second: str) -> str:
    shared: list[str] = []
    for left, right in zip(parts_of(first), parts_of(second), strict=False):
        if left != right:
            break
        shared.append(left)
    return '/'.join(shared)


def moved_target(target: str, tree: Tree) -> str | SplitFolder:
    """Where a path built in code is after the move; a path not tracked
    moves with the folder that holds it."""
    if tree.holds(target) or target in (alias for alias, _ in FOLDER_ALIASES):
        return new_location_of(target, tree)
    return new_path_of(target)


def render_file_base(
    match: re.Match[str], levels: int, literals: tuple[str, ...]
) -> str:
    """``Path(__file__)`` with ``levels`` folders up and ``literals`` joined, in the form of ``match``."""
    resolve = match.group('resolve') or ''
    if (match.group('parents') is not None and levels <= 3) or levels == 1:
        base = f'Path(__file__){resolve}' + '.parent' * levels
    else:
        base = f'Path(__file__){resolve}.parents[{levels - 1}]'
    first = CHAIN_LITERAL.search(match.group('chain'))
    gap, quote = (
        (first.group('gap'), first.group('quote'))
        if first is not None
        else (' / ', "'")
    )
    gap = gap if '\n' in gap else ' / '
    return base + ''.join(f'{gap}{quote}{part}{quote}' for part in literals)


class RootMeaning(Enum):
    """What a bare ``Path(__file__)`` expression that named the old root means after the move.

    Before the move the repository root was also the Python project root and
    the folder that holds the package ``tests``; after it they differ.
    """

    REPOSITORY = 'the repository root'
    PYTHON_PROJECT = 'clients/python/, the folder that holds the package `tests`'


IMPORT_ROOT_USE: Final = re.compile(r'sys\.path|PYTHONPATH|parent_sys_path|\bcwd\s*=')
"""A line that uses a folder as an import root or as the folder a process runs in."""


@dataclass(frozen=True, slots=True)
class RootBinding:
    """A name bound to a bare ``Path(__file__)`` expression that named the old root."""

    name: str
    offset: int
    meaning: RootMeaning


def root_meaning(name: str, text: str, context: FileContext) -> RootMeaning:
    """What the old root bound to ``name`` means after the move.

    In a file of the Python project, a name whose joins all name tests of the
    project, or that is used only as an import root, names the Python
    project; any other name names the repository root.
    """
    if not is_under(context.new_path, PYTHON_PROJECT):
        return RootMeaning.REPOSITORY
    chains = [
        join(
            *(
                literal.group('text')
                for literal in LITERAL.finditer(match.group('chain'))
            )
        )
        for match in re.finditer(
            rf'\b{re.escape(name)}(?P<chain>{LITERAL_CHAIN})', text
        )
    ]
    import_uses = [
        line
        for line in text.splitlines()
        if re.search(rf'\b{re.escape(name)}\b', line) and IMPORT_ROOT_USE.search(line)
    ]
    in_project = all(is_under(new_path_of(target), PYTHON_TESTS) for target in chains)
    return (
        RootMeaning.PYTHON_PROJECT
        if (chains or import_uses) and in_project
        else RootMeaning.REPOSITORY
    )


def root_bindings(text: str, context: FileContext) -> tuple[RootBinding, ...]:
    """The names of ``text`` (before the rules) bound to a bare expression that names the old root."""
    found: list[RootBinding] = []
    for assignment in ROOT_ASSIGNMENT.finditer(text):
        expression = FILE_BASE.match(text, assignment.end())
        if expression is None or expression.group('chain').strip():
            continue
        if folder_up(context.old_path, file_base_of(expression).levels) == '':
            name = assignment.group('name')
            found.append(
                RootBinding(name, expression.start(), root_meaning(name, text, context))
            )
    return tuple(found)


def keeps_old_root_text(
    match: re.Match[str],
    text: str,
    context: FileContext,
    bindings: tuple[RootBinding, ...],
) -> bool:
    """Whether a bare expression that names the old root stays as written: it
    names the Python project (bound so, or used inline as an import root) and
    keeps its distance to the moved file."""
    if not is_under(context.new_path, PYTHON_PROJECT):
        return False
    for binding in bindings:
        if binding.offset == match.start():
            return binding.meaning is RootMeaning.PYTHON_PROJECT
    line_start = text.rfind('\n', 0, match.start()) + 1
    line_end = text.find('\n', match.end())
    return (
        IMPORT_ROOT_USE.search(
            text[line_start : line_end if line_end >= 0 else len(text)]
        )
        is not None
    )


def rewrite_file_base(
    match: re.Match[str], context: FileContext
) -> str | Review | None:
    """The new text of one ``Path(__file__)`` expression; None when unchanged.

    A bare expression (no literals) that names a folder inside the moved
    folder of the file keeps its text: the folder moved with the file.
    """
    base = file_base_of(match)
    old_folder = folder_up(context.old_path, base.levels)
    if old_folder is None:
        return Review(
            context.new_path, 0, 'Path(__file__) climbs above the repository root'
        )
    own_move = move_of(context.old_path)
    if not base.literals and (
        own_move is None
        or stay_of(context.old_path) is not None
        or is_under(old_folder, own_move.old)
    ):
        return None
    target = join(old_folder, *base.literals)
    match moved_target(target, context.old_tree):
        case SplitFolder(path=split):
            return Review(
                context.new_path, 0, f'Path(__file__) names the split folder {split}/'
            )
        case str() as new_target:
            new_folder = dirname(context.new_path)
            shared = '' if not base.literals else common_folder(new_folder, new_target)
            levels = len(parts_of(new_folder)) - len(parts_of(shared)) + 1
            literals = parts_of(new_target)[len(parts_of(shared)) :]
            if levels == base.levels and '/'.join(literals) == '/'.join(
                parts_of('/'.join(base.literals))
            ):
                return None
            return render_file_base(match, levels, literals)


def rewrite_python_file_paths(text: str, context: FileContext) -> Rewrite:
    """Re-point every ``Path(__file__)`` expression of a Python file.

    A bare expression that named the old root names the repository root or
    the Python project after the move (``RootMeaning``). A name that keeps
    the repository root and is also used as an import root is a review.
    """
    if not context.new_path.endswith('.py'):
        return Rewrite(text=text, reviews=())
    reviews: list[Review] = []
    bindings = root_bindings(text, context)

    def replace(match: re.Match[str]) -> str:
        if keeps_old_root_text(match, text, context, bindings):
            return match.group(0)
        match rewrite_file_base(match, context):
            case None:
                return match.group(0)
            case Review(cause=cause):
                reviews.append(
                    Review(context.new_path, line_of(text, match.start()), cause)
                )
                return match.group(0)
            case str() as new_text:
                return new_text

    rewritten = FILE_BASE.sub(replace, text)
    repository_names = [
        b.name
        for b in bindings
        if b.meaning is RootMeaning.REPOSITORY
        and is_under(context.new_path, PYTHON_PROJECT)
    ]
    for number, line in enumerate(rewritten.splitlines(), start=1):
        if IMPORT_ROOT_USE.search(line) and any(
            re.search(rf'\b{re.escape(name)}\b', line) for name in repository_names
        ):
            reviews.append(
                Review(
                    context.new_path,
                    number,
                    'the repository root used as an import root or a cwd',
                )
            )
    return Rewrite(text=rewritten, reviews=tuple(reviews))


def root_names(text: str, context: FileContext) -> tuple[str, ...]:
    """Names bound (in ``text`` after ``rewrite_python_file_paths``) to a bare
    ``Path(__file__)`` expression that names the repository root."""
    names: list[str] = []
    for assignment in ROOT_ASSIGNMENT.finditer(text):
        expression = FILE_BASE.match(text, assignment.end())
        if expression is None or expression.group('chain').strip():
            continue
        if folder_up(context.new_path, file_base_of(expression).levels) == '':
            names.append(assignment.group('name'))
    return tuple(names)


def rewrite_root_chains(text: str, context: FileContext) -> Rewrite:
    """Move the literals joined to a name that holds the repository root.

    ``REPO_ROOT / 'python' / 'horsies'`` becomes ``REPO_ROOT / 'clients' /
    'python' / 'horsies'``, and ``Path('python') / 'horsies'`` becomes
    ``Path('clients') / 'python' / 'horsies'``. Run after
    ``rewrite_python_file_paths``: a name bound to the repository root is
    found in its output; a name that keeps the Python project root is not.
    """
    if not context.new_path.endswith('.py'):
        return Rewrite(text=text, reviews=())
    patterns = [PATH_LITERAL_CHAIN]
    names = root_names(text, context)
    if names:
        patterns.append(
            re.compile(
                rf'\b(?P<head>{"|".join(map(re.escape, names))})(?P<chain>{LITERAL_CHAIN})'
            )
        )
    for pattern in patterns:
        text = pattern.sub(lambda match: root_chain_after_move(match, context), text)
    return Rewrite(text=text, reviews=())


LITERAL_CHAIN: Final = r'(?:\s*/\s*[\'"][^\'"\n]*[\'"])+'
PATH_LITERAL_CHAIN: Final = re.compile(
    r'(?P<head>Path\()(?P<first>\s*[\'"][^\'"\n]*[\'"]\s*\))(?P<chain>(?:\s*/\s*[\'"][^\'"\n]*[\'"])*)',
)
LITERAL: Final = re.compile(r'(?P<quote>[\'"])(?P<text>[^\'"\n]*)(?P=quote)')


def moves_as_cited(target: str, tree: Tree) -> bool:
    """Whether a path built from literals names something that moves: a
    tracked path, or a path under a move of more than one component."""
    found = move_of(target)
    multi_component = found is not None and len(parts_of(found.old)) > 1
    return tree.holds(target) or multi_component


def root_chain_after_move(match: re.Match[str], context: FileContext) -> str:
    """The new text of ``<root> / 'a' / 'b'`` or ``Path('a') / 'b'``; unchanged when it does not move."""
    groups = match.groupdict()
    path_literal = groups.get('first') is not None
    text_with_literals = (groups.get('first') or '') + match.group('chain')
    literals = [
        literal.group('text') for literal in LITERAL.finditer(text_with_literals)
    ]
    target = join(*literals)
    if (
        not literals
        or target.startswith('..')
        or not moves_as_cited(target, context.old_tree)
    ):
        return match.group(0)
    found = move_of(target)
    if path_literal and (found is None or len(parts_of(found.old)) == 1):
        # Path('tests') / ... is relative to a base that the text does not show
        # (the pytest root, the cwd); only a path under a move of more than one
        # component is a repository path in every base.
        return match.group(0)
    match moved_target(target, context.old_tree):
        case SplitFolder():
            return match.group(0)
        case str() as new_target if new_target == target:
            return match.group(0)
        case str() as new_target:
            return render_root_chain(match, literals, parts_of(new_target))


def render_root_chain(
    match: re.Match[str], literals: list[str], new_parts: tuple[str, ...]
) -> str:
    """The chain of ``match`` with ``new_parts``, in the form of the old chain."""
    quote = '"' if '"' in match.group(0) and "'" not in match.group(0) else "'"
    single = len(literals) == 1
    gap = re.match(r'\s*/\s*', match.group('chain'))
    separator = gap.group(0) if gap is not None and '\n' in gap.group(0) else ' / '
    if match.groupdict().get('first') is not None:
        if single:
            return f'Path({quote}{"/".join(new_parts)}{quote})'
        first, *rest = new_parts
        return f'Path({quote}{first}{quote})' + ''.join(
            f'{separator}{quote}{part}{quote}' for part in rest
        )
    if single:
        return f'{match.group("head")}{separator}{quote}{"/".join(new_parts)}{quote}'
    return match.group('head') + ''.join(
        f'{separator}{quote}{part}{quote}' for part in new_parts
    )


# ---------------------------------------------------------------------------
# Rule: configuration files that move, whose paths are relative to them
# ---------------------------------------------------------------------------

CONFIG_FILES: Final = (
    'clients/python/pyproject.toml',
    'clients/python/pyrightconfig.clean.json',
    'clients/python/pyrightconfig.docs.json',
)
"""Moved files whose quoted paths are relative to the file. Their comments
cite paths from the repository root, like any other text."""

QUOTED: Final = re.compile(r'"(?P<value>[^"\n]+)"')


def config_value_after_move(value: str, context: FileContext) -> str | Review | None:
    """The new text of one quoted value of a moved configuration file."""
    tree = context.old_tree
    literal = literal_parts(value)
    rest = parts_of(value)[len(literal) :]
    target = join(dirname(context.old_path), *literal)
    if not literal or target.startswith('..') or not moves_as_cited(target, tree):
        return None
    match new_location_of(target, tree):
        case SplitFolder(path=split) if rest:
            return Review(
                context.new_path, 0, f'glob {value!r} names the split folder {split}/'
            )
        case SplitFolder():
            return None
        case str() as new_target:
            new_value = relative_path(
                join(new_target, *rest), dirname(context.new_path)
            )
            if value.startswith('./') and not new_value.startswith('.'):
                new_value = f'./{new_value}'
            return None if new_value == value else new_value


def rewrite_config_paths(text: str, context: FileContext) -> Rewrite:
    """Re-point the quoted paths of a moved configuration file; cite paths in its comments."""
    reviews: list[Review] = []
    lines = text.splitlines(keepends=True)
    for index, line in enumerate(lines):
        if line.lstrip().startswith('#'):
            lines[index] = rewrite_citations(line, context).text
            continue

        def replace(match: re.Match[str], number: int = index + 1) -> str:
            match config_value_after_move(match.group('value'), context):
                case None:
                    return match.group(0)
                case Review(cause=cause):
                    reviews.append(Review(context.new_path, number, cause))
                    return match.group(0)
                case str() as new_value:
                    return f'"{new_value}"'

        lines[index] = QUOTED.sub(replace, line)
    return Rewrite(text=''.join(lines), reviews=tuple(reviews))


# ---------------------------------------------------------------------------
# Rules in order
# ---------------------------------------------------------------------------

Rule = Callable[[str, FileContext], Rewrite]

TEXT_RULES: Final[tuple[Rule, ...]] = (
    rewrite_relative_paths,
    rewrite_python_file_paths,
    rewrite_root_chains,
    rewrite_github_links,
    rewrite_citations,
    rewrite_module_names,
)
"""The rules of a file that is not in CONFIG_FILES, in order."""

CONFIG_RULES: Final[tuple[Rule, ...]] = (rewrite_config_paths,)


def kept_file_rule(path: str) -> KeptFile | None:
    for rule in KEPT_FILES:
        if PurePosixPath(path).full_match(rule.pattern):
            return rule
    return None


def kept_line_rule(path: str, line: str) -> KeptLine | None:
    for rule in KEPT_LINES:
        if rule.path == path and rule.text in line:
            return rule
    return None


KEPT_MARK: Final = '\x00kept-line-{index}\x00'


def apply_rules(text: str, context: FileContext) -> Rewrite:
    """Every rule of the file, with its kept lines left as they are."""
    lines = text.splitlines(keepends=True)
    kept: dict[str, str] = {}
    for index, line in enumerate(lines):
        if kept_line_rule(context.new_path, line) is not None:
            mark = KEPT_MARK.format(index=index)
            kept[mark] = line
            lines[index] = mark + ('\n' if line.endswith('\n') else '')
    current = ''.join(lines)
    reviews: list[Review] = []
    rules = CONFIG_RULES if context.new_path in CONFIG_FILES else TEXT_RULES
    for rule in rules:
        result = rule(current, context)
        current = result.text
        reviews.extend(result.reviews)
    for mark, line in kept.items():
        current = current.replace(mark + ('\n' if line.endswith('\n') else ''), line, 1)
    return Rewrite(text=current, reviews=tuple(reviews))


# ---------------------------------------------------------------------------
# Exact edits after the rules
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Edit:
    """One exact replacement in a file of the moved tree, after the rules.

    ``old`` is text as the rules leave it, and must occur ``count`` times.
    """

    path: str
    old: str
    new: str
    count: int = 1


TESTS_PACKAGE_DOC_OLD: Final = '"""Tests package for horsies."""\n'
CROSS_CLIENT_TESTS_DOC: Final = (
    '"""The cross-client test harnesses of horsies: parity, perf and the\n'
    'stored-form vectors, each with its own tests.\n'
    '\n'
    'The tests of the Python client are the package `tests` of clients/python/.\n'
    '"""\n'
)
PYTHON_CLIENT_TESTS_DOC: Final = (
    '"""The tests of the Python client of horsies.\n'
    '\n'
    'The cross-client harnesses (parity, perf, the stored-form vectors) are the\n'
    'package `tests` at the repository root.\n'
    '"""\n'
)


UV_PROJECT_OLD: Final = 'permissions: {}\n\njobs:\n'
UV_PROJECT_NEW: Final = (
    'permissions: {}\n'
    '\n'
    '# The Python project is clients/python/. A step that runs uv at the\n'
    '# repository root uses its pyproject.toml, uv.lock and .venv.\n'
    'env:\n'
    '  UV_PROJECT: ${{ github.workspace }}/clients/python\n'
    '\n'
    'jobs:\n'
)
WORKFLOWS_WITH_UV: Final = (
    'coverage.yml',
    'performance.yml',
    'release.yml',
    'tests-full.yml',
    'tests.yml',
)

_PYTHON_SCOPE_OLD: Final = """\
          # horsies._core is built from clients/python/bridge, core
          # and core/derive with the root Cargo and toolchain
          # files, so a change there can change a Python result. A PR that
          # changes only the Rust client, its macros, the generator, rust/, or
          # the format and lint settings cannot: the Python jobs are skipped for
          # it.
          if git diff --quiet "$base" HEAD -- . ':(exclude)clients/rust/horsies/' ':(exclude)clients/rust/macros/' ':(exclude)clients/python/codegen/' ':(exclude)rust/' ':(exclude)rustfmt.toml' ':(exclude)clippy.toml'; then
"""
_PYTHON_SCOPE_NEW: Final = """\
          # horsies._core is built from clients/python/bridge, core and
          # core/derive with the root Cargo and toolchain files, so a change
          # there can change a Python result. A PR that changes only the Rust
          # client (clients/rust/), the generator, or the format and lint
          # settings cannot: the Python jobs are skipped for it.
          if git diff --quiet "$base" HEAD -- . ':(exclude)clients/rust/' ':(exclude)clients/python/codegen/' ':(exclude)rustfmt.toml' ':(exclude)clippy.toml'; then
"""

OLD_CRATES_FOLDERS: Final = 'core/ clients/python/bridge/ clients/python/codegen/ clients/rust/horsies/ clients/rust/macros/'
"""The crates that were under crates/, for a pathspec that named crates/.
``core/`` holds ``core/derive/``."""

HARNESS_UNIT_TESTS_STEP: Final = (
    '      # The unit tests of the cross-client harnesses in the top-level tests/:\n'
    '      # the perf harness and the stored-form vectors. They run from the\n'
    '      # repository root with the pytest settings of the Python project.\n'
    '      - name: Run the unit tests of the cross-client harnesses\n'
    '        run: {uv_run} pytest -c clients/python/pyproject.toml --rootdir . tests/perf/ tests/test_stored_form_*.py -v --tb=short\n'
)
"""A CI step after the unit tests of the Python client; ``{uv_run}`` is the
``uv run`` form of the workflow."""

EDITS: Final[tuple[Edit, ...]] = (
    *(
        Edit(f'.github/workflows/{name}', UV_PROJECT_OLD, UV_PROJECT_NEW)
        for name in WORKFLOWS_WITH_UV
    ),
    Edit(
        '.github/workflows/tests.yml',
        'run: uv build --wheel --python 3.13 --out-dir dist',
        'run: uv build clients/python --wheel --python 3.13 --out-dir dist',
    ),
    Edit(
        '.github/workflows/tests.yml',
        'uv pip install --no-deps dist/horsies-*.whl',
        'uv pip install --python clients/python/.venv --no-deps dist/horsies-*.whl',
        count=5,
    ),
    Edit(
        '.github/workflows/tests.yml',
        'run: uv run --no-sync pyright -p pyrightconfig.clean.json',
        'run: uv run --no-sync pyright -p clients/python/pyrightconfig.clean.json',
    ),
    Edit(
        '.github/workflows/tests.yml',
        'uv run --no-sync python clients/python/tests/docs_examples/extract.py build/doc_examples\n'
        '          uv run --no-sync pyright -p pyrightconfig.docs.json',
        'uv run --no-sync python clients/python/tests/docs_examples/extract.py clients/python/build/doc_examples\n'
        '          uv run --no-sync pyright -p clients/python/pyrightconfig.docs.json',
    ),
    Edit(
        '.github/workflows/tests.yml',
        "        if: steps.changed.outputs.run == 'true'\n"
        '        run: uv run --no-sync horsies check examples.acme.app:app\n',
        "        if: steps.changed.outputs.run == 'true'\n"
        '        working-directory: clients/python\n'
        '        run: uv run --no-sync horsies check examples.acme.app:app\n',
    ),
    Edit(
        '.github/workflows/tests.yml',
        "        if: steps.changed.outputs.run == 'true'\n"
        '        run: uv run --no-sync pyright clients/python/examples/\n',
        "        if: steps.changed.outputs.run == 'true'\n"
        '        working-directory: clients/python\n'
        '        run: uv run --no-sync pyright examples/\n',
    ),
    Edit(
        '.github/workflows/tests.yml',
        'if git diff --quiet "$base" HEAD -- crates/ rust/ website/src/content/docs/rust/',
        'if git diff --quiet "$base" HEAD -- core/ clients/rust/ clients/python/bridge/ clients/python/codegen/'
        ' website/src/content/docs/rust/',
    ),
    Edit('.github/workflows/tests.yml', _PYTHON_SCOPE_OLD, _PYTHON_SCOPE_NEW),
    Edit(
        '.github/workflows/tests.yml',
        '-- clients/rust/examples/showcase/ crates/ Cargo.toml',
        f'-- clients/rust/examples/showcase/ {OLD_CRATES_FOLDERS} Cargo.toml',
    ),
    Edit(
        '.github/workflows/release.yml',
        '("pyproject.toml", ("project", "version")),',
        '("clients/python/pyproject.toml", ("project", "version")),',
    ),
    Edit(
        '.github/workflows/release.yml',
        '        run: uv build\n',
        '        run: uv build clients/python --out-dir dist\n',
    ),
    Edit(
        '.gitignore', '\ntests/test_results\n', '\nclients/python/tests/test_results\n'
    ),
    Edit('tests/__init__.py', TESTS_PACKAGE_DOC_OLD, CROSS_CLIENT_TESTS_DOC),
    Edit(
        'clients/python/tests/__init__.py',
        TESTS_PACKAGE_DOC_OLD,
        PYTHON_CLIENT_TESTS_DOC,
    ),
    Edit(
        '.github/workflows/tests.yml',
        '      - name: Run unit tests\n'
        '        run: uv run --no-sync pytest clients/python/tests/unit/ -v --tb=short\n',
        '      - name: Run unit tests\n'
        '        run: uv run --no-sync pytest clients/python/tests/unit/ -v --tb=short\n'
        '\n' + HARNESS_UNIT_TESTS_STEP.format(uv_run='uv run --no-sync'),
    ),
    Edit(
        '.github/workflows/tests-full.yml',
        '      - name: Run unit tests\n'
        '        run: uv run pytest clients/python/tests/unit/ -v --tb=short\n',
        '      - name: Run unit tests\n'
        '        run: uv run pytest clients/python/tests/unit/ -v --tb=short\n'
        '\n' + HARNESS_UNIT_TESTS_STEP.format(uv_run='uv run'),
    ),
    Edit(
        # A glob over files that moved next to their harness: the citation
        # rule moves it with tests/unit/.
        'tests/stored_form_vectors.py',
        'Python tests (``clients/python/tests/unit/test_stored_form_*.py``)',
        'Python tests (``tests/test_stored_form_*.py``)',
    ),
    Edit(
        'core/tests/fixtures/stored_forms/README.md',
        '`clients/python/tests/unit/test_stored_form_*.py`, helpers in `tests/stored_form_vectors.py`',
        '`tests/test_stored_form_*.py`, helpers in `tests/stored_form_vectors.py`',
    ),
    Edit(
        'clients/python/pyproject.toml',
        '# The package is in python/; pyright resolves it there with or without an install.\n',
        '# The package is in this folder (horsies/); pyright resolves it here with or\n'
        '# without an install.\n',
    ),
    Edit(
        'clients/python/pyproject.toml',
        '    { file = "crates/**/Cargo.toml" },\n    { file = "crates/**/*.rs" },\n',
        '    { file = "bridge/Cargo.toml" },\n'
        '    { file = "bridge/**/*.rs" },\n'
        '    { file = "../../core/Cargo.toml" },\n'
        '    { file = "../../core/derive/Cargo.toml" },\n'
        '    { file = "../../core/**/*.rs" },\n',
    ),
    Edit(
        'core/tests/user_value_text.rs',
        'let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");',
        'let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");',
    ),
    Edit(
        'clients/rust/tests/docs-check/extract.rs',
        'const REPO_ROOT_FROM_CRATE: &str = "../../..";',
        'const REPO_ROOT_FROM_CRATE: &str = "../../../..";',
    ),
    Edit(
        'clients/python/tests/extension/test_stub_surface.py',
        "STUB_PATH: Final = REPOSITORY_ROOT / 'python' / 'horsies' / '_core.pyi'",
        "STUB_PATH: Final = REPOSITORY_ROOT / 'horsies' / '_core.pyi'",
    ),
    Edit(
        'clients/python/tests/public_api/runner.py',
        'REPOSITORY_ROOT: Final = PUBLIC_API_FOLDER.parents[1]',
        'REPOSITORY_ROOT: Final = PUBLIC_API_FOLDER.parents[3]',
    ),
    Edit(
        'clients/python/tests/public_api/runner.py',
        "    REPOSITORY_ROOT / 'python' / 'horsies' / '.agents' / 'skills',\n    REPOSITORY_ROOT / 'showcase',\n",
        "    REPOSITORY_ROOT / 'clients' / 'python' / 'horsies' / '.agents' / 'skills',\n"
        "    REPOSITORY_ROOT / 'clients' / 'python' / 'examples',\n",
    ),
    Edit(
        'clients/python/tests/unit/test_docs_examples_extract.py',
        "package = tmp_path / 'python' / 'horsies'",
        "package = tmp_path / 'clients' / 'python' / 'horsies'",
    ),
    Edit(
        'clients/python/tests/unit/test_docs_examples_extract.py',
        "== ['python_horsies_module.py']",
        "== ['clients_python_horsies_module.py']",
    ),
    Edit(
        'scripts/run_e2e_suite.sh',
        '    uv run pytest "${file}" "$@"',
        '    uv run --project clients/python pytest "${file}" "$@"',
    ),
    Edit(
        'clients/python/examples/README.md',
        'Everything runs from the repository root.',
        'Everything runs from `clients/python/`.',
    ),
    Edit(
        'clients/python/examples/README.md',
        'Run from the repository root as `uv run python -m examples.acme.scenarios <name>`.',
        'Run from `clients/python/` as `uv run python -m examples.acme.scenarios <name>`.',
    ),
    Edit(
        'clients/python/examples/docker-compose.yml',
        '# Postgres for the Acme Clothing showcase.\n#\n#   docker compose -f clients/python/examples/docker-compose.yml up -d\n',
        '# Postgres for the Acme Clothing showcase. From clients/python/:\n#\n#   docker compose -f examples/docker-compose.yml up -d\n',
    ),
    Edit(
        'website/src/content/docs/quick-start/06-showcase.md',
        'Run everything from the\nrepository root.',
        'Run everything from the\n`clients/python/` folder of the repository.',
    ),
    Edit(
        'core/tests/fixtures/stored_forms/README.md',
        '`CARGO_MANIFEST_DIR/../horsies-core/tests/fixtures/stored_forms`',
        '`CARGO_MANIFEST_DIR/../../../core/tests/fixtures/stored_forms`',
    ),
    Edit(
        'clients/rust/tests/docs-check/tests/extract.rs',
        '"crates_horsies_agents_skills_horsies_skill__line88"',
        '"clients_rust_horsies_agents_skills_horsies_skill__line88"',
    ),
    Edit(
        # A pattern for the paths of `git ls-files` at the repository root.
        'clients/python/tests/unit/test_old_package_path_citations.py',
        "        'tests/unit/test_old_package_path_citations.py',\n",
        "        'clients/python/tests/unit/test_old_package_path_citations.py',\n",
    ),
    Edit(
        'clients/python/tests/unit/test_old_package_path_citations.py',
        "        'package is at horsies/',\n    ),\n)\n",
        "        'package is at horsies/',\n"
        '    ),\n'
        '    KeptFile(\n'
        "        'clients/python/pyrightconfig.clean.json',\n"
        "        'its paths are relative to clients/python/, where the package is at '\n"
        "        'horsies/',\n"
        '    ),\n'
        ')\n',
    ),
    Edit(
        # The coverage pass is a module of the Python client's integration
        # tests; the top-level package holds it as a link (NEW_LINKS).
        'tests/parity/snapshot/selfcheck.py',
        '    from tests.integration.history_seeding import ensure_history_seedable\n',
        '    from tests.history_seeding import ensure_history_seedable\n',
    ),
    Edit(
        'clients/rust/tests/frozen-crate-test-fates.tsv',
        'Built and checked by rust/tests/frozen_fates.py.',
        'Built and checked by clients/rust/tests/frozen_fates.py.',
    ),
)
