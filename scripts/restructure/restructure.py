"""Move the repository into the 0.8.0 layout, rewrite the paths, check the result.

Usage:
    uv run --no-project --python 3.13 --with pyyaml==6.0.2 \\
        python scripts/restructure/restructure.py [--commit] <checkout>

<checkout> is a clean checkout of ``program/0.8.0``. The script changes files
only in <checkout>:

1. Preflight: the checkout is clean, every old path of ``move_rules.MOVES``
   is tracked, and no new path exists.
2. ``git mv`` of every move. ``move_rules.STAYS`` stay where they are.
3. ``move_rules.NEW_FILES`` and ``move_rules.NEW_LINKS``.
4. The text rules of ``move_rules`` over every tracked UTF-8 file that no
   ``KeptFile`` keeps, then the exact edits ``move_rules.EDITS``. An edit
   whose anchor is not found its count of times stops the script.
5. Checks. Any failure stops the script with exit code 1:
   - the index holds exactly the moved old files and the added files;
   - ``check_old_paths.py``: no tracked text file cites an old path, and its
     self-test passes;
   - every kept rule still matches a file or a line;
   - every ``include_str!``/``include_bytes!``/``#[path]`` path, the folder of
     an ``include_str!(concat!(...))``, a ``CARGO_MANIFEST_DIR`` join out of
     a crate and every ``path =`` of a Cargo manifest names a tracked path;
   - the workspace members are exactly the crates of the tree;
   - every tracked TOML and YAML file and both pyright configs parse;
   - every ``tests.`` import names a module of the package ``tests`` that
     holds the importing file (``tests/`` or ``clients/python/tests/``),
     apart from the imports of ``move_rules.OPEN_IMPORTS``, and neither
     ``__init__.py`` extends the package path;
   - ``clients/python/pyproject.toml`` names files that exist;
   - nothing is left unstaged or untracked.

Every change is staged. With ``--commit``, after every check passes, the
script writes two commits on the current branch: the renames with unchanged
bytes (so that ``git log --follow`` follows each file), then the edits. It
never pushes and never builds.

The script prints the sites that a rule left for a person ("review") and
the imports that wait for a decision ("open"); they are not failures. Exit
codes: 0 done, 1 a step or a check failed, 2 usage.
"""

from __future__ import annotations

import json
import os
import posixpath
import re
import subprocess
import sys
import tomllib
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import check_old_paths
import move_rules as rules

EXIT_DONE: Final = 0
EXIT_FAILED: Final = 1
EXIT_USAGE: Final = 2

GIT_TIMEOUT_S: Final = 300
"""Seconds that one git command gets."""


@dataclass(frozen=True, slots=True)
class Failure:
    """A failed step or check: what, where and why."""

    subject: str
    cause: str


class GitError(Exception):
    """A git command failed. It stops the script: the checkout is then in an
    unknown state and is thrown away, never repaired."""


def git(repo: Path, *arguments: str, stdin: str | None = None) -> str:
    """The output of one git command in ``repo``; a failure raises GitError."""
    try:
        done = subprocess.run(
            ['git', '-C', str(repo), *arguments],
            input=stdin,
            capture_output=True,
            text=True,
            timeout=GIT_TIMEOUT_S,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise GitError(f'git {" ".join(arguments)}: {error!r}') from error
    if done.returncode != 0:
        raise GitError(
            f'git {" ".join(arguments)} exited {done.returncode}: {done.stderr.strip()}'
        )
    return done.stdout


def tracked(repo: Path) -> tuple[str, ...]:
    return tuple(name for name in git(repo, 'ls-files', '-z').split('\0') if name)


def report(failures: list[Failure], step: str) -> int:
    for failure in failures:
        print(f'FAIL {failure.subject}: {failure.cause}', file=sys.stderr)
    print(f'{step}: {len(failures)} failures')
    return EXIT_FAILED if failures else EXIT_DONE


# ---------------------------------------------------------------------------
# Preflight and the moves
# ---------------------------------------------------------------------------


def preflight(repo: Path, tree: rules.Tree) -> list[Failure]:
    """The checkout is clean, every old path is tracked and no new path exists."""
    failures: list[Failure] = []
    if git(repo, 'rev-parse', '--show-toplevel').strip() != str(repo):
        failures.append(Failure(str(repo), 'not the top folder of a git checkout'))
    if git(repo, 'status', '--porcelain'):
        failures.append(
            Failure(
                str(repo), 'has changes or untracked files; start from a clean checkout'
            )
        )
    for move in rules.MOVES:
        if not tree.holds(move.old):
            failures.append(Failure(move.old, 'an old path of a move is not tracked'))
    for new in {move.new.split('/')[0] for move in rules.MOVES} - {'tests'}:
        if (repo / new).exists():
            failures.append(Failure(new, 'a new top-level path exists already'))
    for stay in rules.STAYS:
        if not tree.holds(stay.path):
            failures.append(Failure(stay.path, 'a path of a Stay is not tracked'))
    return failures


def planned_moves(tree: rules.Tree) -> list[tuple[str, str]]:
    """The ``git mv`` steps: one per move, split into its entries where a Stay is inside it."""
    steps: list[tuple[str, str]] = []
    for move in rules.MOVES:
        steps.extend(entries_to_move(move.old, tree, move))
    return steps


def entries_to_move(
    folder: str, tree: rules.Tree, move: rules.Move
) -> list[tuple[str, str]]:
    """``(old, new)`` for ``folder`` of ``move``, or for its entries when a Stay is inside it.

    An entry that a Stay keeps, or that another move with a longer old path
    moves (``tests/dualrun`` inside ``tests``), is not a step of ``move``.
    The depth is bounded by the depth of the deepest Stay.
    """
    if rules.stay_of(folder) is not None or rules.move_of(folder) != move:
        return []
    if not any(rules.is_under(stay.path, folder) for stay in rules.STAYS):
        return [(folder, rules.new_path_of(folder))]
    depth = len(rules.parts_of(folder)) + 1
    children = sorted(
        {
            '/'.join(rules.parts_of(path)[:depth])
            for path in tree.files
            if rules.is_under(path, folder)
        },
    )
    steps: list[tuple[str, str]] = []
    for child in children:
        steps.extend(entries_to_move(child, tree, move))
    return steps


def move_files(repo: Path, steps: list[tuple[str, str]]) -> None:
    for old, new in steps:
        (repo / new).parent.mkdir(parents=True, exist_ok=True)
        git(repo, 'mv', '--', old, new)


def add_new_files(repo: Path) -> list[str]:
    """Write NEW_FILES and NEW_LINKS; the paths written."""
    written: list[str] = []
    for new_file in rules.NEW_FILES:
        target = repo / new_file.path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((repo / new_file.copy_of).read_bytes())
        written.append(new_file.path)
    for link in rules.NEW_LINKS:
        target = repo / link.path
        target.parent.mkdir(parents=True, exist_ok=True)
        os.symlink(link.target, target)
        written.append(link.path)
    return written


# ---------------------------------------------------------------------------
# Text rules and exact edits
# ---------------------------------------------------------------------------


def read_text(path: Path) -> str | None:
    """The UTF-8 text of a regular file; None for a link, a missing file or non-UTF-8 bytes."""
    if path.is_symlink() or not path.is_file():
        return None
    try:
        return path.read_bytes().decode('utf-8')
    except UnicodeDecodeError:
        return None


@dataclass(frozen=True, slots=True)
class ReviewSite:
    """A review of a rule, with the text of its line after the rules."""

    review: rules.Review
    line_text: str


def rewrite_tree(
    repo: Path, old_paths: dict[str, str], old_tree: rules.Tree
) -> tuple[set[str], list[ReviewSite]]:
    """Apply the text rules to every tracked text file; the changed paths and the reviews."""
    changed: set[str] = set()
    reviews: list[ReviewSite] = []
    for new_path, old_path in sorted(old_paths.items()):
        if rules.kept_file_rule(new_path) is not None or rules.is_under(
            new_path, rules.TOOL_FOLDER
        ):
            continue
        text = read_text(repo / new_path)
        if text is None:
            continue
        context = rules.FileContext(
            old_path=old_path, new_path=new_path, old_tree=old_tree
        )
        result = rules.apply_rules(text, context)
        lines = result.text.splitlines()
        reviews.extend(
            ReviewSite(
                review, lines[review.line - 1] if 0 < review.line <= len(lines) else ''
            )
            for review in result.reviews
        )
        if result.text != text:
            (repo / new_path).write_text(result.text, encoding='utf-8')
            changed.add(new_path)
    return changed, reviews


def covered_by_edit(repo: Path, site: ReviewSite, edited: set[str]) -> bool:
    """Whether an exact edit changed the line of a review."""
    if site.review.path not in edited or not site.line_text:
        return False
    text = read_text(repo / site.review.path) or ''
    return site.line_text not in text.splitlines()


def apply_edits(repo: Path) -> tuple[set[str], list[Failure]]:
    """Apply EDITS. Each anchor must occur exactly its count."""
    changed: set[str] = set()
    failures: list[Failure] = []
    for edit in rules.EDITS:
        text = read_text(repo / edit.path)
        if text is None:
            failures.append(
                Failure(edit.path, 'the file of an edit is missing or not UTF-8 text')
            )
            continue
        found = text.count(edit.old)
        if found != edit.count:
            failures.append(
                Failure(
                    edit.path,
                    f'edit anchor found {found} times, expected {edit.count}: {edit.old[:100]!r}',
                )
            )
            continue
        (repo / edit.path).write_text(
            text.replace(edit.old, edit.new), encoding='utf-8'
        )
        changed.add(edit.path)
    return changed, failures


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def check_index(old_files: Iterable[str], new_files: Iterable[str]) -> list[Failure]:
    """The index holds exactly the moved old files and the added ones, and no
    file that moves is still at its old path (a link added there is not
    that file)."""
    old = set(old_files)
    added = {new_file.path for new_file in rules.NEW_FILES} | {
        link.path for link in rules.NEW_LINKS
    }
    expected = {rules.new_path_of(path) for path in old} | added
    actual = set(new_files)
    failures = [
        Failure(path, 'expected in the index after the move, missing')
        for path in sorted(expected - actual)
    ]
    failures += [
        Failure(path, 'in the index after the move, not expected')
        for path in sorted(actual - expected)
    ]
    failures += [
        Failure(path, 'a tracked file that moves is still at its old path')
        for path in sorted((actual & old) - added)
        if rules.new_path_of(path) != path
    ]
    return failures


def check_kept_rules(repo: Path, paths: tuple[str, ...]) -> list[Failure]:
    """Each kept rule still matches: a KeptFile a tracked file, a KeptLine a line."""
    failures = [
        Failure(rule.pattern, 'kept-file rule matches no tracked file')
        for rule in rules.KEPT_FILES
        if not any(rules.PurePosixPath(path).full_match(rule.pattern) for path in paths)
    ]
    for rule in rules.KEPT_LINES:
        text = read_text(repo / rule.path)
        if text is None or not any(rule.text in line for line in text.splitlines()):
            failures.append(
                Failure(rule.path, f'kept-line rule matches no line: {rule.text!r}')
            )
    return failures


RUST_FILE_PATH: Final = re.compile(
    r'include_(?:str|bytes)!\(\s*"(?P<include>[^"\n]+)"\s*\)|#\[path\s*=\s*"(?P<module>[^"\n]+)"\]',
)
RUST_CONCAT_INCLUDE: Final = re.compile(
    r'include_str!\(concat!\(\s*"(?P<folder>[^"\n]+/)"'
)
RUST_CRATE_JOIN: Final = re.compile(
    r'env!\("CARGO_MANIFEST_DIR"\)\s*\)\s*\.join\(\s*"(?P<rel>\.\.[^"\n]*)"\s*\)'
)
CARGO_PATH: Final = re.compile(r'\bpath\s*=\s*"(?P<rel>[^"\n]+)"')


def check_rust_paths(repo: Path, tree: rules.Tree) -> tuple[int, list[Failure]]:
    """Every file path of a Rust source and of a Cargo manifest names a tracked path.

    No build runs here, so this stands in for the compiler on the moved
    paths: ``include_str!``/``include_bytes!`` and ``#[path]`` (relative to
    the file), the folder of an ``include_str!(concat!(...))``, a
    ``CARGO_MANIFEST_DIR`` join out of the crate, and each ``path =`` of a
    manifest. Returns the count of paths checked.
    """
    count = 0
    failures: list[Failure] = []
    for path in sorted(tree.files):
        if path.endswith('.rs'):
            text = read_text(repo / path) or ''
            sites = [
                (
                    match.start(),
                    rules.join(
                        rules.dirname(path),
                        match.group('include') or match.group('module'),
                    ),
                )
                for match in RUST_FILE_PATH.finditer(text)
            ]
            sites += [
                (m.start(), rules.join(rules.dirname(path), m.group('folder')))
                for m in RUST_CONCAT_INCLUDE.finditer(text)
            ]
            sites += [
                (m.start(), rules.join(tree.package_root_of(path), m.group('rel')))
                for m in RUST_CRATE_JOIN.finditer(text)
            ]
        elif path.endswith('Cargo.toml'):
            text = read_text(repo / path) or ''
            sites = [
                (m.start(), rules.join(rules.dirname(path), m.group('rel')))
                for m in CARGO_PATH.finditer(text)
            ]
        else:
            continue
        for offset, target in sites:
            line_start = text.rfind('\n', 0, offset) + 1
            if text[line_start:offset].lstrip().startswith('//'):
                continue
            count += 1
            if not tree.holds(target):
                failures.append(
                    Failure(
                        f'{path}:{rules.line_of(text, offset)}',
                        f'names no tracked path: {target}',
                    )
                )
    return count, failures


def check_workspace(repo: Path, tree: rules.Tree) -> tuple[int, list[Failure]]:
    """The workspace members are the crates of the tree, and each has a manifest."""
    config = tomllib.loads((repo / 'Cargo.toml').read_text(encoding='utf-8'))
    members: list[str] = config['workspace']['members']
    failures = [
        Failure('Cargo.toml', f'member {member} has no Cargo.toml')
        for member in members
        if f'{member}/Cargo.toml' not in tree.files
    ]
    crates = {
        rules.dirname(path) for path in tree.files if path.endswith('/Cargo.toml')
    }
    failures += [
        Failure(f'{crate}/Cargo.toml', 'a crate that is not a workspace member')
        for crate in sorted(crates - set(members))
    ]
    return len(members), failures


def check_parse(repo: Path, tree: rules.Tree) -> tuple[int, list[Failure]]:
    """Every tracked TOML and YAML file and every pyright config parses."""
    try:
        import yaml
    except ImportError:
        return 0, [
            Failure('PyYAML', 'not importable; run with `uv run --with pyyaml==6.0.2`')
        ]

    def parse_yaml(text: str) -> object:
        return list(yaml.safe_load_all(text))

    count = 0
    failures: list[Failure] = []
    for path in sorted(tree.files):
        name = posixpath.basename(path)
        parse: Callable[[str], object]
        match name:
            case _ if name.endswith('.toml'):
                parse = tomllib.loads
            case _ if name.endswith(('.yml', '.yaml')):
                parse = parse_yaml
            case _ if name.startswith('pyrightconfig') and name.endswith('.json'):
                parse = json.loads
            case _:
                continue
        text = read_text(repo / path)
        if text is None:
            continue
        count += 1
        try:
            parse(text)
        except (tomllib.TOMLDecodeError, json.JSONDecodeError, yaml.YAMLError) as error:
            failures.append(Failure(path, f'{type(error).__name__}: {error}'))
    return count, failures


TESTS_IMPORT: Final = re.compile(
    r'^\s*(?:from\s+(?P<from>tests(?:\.\w+)*)\s+import|import\s+(?P<import>tests(?:\.\w+)*))',
    re.MULTILINE,
)
CROSS_CLIENT_TESTS: Final = 'tests'
TEST_PACKAGE_ROOTS: Final = (CROSS_CLIENT_TESTS, rules.PYTHON_TESTS)
"""The folders of the two packages ``tests``: the cross-client harnesses, and
the Python client."""


@dataclass(frozen=True, slots=True)
class OpenSite:
    """An import of ``move_rules.OPEN_IMPORTS``: where, and its decision."""

    site: str
    module: str
    decision: str


def package_of(path: str) -> str:
    """The folder of the package ``tests`` that a ``tests.`` import of ``path`` names."""
    if rules.is_under(path, rules.PYTHON_PROJECT):
        return rules.PYTHON_TESTS
    return CROSS_CLIENT_TESTS


def module_exists(module: str, package: str, tree: rules.Tree) -> bool:
    """Whether ``tests.a.b`` is a module or a package in the folder ``package``."""
    base = '/'.join((package, *module.split('.')[1:]))
    return f'{base}.py' in tree.files or base in tree.folders


def open_import_of(path: str, module: str) -> rules.OpenImport | None:
    """The entry of ``move_rules.OPEN_IMPORTS`` that covers an import of ``path``."""
    for entry in rules.OPEN_IMPORTS:
        if rules.is_under(path, entry.folder) and (
            module == entry.module or module.startswith(f'{entry.module}.')
        ):
            return entry
    return None


@dataclass(frozen=True, slots=True)
class ImportCheck:
    """The ``tests.`` imports checked, the failures, and the open imports."""

    count: int
    failures: list[Failure]
    open_sites: list[OpenSite]


def check_test_imports(repo: Path, tree: rules.Tree) -> ImportCheck:
    """Every ``tests.`` import of a tracked Python file names a module of its own
    package ``tests``, or is an open import; neither package extends its path."""
    count = 0
    failures: list[Failure] = []
    open_sites: list[OpenSite] = []
    matched: set[rules.OpenImport] = set()
    for path in sorted(tree.files):
        if not path.endswith('.py') or rules.kept_file_rule(path) is not None:
            continue
        text = read_text(repo / path) or ''
        package = package_of(path)
        for match in TESTS_IMPORT.finditer(text):
            module = match.group('from') or match.group('import')
            count += 1
            site = f'{path}:{rules.line_of(text, match.start())}'
            if module_exists(module, package, tree):
                continue
            match open_import_of(path, module):
                case None:
                    failures.append(
                        Failure(site, f'imports {module}, which is not in {package}/')
                    )
                case entry:
                    matched.add(entry)
                    open_sites.append(OpenSite(site, module, entry.decision))
    failures += [
        Failure(entry.folder, f'open import {entry.module} matches no import')
        for entry in rules.OPEN_IMPORTS
        if entry not in matched
    ]
    for root in TEST_PACKAGE_ROOTS:
        text = read_text(repo / root / '__init__.py')
        if text is None:
            failures.append(Failure(f'{root}/__init__.py', 'missing'))
        elif 'extend_path' in text:
            failures.append(
                Failure(
                    f'{root}/__init__.py',
                    'extends the package path; the two packages `tests` stay apart',
                )
            )
    return ImportCheck(count=count, failures=failures, open_sites=open_sites)


def check_pyproject(repo: Path, tree: rules.Tree) -> list[Failure]:
    """``clients/python/pyproject.toml`` names files that exist, from its own folder."""
    project = rules.PYTHON_PROJECT
    config = tomllib.loads(
        (repo / project / 'pyproject.toml').read_text(encoding='utf-8')
    )
    maturin = config['tool']['maturin']
    failures: list[Failure] = []
    manifest = rules.join(project, maturin['manifest-path'])
    if manifest not in tree.files:
        failures.append(
            Failure('tool.maturin.manifest-path', f'{manifest} is not tracked')
        )
    package = rules.join(
        project, maturin.get('python-source', '.'), 'horsies', '__init__.py'
    )
    if package not in tree.files:
        failures.append(
            Failure('tool.maturin.python-source', f'{package} is not tracked')
        )
    for entry in maturin.get('include', []):
        if not list((repo / project).glob(entry['path'])) and not glob_outside(
            repo / project, entry['path']
        ):
            failures.append(
                Failure('tool.maturin.include', f'{entry["path"]!r} matches no file')
            )
    for field in ('readme',):
        if not (repo / project / config['project'][field]).is_file():
            failures.append(
                Failure(
                    f'project.{field}',
                    f'{config["project"][field]} is not a file next to pyproject.toml',
                )
            )
    for license_file in config['project'].get('license-files', []):
        if not (repo / project / license_file).is_file():
            failures.append(
                Failure(
                    'project.license-files',
                    f'{license_file} is not a file next to pyproject.toml',
                )
            )
    return failures


def glob_outside(folder: Path, pattern: str) -> bool:
    """Whether a glob that starts with ``../`` matches a file (Path.glob refuses ``..``)."""
    parts = pattern.split('/')
    base = folder
    while parts and parts[0] == '..':
        base, parts = base.parent, parts[1:]
    return bool(parts) and any(base.glob('/'.join(parts)))


def check_clean_stage(repo: Path) -> list[Failure]:
    failures: list[Failure] = []
    if git(repo, 'diff', '--name-only'):
        failures.append(Failure(str(repo), 'a change is not staged'))
    if git(repo, 'ls-files', '--others', '--exclude-standard'):
        failures.append(Failure(str(repo), 'an untracked file is left'))
    return failures


def run_checks(repo: Path, old_files: tuple[str, ...]) -> list[Failure]:
    paths = tracked(repo)
    tree = rules.Tree.of(paths)
    failures = check_index(old_files, paths)
    findings = check_old_paths.scan(repo, paths)
    failures += [
        Failure(f'{f.path}:{f.line_number}', f'old path {f.cited!r} left: {f.line}')
        for f in findings
    ]
    failures += [
        Failure('check_old_paths self-test', wrong)
        for wrong in check_old_paths.self_test()
    ]
    failures += check_kept_rules(repo, paths)
    rust_count, rust_failures = check_rust_paths(repo, tree)
    member_count, member_failures = check_workspace(repo, tree)
    parse_count, parse_failures = check_parse(repo, tree)
    imports = check_test_imports(repo, tree)
    failures += (
        rust_failures
        + member_failures
        + parse_failures
        + imports.failures
        + check_pyproject(repo, tree)
    )
    failures += check_clean_stage(repo)
    print(
        f'Rust and Cargo paths checked: {rust_count}; workspace members: {member_count}'
    )
    print(
        f'TOML, YAML and pyright files parsed: {parse_count}; tests. imports checked: {imports.count}'
    )
    for site in imports.open_sites:
        print(f'open {site.site}: imports {site.module}')
    decisions = sorted({site.decision for site in imports.open_sites})
    for decision in decisions:
        print(f'open decision: {decision}')
    print(f'open imports: {len(imports.open_sites)}')
    return failures


# ---------------------------------------------------------------------------
# The two commits
# ---------------------------------------------------------------------------

RENAME_MESSAGE: Final = """\
Move the crates and the clients into the 0.8.0 layout

core/ holds the core crate and its derives (core/derive/); clients/python/
the package, the extension module crate, the generator, the Python tests
and examples; clients/rust/ the Rust client, its macros, tests and
examples. tests/ keeps the cross-client parity and perf harnesses and the
stored-form vectors, with the unit tests of perf and of the vectors; the
snapshot modules of the parity harness move from tests/dualrun/ to
tests/parity/snapshot/. Every file keeps its bytes; the next commit changes
the paths. This commit alone does not build.
"""

EDIT_MESSAGE: Final = """\
Point every path at the 0.8.0 layout

The Cargo workspace, the crate manifests, pyproject.toml, the pyright
configs, the CI workflows, the docs and the tests follow the move. tests/
and clients/python/tests/ are two packages named `tests`: the cross-client
harnesses, and the tests of the Python client. The four Python client test
modules that the cross-client harnesses import are links in tests/.
"""


def write_commits(
    repo: Path, old_commit: str, old_files: tuple[str, ...]
) -> list[Failure]:
    """Write the rename commit and the edit commit on the current branch."""
    index_file = Path(
        git(
            repo,
            'rev-parse',
            '--path-format=absolute',
            '--git-path',
            'restructure-rename-index',
        ).strip()
    )
    environment = {**os.environ, 'GIT_INDEX_FILE': str(index_file)}

    def git_rename_index(*arguments: str, stdin: str | None = None) -> str:
        done = subprocess.run(
            ['git', '-C', str(repo), *arguments],
            input=stdin,
            capture_output=True,
            text=True,
            env=environment,
            timeout=GIT_TIMEOUT_S,
            check=False,
        )
        if done.returncode != 0:
            raise GitError(
                f'git {" ".join(arguments)} exited {done.returncode}: {done.stderr.strip()}'
            )
        return done.stdout

    git_rename_index('read-tree', old_commit)
    entries: list[str] = []
    moved = 0
    for line in git(repo, 'ls-tree', '-r', '-z', old_commit).split('\0'):
        if not line:
            continue
        meta, path = line.split('\t', 1)
        mode, _, blob = meta.split(' ')
        new_path = rules.new_path_of(path)
        if new_path != path:
            moved += 1
            entries.append(f'0 {"0" * 40}\t{path}')
            entries.append(f'{mode} {blob}\t{new_path}')
    git_rename_index(
        'update-index', '-z', '--index-info', stdin='\0'.join(entries) + '\0'
    )
    rename_tree = git_rename_index('write-tree').strip()
    index_file.unlink()
    rename_commit = git(
        repo,
        'commit-tree',
        rename_tree,
        '-p',
        old_commit,
        '-F',
        '-',
        stdin=RENAME_MESSAGE,
    ).strip()
    statuses = git(
        repo, 'diff', '-M', '--name-status', old_commit, rename_commit
    ).splitlines()
    exact = sum(1 for status in statuses if status.startswith('R100'))
    if exact != moved or len(statuses) != moved:
        return [
            Failure(
                rename_commit,
                f'{exact} exact renames and {len(statuses)} entries, expected {moved} exact renames',
            )
        ]
    final_commit = git(
        repo,
        'commit-tree',
        git(repo, 'write-tree').strip(),
        '-p',
        rename_commit,
        '-F',
        '-',
        stdin=EDIT_MESSAGE,
    ).strip()
    git(repo, 'update-ref', '-m', 'restructure', 'HEAD', final_commit, old_commit)
    print(f'rename commit: {rename_commit} ({moved} exact renames)')
    print(f'edit commit: {final_commit}')
    print('Two commits are written on the current branch. Nothing is pushed.')
    return []


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def restructure(repo: Path, commit: bool) -> int:
    old_commit = git(repo, 'rev-parse', 'HEAD').strip()
    old_files = tracked(repo)
    old_tree = rules.Tree.of(old_files)
    if report(preflight(repo, old_tree), 'preflight'):
        return EXIT_FAILED
    steps = planned_moves(old_tree)
    move_files(repo, steps)
    print(f'git mv steps: {len(steps)}')
    added = add_new_files(repo)
    old_paths = {rules.new_path_of(path): path for path in old_files}
    old_paths |= {new_file.path: new_file.copy_of for new_file in rules.NEW_FILES}
    rewritten, reviews = rewrite_tree(repo, old_paths, old_tree)
    edited, edit_failures = apply_edits(repo)
    print(
        f'text rules: {len(rewritten)} files; exact edits: {len(rules.EDITS)} in {len(edited)} files'
    )
    if report(edit_failures, 'exact edits'):
        return EXIT_FAILED
    changed = sorted(rewritten | edited | set(added))
    git(
        repo,
        'add',
        '--pathspec-from-file=-',
        '--pathspec-file-nul',
        stdin='\0'.join(changed),
    )
    open_sites = [site for site in reviews if not covered_by_edit(repo, site, edited)]
    for site in open_sites:
        print(f'review {site.review.path}:{site.review.line}: {site.review.cause}')
    print(
        f'review sites: {len(open_sites)} open, {len(reviews) - len(open_sites)} changed by an exact edit'
    )
    if report(run_checks(repo, old_files), 'checks'):
        return EXIT_FAILED
    summary = git(repo, 'diff', '--cached', '-M', '--name-status').splitlines()
    renames = sum(1 for line in summary if line.startswith('R'))
    exact = sum(1 for line in summary if line.startswith('R100'))
    print(
        f'staged: {len(summary)} entries; {renames} renames ({exact} unchanged), {len(summary) - renames} other'
    )
    if not commit:
        print(
            f'The move is staged and checked. Review it with: git -C {repo} diff --cached -M --stat'
        )
        return EXIT_DONE
    return report(write_commits(repo, old_commit, old_files), 'commits')


def main(arguments: list[str]) -> int:
    match arguments:
        case ['--commit', checkout]:
            commit = True
        case [checkout] if not checkout.startswith('-'):
            commit = False
        case _:
            print(__doc__, file=sys.stderr)
            return EXIT_USAGE
    if sys.version_info < (3, 13):
        print(
            'restructure: needs Python 3.13 or later (PurePosixPath.full_match)',
            file=sys.stderr,
        )
        return EXIT_USAGE
    repo = Path(checkout).resolve()
    os.environ['GIT_LITERAL_PATHSPECS'] = '1'
    try:
        return restructure(repo, commit)
    except GitError as error:
        print(f'FAIL git: {error}', file=sys.stderr)
        return EXIT_FAILED


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
