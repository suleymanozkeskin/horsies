"""Edit rules and checks of the package move.

The move puts the Python package at ``python/horsies/`` and the baseline SQL
files at ``crates/horsies-core/baseline/``. ``scripts/package-move.sh`` does
the ``git mv`` steps and calls this file for the text changes and the checks.

Commands (``<repo>`` is the checkout that is moved):

    move_rules.py references <repo> <changed-list>
        Apply the exact edits and the path rules to the tracked text files.
        Write the changed paths, one per line, to <changed-list>.
    move_rules.py pyproject <repo>
        Switch pyproject.toml from setuptools to maturin.
    move_rules.py check <repo> <old-commit>
        Check the moved tree against <old-commit>, the commit before the move.

It reads and writes files under <repo> only. It runs no build and no git
command that writes. Exit codes: 0 done, 1 an edit anchor or a check failed,
2 usage.
"""

from __future__ import annotations

import json
import posixpath
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Final

OLD_PACKAGE: Final = 'horsies'
NEW_PACKAGE: Final = 'python/horsies'
OLD_SQL_FOLDER: Final = 'horsies/core/baseline/sql'
NEW_SQL_FOLDER: Final = 'crates/horsies-core/baseline'

EXIT_DONE: Final = 0
EXIT_FAILED: Final = 1
EXIT_USAGE: Final = 2


@dataclass(frozen=True, slots=True)
class KeptFile:
    """A file that keeps its old-path text, and why."""

    pattern: str
    reason: str


@dataclass(frozen=True, slots=True)
class KeptLine:
    """A line that keeps an old-path text, and why.

    A line of ``path`` that contains ``text`` is not rewritten, and the check
    allows it. Each entry must match at least one line.
    """

    path: str
    text: str
    reason: str


@dataclass(frozen=True, slots=True)
class Edit:
    """One exact text replacement. ``old`` must occur exactly once."""

    path: str
    old: str
    new: str


@dataclass(frozen=True, slots=True)
class Failure:
    """A failed edit or check: what, where and why."""

    subject: str
    cause: str


WHEEL_PATH: Final = 'a path inside the built wheel, where the package is horsies/'
INSTALLED_PATH: Final = 'the path in the installed package'

KEPT_FILES: Final = (
    KeptFile('CHANGELOG.md', 'release history'),
    KeptFile(
        'website/src/content/docs/changelog.md',
        'release history; tests/unit/test_changelog_parity.py keeps it equal to CHANGELOG.md',
    ),
    KeptFile('crates/horsies/**', 'the frozen Rust client; CI refuses a change under crates/horsies/'),
    KeptFile(
        f'{NEW_SQL_FOLDER}/*.sql',
        'migration 1000; a changed byte changes its checksum',
    ),
    KeptFile('scripts/package-move.sh', 'this tool'),
    KeptFile('scripts/package-move/**', 'this tool'),
)

KEPT_LINES: Final = (
    KeptLine('.github/workflows/release.yml', 'horsies/web/static', WHEEL_PATH),
    KeptLine('.github/workflows/release.yml', 'horsies/.agents/skills/horsies/', WHEEL_PATH),
    KeptLine(
        'python/horsies/core/skills_installer.py',
        'ships inside the wheel at ``horsies/.agents/skills/horsies/``',
        WHEEL_PATH,
    ),
    KeptLine(
        'website/src/content/docs/cli.md',
        '`horsies/.agents/skills/horsies/`. The wheel contains it',
        INSTALLED_PATH,
    ),
    KeptLine(
        'website/src/content/docs/questions-and-answers.md',
        'one skill folder, `horsies/.agents/skills/horsies/`',
        INSTALLED_PATH,
    ),
    KeptLine(
        'website/public/llms.txt',
        'one skill folder: `horsies/.agents/skills/horsies/`',
        INSTALLED_PATH,
    ),
    KeptLine(
        'website/public/llms.txt',
        'monitoring.md, horsies/.agents/skills/horsies, horsies skills install',
        INSTALLED_PATH,
    ),
    KeptLine(
        'website/src/content/docs/monitoring/web-ui-deployment.md',
        'The SPA is served from `horsies/web/static/` in the package',
        INSTALLED_PATH,
    ),
    KeptLine(
        'website/src/content/docs/migrations/migration-to-0-6-0.md',
        'The agent skill files are in `horsies/.agents/skills/horsies/`',
        INSTALLED_PATH,
    ),
)

_PYTHON_SCOPE_OLD: Final = """\
          # A PR that changes only Rust files (crates/, rust/, the Cargo and
          # toolchain files) cannot change a Python result: the Python jobs
          # are skipped for it.
          if git diff --quiet "$base" HEAD -- . ':(exclude)crates/' ':(exclude)rust/' ':(exclude)Cargo.toml' ':(exclude)Cargo.lock' ':(exclude)rust-toolchain.toml' ':(exclude)rustfmt.toml' ':(exclude)clippy.toml'; then
"""
_PYTHON_SCOPE_NEW: Final = """\
          # horsies._core is built from crates/horsies-python, crates/horsies-core
          # and crates/horsies-macros with the root Cargo and toolchain files, so
          # a change there can change a Python result. A PR that changes only the
          # frozen Rust client, the generator, rust/, or the format and lint
          # settings cannot: the Python jobs are skipped for it.
          if git diff --quiet "$base" HEAD -- . ':(exclude)crates/horsies/' ':(exclude)crates/horsies-codegen/' ':(exclude)rust/' ':(exclude)rustfmt.toml' ':(exclude)clippy.toml'; then
"""

_PACKAGE_DATA_TEST_OLD: Final = """\
def _matches_package_glob(relative: str, pattern: str) -> bool:
    \"\"\"Match like setuptools: ``*`` stays inside one path part, ``**`` crosses parts.\"\"\"
    regex = ''.join(
        '.*' if token == '**' else '[^/]*' if token == '*' else re.escape(token)
        for token in re.split(r'(\\*\\*|\\*)', pattern)
    )
    return re.fullmatch(regex, relative) is not None


class TestPackageData:
    def test_package_data_covers_every_skill_file(self) -> None:
        config = tomllib.loads((_REPO_ROOT / 'pyproject.toml').read_text())
        patterns = config['tool']['setuptools']['package-data']['horsies']
        package_root = _REPO_ROOT / 'horsies'

        uncovered = [
            relative
            for path in sorted((package_root / '.agents').rglob('*'))
            if path.is_file()
            for relative in [path.relative_to(package_root).as_posix()]
            if not any(_matches_package_glob(relative, pattern) for pattern in patterns)
        ]
"""
_PACKAGE_DATA_TEST_NEW: Final = """\
def _matches_package_glob(relative: str, pattern: str) -> bool:
    \"\"\"Match like a maturin include glob: ``*`` stays inside one path part, ``**`` crosses parts.\"\"\"
    regex = ''.join(
        '.*' if token == '**' else '[^/]*' if token == '*' else re.escape(token)
        for token in re.split(r'(\\*\\*|\\*)', pattern)
    )
    return re.fullmatch(regex, relative) is not None


def _puts_files_into_the_wheel(entry: dict[str, object]) -> bool:
    \"\"\"Whether a ``[tool.maturin] include`` entry targets the wheel.

    maturin takes ``format`` as ``'sdist'``, ``'wheel'`` or a list of them.
    \"\"\"
    match entry['format']:
        case 'wheel' | ['wheel'] | ['sdist', 'wheel'] | ['wheel', 'sdist']:
            return True
        case _:
            return False


class TestPackageData:
    def test_package_data_covers_every_skill_file(self) -> None:
        config = tomllib.loads((_REPO_ROOT / 'pyproject.toml').read_text())
        patterns = [
            str(entry['path'])
            for entry in config['tool']['maturin']['include']
            if _puts_files_into_the_wheel(entry)
        ]
        package_root = _REPO_ROOT / 'python' / 'horsies'

        uncovered = [
            relative
            for path in sorted((package_root / '.agents').rglob('*'))
            if path.is_file()
            for relative in [path.relative_to(_REPO_ROOT).as_posix()]
            if not any(_matches_package_glob(relative, pattern) for pattern in patterns)
        ]
"""

EDITS: Final = (
    Edit(
        '.gitignore',
        '# built web UI assets (produced by the webui build, never committed)\nhorsies/web/static/\n',
        '# built web UI assets (produced by the webui build, never committed)\n'
        'python/horsies/web/static/\n'
        '\n'
        '# horsies._core as a local maturin build places it next to the sources\n'
        'python/horsies/_core.*.so\n',
    ),
    Edit('.github/workflows/tests.yml', _PYTHON_SCOPE_OLD, _PYTHON_SCOPE_NEW),
    Edit(
        '.github/workflows/tests.yml',
        'elif git diff --quiet "$base" HEAD -- showcase/ horsies/; then',
        'elif git diff --quiet "$base" HEAD -- showcase/ python/horsies/ crates/horsies-python/'
        ' crates/horsies-core/ crates/horsies-macros/ Cargo.toml Cargo.lock rust-toolchain.toml; then',
    ),
    Edit(
        '.github/workflows/tests.yml',
        '      # compiling against the library as the library changes, which is why a\n'
        '      # change under horsies/ also triggers this job.\n',
        '      # compiling against the library as the library changes, which is why a\n'
        '      # change to python/horsies/ or to the crates of horsies._core also\n'
        '      # triggers this job.\n',
    ),
    Edit(
        'pyrightconfig.docs.json',
        '"extraPaths": ["tests/docs_examples/stubs"],',
        '"extraPaths": ["tests/docs_examples/stubs", "python"],',
    ),
    Edit(
        'tests/docs_examples/extract.py',
        "    Path('horsies') / '.agents' / 'skills',\n",
        "    Path('python') / 'horsies' / '.agents' / 'skills',\n",
    ),
    Edit(
        'tests/docs_examples/extract.py',
        "MODULE_ROOTS: Final = (Path('horsies'),)",
        "MODULE_ROOTS: Final = (Path('python') / 'horsies',)",
    ),
    Edit(
        'tests/unit/test_docs_examples_extract.py',
        "        package = tmp_path / 'horsies'\n        package.mkdir()\n",
        "        package = tmp_path / 'python' / 'horsies'\n        package.mkdir(parents=True)\n",
    ),
    Edit(
        'tests/unit/test_docs_examples_extract.py',
        "        assert find_modules(tmp_path) == (Path('horsies/module.py'),)\n"
        "        assert [path.name for path in output.glob('*.py')] == ['horsies_module.py']\n",
        "        assert find_modules(tmp_path) == (Path('python/horsies/module.py'),)\n"
        "        assert [path.name for path in output.glob('*.py')] == ['python_horsies_module.py']\n",
    ),
    Edit(
        'tests/unit/test_docs_enum_lists.py',
        "_SKILLS_DIR = _REPO_ROOT / 'horsies' / '.agents' / 'skills'",
        "_SKILLS_DIR = _REPO_ROOT / 'python' / 'horsies' / '.agents' / 'skills'",
    ),
    Edit(
        'tests/unit/test_execution_class.py',
        "_PACKAGE_ROOT = Path(__file__).resolve().parents[2] / 'horsies'",
        "_PACKAGE_ROOT = Path(__file__).resolve().parents[2] / 'python' / 'horsies'",
    ),
    Edit(
        'tests/unit/test_json_boundary_inventory.py',
        "_RUNTIME_ROOT: Final = _REPO_ROOT / 'horsies'",
        "_RUNTIME_ROOT: Final = _REPO_ROOT / 'python' / 'horsies'",
    ),
    Edit(
        'tests/unit/test_json_boundary_inventory.py',
        'Scope: every ``.py`` file under ``horsies/``.',
        'Scope: every ``.py`` file under ``python/horsies/``.',
    ),
    Edit(
        'tests/unit/test_skills_installer.py',
        "_SKILL_DIR = _REPO_ROOT / 'horsies' / '.agents' / 'skills' / 'horsies'",
        "_SKILL_DIR = _REPO_ROOT / 'python' / 'horsies' / '.agents' / 'skills' / 'horsies'",
    ),
    Edit('tests/unit/test_skills_installer.py', _PACKAGE_DATA_TEST_OLD, _PACKAGE_DATA_TEST_NEW),
    Edit(
        'tests/unit/test_terminal_writer_inventory.py',
        "_RUNTIME_ROOT = Path(__file__).resolve().parents[2] / 'horsies'\n",
        "_REPO_ROOT = Path(__file__).resolve().parents[2]\n"
        "_RUNTIME_ROOT = _REPO_ROOT / 'python' / 'horsies'\n",
    ),
    Edit(
        'website/src/content/docs/cli.md',
        '`horsies/.agents/skills/horsies/`. The wheel contains it, and a source checkout\n'
        'has it at the same path. Copy it into your project with\n',
        '`horsies/.agents/skills/horsies/`. The wheel contains it; a source checkout has\n'
        'it at `python/horsies/.agents/skills/horsies/`. Copy it into your project with\n',
    ),
    Edit(
        'website/src/content/docs/questions-and-answers.md',
        '`horsies/.agents/skills/horsies/`. A source checkout has it at the same path.',
        '`horsies/.agents/skills/horsies/`. A source checkout has it at'
        ' `python/horsies/.agents/skills/horsies/`.',
    ),
    Edit(
        'website/public/llms.txt',
        '`horsies/.agents/skills/horsies/` (a source checkout has the same path)',
        '`horsies/.agents/skills/horsies/` (a source checkout has it at'
        ' `python/horsies/.agents/skills/horsies/`)',
    ),
    Edit(
        'website/src/content/docs/monitoring/web-ui-deployment.md',
        'The SPA is served from `horsies/web/static/`, which CI builds into the wheel.',
        'The SPA is served from `horsies/web/static/` in the package, which CI builds into the wheel.',
    ),
)

REPEATED_EDITS: Final = (
    # (path, old, new, count): the same replacement at a fixed number of sites.
    (
        'tests/unit/test_terminal_writer_inventory.py',
        'path.relative_to(_RUNTIME_ROOT.parent)',
        'path.relative_to(_REPO_ROOT)',
        4,
    ),
)

# A cited path to the SQL folder that is not relative ("../") is moved to the
# crate. The relative include_str! paths of the core are set by
# sql-through-core.patch; the check fails on any one left.
_SQL_CITATION: Final = re.compile(
    r'(?<![A-Za-z0-9_.@/-])' + re.escape(OLD_SQL_FOLDER) + r'(?=/|`|\'|"|\s|$)'
)
_SQL_ANY: Final = re.compile(r'core/baseline/sql')
_REPO_JOIN: Final = re.compile(
    r"(?:_REPO_ROOT|_REPOSITORY|REPO_ROOT|REPO|repo_root|parents\[\d+\])\s*/\s*['\"]horsies['\"]"
    r"|Path\(\s*['\"]horsies['\"]\s*\)"
)


def package_citation(top_folders: tuple[str, ...], top_files: tuple[str, ...]) -> re.Pattern[str]:
    """A repository path that starts at the old package folder.

    Group 1 is a leading ``../`` chain. A path to the SQL folder is left to
    ``_SQL_CITATION``. A Cargo feature such as ``horsies/web`` has no ``/``
    after the folder name and does not match.
    """
    folders = '|'.join(re.escape(name) for name in top_folders)
    files = '|'.join(re.escape(name) for name in top_files)
    return re.compile(
        r'(?<![A-Za-z0-9_.@/-])((?:\.\./)*)horsies/'
        r'(?!core/baseline/sql(?![A-Za-z0-9_.-]))'
        rf'(?=(?:{folders})/|(?:{files})(?![A-Za-z0-9_.-]))'
    )


def run_git(repo: Path, *arguments: str) -> str:
    """Output of a read-only git command; a failure raises CalledProcessError."""
    return subprocess.run(
        ['git', '-C', str(repo), *arguments],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def tracked_files(repo: Path) -> tuple[str, ...]:
    """The paths in the index."""
    return tuple(name for name in run_git(repo, 'ls-files', '-z').split('\0') if name)


def package_top_level(repo: Path) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """The folders and the files directly under the moved package folder."""
    prefix = f'{NEW_PACKAGE}/'
    folders: set[str] = set()
    files: set[str] = set()
    for name in tracked_files(repo):
        if not name.startswith(prefix):
            continue
        rest = name.removeprefix(prefix)
        head, separator, _ = rest.partition('/')
        match separator:
            case '/':
                folders.add(head)
            case _:
                files.add(head)
    return tuple(sorted(folders)), tuple(sorted(files))


def is_kept_file(path: str) -> KeptFile | None:
    """The rule that keeps ``path`` unchanged, if one does."""
    for rule in KEPT_FILES:
        if PurePosixPath(path).full_match(rule.pattern):
            return rule
    return None


def kept_line_rule(path: str, line: str) -> KeptLine | None:
    """The rule that keeps ``line`` of ``path``, if one does."""
    for rule in KEPT_LINES:
        if rule.path == path and rule.text in line:
            return rule
    return None


def read_text(path: Path) -> str | None:
    """The UTF-8 text of a file; None for a file that is not UTF-8 text."""
    try:
        return path.read_bytes().decode('utf-8')
    except UnicodeDecodeError:
        return None


def apply_edits(repo: Path) -> tuple[set[str], list[Failure]]:
    """Apply EDITS and REPEATED_EDITS. Each anchor must match its count."""
    changed: set[str] = set()
    failures: list[Failure] = []
    planned = [(edit.path, edit.old, edit.new, 1) for edit in EDITS] + list(REPEATED_EDITS)
    for path, old, new, count in planned:
        file = repo / path
        if not file.is_file():
            failures.append(Failure(path, 'the file of an edit is missing'))
            continue
        text = read_text(file)
        if text is None:
            failures.append(Failure(path, 'the file of an edit is not UTF-8 text'))
            continue
        found = text.count(old)
        if found != count:
            failures.append(
                Failure(path, f'edit anchor found {found} times, expected {count}: {old[:80]!r}')
            )
            continue
        file.write_text(text.replace(old, new), encoding='utf-8')
        changed.add(path)
    return changed, failures


def rewrite_line(line: str, citation: re.Pattern[str]) -> str:
    """The line with the SQL folder and the package folder at their new paths."""
    moved_sql = _SQL_CITATION.sub(NEW_SQL_FOLDER, line)
    return citation.sub(lambda match: f'{match.group(1)}{NEW_PACKAGE}/', moved_sql)


def apply_path_rules(repo: Path) -> set[str]:
    """Rewrite old-path citations in every tracked text file that is not kept."""
    folders, files = package_top_level(repo)
    citation = package_citation(folders, files)
    changed: set[str] = set()
    for path in tracked_files(repo):
        if is_kept_file(path) is not None:
            continue
        file = repo / path
        if file.is_symlink() or not file.is_file():
            continue
        text = read_text(file)
        if text is None:
            continue
        lines = text.splitlines(keepends=True)
        rewritten = [
            line if kept_line_rule(path, line) is not None else rewrite_line(line, citation)
            for line in lines
        ]
        if rewritten != lines:
            file.write_text(''.join(rewritten), encoding='utf-8')
            changed.add(path)
    return changed


def command_references(repo: Path, changed_list: Path) -> int:
    """Apply the edits, then the path rules; write the changed paths."""
    edited, failures = apply_edits(repo)
    for failure in failures:
        print(f'FAIL edit {failure.subject}: {failure.cause}', file=sys.stderr)
    if failures:
        return EXIT_FAILED
    rewritten = apply_path_rules(repo)
    changed = sorted(edited | rewritten)
    changed_list.write_text(''.join(f'{path}\n' for path in changed), encoding='utf-8')
    print(f'edits: {len(EDITS) + sum(r[3] for r in REPEATED_EDITS)} sites in {len(edited)} files')
    print(f'path rules: {len(rewritten)} files')
    print(f'changed: {len(changed)} files')
    return EXIT_DONE


_BUILD_SYSTEM_OLD: Final = """\
[build-system]
# >=69: recursive "**" globs in package-data need setuptools >= 62.3, and
# the built wheel must carry the whole web/static tree.
requires = ["setuptools>=69", "wheel"]
build-backend = "setuptools.build_meta"
"""
_BUILD_SYSTEM_NEW: Final = """\
[build-system]
requires = ["maturin>=1.16,<2"]
build-backend = "maturin"
"""
_SETUPTOOLS_OLD: Final = """\
[tool.setuptools.packages.find]
where = ["."]
include = ["horsies*"]

[tool.setuptools]
license-files = ["LICENSE"]

[tool.setuptools.package-data]
horsies = [".agents/skills/horsies/*.md", "web/static/**", "core/baseline/sql/*.sql"]
"""
_MATURIN_NEW: Final = """\
[tool.maturin]
# The extension module horsies._core, built from crates/horsies-python.
manifest-path = "crates/horsies-python/Cargo.toml"
module-name = "horsies._core"
python-source = "python"
features = ["extension-module"]
# maturin packs every file under python/horsies/ that .gitignore does not
# exclude. The built web UI is git-ignored, so it is listed here; the skill
# files are listed so that the package data is stated, not implied. The
# baseline SQL files are compiled into horsies._core; the sdist carries them
# so that a build from the sdist can compile them.
include = [
    { path = "python/horsies/web/static/**/*", format = ["sdist", "wheel"] },
    { path = "python/horsies/.agents/skills/horsies/*.md", format = ["sdist", "wheel"] },
    { path = "crates/horsies-core/baseline/*.sql", format = "sdist" },
]
"""
_PYRIGHT_OLD: Final = '[tool.pyright]\ntypeCheckingMode = "strict"\n'
_PYRIGHT_NEW: Final = """\
[tool.pyright]
typeCheckingMode = "strict"
# The package is in python/; pyright resolves it there with or without an install.
extraPaths = ["python"]
"""
_UV_OLD: Final = '[tool.uv]\nexclude-newer = "7 days"\n'
_UV_NEW: Final = """\
[tool.uv]
exclude-newer = "7 days"
# uv rebuilds the local install when one of these changes: the Rust sources of
# horsies._core and the files that it compiles in.
cache-keys = [
    { file = "pyproject.toml" },
    { file = "Cargo.toml" },
    { file = "Cargo.lock" },
    { file = "crates/**/Cargo.toml" },
    { file = "crates/**/*.rs" },
    { file = "crates/horsies-core/baseline/*.sql" },
    { file = "crates/horsies-core/migrations/*.sql" },
    { file = "crates/horsies-core/webui-dist/**/*" },
]
"""
PYPROJECT_EDITS: Final = (
    (_BUILD_SYSTEM_OLD, _BUILD_SYSTEM_NEW),
    ('license = "MIT"\n', 'license = "MIT"\nlicense-files = ["LICENSE"]\n'),
    (_SETUPTOOLS_OLD, _MATURIN_NEW),
    (_PYRIGHT_OLD, _PYRIGHT_NEW),
    (_UV_OLD, _UV_NEW),
)


def command_pyproject(repo: Path) -> int:
    """Switch pyproject.toml to maturin. Every anchor must occur once."""
    file = repo / 'pyproject.toml'
    text = file.read_text(encoding='utf-8')
    failures = [
        Failure('pyproject.toml', f'anchor found {text.count(old)} times, expected 1: {old[:60]!r}')
        for old, _ in PYPROJECT_EDITS
        if text.count(old) != 1
    ]
    for failure in failures:
        print(f'FAIL {failure.subject}: {failure.cause}', file=sys.stderr)
    if failures:
        return EXIT_FAILED
    for old, new in PYPROJECT_EDITS:
        text = text.replace(old, new)
    file.write_text(text, encoding='utf-8')
    print(f'pyproject.toml: {len(PYPROJECT_EDITS)} blocks replaced')
    return EXIT_DONE


def old_package_files(repo: Path, old_commit: str) -> tuple[str, ...]:
    """The files under the old package folder at ``old_commit``."""
    listing = run_git(repo, 'ls-tree', '-r', '-z', '--name-only', old_commit, '--', f'{OLD_PACKAGE}/')
    return tuple(name for name in listing.split('\0') if name)


def new_path_of(old_path: str) -> str:
    """Where a file of the old package folder is after the move."""
    sql_prefix = f'{OLD_SQL_FOLDER}/'
    if old_path.startswith(sql_prefix):
        return f'{NEW_SQL_FOLDER}/{old_path.removeprefix(sql_prefix)}'
    return f'python/{old_path}'


def check_moved_files(repo: Path, old_commit: str) -> list[Failure]:
    """Every old file is at its new path; nothing is at the old paths."""
    failures: list[Failure] = []
    index = set(tracked_files(repo))
    old_files = old_package_files(repo, old_commit)
    if not old_files:
        failures.append(Failure(old_commit, f'no file under {OLD_PACKAGE}/ at the old commit'))
    expected_package = {new_path_of(name) for name in old_files if not name.startswith(OLD_SQL_FOLDER)}
    actual_package = {name for name in index if name.startswith(f'{NEW_PACKAGE}/')}
    for name in sorted(expected_package - actual_package):
        failures.append(Failure(name, 'a file of the old package is not at its new path'))
    for name in sorted(actual_package - expected_package):
        failures.append(Failure(name, 'a file under the new package path that the old package did not have'))
    left = sorted(name for name in index if name.startswith(f'{OLD_PACKAGE}/'))
    for name in left:
        failures.append(Failure(name, 'a tracked file is still at the old package path'))
    if (repo / OLD_PACKAGE).exists():
        failures.append(Failure(OLD_PACKAGE, 'the old package folder still exists on disk'))
    if (repo / NEW_PACKAGE / 'core' / 'baseline' / 'sql').exists():
        failures.append(Failure(f'{NEW_PACKAGE}/core/baseline/sql', 'the SQL folder exists in the package'))
    return failures


def check_sql_bytes(repo: Path, old_commit: str) -> tuple[int, list[Failure]]:
    """Each SQL file is in the core crate with its exact old bytes."""
    failures: list[Failure] = []
    old_sql = [name for name in old_package_files(repo, old_commit) if name.startswith(f'{OLD_SQL_FOLDER}/')]
    if not old_sql:
        failures.append(Failure(OLD_SQL_FOLDER, 'no SQL file at the old commit'))
    for name in old_sql:
        old_blob = run_git(repo, 'rev-parse', f'{old_commit}:{name}').strip()
        new_name = new_path_of(name)
        listed = run_git(repo, 'ls-files', '-s', '--', new_name).split()
        match listed:
            case [_, new_blob, _, _]:
                if new_blob != old_blob:
                    failures.append(Failure(new_name, f'bytes changed: blob {new_blob}, was {old_blob}'))
            case _:
                failures.append(Failure(new_name, 'not in the index'))
    in_crate = [name for name in tracked_files(repo) if name.startswith(f'{NEW_SQL_FOLDER}/')]
    if len(in_crate) != len(old_sql):
        failures.append(Failure(NEW_SQL_FOLDER, f'{len(in_crate)} files, expected {len(old_sql)}'))
    return len(old_sql), failures


@dataclass(frozen=True, slots=True)
class CitationScan:
    """Old-path text left in the tree: kept with a reason, or a failure."""

    kept_by_file: dict[str, int]
    kept_by_line: dict[str, int]
    failures: list[Failure]


def scan_citations(repo: Path) -> CitationScan:
    """Find old-path text in every tracked text file."""
    folders, files = package_top_level(repo)
    citation = package_citation(folders, files)
    kept_by_file: dict[str, int] = {}
    kept_by_line: dict[str, int] = {}
    failures: list[Failure] = []
    for path in tracked_files(repo):
        file = repo / path
        if file.is_symlink() or not file.is_file():
            continue
        text = read_text(file)
        if text is None:
            continue
        kept_file = is_kept_file(path)
        for number, line in enumerate(text.splitlines(), start=1):
            hit = citation.search(line) or _SQL_ANY.search(line) or _REPO_JOIN.search(line)
            if hit is None:
                continue
            if kept_file is not None:
                kept_by_file[kept_file.pattern] = kept_by_file.get(kept_file.pattern, 0) + 1
                continue
            kept_line = kept_line_rule(path, line)
            if kept_line is not None:
                kept_by_line[kept_line.reason] = kept_by_line.get(kept_line.reason, 0) + 1
                continue
            failures.append(Failure(f'{path}:{number}', f'old path left: {line.strip()[:120]}'))
    return CitationScan(kept_by_file=kept_by_file, kept_by_line=kept_by_line, failures=failures)


def check_kept_lines_match(repo: Path) -> list[Failure]:
    """Each KEPT_LINES entry matches at least one line, so none is stale."""
    failures: list[Failure] = []
    for rule in KEPT_LINES:
        file = repo / rule.path
        if not file.is_file():
            failures.append(Failure(rule.path, 'the file of a kept-line rule is missing'))
            continue
        text = read_text(file)
        if text is None:
            failures.append(Failure(rule.path, 'the file of a kept-line rule is not UTF-8 text'))
        elif not any(rule.text in line for line in text.splitlines()):
            failures.append(Failure(rule.path, f'kept-line rule matches no line: {rule.text!r}'))
    return failures


CORE_MIGRATIONS: Final = 'crates/horsies-core/src/broker/migrations.rs'
"""The core file whose ``baseline_sql_files!`` list compiles the baseline SQL files in."""

_RUST_LITERAL_INCLUDE: Final = re.compile(r'include_(?:str|bytes)!\(\s*"([^"]+)"\s*,?\s*\)')
_RUST_BASELINE_INCLUDE: Final = re.compile(r'include_str!\(concat!\(\s*"([^"]+)"\s*,\s*\$file\s*\)\)')
_RUST_BASELINE_LIST: Final = re.compile(r'^baseline_sql_files!\(\n(.*?)^\);$', re.MULTILINE | re.DOTALL)


def resolve_include(source: str, relative: str) -> str:
    """The repository path that an include path written in ``source`` names."""
    return posixpath.normpath(posixpath.join(posixpath.dirname(source), relative))


def check_rust_includes(repo: Path) -> tuple[int, list[Failure]]:
    """Each literal ``include_str!``/``include_bytes!`` path of a tracked Rust file names a tracked file.

    No build runs here, so this is the check that a moved file is still where
    the Rust sources include it from. Returns the count of literal paths.
    """
    index = tracked_files(repo)
    tracked = set(index)
    count = 0
    failures: list[Failure] = []
    for path in index:
        if not path.endswith('.rs'):
            continue
        text = read_text(repo / path)
        if text is None:
            failures.append(Failure(path, 'a Rust file that is not UTF-8 text'))
            continue
        for match in _RUST_LITERAL_INCLUDE.finditer(text):
            count += 1
            target = resolve_include(path, match.group(1))
            if target not in tracked:
                line = text.count('\n', 0, match.start()) + 1
                failures.append(
                    Failure(f'{path}:{line}', f'include path {match.group(1)!r} names no tracked file: {target}')
                )
    return count, failures


def check_baseline_list(repo: Path) -> tuple[int, list[Failure]]:
    """The core's ``baseline_sql_files!`` list is the SQL files of the crate folder, included from there.

    Returns the count of listed files.
    """
    text = read_text(repo / CORE_MIGRATIONS) if (repo / CORE_MIGRATIONS).is_file() else None
    if text is None:
        return 0, [Failure(CORE_MIGRATIONS, 'missing, or not UTF-8 text')]
    failures: list[Failure] = []
    folders = sorted({resolve_include(CORE_MIGRATIONS, m.group(1)) for m in _RUST_BASELINE_INCLUDE.finditer(text)})
    if folders != [NEW_SQL_FOLDER]:
        failures.append(Failure(CORE_MIGRATIONS, f'the baseline files are included from {folders}, expected {NEW_SQL_FOLDER}'))
    in_folder = sorted(
        name.removeprefix(f'{NEW_SQL_FOLDER}/')
        for name in tracked_files(repo)
        if name.startswith(f'{NEW_SQL_FOLDER}/') and name.endswith('.sql')
    )
    match _RUST_BASELINE_LIST.search(text):
        case None:
            failures.append(Failure(CORE_MIGRATIONS, 'no baseline_sql_files! list'))
            return 0, failures
        case listing:
            listed = re.findall(r'"([^"]+)"', listing.group(1))
    if listed != in_folder:
        failures.append(Failure(CORE_MIGRATIONS, f'baseline_sql_files! lists {listed}; the folder holds {in_folder}'))
    return len(listed), failures


def check_parse(repo: Path) -> list[Failure]:
    """Every tracked TOML file and the pyright configs parse."""
    failures: list[Failure] = []
    for path in tracked_files(repo):
        file = repo / path
        match PurePosixPath(path):
            case name if name.suffix == '.toml':
                parse = tomllib.loads
            case name if name.full_match('pyrightconfig*.json'):
                parse = json.loads
            case _:
                continue
        try:
            parse(file.read_text(encoding='utf-8'))
        except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError, json.JSONDecodeError) as exc:
            failures.append(Failure(path, f'{type(exc).__name__}: {exc}'))
    return failures


def _expect(failures: list[Failure], subject: str, actual: object, expected: object) -> None:
    if actual != expected:
        failures.append(Failure(subject, f'{actual!r}, expected {expected!r}'))


def check_pyproject(repo: Path) -> list[Failure]:
    """pyproject.toml builds with maturin and names files that exist."""
    failures: list[Failure] = []
    config = tomllib.loads((repo / 'pyproject.toml').read_text(encoding='utf-8'))
    build = config['build-system']
    _expect(failures, 'build-system.build-backend', build.get('build-backend'), 'maturin')
    _expect(failures, 'build-system.requires', build.get('requires'), ['maturin>=1.16,<2'])
    tool = config['tool']
    _expect(failures, 'tool.setuptools', 'setuptools' in tool, False)
    maturin = tool.get('maturin', {})
    _expect(failures, 'tool.maturin.python-source', maturin.get('python-source'), 'python')
    _expect(failures, 'tool.maturin.module-name', maturin.get('module-name'), 'horsies._core')
    _expect(failures, 'tool.maturin.features', maturin.get('features'), ['extension-module'])
    manifest = repo / str(maturin.get('manifest-path'))
    if not manifest.is_file():
        failures.append(Failure('tool.maturin.manifest-path', f'{manifest} is not a file'))
    else:
        crate = tomllib.loads(manifest.read_text(encoding='utf-8'))
        _expect(failures, f'{manifest.name} [lib] name', crate.get('lib', {}).get('name'), '_core')
        if 'extension-module' not in crate.get('features', {}):
            failures.append(Failure(str(manifest), 'no feature extension-module'))
    if not (repo / 'python' / 'horsies' / '__init__.py').is_file():
        failures.append(Failure('python/horsies/__init__.py', 'missing'))
    patterns = [str(entry.get('path')) for entry in maturin.get('include', [])]
    for pattern, minimum in (
        ('python/horsies/.agents/skills/horsies/*.md', 1),
        (f'{NEW_SQL_FOLDER}/*.sql', 1),
        ('python/horsies/web/static/**/*', 0),
    ):
        if pattern not in patterns:
            failures.append(Failure('tool.maturin.include', f'no entry {pattern!r}'))
        elif len(list(repo.glob(pattern))) < minimum:
            failures.append(Failure('tool.maturin.include', f'{pattern!r} matches no file'))
    _expect(failures, 'tool.pyright.extraPaths', tool.get('pyright', {}).get('extraPaths'), ['python'])
    if 'cache-keys' not in tool.get('uv', {}):
        failures.append(Failure('tool.uv.cache-keys', 'missing'))
    _expect(failures, 'project.license-files', config['project'].get('license-files'), ['LICENSE'])
    return failures


def command_check(repo: Path, old_commit: str) -> int:
    """Run every check; print the counts and each failure."""
    failures = check_moved_files(repo, old_commit)
    sql_count, sql_failures = check_sql_bytes(repo, old_commit)
    failures += sql_failures
    scan = scan_citations(repo)
    failures += scan.failures
    failures += check_kept_lines_match(repo)
    include_count, include_failures = check_rust_includes(repo)
    failures += include_failures
    listed_count, list_failures = check_baseline_list(repo)
    failures += list_failures
    failures += check_parse(repo)
    failures += check_pyproject(repo)
    moved = len(old_package_files(repo, old_commit))
    print(f'files moved: {moved} ({moved - sql_count} to {NEW_PACKAGE}/, {sql_count} to {NEW_SQL_FOLDER}/)')
    print(f'Rust include paths checked: {include_count}; baseline_sql_files! entries: {listed_count}')
    for pattern, count in sorted(scan.kept_by_file.items()):
        print(f'kept old-path lines, file rule {pattern}: {count}')
    for reason, count in sorted(scan.kept_by_line.items()):
        print(f'kept old-path lines, {reason}: {count}')
    for failure in failures:
        print(f'FAIL {failure.subject}: {failure.cause}', file=sys.stderr)
    print(f'checks: {len(failures)} failures')
    return EXIT_FAILED if failures else EXIT_DONE


def main(arguments: list[str]) -> int:
    match arguments:
        case ['references', repo, changed_list]:
            return command_references(Path(repo), Path(changed_list))
        case ['pyproject', repo]:
            return command_pyproject(Path(repo))
        case ['check', repo, old_commit]:
            return command_check(Path(repo), old_commit)
        case _:
            print(__doc__, file=sys.stderr)
            return EXIT_USAGE


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
