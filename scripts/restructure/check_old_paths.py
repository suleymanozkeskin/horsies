"""No tracked text file cites a path from before the 0.8.0 restructure.

Usage: check_old_paths.py <checkout>

<checkout> is a checkout after the restructure. The check reads every file of
its git index that is UTF-8 text, apart from the files of
``move_rules.KEPT_FILES`` and the lines of ``move_rules.KEPT_LINES``, and
finds:

- a path that starts at a top-level folder that the restructure emptied:
  ``crates/``, ``python/``, ``rust/``, ``showcase/``;
- a path ``tests/<entry>`` whose entry moved to ``clients/python/tests/``,
  in a file outside the Python project (inside ``clients/python/``, a
  ``tests/`` path is relative to the project, as pytest node ids are);
- a path under a ``tests/`` entry that moved on its own (``tests/dualrun/``,
  the unit tests of the harnesses in ``tests/unit/``), in any file;
- a GitHub source link to such a path, or such a path after
  ``$WORKSPACE/`` or ``./``;
- a Python path built from literals that start at such a folder
  (``ROOT / 'crates' / 'horsies-core'``);
- the module paths ``showcase.acme`` and ``tests.dualrun``;
- a path from one old crate folder to another (``../horsies-core``);
- a name built from an old path (``crates_horsies_agents_skills``).

A path that names a tracked file or folder next to the citing file, or in
its crate or Python project, is a local path and not a finding: the core has
a ``tests/`` folder of its own.

Before the scan, the check runs its patterns on planted lines with known
answers, so that a pattern that matches nothing fails the check instead of
passing it.

It reads files under <checkout> and runs ``git ls-files``; it writes
nothing. Exit codes: 0 no finding, 1 a finding or a failed self-test, 2
usage or git failure.
"""

from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import move_rules as rules

EXIT_CLEAN: Final = 0
EXIT_FINDINGS: Final = 1
EXIT_USAGE: Final = 2

GIT_TIMEOUT_S: Final = 60
"""Seconds that ``git ls-files`` gets to list the index."""

GONE_FOLDER_CHAIN: Final = re.compile(
    '|'.join(
        r'[\'"]{}[\'"]\s*\)?\s*/\s*[\'"]{}[\'"]'.format(
            *map(re.escape, rules.parts_of(move.old)[:2])
        )
        for move in rules.MOVES
        if len(rules.parts_of(move.old)) > 1
        and rules.parts_of(move.old)[0] in rules.OLD_FOLDERS_GONE
    )
    + r'|[\'"]showcase[\'"]\s*\)?\s*/',
)
"""Literals joined with ``/`` that name an old path under an emptied folder:
``'crates' / 'horsies-core'``. A chain that starts at ``'tests'`` is relative
to a base that the text does not show (the pytest root, the cwd)."""

NEW_CHAIN_HEAD: Final = re.compile(r'[\'"]clients[\'"]\s*\)?\s*/\s*$')
"""The new form ``'clients' / 'python' / 'horsies'`` holds the old pair; this is its head."""


BARE_FOLDERS_FLAGGED: Final = ('crates', 'python', 'showcase')
"""Emptied folders that a bare citation (``crates/``) still names. A bare
``rust/`` names the Rust section of the docs site as often as the old folder,
so only its moved entries (``rust/tests/...``) are findings."""

OLD_SIBLING: Final = re.compile(
    r'(?<![A-Za-z0-9_.-])\.\./horsies-(?:core|macros|python|codegen)\b'
)
"""A path from one old crate folder to another: ``../horsies-core``."""

OLD_DERIVED_NAME: Final = re.compile(
    r'(?<![A-Za-z0-9_])(?:crates_horsies|python_horsies|rust_(?:tests|examples|showcase|scripts))_',
)
"""A name built from an old path with ``_`` for ``/``: ``crates_horsies_agents_skills``."""

OLD_MODULE: Final = re.compile(r'(?<![A-Za-z0-9_.])(?:showcase\.acme|tests\.dualrun)\b')

ROOTED_PATH: Final = re.compile(
    r'(?:\$WORKSPACE/|(?<![A-Za-z0-9_./-])\./)'
    r'(?P<path>[A-Za-z0-9_][A-Za-z0-9_.-]*(?:/[A-Za-z0-9_.*-]*)+)',
)
"""A path after ``$WORKSPACE/`` (trybuild) or ``./``, which the citation
pattern does not start at: ``./rust/scripts/coverage.sh``."""

GITHUB_PATH: Final = re.compile(
    r'github\.com/suleymanozkeskin/horsies/(?:blob|tree|edit|raw)/[A-Za-z0-9_.-]+/(?P<path>[A-Za-z0-9_.@/-]+)',
)


@dataclass(frozen=True, slots=True)
class Finding:
    """One line that cites an old path."""

    path: str
    line_number: int
    cited: str
    line: str


@dataclass(frozen=True, slots=True)
class GitFailed:
    """``git ls-files`` did not list the index: why."""

    cause: str


def old_path_cited(cited: str, citing_file: str, tree: rules.Tree) -> bool:
    """Whether ``cited``, a path from the repository root, is a path from before the move."""
    clean = '/'.join(rules.literal_parts(cited.rstrip(rules.TRAILING_PUNCTUATION)))
    parts = rules.parts_of(clean)
    if not parts or tree.holds(clean):
        return False
    for base in (rules.dirname(citing_file), tree.package_root_of(citing_file)):
        if base and tree.holds(rules.join(base, clean)):
            return False
    if parts[0] in rules.OLD_FOLDERS_GONE:
        return (len(parts) == 1 and parts[0] in BARE_FOLDERS_FLAGGED) or rules.move_of(
            clean
        ) is not None
    match rules.move_of(clean):
        case rules.Move(old=old) if parts[0] == 'tests' and old != 'tests':
            return True
        case _:
            pass
    if parts[0] == 'tests' and rules.is_under(citing_file, rules.PYTHON_PROJECT):
        return False
    if parts[0] == 'tests' and len(parts) > 1:
        moved_entry = f'{rules.PYTHON_TESTS}/{parts[1]}'
        return tree.holds(moved_entry) and not tree.holds(f'tests/{parts[1]}')
    return False


def findings_in_line(line: str, citing_file: str, tree: rules.Tree) -> list[str]:
    """The old paths that one line cites."""
    found = [
        match.group('path')
        for match in rules.CITATION.finditer(line)
        if old_path_cited(match.group('path'), citing_file, tree)
    ]
    found += [
        match.group('path')
        for match in ROOTED_PATH.finditer(line)
        if old_path_cited(match.group('path'), citing_file, tree)
    ]
    found += [
        match.group('path')
        for match in GITHUB_PATH.finditer(line)
        if old_path_cited(match.group('path'), '', tree)
    ]
    found += [
        match.group(0)
        for match in GONE_FOLDER_CHAIN.finditer(line)
        if NEW_CHAIN_HEAD.search(line[: match.start()]) is None
    ]
    found += [match.group(0) for match in OLD_MODULE.finditer(line)]
    found += [match.group(0) for match in OLD_SIBLING.finditer(line)]
    found += [match.group(0) for match in OLD_DERIVED_NAME.finditer(line)]
    return found


def findings_in_file(repo: Path, path: str, tree: rules.Tree) -> list[Finding]:
    """The citing lines of one tracked file. A symbolic link, a file not in
    the working tree, or a file that is not UTF-8 text has none."""
    file = repo / path
    if (
        rules.is_under(path, rules.TOOL_FOLDER)
        or rules.kept_file_rule(path) is not None
    ):
        return []
    if file.is_symlink() or not file.is_file():
        return []
    try:
        text = file.read_bytes().decode('utf-8')
    except UnicodeDecodeError:
        return []
    findings: list[Finding] = []
    for number, line in enumerate(text.splitlines(), start=1):
        if rules.kept_line_rule(path, line) is not None:
            continue
        for cited in findings_in_line(line, path, tree):
            findings.append(
                Finding(
                    path=path, line_number=number, cited=cited, line=line.strip()[:160]
                )
            )
    return findings


def tracked_paths(repo: Path) -> tuple[str, ...] | GitFailed:
    try:
        listing = subprocess.run(
            ['git', '-C', str(repo), 'ls-files', '-z'],
            capture_output=True,
            text=True,
            timeout=GIT_TIMEOUT_S,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return GitFailed(cause=repr(error))
    match listing.returncode:
        case 0:
            return tuple(name for name in listing.stdout.split('\0') if name)
        case code:
            return GitFailed(
                cause=f'git ls-files exited {code}: {listing.stderr.strip()}'
            )


def scan(repo: Path, paths: tuple[str, ...]) -> list[Finding]:
    """Every finding of every tracked file."""
    tree = rules.Tree.of(paths)
    return [finding for path in paths for finding in findings_in_file(repo, path, tree)]


PLANTED_TREE: Final = rules.Tree.of(
    (
        'Cargo.toml',
        'core/Cargo.toml',
        'core/src/lib.rs',
        'core/derive/Cargo.toml',
        'core/derive/src/lib.rs',
        'tests/perf/test_perf_statistics.py',
        'core/tests/fixtures/stored_forms/README.md',
        'clients/python/pyproject.toml',
        'clients/python/tests/unit/test_a.py',
        'clients/python/horsies/__init__.py',
        'tests/parity/runner.py',
        'tests/parity/snapshot/capture.py',
        'website/src/content/docs/rust/tasks.md',
        'website/src/content/docs/index.md',
    ),
)

PLANTED: Final = (
    # (citing file, line, whether the line cites an old path)
    ('README.md', 'see crates/horsies-core/src/lib.rs:12', True),
    ('README.md', 'crates/horsies-core-derive/src/lib.rs', True),
    ('README.md', 'core/derive/src/lib.rs', False),
    ('README.md', "ROOT / 'crates' / 'horsies-core-derive'", True),
    ('README.md', 'horsies-core-derive = { path = "../horsies-core-derive" }', True),
    ('README.md', 'horsies-core-derive = { path = "derive" }', False),
    ('README.md', 'tests/unit/test_perf_statistics.py', True),
    ('README.md', 'tests/perf/test_perf_statistics.py', False),
    ('README.md', '`python/horsies/core/cli.py`', True),
    ('README.md', 'rust/tests/worker/src/main.rs', True),
    ('README.md', 'uv run horsies check showcase/acme/app.py', True),
    ('README.md', 'tests/unit/test_a.py', True),
    (
        'README.md',
        'https://github.com/suleymanozkeskin/horsies/tree/main/crates/horsies',
        True,
    ),
    ('README.md', "ROOT / 'crates' / 'horsies-core'", True),
    ('README.md', "Path('python') / 'horsies'", True),
    ('README.md', 'python -m showcase.acme.scenarios seed', True),
    ('README.md', "ROOT / 'rust' / 'tests' / 'worker'", True),
    ('README.md', "ROOT / 'clients' / 'rust' / 'tests' / 'worker'", False),
    ('README.md', 'Path::new("rust/tasks/errors.md")', False),
    ('README.md', 'the crates/ folder', True),
    ('README.md', '`CARGO_MANIFEST_DIR/../horsies-core/tests/fixtures`', True),
    ('README.md', '"crates_horsies_agents_skills_horsies_skill__line88"', True),
    ('README.md', '"clients_rust_horsies_agents_skills_horsies_skill__line88"', False),
    ('README.md', 'horsies = { path = "../horsies" }', False),
    ('README.md', 'core/src/lib.rs and clients/python/horsies/__init__.py', False),
    ('README.md', 'tests/parity/runner.py', False),
    ('README.md', 'tests/dualrun/capture.py', True),
    ('README.md', '  --> $WORKSPACE/crates/horsies/src/workflow/node.rs', True),
    ('README.md', '  --> $WORKSPACE/core/src/lib.rs', False),
    ('README.md', 'Usage: ./rust/scripts/coverage.sh [summary]', True),
    ('README.md', 'Usage: ./core/src/lib.rs', False),
    ('clients/python/tests/unit/test_a.py', 'tests/dualrun/capture.py', True),
    ('README.md', 'tests/parity/snapshot/capture.py', False),
    ('README.md', 'from tests.dualrun.capture import capture_snapshot', True),
    ('README.md', 'from tests.parity.snapshot.capture import capture_snapshot', False),
    ('README.md', 'horsies = { features = ["horsies/web"] }', False),
    ('README.md', 'https://example.com/python/horsies/core', False),
    ('core/src/lib.rs', 'tests/fixtures/stored_forms/README.md', False),
    (
        'clients/python/tests/unit/test_a.py',
        "case_name='tests/integration/test_x.py::test_y'",
        False,
    ),
    ('README.md', "MODULE_ROOTS = (Path('clients') / 'python' / 'horsies',)", False),
    ('README.md', 'website/src/content/docs/rust/ and the pages under rust/', False),
    ('website/src/content/docs/index.md', '[tasks](rust/tasks.md)', False),
)


def self_test() -> list[str]:
    """The planted lines that the patterns answer wrongly."""
    wrong: list[str] = []
    for citing_file, line, expected in PLANTED:
        found = bool(findings_in_line(line, citing_file, PLANTED_TREE))
        if found != expected:
            wrong.append(f'{citing_file}: {line!r}: found={found}, expected={expected}')
    return wrong


def main(arguments: list[str]) -> int:
    match arguments:
        case [checkout]:
            repo = Path(checkout).resolve()
        case _:
            print(__doc__, file=sys.stderr)
            return EXIT_USAGE
    wrong = self_test()
    for line in wrong:
        print(f'FAIL self-test: {line}', file=sys.stderr)
    if wrong:
        return EXIT_FINDINGS
    match tracked_paths(repo):
        case GitFailed(cause=cause):
            print(f'FAIL git ls-files in {repo}: {cause}', file=sys.stderr)
            return EXIT_USAGE
        case tuple() as paths:
            findings = scan(repo, paths)
    for finding in findings:
        print(f'{finding.path}:{finding.line_number}: {finding.cited}: {finding.line}')
    print(f'old-path check: {len(paths)} tracked files, {len(findings)} findings')
    return EXIT_FINDINGS if findings else EXIT_CLEAN


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
