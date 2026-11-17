"""No tracked text file cites a package path from before the package move.

The Python package moved from the repository root to ``python/horsies/``, and
the package's SQL folder ``core/baseline/sql/`` moved to
``crates/horsies-core/baseline/``. A path that starts at the old package
folder and goes on with ``core/`` or ``monitoring/`` names a file that is not
at that path any more. The scan reads every file of the git index that is
UTF-8 text, apart from the files of ``KEPT_FILES``.

The fix for a finding: cite ``python/horsies/<rest>`` for a package file, and
``crates/horsies-core/baseline/<file>`` for a file of the old SQL folder.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Final

import pytest

pytestmark = [pytest.mark.unit]

REPO_ROOT: Final = Path(__file__).resolve().parents[2]

OLD_PACKAGE_CITATION: Final = re.compile(
    r'(?<![A-Za-z0-9_.@/-])horsies/(?:core|monitoring)/',
)
"""The old package folder, then ``core/`` or ``monitoring/``. A character
before it that can be part of a path (``python/horsies/``, ``crates/horsies/``,
a URL) means that it is not a path from the repository root."""

GIT_LIST_TIMEOUT_S: Final = 60
"""Seconds that ``git ls-files`` gets to list the index."""


@dataclass(frozen=True, slots=True)
class KeptFile:
    """Tracked files that keep their old-path citations, and why."""

    pattern: str
    reason: str


KEPT_FILES: Final = (
    KeptFile(
        'crates/horsies-core/baseline/*.sql',
        'the baseline migration text; a changed byte changes its stored checksum',
    ),
    KeptFile(
        'tests/unit/test_old_package_path_citations.py',
        'this check names the old paths',
    ),
)


@dataclass(frozen=True, slots=True)
class Citation:
    """One line of a tracked file that cites an old package path."""

    path: str
    line_number: int
    line: str


@dataclass(frozen=True, slots=True)
class GitDidNotRun:
    """``git ls-files`` did not finish: git is missing, or it ran past
    ``GIT_LIST_TIMEOUT_S``."""

    cause: str


def list_index() -> subprocess.CompletedProcess[str] | GitDidNotRun:
    """The finished ``git ls-files -z`` of this checkout, or why it did not
    finish."""
    try:
        return subprocess.run(
            ['git', '-C', str(REPO_ROOT), 'ls-files', '-z'],
            capture_output=True,
            text=True,
            timeout=GIT_LIST_TIMEOUT_S,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return GitDidNotRun(cause=repr(error))


def tracked_paths() -> tuple[str, ...]:
    """The paths of this checkout's git index. A failed listing fails the
    test with git's message."""
    match list_index():
        case GitDidNotRun(cause=cause):
            pytest.fail(f'git ls-files in {REPO_ROOT} did not finish: {cause}')
        case listing if listing.returncode == 0:
            return tuple(name for name in listing.stdout.split('\0') if name)
        case listing:
            pytest.fail(
                f'git ls-files in {REPO_ROOT} exited {listing.returncode}: '
                f'{listing.stderr.strip()}',
            )


def is_kept(path: str, rule: KeptFile) -> bool:
    return PurePosixPath(path).full_match(rule.pattern)


def citations_in(path: str) -> tuple[Citation, ...]:
    """The citing lines of one tracked file. A file that is not in the
    working tree, a symbolic link, or a file that is not UTF-8 text has
    none."""
    file = REPO_ROOT / path
    if file.is_symlink() or not file.is_file():
        return ()
    try:
        text = file.read_bytes().decode('utf-8')
    except UnicodeDecodeError:
        return ()
    return tuple(
        Citation(path=path, line_number=number, line=line.strip())
        for number, line in enumerate(text.splitlines(), start=1)
        if OLD_PACKAGE_CITATION.search(line) is not None
    )


@pytest.fixture(scope='module')
def citations_by_path() -> dict[str, tuple[Citation, ...]]:
    found: dict[str, tuple[Citation, ...]] = {}
    for path in tracked_paths():
        citations = citations_in(path)
        if citations:
            found[path] = citations
    return found


def test_no_tracked_file_cites_an_old_package_path(
    citations_by_path: dict[str, tuple[Citation, ...]],
) -> None:
    findings = [
        citation
        for path, citations in sorted(citations_by_path.items())
        if not any(is_kept(path, rule) for rule in KEPT_FILES)
        for citation in citations
    ]
    report = '\n'.join(
        f'{citation.path}:{citation.line_number}: {citation.line}' for citation in findings
    )
    assert not findings, (
        'old package paths cited; cite python/horsies/<rest>, or '
        f'crates/horsies-core/baseline/<file> for the old SQL folder:\n{report}'
    )


@pytest.mark.parametrize('rule', KEPT_FILES, ids=[rule.pattern for rule in KEPT_FILES])
def test_each_kept_rule_covers_a_citing_file(
    rule: KeptFile,
    citations_by_path: dict[str, tuple[Citation, ...]],
) -> None:
    covered = [path for path in citations_by_path if is_kept(path, rule)]
    assert covered, f'remove the kept rule {rule.pattern!r}: no tracked file under it cites an old path'


@pytest.mark.parametrize(
    'line',
    [
        'see horsies/core/brokers/postgres.py:120',
        'Replaces: `horsies/monitoring/queries.py` (list_tasks)',
        "include_str!(\"horsies/core/baseline/sql/0001_tables.sql\")",
        '// horsies/core/worker/runtime.py',
    ],
)
def test_the_pattern_finds_an_old_path(line: str) -> None:
    assert OLD_PACKAGE_CITATION.search(line) is not None


@pytest.mark.parametrize(
    'line',
    [
        'see python/horsies/core/brokers/postgres.py:120',
        'crates/horsies/src/core/workflow/context.rs',
        'crates/horsies-core/baseline/0001_tables.sql',
        'from horsies.core.brokers import postgres',
        'served from horsies/web/static/ in the wheel',
        'https://github.com/owner/horsies/core/brokers/postgres.py',
    ],
)
def test_the_pattern_leaves_a_current_path(line: str) -> None:
    assert OLD_PACKAGE_CITATION.search(line) is None
