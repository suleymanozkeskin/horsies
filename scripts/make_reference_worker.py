"""Copy the Python worker parent and its version-1 child into
``tests/dualrun/reference/worker_parent/`` from a git ref.

The whole-run pairs (``tests/dualrun/worker_run.py``) start the reference
side from this copy: ``python -m tests.dualrun.reference.worker_parent.cli
worker``. The copy is every module of ``python/horsies/core/worker/`` at the
ref, but ``KEPT_IN_PRODUCTION``, and ``python/horsies/core/cli.py``. Each
``horsies.core.worker.<module>`` name of a copied module is rewritten to the
copy, in imports and in text (the child module that the parent starts is
named in a string), so the reference parent starts the reference child.

Usage: ``python scripts/make_reference_worker.py <repo root> <git ref>``

Reads the ref with ``git show``; writes only under the target folder, which
it creates when absent. Exits non-zero with the git error when the ref or a
file cannot be read.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from collections.abc import Sequence
from typing import Final

KEPT_IN_PRODUCTION: Final = frozenset({'config', 'claim_plan', 'cpu_placement', 'profile_fit'})
"""Worker modules that the copy imports from production. The production
models hold their types (``WorkerProfile`` holds ``ChildPreload`` of
``config`` and ``ParentCpu`` of ``cpu_placement``; ``config`` gives the
``ClaimPlan`` of ``claim_plan``), so the copy and production must share one
class of each. ``profile_fit`` imports only these and the text of a module
name."""

PACKAGE: Final = 'tests.dualrun.reference.worker_parent'
"""The import name of the copy."""

TARGET: Final = ('tests', 'dualrun', 'reference', 'worker_parent')
"""The folder of the copy, under the repository root."""

WORKER_SOURCE: Final = 'python/horsies/core/worker'
CLI_SOURCE: Final = 'python/horsies/core/cli.py'

_WORKER_NAME: Final = re.compile(r'horsies\.core\.worker\.(\w+)')


def git_show(repo: str, ref: str, path: str) -> str:
    """The text of ``path`` at ``ref``. Raises ``CalledProcessError`` when git
    cannot read it."""
    return subprocess.run(
        ['git', '-C', repo, 'show', f'{ref}:{path}'],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def worker_modules(repo: str, ref: str) -> list[str]:
    """The module names of the worker folder at ``ref``, sorted."""
    listing = subprocess.run(
        ['git', '-C', repo, 'ls-tree', '--name-only', ref, f'{WORKER_SOURCE}/'],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    return sorted(
        os.path.basename(path)[: -len('.py')] for path in listing if path.endswith('.py')
    )


def rewrite(text: str, copied: frozenset[str]) -> str:
    """``text`` with each name of a copied worker module pointed at the copy.
    Pure."""

    def module(match: re.Match[str]) -> str:
        name = match.group(1)
        match name in copied:
            case True:
                return f'{PACKAGE}.worker.{name}'
            case False:
                return match.group(0)

    text = _WORKER_NAME.sub(module, text)
    return text.replace('from horsies.core.worker import ', f'from {PACKAGE}.worker import ')


def package_init(ref_commit: str, role: str) -> str:
    """The ``__init__.py`` of a folder of the copy, naming its source."""
    return (
        f'"""{role}, copied from commit {ref_commit} by\n'
        '``scripts/make_reference_worker.py``. Do not edit: the copy is the\n'
        'reference side of the whole-run pairs, and the last step of the core\n'
        'switch deletes it."""\n'
    )


def resolve_commit(repo: str, ref: str) -> str:
    """The full commit id of ``ref``."""
    return subprocess.run(
        ['git', '-C', repo, 'rev-parse', '--verify', f'{ref}^{{commit}}'],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def write(path: str, text: str) -> None:
    with open(path, 'w', encoding='utf-8') as out:
        _ = out.write(text)


def main(argv: Sequence[str]) -> int:
    match argv:
        case [repo, ref]:
            pass
        case _:
            sys.stderr.write('usage: make_reference_worker.py <repo root> <git ref>\n')
            return 2
    commit = resolve_commit(repo, ref)
    target = os.path.join(repo, *TARGET)
    worker_dir = os.path.join(target, 'worker')
    os.makedirs(worker_dir, exist_ok=True)
    copied = frozenset(
        name for name in worker_modules(repo, ref) if name not in KEPT_IN_PRODUCTION
    )
    for name in sorted(copied):
        text = git_show(repo, ref, f'{WORKER_SOURCE}/{name}.py')
        write(os.path.join(worker_dir, f'{name}.py'), rewrite(text, copied))
    write(
        os.path.join(target, 'cli.py'),
        rewrite(git_show(repo, ref, CLI_SOURCE), copied),
    )
    write(
        os.path.join(target, '__init__.py'),
        package_init(commit, 'The Python worker parent of the reference side'),
    )
    write(
        os.path.join(worker_dir, '__init__.py'),
        package_init(commit, 'The worker modules of the reference side'),
    )
    sys.stdout.write(f'copied {len(copied)} worker modules and cli.py from {commit}\n')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
