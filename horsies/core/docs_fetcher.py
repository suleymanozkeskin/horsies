"""
Fetch horsies documentation locally for AI agents.

Two strategies: git sparse checkout (primary, fast) with
urllib tarball fallback (no git required). Idempotent —
run once to download, run again to update.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path


REPO_URL = 'https://github.com/suleymanozkeskin/horsies'
DEFAULT_OUTPUT_DIR = '.horsies-docs'
DOCS_REPO_PATH = 'website/src/content/docs'
LLMS_REPO_PATH = 'website/public/llms.txt'
DEFAULT_BRANCH = 'main'
TARBALL_URL = f'{REPO_URL}/archive/refs/heads/{DEFAULT_BRANCH}.tar.gz'

_GIT_TIMEOUT_SECONDS = 60


class DocsFetchError(Exception):
    """Raised when documentation fetching fails."""


def fetch_docs(*, output_dir: str = DEFAULT_OUTPUT_DIR) -> int:
    """Fetch horsies docs into output_dir. Returns file count."""
    output_path = Path(output_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if _has_git():
        _print_status('Fetching docs via git sparse checkout...')
        try:
            count = _fetch_via_sparse_checkout(output_path)
            _print_status(f'Done. {count} files written to {output_dir}/')
            return count
        except DocsFetchError as exc:
            _print_status(f'Git sparse checkout failed ({exc}), falling back to tarball...')

    _print_status('Fetching docs via tarball download...')
    count = _fetch_via_tarball(output_path)
    _print_status(f'Done. {count} files written to {output_dir}/')
    return count


def _has_git() -> bool:
    """Check if git is available and supports sparse-checkout."""
    if shutil.which('git') is None:
        return False
    try:
        result = subprocess.run(
            ['git', 'sparse-checkout', '--help'],
            capture_output=True,
            timeout=10,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def _run_git(args: list[str], cwd: Path) -> None:
    """Run a git command, raising DocsFetchError on failure."""
    try:
        result = subprocess.run(
            ['git', *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=_GIT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise DocsFetchError(
            f'git {args[0]} timed out after {_GIT_TIMEOUT_SECONDS}s',
        ) from exc
    except OSError as exc:
        raise DocsFetchError(f'git {args[0]} failed: {exc}') from exc

    if result.returncode != 0:
        stderr_text = result.stderr.strip()
        raise DocsFetchError(
            f'git {args[0]} failed (exit {result.returncode}): {stderr_text}',
        )


def _fetch_via_sparse_checkout(output_dir: Path) -> int:
    """Clone with sparse checkout and copy docs to output_dir."""
    with tempfile.TemporaryDirectory(prefix='horsies-docs-') as tmpdir:
        tmp = Path(tmpdir)
        clone_dir = tmp / 'repo'

        _run_git(
            [
                'clone', '--filter=blob:none', '--no-checkout',
                '--depth=1', f'{REPO_URL}.git', str(clone_dir),
            ],
            cwd=tmp,
        )
        _run_git(
            ['sparse-checkout', 'init', '--cone'],
            cwd=clone_dir,
        )
        _run_git(
            ['sparse-checkout', 'set', DOCS_REPO_PATH, LLMS_REPO_PATH],
            cwd=clone_dir,
        )
        _run_git(
            ['checkout', DEFAULT_BRANCH],
            cwd=clone_dir,
        )

        docs_src = clone_dir / DOCS_REPO_PATH
        llms_src = clone_dir / LLMS_REPO_PATH
        return _assemble_output(docs_src, llms_src, output_dir)


def _fetch_via_tarball(output_dir: Path) -> int:
    """Download tarball via urllib and extract docs to output_dir."""
    with tempfile.TemporaryDirectory(prefix='horsies-docs-') as tmpdir:
        tmp = Path(tmpdir)
        tarball_path = tmp / 'repo.tar.gz'

        try:
            urllib.request.urlretrieve(
                TARBALL_URL,
                tarball_path,
            )
        except Exception as exc:
            raise DocsFetchError(
                f'failed to download tarball from {TARBALL_URL}: {exc}',
            ) from exc

        try:
            with tarfile.open(tarball_path, 'r:gz') as tf:
                members = tf.getnames()
                prefix = members[0].split('/')[0] if members else ''
                tf.extractall(path=tmp, filter='data')
        except tarfile.TarError as exc:
            raise DocsFetchError(f'corrupt tarball: {exc}') from exc

        if not members:
            raise DocsFetchError('tarball is empty')

        docs_src = tmp / prefix / DOCS_REPO_PATH
        llms_src = tmp / prefix / LLMS_REPO_PATH
        return _assemble_output(docs_src, llms_src, output_dir)


def _assemble_output(
    docs_src: Path,
    llms_src: Path,
    output_dir: Path,
) -> int:
    """Copy docs tree and llms.txt into output_dir. Returns file count."""
    if not docs_src.is_dir():
        raise DocsFetchError(
            f'docs source directory not found: {docs_src}',
        )

    # Atomic-ish swap: copy into a temp sibling first so that a copytree
    # failure does not destroy the existing output_dir.
    tmp_output = output_dir.with_name(output_dir.name + '.tmp')
    if tmp_output.exists():
        shutil.rmtree(tmp_output)

    shutil.copytree(docs_src, tmp_output)

    # Copy llms.txt if it exists (not fatal if missing)
    if llms_src.is_file():
        shutil.copy2(llms_src, tmp_output / 'llms.txt')

    # Swap: remove old, rename new
    if output_dir.exists():
        shutil.rmtree(output_dir)
    tmp_output.rename(output_dir)

    file_count = sum(1 for _ in output_dir.rglob('*') if _.is_file())
    if file_count == 0:
        raise DocsFetchError('no files were copied to output directory')

    return file_count


def _print_status(message: str) -> None:
    """Print status message to stderr."""
    print(message, file=sys.stderr)
