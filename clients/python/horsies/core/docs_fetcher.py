"""
Fetch horsies documentation locally for AI agents.

Two strategies: git sparse checkout (primary, fast) with
urllib tarball fallback (no git required). Idempotent —
run once to download, run again to update.

The docs match the installed version: the download uses the release tag
``v{version}``. The ``main`` branch is used only when the caller asks for it.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Final, assert_never

from horsies.core.types.result import Err, Ok, Result


REPO_URL = 'https://github.com/suleymanozkeskin/horsies'
DEFAULT_OUTPUT_DIR = '.horsies-docs'
DOCS_REPO_PATH = 'website/src/content/docs'
LLMS_REPO_PATH = 'website/public/llms.txt'
MAIN_BRANCH = 'main'
RELEASE_VERSION_PATTERN: Final = re.compile(r'\d+\.\d+\.\d+')

_GIT_TIMEOUT_SECONDS = 60
_TARBALL_TIMEOUT_SECONDS = 60


class DocsFetchError(Exception):
    """Raised when documentation fetching fails."""


@dataclass(frozen=True, slots=True)
class ReleaseTag:
    """The docs of one release. ``name`` is the git tag, for example ``v0.5.5``."""

    name: str


@dataclass(frozen=True, slots=True)
class MainBranch:
    """The docs of the ``main`` branch. They can be ahead of the installed version."""


DocsRef = ReleaseTag | MainBranch


@dataclass(frozen=True, slots=True)
class VersionHasNoTag:
    """The installed version is not a plain release number, so no tag matches it.

    Examples: ``0.5.6.dev0``, ``0.5.5+local``, ``0.6.0rc1``.
    """

    installed_version: str

    def message(self) -> str:
        return (
            f'installed version {self.installed_version!r} is not a release '
            'version, so no release tag matches it. '
            f'Run get-docs with --ref {MAIN_BRANCH} to fetch the {MAIN_BRANCH} '
            'branch docs, which can differ from this version.'
        )


def resolve_docs_ref(
    *,
    installed_version: str,
    use_main_branch: bool,
) -> Result[DocsRef, VersionHasNoTag]:
    """Choose the git ref to download. Pure: it does no I/O.

    ``use_main_branch`` selects ``main`` for any version. Otherwise a version
    of the form ``N.N.N`` selects the tag ``vN.N.N``, and any other version
    returns ``VersionHasNoTag``. The tag is not checked here: a release number
    with no published tag fails later, in the download.
    """
    if use_main_branch:
        return Ok(MainBranch())
    if RELEASE_VERSION_PATTERN.fullmatch(installed_version) is None:
        return Err(VersionHasNoTag(installed_version=installed_version))
    return Ok(ReleaseTag(name=f'v{installed_version}'))


def _git_ref_name(ref: DocsRef) -> str:
    match ref:
        case ReleaseTag(name=name):
            return name
        case MainBranch():
            return MAIN_BRANCH
        case _:
            assert_never(ref)


def _tarball_url(ref: DocsRef) -> str:
    match ref:
        case ReleaseTag(name=name):
            return f'{REPO_URL}/archive/refs/tags/{name}.tar.gz'
        case MainBranch():
            return f'{REPO_URL}/archive/refs/heads/{MAIN_BRANCH}.tar.gz'
        case _:
            assert_never(ref)


def fetch_docs(*, ref: DocsRef, output_dir: str = DEFAULT_OUTPUT_DIR) -> int:
    """Fetch horsies docs of ``ref`` into output_dir. Returns file count.

    Strategy: git sparse checkout, falling back to tarball on
    DocsFetchError (the one type-branching seam in this module).

    Raises:
        DocsFetchError: both strategies failed.
        OSError: filesystem failure (mkdir/copy/swap). Recovery seam is
            ``get_docs_command``: print and exit 1.
    """
    output_path = Path(output_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if _has_git():
        _print_status('Fetching docs via git sparse checkout...')
        try:
            count = _fetch_via_sparse_checkout(output_path, ref)
            _print_status(f'Done. {count} files written to {output_dir}/')
            return count
        except DocsFetchError as exc:
            _print_status(f'Git sparse checkout failed ({exc}), falling back to tarball...')

    _print_status('Fetching docs via tarball download...')
    count = _fetch_via_tarball(output_path, ref)
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


def _fetch_via_sparse_checkout(output_dir: Path, ref: DocsRef) -> int:
    """Clone with sparse checkout and copy docs to output_dir."""
    with tempfile.TemporaryDirectory(prefix='horsies-docs-') as tmpdir:
        tmp = Path(tmpdir)
        clone_dir = tmp / 'repo'

        _run_git(
            [
                'clone', '--filter=blob:none', '--no-checkout',
                '--depth=1', '--branch', _git_ref_name(ref),
                f'{REPO_URL}.git', str(clone_dir),
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
            ['checkout', _git_ref_name(ref)],
            cwd=clone_dir,
        )

        docs_src = clone_dir / DOCS_REPO_PATH
        llms_src = clone_dir / LLMS_REPO_PATH
        return _assemble_output(docs_src, llms_src, output_dir)


def _fetch_via_tarball(output_dir: Path, ref: DocsRef) -> int:
    """Download tarball via urllib and extract docs to output_dir."""
    tarball_url = _tarball_url(ref)
    with tempfile.TemporaryDirectory(prefix='horsies-docs-') as tmpdir:
        tmp = Path(tmpdir)
        tarball_path = tmp / 'repo.tar.gz'

        # urlopen with an explicit timeout: urlretrieve has no timeout param
        # and falls back to the (usually None) global socket timeout, so a
        # stalled server could hang the fetch forever.
        try:
            with urllib.request.urlopen(
                tarball_url,
                timeout=_TARBALL_TIMEOUT_SECONDS,
            ) as response, open(tarball_path, 'wb') as out:
                shutil.copyfileobj(response, out)
        except Exception as exc:
            raise DocsFetchError(
                f'failed to download tarball from {tarball_url}: {exc}',
            ) from exc

        try:
            with tarfile.open(tarball_path, 'r:gz') as tf:
                members = tf.getnames()
                if not members:
                    raise DocsFetchError('tarball is empty')
                # The archive's top-level directory (e.g. 'horsies-main/').
                # Verify every member shares it instead of trusting members[0]
                # blindly, so an unexpected layout fails loudly here rather
                # than extracting to the wrong place.
                prefix = members[0].split('/')[0]
                if not all(
                    m == prefix or m.startswith(f'{prefix}/') for m in members
                ):
                    raise DocsFetchError(
                        'unexpected tarball layout: members do not share a '
                        'single top-level directory',
                    )
                tf.extractall(path=tmp, filter='data')
        except tarfile.TarError as exc:
            raise DocsFetchError(f'corrupt tarball: {exc}') from exc

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
