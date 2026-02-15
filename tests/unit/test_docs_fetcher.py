"""Unit tests for docs_fetcher module. All I/O is mocked."""

from __future__ import annotations

import io
import subprocess
import tarfile
from pathlib import Path
from unittest import mock

import pytest

from horsies.core.docs_fetcher import (
    DEFAULT_OUTPUT_DIR,
    DocsFetchError,
    _assemble_output,
    _fetch_via_sparse_checkout,
    _fetch_via_tarball,
    _has_git,
    fetch_docs,
)

pytestmark = pytest.mark.unit


# =============================================================================
# Helpers
# =============================================================================


def _make_test_tarball(prefix: str = 'horsies-main') -> bytes:
    """Build a minimal in-memory tarball mimicking the repo structure."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w:gz') as tf:
        # Add docs directory with a sample file
        docs_path = f'{prefix}/website/src/content/docs'

        index_content = b'# Quick Start\nGet started with horsies.'
        info = tarfile.TarInfo(name=f'{docs_path}/index.mdx')
        info.size = len(index_content)
        tf.addfile(info, io.BytesIO(index_content))

        guide_content = b'# CLI Guide\nUsage information.'
        info2 = tarfile.TarInfo(name=f'{docs_path}/cli.md')
        info2.size = len(guide_content)
        tf.addfile(info2, io.BytesIO(guide_content))

        # Add llms.txt
        llms_content = b'horsies task queue library'
        info3 = tarfile.TarInfo(name=f'{prefix}/website/public/llms.txt')
        info3.size = len(llms_content)
        tf.addfile(info3, io.BytesIO(llms_content))

    return buf.getvalue()


def _make_docs_tree(base: Path) -> Path:
    """Create a minimal docs directory tree for testing _assemble_output."""
    docs_dir = base / 'docs'
    docs_dir.mkdir(parents=True)
    (docs_dir / 'index.mdx').write_text('# Index')
    (docs_dir / 'cli.md').write_text('# CLI')
    sub = docs_dir / 'concepts'
    sub.mkdir()
    (sub / 'overview.md').write_text('# Overview')
    return docs_dir


# =============================================================================
# TestHasGit
# =============================================================================


class TestHasGit:
    """Tests for _has_git."""

    def test_git_available_with_sparse_checkout(self) -> None:
        with (
            mock.patch('horsies.core.docs_fetcher.shutil.which', return_value='/usr/bin/git'),
            mock.patch(
                'horsies.core.docs_fetcher.subprocess.run',
                return_value=mock.Mock(returncode=0),
            ),
        ):
            assert _has_git() is True

    def test_git_not_on_path(self) -> None:
        with mock.patch('horsies.core.docs_fetcher.shutil.which', return_value=None):
            assert _has_git() is False

    def test_sparse_checkout_not_supported(self) -> None:
        with (
            mock.patch('horsies.core.docs_fetcher.shutil.which', return_value='/usr/bin/git'),
            mock.patch(
                'horsies.core.docs_fetcher.subprocess.run',
                return_value=mock.Mock(returncode=1),
            ),
        ):
            assert _has_git() is False

    def test_sparse_checkout_times_out(self) -> None:
        with (
            mock.patch('horsies.core.docs_fetcher.shutil.which', return_value='/usr/bin/git'),
            mock.patch(
                'horsies.core.docs_fetcher.subprocess.run',
                side_effect=subprocess.TimeoutExpired(cmd='git', timeout=10),
            ),
        ):
            assert _has_git() is False

    def test_os_error_returns_false(self) -> None:
        with (
            mock.patch('horsies.core.docs_fetcher.shutil.which', return_value='/usr/bin/git'),
            mock.patch(
                'horsies.core.docs_fetcher.subprocess.run',
                side_effect=OSError('permission denied'),
            ),
        ):
            assert _has_git() is False


# =============================================================================
# TestAssembleOutput
# =============================================================================


class TestAssembleOutput:
    """Tests for _assemble_output."""

    def test_copies_docs_tree(self, tmp_path: Path) -> None:
        docs_src = _make_docs_tree(tmp_path / 'src')
        llms_src = tmp_path / 'src' / 'llms.txt'
        llms_src.write_text('llms content')
        output = tmp_path / 'output'

        count = _assemble_output(docs_src, llms_src, output)

        assert output.is_dir()
        assert (output / 'index.mdx').read_text() == '# Index'
        assert (output / 'cli.md').read_text() == '# CLI'
        assert (output / 'concepts' / 'overview.md').read_text() == '# Overview'
        assert (output / 'llms.txt').read_text() == 'llms content'
        assert count == 4  # 3 docs + llms.txt

    def test_overwrites_existing_output(self, tmp_path: Path) -> None:
        docs_src = _make_docs_tree(tmp_path / 'src')
        llms_src = tmp_path / 'src' / 'llms.txt'
        llms_src.write_text('llms')
        output = tmp_path / 'output'

        # First run
        _assemble_output(docs_src, llms_src, output)
        # Add a stale file
        (output / 'stale.md').write_text('should be gone')

        # Second run (idempotent)
        count = _assemble_output(docs_src, llms_src, output)

        assert not (output / 'stale.md').exists()
        assert count == 4

    def test_raises_on_missing_docs_src(self, tmp_path: Path) -> None:
        missing = tmp_path / 'nonexistent'
        llms_src = tmp_path / 'llms.txt'
        output = tmp_path / 'output'

        with pytest.raises(DocsFetchError, match='docs source directory not found'):
            _assemble_output(missing, llms_src, output)

    def test_handles_no_llms_txt_gracefully(self, tmp_path: Path) -> None:
        docs_src = _make_docs_tree(tmp_path / 'src')
        llms_src = tmp_path / 'src' / 'nonexistent_llms.txt'
        output = tmp_path / 'output'

        count = _assemble_output(docs_src, llms_src, output)

        assert not (output / 'llms.txt').exists()
        assert count == 3  # 3 docs, no llms.txt

    def test_raises_on_empty_docs(self, tmp_path: Path) -> None:
        empty_dir = tmp_path / 'empty_docs'
        empty_dir.mkdir()
        llms_src = tmp_path / 'llms.txt'
        output = tmp_path / 'output'

        with pytest.raises(DocsFetchError, match='no files were copied'):
            _assemble_output(empty_dir, llms_src, output)


# =============================================================================
# TestFetchViaSparseCheckout
# =============================================================================


class TestFetchViaSparseCheckout:
    """Tests for _fetch_via_sparse_checkout."""

    def test_calls_git_commands_in_order(self, tmp_path: Path) -> None:
        calls: list[list[str]] = []

        def fake_run(
            args: list[str],
            *,
            cwd: Path,
            capture_output: bool,
            text: bool,
            timeout: int,
        ) -> mock.Mock:
            calls.append(args)
            # After checkout, create fake docs structure
            if args[1] == 'checkout':
                docs_dir = cwd / 'website' / 'src' / 'content' / 'docs'
                docs_dir.mkdir(parents=True)
                (docs_dir / 'index.mdx').write_text('# Index')
                llms_path = cwd / 'website' / 'public'
                llms_path.mkdir(parents=True)
                (llms_path / 'llms.txt').write_text('llms')
            return mock.Mock(returncode=0, stderr='')

        output = tmp_path / 'output'
        with mock.patch('horsies.core.docs_fetcher.subprocess.run', side_effect=fake_run):
            count = _fetch_via_sparse_checkout(output)

        assert count == 2  # index.mdx + llms.txt
        assert len(calls) == 4
        assert calls[0][1] == 'clone'
        assert calls[1][1] == 'sparse-checkout'
        assert calls[1][2] == 'init'
        assert calls[2][1] == 'sparse-checkout'
        assert calls[2][2] == 'set'
        assert calls[3][1] == 'checkout'

    def test_raises_on_clone_failure(self, tmp_path: Path) -> None:
        with mock.patch(
            'horsies.core.docs_fetcher.subprocess.run',
            return_value=mock.Mock(returncode=128, stderr='fatal: repo not found'),
        ):
            with pytest.raises(DocsFetchError, match='git clone failed'):
                _fetch_via_sparse_checkout(tmp_path / 'output')

    def test_raises_on_timeout(self, tmp_path: Path) -> None:
        with mock.patch(
            'horsies.core.docs_fetcher.subprocess.run',
            side_effect=subprocess.TimeoutExpired(cmd='git', timeout=60),
        ):
            with pytest.raises(DocsFetchError, match='timed out'):
                _fetch_via_sparse_checkout(tmp_path / 'output')


# =============================================================================
# TestFetchViaTarball
# =============================================================================


class TestFetchViaTarball:
    """Tests for _fetch_via_tarball."""

    def test_extracts_from_synthetic_tarball(self, tmp_path: Path) -> None:
        tarball_data = _make_test_tarball()

        def fake_urlretrieve(url: str, path: Path) -> None:
            Path(path).write_bytes(tarball_data)

        output = tmp_path / 'output'
        with mock.patch(
            'horsies.core.docs_fetcher.urllib.request.urlretrieve',
            side_effect=fake_urlretrieve,
        ):
            count = _fetch_via_tarball(output)

        assert count == 3  # index.mdx + cli.md + llms.txt
        assert (output / 'index.mdx').exists()
        assert (output / 'cli.md').exists()
        assert (output / 'llms.txt').exists()

    def test_detects_custom_prefix(self, tmp_path: Path) -> None:
        tarball_data = _make_test_tarball(prefix='horsies-v2.0')

        def fake_urlretrieve(url: str, path: Path) -> None:
            Path(path).write_bytes(tarball_data)

        output = tmp_path / 'output'
        with mock.patch(
            'horsies.core.docs_fetcher.urllib.request.urlretrieve',
            side_effect=fake_urlretrieve,
        ):
            count = _fetch_via_tarball(output)

        assert count == 3

    def test_raises_on_download_failure(self, tmp_path: Path) -> None:
        with mock.patch(
            'horsies.core.docs_fetcher.urllib.request.urlretrieve',
            side_effect=Exception('connection refused'),
        ):
            with pytest.raises(DocsFetchError, match='failed to download tarball'):
                _fetch_via_tarball(tmp_path / 'output')

    def test_raises_on_corrupt_tarball(self, tmp_path: Path) -> None:
        def fake_urlretrieve(url: str, path: Path) -> None:
            Path(path).write_bytes(b'not a tarball')

        with mock.patch(
            'horsies.core.docs_fetcher.urllib.request.urlretrieve',
            side_effect=fake_urlretrieve,
        ):
            with pytest.raises(DocsFetchError, match='corrupt tarball'):
                _fetch_via_tarball(tmp_path / 'output')

    def test_raises_on_empty_tarball(self, tmp_path: Path) -> None:
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode='w:gz'):
            pass  # empty tarball
        empty_tarball = buf.getvalue()

        def fake_urlretrieve(url: str, path: Path) -> None:
            Path(path).write_bytes(empty_tarball)

        with mock.patch(
            'horsies.core.docs_fetcher.urllib.request.urlretrieve',
            side_effect=fake_urlretrieve,
        ):
            with pytest.raises(DocsFetchError, match='tarball is empty'):
                _fetch_via_tarball(tmp_path / 'output')


# =============================================================================
# TestFetchDocs
# =============================================================================


class TestFetchDocs:
    """Tests for the public fetch_docs entry point."""

    def test_uses_git_when_available(self, tmp_path: Path) -> None:
        output = tmp_path / 'docs'
        with (
            mock.patch('horsies.core.docs_fetcher._has_git', return_value=True),
            mock.patch(
                'horsies.core.docs_fetcher._fetch_via_sparse_checkout',
                return_value=10,
            ) as mock_git,
            mock.patch('horsies.core.docs_fetcher._print_status'),
        ):
            count = fetch_docs(output_dir=str(output))

        assert count == 10
        mock_git.assert_called_once_with(output)

    def test_falls_back_when_no_git(self, tmp_path: Path) -> None:
        output = tmp_path / 'docs'
        with (
            mock.patch('horsies.core.docs_fetcher._has_git', return_value=False),
            mock.patch(
                'horsies.core.docs_fetcher._fetch_via_tarball',
                return_value=5,
            ) as mock_tarball,
            mock.patch('horsies.core.docs_fetcher._print_status'),
        ):
            count = fetch_docs(output_dir=str(output))

        assert count == 5
        mock_tarball.assert_called_once_with(output)

    def test_falls_back_when_git_fails(self, tmp_path: Path) -> None:
        output = tmp_path / 'docs'
        with (
            mock.patch('horsies.core.docs_fetcher._has_git', return_value=True),
            mock.patch(
                'horsies.core.docs_fetcher._fetch_via_sparse_checkout',
                side_effect=DocsFetchError('clone failed'),
            ),
            mock.patch(
                'horsies.core.docs_fetcher._fetch_via_tarball',
                return_value=7,
            ) as mock_tarball,
            mock.patch('horsies.core.docs_fetcher._print_status'),
        ):
            count = fetch_docs(output_dir=str(output))

        assert count == 7
        mock_tarball.assert_called_once_with(output)

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        output = tmp_path / 'nested' / 'deep' / 'docs'
        with (
            mock.patch('horsies.core.docs_fetcher._has_git', return_value=False),
            mock.patch(
                'horsies.core.docs_fetcher._fetch_via_tarball',
                return_value=3,
            ),
            mock.patch('horsies.core.docs_fetcher._print_status'),
        ):
            fetch_docs(output_dir=str(output))

        assert (tmp_path / 'nested' / 'deep').is_dir()

    def test_default_output_dir(self) -> None:
        assert DEFAULT_OUTPUT_DIR == '.horsies-docs'

    def test_custom_output_dir(self, tmp_path: Path) -> None:
        custom = tmp_path / 'my-docs'
        with (
            mock.patch('horsies.core.docs_fetcher._has_git', return_value=False),
            mock.patch(
                'horsies.core.docs_fetcher._fetch_via_tarball',
                return_value=2,
            ) as mock_tarball,
            mock.patch('horsies.core.docs_fetcher._print_status'),
        ):
            fetch_docs(output_dir=str(custom))

        mock_tarball.assert_called_once_with(custom)

    def test_propagates_tarball_error_when_both_fail(self, tmp_path: Path) -> None:
        output = tmp_path / 'docs'
        with (
            mock.patch('horsies.core.docs_fetcher._has_git', return_value=True),
            mock.patch(
                'horsies.core.docs_fetcher._fetch_via_sparse_checkout',
                side_effect=DocsFetchError('git failed'),
            ),
            mock.patch(
                'horsies.core.docs_fetcher._fetch_via_tarball',
                side_effect=DocsFetchError('download failed'),
            ),
            mock.patch('horsies.core.docs_fetcher._print_status'),
        ):
            with pytest.raises(DocsFetchError, match='download failed'):
                fetch_docs(output_dir=str(output))
