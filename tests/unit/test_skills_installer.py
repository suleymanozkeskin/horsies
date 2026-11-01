# pyright: reportPrivateUsage=false
# These tests patch module-private seams (the packaged-folder root and the writer).
"""Unit tests for the skills installer and the ``horsies skills install`` command.

Each outcome of ``install_skill`` has one test. The command tests check the
exit code and the message of each outcome. The package tests check that the
skill folder is shaped as the installer and the wheel expect.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

import pytest

from horsies.core import cli, skills_installer
from horsies.core.skills_installer import (
    MAX_SKILL_FILE_COUNT,
    ChangedCopyExists,
    DestinationUnchanged,
    NewCopyInPlaceBackupLeft,
    OldCopyAtBackup,
    PackagedSkillMissing,
    SkillAlreadyCurrent,
    SkillInstalled,
    SkillIoFailed,
    TargetNotDirectory,
    describe_install_error,
    differing_paths,
    install_skill,
    read_packaged_skill,
)

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SKILL_DIR = _REPO_ROOT / 'horsies' / '.agents' / 'skills' / 'horsies'
_REFERENCE_FILES = ('tasks.md', 'workflows.md', 'configs.md', 'monitoring.md')


def _packaged() -> dict[str, bytes]:
    return read_packaged_skill().unwrap()


# ---------------------------------------------------------------------------
# read_packaged_skill
# ---------------------------------------------------------------------------


class TestReadPackagedSkill:
    def test_reads_every_file_of_the_skill_folder(self) -> None:
        files = _packaged()

        assert set(files) == {'SKILL.md', *_REFERENCE_FILES}

    def test_missing_folder_is_reported(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(skills_installer, '_packaged_skill_root', lambda: tmp_path / 'absent')

        error = read_packaged_skill().unwrap_err()

        assert error.reason == 'folder not found'

    def test_folder_without_skill_file_is_reported(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / 'tasks.md').write_text('x')
        monkeypatch.setattr(skills_installer, '_packaged_skill_root', lambda: tmp_path)

        error = read_packaged_skill().unwrap_err()

        assert error.reason == 'SKILL.md not found'

    def test_file_count_above_the_bound_is_reported(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / 'SKILL.md').write_text('x')
        for index in range(MAX_SKILL_FILE_COUNT):
            (tmp_path / f'extra-{index}.md').write_text('x')
        monkeypatch.setattr(skills_installer, '_packaged_skill_root', lambda: tmp_path)

        error = read_packaged_skill().unwrap_err()

        assert 'more than' in error.reason

    def test_nesting_above_the_bound_is_reported(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        deep = tmp_path.joinpath(*('d',) * (skills_installer.MAX_SKILL_DIRECTORY_DEPTH + 2))
        deep.mkdir(parents=True)
        (tmp_path / 'SKILL.md').write_text('x')
        monkeypatch.setattr(skills_installer, '_packaged_skill_root', lambda: tmp_path)

        error = read_packaged_skill().unwrap_err()

        assert 'nest deeper' in error.reason


# ---------------------------------------------------------------------------
# install_skill
# ---------------------------------------------------------------------------


class TestInstallSkill:
    def test_installs_into_a_missing_target(self, tmp_path: Path) -> None:
        target = tmp_path / 'nested' / 'skills'

        outcome = install_skill(target=target, overwrite_changed=False).unwrap()

        packaged = _packaged()
        assert outcome == SkillInstalled(destination=target / 'horsies', file_count=len(packaged))
        for name, content in packaged.items():
            assert (target / 'horsies' / name).read_bytes() == content

    def test_identical_copy_is_left_alone(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        install_skill(target=tmp_path, overwrite_changed=False).unwrap()

        def fail_if_called(*args: object) -> None:
            raise AssertionError('an identical copy must not be written again')

        monkeypatch.setattr(skills_installer, '_write_swapped', fail_if_called)
        outcome = install_skill(target=tmp_path, overwrite_changed=False).unwrap()

        assert outcome == SkillAlreadyCurrent(
            destination=tmp_path / 'horsies', file_count=len(_packaged())
        )

    def test_edited_file_is_refused_without_force(self, tmp_path: Path) -> None:
        install_skill(target=tmp_path, overwrite_changed=False).unwrap()
        edited = tmp_path / 'horsies' / 'tasks.md'
        edited.write_text('my notes')

        error = install_skill(target=tmp_path, overwrite_changed=False).unwrap_err()

        assert error == ChangedCopyExists(
            destination=tmp_path / 'horsies', differing_paths=('tasks.md',)
        )
        assert edited.read_text() == 'my notes'

    def test_missing_and_extra_files_are_refused_without_force(self, tmp_path: Path) -> None:
        install_skill(target=tmp_path, overwrite_changed=False).unwrap()
        (tmp_path / 'horsies' / 'configs.md').unlink()
        (tmp_path / 'horsies' / 'own.md').write_text('mine')

        error = install_skill(target=tmp_path, overwrite_changed=False).unwrap_err()

        assert error == ChangedCopyExists(
            destination=tmp_path / 'horsies', differing_paths=('configs.md', 'own.md')
        )
        assert (tmp_path / 'horsies' / 'own.md').read_text() == 'mine'

    def test_force_replaces_a_changed_copy(self, tmp_path: Path) -> None:
        install_skill(target=tmp_path, overwrite_changed=False).unwrap()
        (tmp_path / 'horsies' / 'tasks.md').write_text('my notes')
        (tmp_path / 'horsies' / 'own.md').write_text('mine')

        outcome = install_skill(target=tmp_path, overwrite_changed=True).unwrap()

        assert isinstance(outcome, SkillInstalled)
        assert not (tmp_path / 'horsies' / 'own.md').exists()
        assert (tmp_path / 'horsies' / 'tasks.md').read_bytes() == _packaged()['tasks.md']
        assert [p.name for p in tmp_path.iterdir()] == ['horsies']

    def test_target_that_is_a_file_is_refused(self, tmp_path: Path) -> None:
        target = tmp_path / 'skills'
        target.write_text('not a directory')

        error = install_skill(target=target, overwrite_changed=True).unwrap_err()

        assert error == TargetNotDirectory(path=target)

    def test_destination_that_is_a_file_is_refused(self, tmp_path: Path) -> None:
        (tmp_path / 'horsies').write_text('not a directory')

        error = install_skill(target=tmp_path, overwrite_changed=True).unwrap_err()

        assert error == TargetNotDirectory(path=tmp_path / 'horsies')

    def test_missing_packaged_folder_is_returned_unchanged(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(skills_installer, '_packaged_skill_root', lambda: tmp_path / 'absent')

        error = install_skill(target=tmp_path / 'out', overwrite_changed=False).unwrap_err()

        assert isinstance(error, PackagedSkillMissing)
        assert not (tmp_path / 'out').exists()

    def test_filesystem_failure_is_returned_with_its_cause(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fail(*args: object) -> None:
            raise PermissionError(13, 'Permission denied', str(tmp_path / 'horsies'))

        monkeypatch.setattr(skills_installer, '_write_swapped', fail)

        error = install_skill(target=tmp_path, overwrite_changed=False).unwrap_err()

        assert isinstance(error, SkillIoFailed)
        assert error.path == tmp_path / 'horsies'
        assert 'Permission denied' in error.cause


def _populate_changed_copy(tmp_path: Path) -> Path:
    install_skill(target=tmp_path, overwrite_changed=False).unwrap()
    edited = tmp_path / 'horsies' / 'tasks.md'
    edited.write_text('old copy')
    return edited


def _fail_nth_rename(monkeypatch: pytest.MonkeyPatch, failing: set[int]) -> list[tuple[Path, Path]]:
    real = skills_installer._rename_path
    calls: list[tuple[Path, Path]] = []

    def rename(source: Path, destination: Path) -> None:
        calls.append((source, destination))
        if len(calls) in failing:
            raise OSError(5, 'injected rename failure', str(source))
        real(source, destination)

    monkeypatch.setattr(skills_installer, '_rename_path', rename)
    return calls


def _leftovers(tmp_path: Path) -> list[str]:
    return sorted(p.name for p in tmp_path.iterdir() if p.name != 'horsies')


class TestSwapFailures:
    def test_backup_rename_failure_keeps_the_old_copy(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        edited = _populate_changed_copy(tmp_path)
        _fail_nth_rename(monkeypatch, {1})

        error = install_skill(target=tmp_path, overwrite_changed=True).unwrap_err()

        assert isinstance(error, SkillIoFailed)
        assert error.destination_state == DestinationUnchanged()
        assert edited.read_text() == 'old copy'
        assert _leftovers(tmp_path) == []

    def test_install_rename_failure_restores_the_old_copy(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        edited = _populate_changed_copy(tmp_path)
        _fail_nth_rename(monkeypatch, {2})

        error = install_skill(target=tmp_path, overwrite_changed=True).unwrap_err()

        assert isinstance(error, SkillIoFailed)
        assert error.destination_state == DestinationUnchanged()
        assert edited.read_text() == 'old copy'
        assert _leftovers(tmp_path) == []

    def test_failed_restore_reports_the_backup_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _populate_changed_copy(tmp_path)
        calls = _fail_nth_rename(monkeypatch, {2, 3})

        error = install_skill(target=tmp_path, overwrite_changed=True).unwrap_err()

        assert isinstance(error, SkillIoFailed)
        backup = calls[0][1]
        assert error.destination_state == OldCopyAtBackup(backup=backup)
        assert (backup / 'tasks.md').read_text() == 'old copy'
        assert not (tmp_path / 'horsies').exists()

    def test_failed_backup_removal_reports_the_backup_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _populate_changed_copy(tmp_path)
        calls = _fail_nth_rename(monkeypatch, set())

        def remove(path: Path) -> None:
            raise OSError(16, 'injected removal failure', str(path))

        monkeypatch.setattr(skills_installer, '_remove_tree', remove)

        error = install_skill(target=tmp_path, overwrite_changed=True).unwrap_err()

        assert isinstance(error, SkillIoFailed)
        assert error.destination_state == NewCopyInPlaceBackupLeft(backup=calls[0][1])
        assert (tmp_path / 'horsies' / 'tasks.md').read_bytes() == _packaged()['tasks.md']
        assert (calls[0][1] / 'tasks.md').read_text() == 'old copy'

    def test_first_install_rename_failure_leaves_no_copy_and_no_staging(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _fail_nth_rename(monkeypatch, {1})

        error = install_skill(target=tmp_path, overwrite_changed=False).unwrap_err()

        assert isinstance(error, SkillIoFailed)
        assert error.destination_state == DestinationUnchanged()
        assert list(tmp_path.iterdir()) == []

    def test_staging_write_failure_keeps_the_old_copy(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        edited = _populate_changed_copy(tmp_path)

        def write(staging: Path, packaged: dict[str, bytes]) -> None:
            raise OSError(28, 'injected write failure', str(staging))

        monkeypatch.setattr(skills_installer, '_write_staging', write)

        error = install_skill(target=tmp_path, overwrite_changed=True).unwrap_err()

        assert isinstance(error, SkillIoFailed)
        assert error.destination_state == DestinationUnchanged()
        assert edited.read_text() == 'old copy'
        assert _leftovers(tmp_path) == []


class TestInstalledCopyBounds:
    def test_too_many_installed_files_count_as_a_changed_copy(self, tmp_path: Path) -> None:
        install_skill(target=tmp_path, overwrite_changed=False).unwrap()
        for index in range(MAX_SKILL_FILE_COUNT):
            (tmp_path / 'horsies' / f'extra-{index}.md').write_text('x')

        error = install_skill(target=tmp_path, overwrite_changed=False).unwrap_err()

        assert isinstance(error, ChangedCopyExists)
        assert len(error.differing_paths) == 1

    def test_deep_installed_nesting_counts_as_a_changed_copy(self, tmp_path: Path) -> None:
        install_skill(target=tmp_path, overwrite_changed=False).unwrap()
        deep = tmp_path.joinpath(
            'horsies', *('d',) * (skills_installer.MAX_SKILL_DIRECTORY_DEPTH + 2)
        )
        deep.mkdir(parents=True)

        error = install_skill(target=tmp_path, overwrite_changed=False).unwrap_err()

        assert isinstance(error, ChangedCopyExists)

    def test_force_replaces_a_copy_that_exceeds_the_bounds(self, tmp_path: Path) -> None:
        install_skill(target=tmp_path, overwrite_changed=False).unwrap()
        for index in range(MAX_SKILL_FILE_COUNT):
            (tmp_path / 'horsies' / f'extra-{index}.md').write_text('x')

        outcome = install_skill(target=tmp_path, overwrite_changed=True).unwrap()

        assert isinstance(outcome, SkillInstalled)
        assert sorted(p.name for p in (tmp_path / 'horsies').iterdir()) == sorted(_packaged())


class TestDifferingPaths:
    def test_equal_sets_have_no_difference(self) -> None:
        assert differing_paths({'a': b'1'}, {'a': b'1'}) == ()

    def test_changed_missing_and_extra_are_listed_sorted(self) -> None:
        packaged = {'a': b'1', 'b': b'2', 'c': b'3'}
        installed = {'a': b'1', 'b': b'x', 'z': b'9'}

        assert differing_paths(packaged, installed) == ('b', 'c', 'z')


class TestDescribeInstallError:
    def test_every_error_names_its_subject(self, tmp_path: Path) -> None:
        messages = [
            describe_install_error(PackagedSkillMissing(location='loc', reason='gone')),
            describe_install_error(TargetNotDirectory(path=tmp_path)),
            describe_install_error(
                ChangedCopyExists(destination=tmp_path, differing_paths=('tasks.md',))
            ),
            describe_install_error(
                SkillIoFailed(
                    path=tmp_path,
                    operation='install',
                    cause='boom',
                    destination_state=OldCopyAtBackup(backup=tmp_path / 'old'),
                )
            ),
        ]

        assert 'loc' in messages[0] and 'gone' in messages[0]
        assert str(tmp_path) in messages[1]
        assert 'tasks.md' in messages[2] and '--force' in messages[2]
        assert 'boom' in messages[3] and str(tmp_path) in messages[3]
        assert str(tmp_path / 'old') in messages[3]


# ---------------------------------------------------------------------------
# horsies skills install (command)
# ---------------------------------------------------------------------------


def _run_cli(monkeypatch: pytest.MonkeyPatch, *argv: str) -> None:
    monkeypatch.setattr(sys, 'argv', ['horsies', *argv])
    cli.main()


class TestSkillsInstallCommand:
    def test_installs_and_exits_zero(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _run_cli(monkeypatch, 'skills', 'install', '--target', str(tmp_path))

        assert (tmp_path / 'horsies' / 'SKILL.md').is_file()
        assert f'installed {len(_packaged())} files' in capsys.readouterr().out

    def test_current_copy_exits_zero(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _run_cli(monkeypatch, 'skills', 'install', '--target', str(tmp_path))
        capsys.readouterr()

        _run_cli(monkeypatch, 'skills', 'install', '--target', str(tmp_path))

        assert 'already current' in capsys.readouterr().out

    def test_changed_copy_exits_one_without_force(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _run_cli(monkeypatch, 'skills', 'install', '--target', str(tmp_path))
        (tmp_path / 'horsies' / 'SKILL.md').write_text('mine')
        capsys.readouterr()

        with pytest.raises(SystemExit) as exit_info:
            _run_cli(monkeypatch, 'skills', 'install', '--target', str(tmp_path))

        assert exit_info.value.code == 1
        assert '--force' in capsys.readouterr().err
        assert (tmp_path / 'horsies' / 'SKILL.md').read_text() == 'mine'

    def test_force_flag_replaces_the_changed_copy(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _run_cli(monkeypatch, 'skills', 'install', '--target', str(tmp_path))
        (tmp_path / 'horsies' / 'SKILL.md').write_text('mine')

        _run_cli(monkeypatch, 'skills', 'install', '--target', str(tmp_path), '--force')

        assert (tmp_path / 'horsies' / 'SKILL.md').read_bytes() == _packaged()['SKILL.md']

    def test_target_that_is_a_file_exits_one(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        target = tmp_path / 'file'
        target.write_text('x')

        with pytest.raises(SystemExit) as exit_info:
            _run_cli(monkeypatch, 'skills', 'install', '--target', str(target))

        assert exit_info.value.code == 1
        assert 'not a directory' in capsys.readouterr().err

    def test_missing_target_flag_exits_two(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(SystemExit) as exit_info:
            _run_cli(monkeypatch, 'skills', 'install')

        assert exit_info.value.code == 2
        assert '--target' in capsys.readouterr().err


# ---------------------------------------------------------------------------
# The skill folder and the package data
# ---------------------------------------------------------------------------


class TestSkillFolderShape:
    def test_skill_file_has_the_frontmatter_the_agent_tools_read(self) -> None:
        text = (_SKILL_DIR / 'SKILL.md').read_text()

        assert text.startswith('---\nname: horsies\ndescription: ')
        header = text.split('\n---\n', 1)[0]
        for signal in ('import horsies', '@app.task', 'TaskResult', 'WorkflowDefinition'):
            assert signal in header

    @pytest.mark.parametrize('name', _REFERENCE_FILES)
    def test_reference_file_has_no_frontmatter(self, name: str) -> None:
        assert not (_SKILL_DIR / name).read_text().startswith('---')

    def test_the_old_flat_skill_files_are_gone(self) -> None:
        flat = [p.name for p in _SKILL_DIR.parent.glob('*.md')]

        assert flat == []


def _matches_package_glob(relative: str, pattern: str) -> bool:
    """Match like setuptools: ``*`` stays inside one path part, ``**`` crosses parts."""
    regex = ''.join(
        '.*' if token == '**' else '[^/]*' if token == '*' else re.escape(token)
        for token in re.split(r'(\*\*|\*)', pattern)
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

        assert uncovered == []
