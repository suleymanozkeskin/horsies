"""
Copy the horsies agent skill folder from the installed package into a project.

The skill folder ships inside the wheel at ``horsies/.agents/skills/horsies/``.
``install_skill`` copies it to ``<target>/horsies/``. It never overwrites a
copy that differs from the packaged files unless the caller sets
``overwrite_changed``.
"""

from __future__ import annotations

import shutil
import tempfile
import uuid
from dataclasses import dataclass, replace
from importlib import resources
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Final, assert_never

from horsies.core.types.result import Err, Ok, Result


SKILL_FOLDER_NAME: Final = 'horsies'
_PACKAGE_NAME: Final = 'horsies'
_PACKAGED_SKILL_PARTS: Final = ('.agents', 'skills', SKILL_FOLDER_NAME)
MAX_SKILL_DIRECTORY_DEPTH: Final = 4
MAX_SKILL_FILE_COUNT: Final = 64

SkillFiles = dict[str, bytes]


# ---------------------------------------------------------------------------
# Outcomes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SkillInstalled:
    """The skill folder was written: it did not exist, or it was overwritten."""

    destination: Path
    file_count: int


@dataclass(frozen=True, slots=True)
class SkillAlreadyCurrent:
    """The folder already holds exactly the packaged files. Nothing was written."""

    destination: Path
    file_count: int


InstallOutcome = SkillInstalled | SkillAlreadyCurrent


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PackagedSkillMissing:
    """The installed package has no usable skill folder. Nothing was written."""

    location: str
    reason: str


@dataclass(frozen=True, slots=True)
class TargetNotDirectory:
    """The target path, or the skill path inside it, exists and is not a directory.

    Nothing was written.
    """

    path: Path


@dataclass(frozen=True, slots=True)
class ChangedCopyExists:
    """The destination differs from the packaged files and ``--force`` is not set.

    ``differing_paths`` are relative paths: files that were edited, files that
    are missing, and files that the package does not have. Nothing was written.
    """

    destination: Path
    differing_paths: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DestinationUnchanged:
    """The destination is as it was before the call: the old copy, or no copy."""


@dataclass(frozen=True, slots=True)
class NewCopyInPlaceBackupLeft:
    """The new copy is in place. The old copy is still at ``backup``; remove it by hand."""

    backup: Path


@dataclass(frozen=True, slots=True)
class OldCopyAtBackup:
    """The destination has no copy. The old copy is at ``backup``; move it back by hand."""

    backup: Path


DestinationState = DestinationUnchanged | NewCopyInPlaceBackupLeft | OldCopyAtBackup


@dataclass(frozen=True, slots=True)
class StagingRemoved:
    """No staging folder is left beside the destination."""


@dataclass(frozen=True, slots=True)
class StagingLeftAt:
    """The staging folder could not be removed. Remove ``path`` by hand."""

    path: Path


StagingState = StagingRemoved | StagingLeftAt


@dataclass(frozen=True, slots=True)
class SkillIoFailed:
    """A filesystem call failed.

    ``destination_state`` says what is true at the destination.
    ``staging_state`` says whether the staging folder is gone.
    """

    path: Path
    operation: str
    cause: str
    destination_state: DestinationState
    staging_state: StagingState


@dataclass(frozen=True, slots=True)
class TreeBoundExceeded:
    """A folder has more files or deeper nesting than the skill bounds allow.

    ``path`` is the relative path where the walk stopped.
    """

    path: str
    reason: str


SkillInstallError = (
    PackagedSkillMissing | TargetNotDirectory | ChangedCopyExists | SkillIoFailed
)


def describe_install_error(error: SkillInstallError) -> str:
    """Return the message that the CLI prints for ``error``."""
    match error:
        case PackagedSkillMissing(location=location, reason=reason):
            return f'the installed package has no skill folder at {location}: {reason}'
        case TargetNotDirectory(path=path):
            return f'{path} exists and is not a directory'
        case ChangedCopyExists(destination=destination, differing_paths=paths):
            listed = ', '.join(paths)
            return (
                f'{destination} differs from the packaged skill ({listed}). '
                'Nothing was changed. Run again with --force to replace it.'
            )
        case SkillIoFailed(
            path=path,
            operation=operation,
            cause=cause,
            destination_state=state,
            staging_state=staging,
        ):
            return (
                f'{operation} failed for {path}: {cause}. {_describe_state(state)}'
                f'{_describe_staging(staging)}'
            )
        case _:
            assert_never(error)


def _describe_state(state: DestinationState) -> str:
    match state:
        case DestinationUnchanged():
            return 'The destination is unchanged.'
        case NewCopyInPlaceBackupLeft(backup=backup):
            return f'The new copy is in place. The old copy is left at {backup}. Remove it by hand.'
        case OldCopyAtBackup(backup=backup):
            return f'The destination has no copy. The old copy is at {backup}. Move it back by hand.'
        case _:
            assert_never(state)


def _describe_staging(state: StagingState) -> str:
    match state:
        case StagingRemoved():
            return ''
        case StagingLeftAt(path=path):
            return f' A staging folder is left at {path}. Remove it by hand.'
        case _:
            assert_never(state)


# ---------------------------------------------------------------------------
# Reading files
# ---------------------------------------------------------------------------


def _packaged_skill_root() -> Traversable:
    root = resources.files(_PACKAGE_NAME)
    for part in _PACKAGED_SKILL_PARTS:
        root = root.joinpath(part)
    return root


def _collect_files(
    directory: Traversable,
    prefix: str,
    depth: int,
    collected: SkillFiles,
) -> Result[None, TreeBoundExceeded]:
    """Add every file under ``directory`` to ``collected``, within the named bounds."""
    if depth > MAX_SKILL_DIRECTORY_DEPTH:
        return Err(
            TreeBoundExceeded(
                path=prefix,
                reason=f'directories nest deeper than {MAX_SKILL_DIRECTORY_DEPTH}',
            ),
        )
    for entry in sorted(directory.iterdir(), key=lambda item: item.name):
        relative = f'{prefix}{entry.name}'
        if entry.is_dir():
            match _collect_files(entry, f'{relative}/', depth + 1, collected):
                case Err(exceeded):
                    return Err(exceeded)
                case Ok():
                    continue
        if len(collected) >= MAX_SKILL_FILE_COUNT:
            return Err(
                TreeBoundExceeded(
                    path=relative,
                    reason=f'more than {MAX_SKILL_FILE_COUNT} files',
                ),
            )
        collected[relative] = entry.read_bytes()
    return Ok(None)


def read_packaged_skill() -> Result[SkillFiles, PackagedSkillMissing]:
    """Read every file of the packaged skill folder into memory.

    Keys are relative POSIX paths. Fails when the folder is absent, has no
    ``SKILL.md``, or exceeds the depth or file-count bound.
    """
    root = _packaged_skill_root()
    location = '/'.join(_PACKAGED_SKILL_PARTS)
    try:
        if not root.is_dir():
            return Err(PackagedSkillMissing(location=location, reason='folder not found'))
        files: SkillFiles = {}
        walked = _collect_files(root, '', 0, files)
    except OSError as exc:
        return Err(PackagedSkillMissing(location=location, reason=str(exc)))
    match walked:
        case Err(exceeded):
            return Err(PackagedSkillMissing(location=location, reason=exceeded.reason))
        case Ok():
            pass
    if 'SKILL.md' not in files:
        return Err(PackagedSkillMissing(location=location, reason='SKILL.md not found'))
    return Ok(files)


def _read_installed_files(destination: Path) -> Result[SkillFiles, TreeBoundExceeded]:
    """Read the installed copy with the bounds that the packaged read uses."""
    installed: SkillFiles = {}
    match _collect_files(destination, '', 0, installed):
        case Err(exceeded):
            return Err(exceeded)
        case Ok():
            return Ok(installed)


def differing_paths(packaged: SkillFiles, installed: SkillFiles) -> tuple[str, ...]:
    """Return the sorted relative paths where the two file sets differ."""
    names = set(packaged) | set(installed)
    return tuple(sorted(name for name in names if packaged.get(name) != installed.get(name)))


# ---------------------------------------------------------------------------
# Writing files
# ---------------------------------------------------------------------------


def _rename_path(source: Path, destination: Path) -> None:
    source.rename(destination)


def _remove_tree(path: Path) -> None:
    shutil.rmtree(path)


def _write_staging(staging: Path, packaged: SkillFiles) -> None:
    for relative, content in packaged.items():
        file_path = staging / relative
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(content)


def _failed(
    exc: OSError,
    fallback: Path,
    operation: str,
    state: DestinationState,
) -> Err[SkillIoFailed]:
    return Err(
        SkillIoFailed(
            path=_error_path(exc, fallback),
            operation=operation,
            cause=str(exc),
            destination_state=state,
            staging_state=StagingRemoved(),
        ),
    )


def _swap_in(
    staging: Path,
    destination: Path,
) -> Result[None, SkillIoFailed]:
    """Move ``staging`` to ``destination``. An old copy moves to a backup first.

    Order: old copy to backup, staging to destination, backup removed. Each
    failure reports what is true: the old copy kept, the old copy at the
    backup path, or the new copy in place with a backup left.
    """
    if not destination.exists():
        try:
            _rename_path(staging, destination)
        except OSError as exc:
            return _failed(exc, destination, 'install', DestinationUnchanged())
        return Ok(None)
    backup = destination.with_name(f'.horsies-skill-old-{uuid.uuid4().hex}')
    try:
        _rename_path(destination, backup)
    except OSError as exc:
        return _failed(exc, destination, 'back up the old copy', DestinationUnchanged())
    try:
        _rename_path(staging, destination)
    except OSError as exc:
        try:
            _rename_path(backup, destination)
        except OSError:
            return _failed(exc, destination, 'install', OldCopyAtBackup(backup=backup))
        return _failed(exc, destination, 'install', DestinationUnchanged())
    try:
        _remove_tree(backup)
    except OSError as exc:
        return _failed(
            exc, backup, 'remove the backup', NewCopyInPlaceBackupLeft(backup=backup)
        )
    return Ok(None)


def _write_swapped(destination: Path, packaged: SkillFiles) -> Result[None, SkillIoFailed]:
    """Write ``packaged`` beside ``destination``, then swap it into place.

    Raises:
        OSError: the staging folder could not be created. Nothing changed.
    """
    staging = Path(tempfile.mkdtemp(prefix='.horsies-skill-', dir=destination.parent))
    try:
        _write_staging(staging, packaged)
    except OSError as exc:
        return _remove_staging(
            staging, _failed(exc, staging, 'write the new copy', DestinationUnchanged())
        )
    match _swap_in(staging, destination):
        case Err(failure):
            return _remove_staging(staging, Err(failure))
        case Ok():
            return Ok(None)


def _remove_staging(staging: Path, failed: Err[SkillIoFailed]) -> Err[SkillIoFailed]:
    """Remove ``staging`` after a failure. A failed removal is named in the error."""
    try:
        _remove_tree(staging)
    except OSError:
        return Err(replace(failed.err_value, staging_state=StagingLeftAt(path=staging)))
    return failed


def install_skill(
    *,
    target: Path,
    overwrite_changed: bool,
) -> Result[InstallOutcome, SkillInstallError]:
    """Copy the packaged skill folder to ``<target>/horsies/``.

    Outcomes:
        ``SkillInstalled``: the destination was absent, or it differed and
            ``overwrite_changed`` is set.
        ``SkillAlreadyCurrent``: the destination holds exactly the packaged
            files. Nothing is written.

    Errors are listed in ``SkillInstallError``. Every error except
    ``SkillIoFailed`` leaves the filesystem unchanged.
    """
    match read_packaged_skill():
        case Err(error):
            return Err(error)
        case Ok(packaged):
            pass
    destination = target / SKILL_FOLDER_NAME
    try:
        return _install_checked(
            target=target,
            destination=destination,
            packaged=packaged,
            overwrite_changed=overwrite_changed,
        )
    except OSError as exc:
        return Err(
            SkillIoFailed(
                path=_error_path(exc, destination),
                operation='install',
                cause=str(exc),
                destination_state=DestinationUnchanged(),
                staging_state=StagingRemoved(),
            ),
        )


def _error_path(exc: OSError, fallback: Path) -> Path:
    """Return the path named by ``exc``, or ``fallback`` when it names none."""
    return Path(str(exc.filename)) if exc.filename is not None else fallback


def _install_checked(
    *,
    target: Path,
    destination: Path,
    packaged: SkillFiles,
    overwrite_changed: bool,
) -> Result[InstallOutcome, SkillInstallError]:
    if target.exists() and not target.is_dir():
        return Err(TargetNotDirectory(path=target))
    if destination.exists() and not destination.is_dir():
        return Err(TargetNotDirectory(path=destination))
    if destination.is_dir():
        match _read_installed_files(destination):
            case Ok(installed):
                differing = differing_paths(packaged, installed)
            case Err(exceeded):
                differing = (exceeded.path,)
        if not differing:
            return Ok(SkillAlreadyCurrent(destination=destination, file_count=len(packaged)))
        if not overwrite_changed:
            return Err(ChangedCopyExists(destination=destination, differing_paths=differing))
    target.mkdir(parents=True, exist_ok=True)
    match _write_swapped(destination, packaged):
        case Err(failure):
            return Err(failure)
        case Ok():
            pass
    return Ok(SkillInstalled(destination=destination, file_count=len(packaged)))
