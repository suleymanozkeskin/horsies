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
from dataclasses import dataclass
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
class SkillIoFailed:
    """A filesystem call failed. The destination can hold partial effects.

    A failed overwrite leaves the old copy in place when the failure happens
    before the swap, and can leave no copy when it happens during the swap.
    """

    path: Path
    operation: str
    cause: str


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
        case SkillIoFailed(path=path, operation=operation, cause=cause):
            return f'{operation} failed for {path}: {cause}'
        case _:
            assert_never(error)


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
) -> str | None:
    """Add every file under ``directory`` to ``collected``. Return a reason on failure."""
    if depth > MAX_SKILL_DIRECTORY_DEPTH:
        return f'directories nest deeper than {MAX_SKILL_DIRECTORY_DEPTH}'
    for entry in sorted(directory.iterdir(), key=lambda item: item.name):
        relative = f'{prefix}{entry.name}'
        if entry.is_dir():
            reason = _collect_files(entry, f'{relative}/', depth + 1, collected)
            if reason is not None:
                return reason
            continue
        if len(collected) >= MAX_SKILL_FILE_COUNT:
            return f'more than {MAX_SKILL_FILE_COUNT} files'
        collected[relative] = entry.read_bytes()
    return None


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
        reason = _collect_files(root, '', 0, files)
    except OSError as exc:
        return Err(PackagedSkillMissing(location=location, reason=str(exc)))
    if reason is not None:
        return Err(PackagedSkillMissing(location=location, reason=reason))
    if 'SKILL.md' not in files:
        return Err(PackagedSkillMissing(location=location, reason='SKILL.md not found'))
    return Ok(files)


def _read_installed_files(destination: Path) -> SkillFiles:
    installed: SkillFiles = {}
    for path in sorted(destination.rglob('*')):
        if path.is_file():
            installed[path.relative_to(destination).as_posix()] = path.read_bytes()
    return installed


def differing_paths(packaged: SkillFiles, installed: SkillFiles) -> tuple[str, ...]:
    """Return the sorted relative paths where the two file sets differ."""
    names = set(packaged) | set(installed)
    return tuple(sorted(name for name in names if packaged.get(name) != installed.get(name)))


# ---------------------------------------------------------------------------
# Writing files
# ---------------------------------------------------------------------------


def _write_swapped(destination: Path, packaged: SkillFiles) -> None:
    """Write ``packaged`` beside ``destination``, then swap it into place.

    Raises:
        OSError: a filesystem call failed.
    """
    staging = Path(tempfile.mkdtemp(prefix='.horsies-skill-', dir=destination.parent))
    try:
        for relative, content in packaged.items():
            file_path = staging / relative
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_bytes(content)
        if destination.exists():
            shutil.rmtree(destination)
        staging.rename(destination)
    except OSError:
        shutil.rmtree(staging, ignore_errors=True)
        raise


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
            SkillIoFailed(path=_error_path(exc, destination), operation='install', cause=str(exc)),
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
        differing = differing_paths(packaged, _read_installed_files(destination))
        if not differing:
            return Ok(SkillAlreadyCurrent(destination=destination, file_count=len(packaged)))
        if not overwrite_changed:
            return Err(ChangedCopyExists(destination=destination, differing_paths=differing))
    target.mkdir(parents=True, exist_ok=True)
    _write_swapped(destination, packaged)
    return Ok(SkillInstalled(destination=destination, file_count=len(packaged)))
