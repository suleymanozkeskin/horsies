"""Take the public API snapshot and the pyright view of the installed package.

Both tools run as child processes of the interpreter of this environment,
with ``-I`` and in a folder outside the checkout (``snapshot.py``,
``pyright_view.py``). The scope is the docs, the skill files and the
showcase of this checkout, plus the imported names of the baseline.

The baseline files are ``baseline.json`` (runtime snapshot) and
``baseline-pyright.json`` (pyright view). To approve an intended change of
the public API, write them from the installed build and commit them with the
change::

    uv run --no-sync python -m tests.public_api.runner

Exit codes of the update: 0 written; 2 a tool failed or a baseline file
cannot be written (the message is printed). When a tool fails, no baseline
file changes; when the second copy fails, the first file is already written.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Final, assert_never

from tests.public_api.compare import Err, JsonValue, Ok, Result

PUBLIC_API_FOLDER: Final = Path(__file__).resolve().parent
REPOSITORY_ROOT: Final = PUBLIC_API_FOLDER.parents[3]
SNAPSHOT_TOOL: Final = PUBLIC_API_FOLDER / 'snapshot.py'
PYRIGHT_VIEW_TOOL: Final = PUBLIC_API_FOLDER / 'pyright_view.py'
BASELINE_SNAPSHOT: Final = PUBLIC_API_FOLDER / 'baseline.json'
BASELINE_PYRIGHT_VIEW: Final = PUBLIC_API_FOLDER / 'baseline-pyright.json'
DOC_ROOTS: Final = (
    REPOSITORY_ROOT / 'website' / 'src' / 'content' / 'docs',
    REPOSITORY_ROOT / 'clients' / 'python' / 'horsies' / '.agents' / 'skills',
    REPOSITORY_ROOT / 'clients' / 'python' / 'examples',
)
# One run of a tool; pyright over the package takes seconds.
TOOL_TIMEOUT_SECONDS: Final = 900
SNAPSHOT_FILE: Final = 'snapshot.json'
PYRIGHT_VIEW_FILE: Final = 'pyright-view.json'

EXIT_WRITTEN: Final = 0
EXIT_FAILED: Final = 2


@dataclass(frozen=True, slots=True)
class PyrightMissing:
    """No ``pyright`` command on PATH."""


@dataclass(frozen=True, slots=True)
class ToolFailed:
    tool: str
    cause: str


@dataclass(frozen=True, slots=True)
class OutputUnreadable:
    path: Path
    cause: str


type RunFailure = PyrightMissing | ToolFailed | OutputUnreadable


@dataclass(frozen=True, slots=True)
class CurrentApi:
    """The snapshot and the pyright view of the installed build, and their files."""

    snapshot: JsonValue
    pyright_view: JsonValue
    snapshot_file: Path
    pyright_view_file: Path


def run_tool(tool: Path, arguments: list[str], folder: Path) -> Result[None, ToolFailed]:
    """Run one tool with ``python -I`` in ``folder``, at most ``TOOL_TIMEOUT_SECONDS``."""
    command = [sys.executable, '-I', str(tool), *arguments]
    try:
        completed = subprocess.run(
            command,
            cwd=folder,
            capture_output=True,
            text=True,
            timeout=TOOL_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return Err(ToolFailed(tool.name, f'{type(exc).__name__}: {exc}'))
    if completed.returncode != 0:
        return Err(ToolFailed(tool.name, f'exit code {completed.returncode}: {completed.stderr}'))
    return Ok(None)


def read_json(path: Path) -> Result[JsonValue, OutputUnreadable]:
    try:
        document: JsonValue = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return Err(OutputUnreadable(path, f'{type(exc).__name__}: {exc}'))
    return Ok(document)


def take_current_api(folder: Path) -> Result[CurrentApi, RunFailure]:
    """The snapshot and the pyright view of the installed build, written to ``folder``.

    ``folder`` must be outside the checkout, so the checkout cannot shadow
    the installed package.
    """
    pyright = shutil.which('pyright')
    if pyright is None:
        return Err(PyrightMissing())
    snapshot_file = folder / SNAPSHOT_FILE
    view_file = folder / PYRIGHT_VIEW_FILE
    docs = [argument for root in DOC_ROOTS for argument in ('--docs', str(root))]
    scope = ['--scope-from', str(BASELINE_SNAPSHOT), '--output', str(snapshot_file)]
    match run_tool(SNAPSHOT_TOOL, [*docs, *scope], folder):
        case Err(failed):
            return Err(failed)
        case Ok():
            pass
    view_arguments = ['--pyright', pyright, '--python', sys.executable]
    view_arguments += ['--snapshot', str(snapshot_file), '--output', str(view_file)]
    match run_tool(PYRIGHT_VIEW_TOOL, view_arguments, folder):
        case Err(failed):
            return Err(failed)
        case Ok():
            pass
    match read_json(snapshot_file), read_json(view_file):
        case Ok(snapshot), Ok(view):
            return Ok(CurrentApi(snapshot, view, snapshot_file, view_file))
        case Err(unreadable), _:
            return Err(unreadable)
        case _, Err(unreadable):
            return Err(unreadable)


def describe_failure(failure: RunFailure) -> str:
    match failure:
        case PyrightMissing():
            return 'no pyright command on PATH; run inside the dev environment (uv run --no-sync)'
        case ToolFailed(tool, cause):
            return f'{tool} failed: {cause}'
        case OutputUnreadable(path, cause):
            return f'the tool output {path} cannot be read: {cause}'
        case _:
            assert_never(failure)


def main() -> int:
    """Write the baseline files from the installed build."""
    with tempfile.TemporaryDirectory(prefix='horsies-public-api-') as folder:
        match take_current_api(Path(folder)):
            case Err(failure):
                print(describe_failure(failure), file=sys.stderr)
                return EXIT_FAILED
            case Ok(current):
                pass
        try:
            shutil.copyfile(current.snapshot_file, BASELINE_SNAPSHOT)
            shutil.copyfile(current.pyright_view_file, BASELINE_PYRIGHT_VIEW)
        except OSError as exc:
            print(f'the baseline files cannot be written: {type(exc).__name__}: {exc}', file=sys.stderr)
            return EXIT_FAILED
    print(f'wrote {BASELINE_SNAPSHOT} and {BASELINE_PYRIGHT_VIEW}')
    return EXIT_WRITTEN


if __name__ == '__main__':
    raise SystemExit(main())
