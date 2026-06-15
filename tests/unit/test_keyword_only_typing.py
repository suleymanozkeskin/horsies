"""Call-site typing guarantee for keyword-only task params.

The point of keyword-only task params is that ``ParamSpec`` turns a positional
task call into a static type error. ``ParamSpec`` is erased at runtime, so the
guarantee can only be verified by a type checker — not by ``inspect``. This runs
pyright over a fixture and asserts a positional ``.send(...)`` errors while the
keyword form does not, guarding against a future change to the send/ParamSpec
typing silently regressing the call-site catch.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

# A keyword-only task plus two call sites. The keyword call must type-check; the
# positional call must be a call-site error (ParamSpec carries the keyword-only
# parameter kind through `.send`). The database_url ignore keeps the fixture's
# only relevant diagnostics on the two call lines.
_FIXTURE = """\
from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskError, TaskResult

app = Horsies(
    config=AppConfig(
        broker=PostgresConfig(database_url='postgresql+psycopg://u:p@localhost/db'),  # pyright: ignore[reportArgumentType]
    )
)


@app.task('charge')
def charge(*, user_id: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=user_id)


ok_call = charge.send(user_id=1)
bad_call = charge.send(1)
"""

_OK_LINE_SRC = 'ok_call = charge.send(user_id=1)'
_BAD_LINE_SRC = 'bad_call = charge.send(1)'


def _pyright_available() -> bool:
    """True if `python -m pyright` can run in this environment."""
    try:
        subprocess.run(
            [sys.executable, '-m', 'pyright', '--version'],
            capture_output=True,
            check=True,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def _line_of(marker: str) -> int:
    """1-based line number of ``marker`` within the fixture."""
    for index, line in enumerate(_FIXTURE.splitlines(), start=1):
        if line == marker:
            return index
    raise AssertionError(f'marker not found in fixture: {marker!r}')


@pytest.mark.unit
@pytest.mark.skipif(not _pyright_available(), reason='pyright not installed')
def test_positional_send_is_a_call_site_type_error(tmp_path: Path) -> None:
    """Positional `.send(...)` on a keyword-only task fails pyright; keyword is clean."""
    fixture = tmp_path / 'kwonly_fixture.py'
    fixture.write_text(_FIXTURE)

    proc = subprocess.run(
        [sys.executable, '-m', 'pyright', '--outputjson', str(fixture)],
        capture_output=True,
        text=True,
    )
    report = json.loads(proc.stdout)
    errors = [d for d in report['generalDiagnostics'] if d['severity'] == 'error']

    # If horsies can't be resolved here, the guarantee is unverifiable — skip
    # rather than report a false regression.
    if any(d.get('rule') == 'reportMissingImports' for d in errors):
        pytest.skip('pyright could not resolve horsies in this environment')

    error_lines = {d['range']['start']['line'] + 1 for d in errors}
    bad_line = _line_of(_BAD_LINE_SRC)
    ok_line = _line_of(_OK_LINE_SRC)

    assert bad_line in error_lines, (
        f'positional `{_BAD_LINE_SRC}` (line {bad_line}) must be a type error; '
        f'pyright errored on lines {sorted(error_lines)}'
    )
    assert ok_line not in error_lines, (
        f'keyword `{_OK_LINE_SRC}` (line {ok_line}) must type-check cleanly; '
        f'pyright errored on lines {sorted(error_lines)}'
    )
