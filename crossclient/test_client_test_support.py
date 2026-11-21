"""An import of ``crossclient`` makes the Python client's package ``tests`` importable.

The probe runs in a new process with only the repository root on
``PYTHONPATH``, as the parity runner starts its send process, and with
``-P``, as the ``horsies`` script runs. It first removes ``clients/python/``
from ``sys.path``: an editable install of the client puts that folder there
already, and a wheel install does not, so the probe tests the package root
alone.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Final

import pytest

from crossclient import PYTHON_CLIENT_PROJECT

pytestmark = [pytest.mark.unit]

REPOSITORY_ROOT: Final = PYTHON_CLIENT_PROJECT.parents[1]
PROBE_TIMEOUT_S: Final = 60
"""Seconds that the probe process gets to import the two packages."""

_PROBE: Final = '''
import sys
from pathlib import Path

client_project = Path(sys.argv[1]).resolve()
sys.path[:] = [entry for entry in sys.path if Path(entry).resolve() != client_project]
import crossclient
import tests.sqlalchemy_url
print(Path(tests.sqlalchemy_url.__file__).resolve())
print(Path(sys.path[-1]).resolve() == client_project)
'''


def _probe() -> tuple[Path, bool]:
    completed = subprocess.run(
        [sys.executable, '-P', '-c', _PROBE, str(PYTHON_CLIENT_PROJECT)],
        cwd=REPOSITORY_ROOT,
        env={**os.environ, 'PYTHONPATH': str(REPOSITORY_ROOT)},
        capture_output=True,
        text=True,
        timeout=PROBE_TIMEOUT_S,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    module_file, client_project_is_last = completed.stdout.splitlines()
    return Path(module_file), client_project_is_last == 'True'


def test_the_package_tests_of_a_harness_process_is_the_python_client_tests() -> None:
    module_file, _ = _probe()

    assert module_file == PYTHON_CLIENT_PROJECT / 'tests' / 'sqlalchemy_url.py'


def test_the_client_project_goes_last_on_the_import_path() -> None:
    _, client_project_is_last = _probe()

    assert client_project_is_last
