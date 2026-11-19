"""The drain worker of ``claim-finish`` finds its app from the repository root.

The ``horsies`` script puts its own folder first on ``sys.path``, not its
working directory, and the CLI adds the working directory only when that
folder holds a ``pyproject.toml``. The repository root holds none (the Python
project is ``clients/python/``), so the environment that the harness gives the
worker puts the root on the import path. The probe below runs with ``-P``: like
the script, it does not put its working directory on ``sys.path``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Final

import pytest

from tests.perf import worker_drain
from tests.perf.worker_process import WorkerStartFailed

pytestmark = [pytest.mark.unit]

FIND_SPEC_TIMEOUT_S: Final = 30
"""Seconds that the probe process gets to find the app module."""

_UNUSED_DSN: Final = 'postgresql://postgres@127.0.0.1:1/never_opened'


def test_the_drain_worker_finds_its_app_module_without_its_working_directory_on_the_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: dict[str, object] = {}

    def record_start(**arguments: object) -> WorkerStartFailed:
        recorded.update(arguments)
        return WorkerStartFailed(command=(), cause='recorded, not started')

    monkeypatch.setattr(worker_drain, 'start_worker', record_start)
    _ = worker_drain.start_drain_worker(_UNUSED_DSN)
    environment = recorded['environment']
    working_directory = recorded['working_directory']
    assert isinstance(environment, dict)
    assert isinstance(working_directory, Path)
    module = worker_drain.WORKER_APP_LOCATOR.partition(':')[0]
    probe = subprocess.run(
        [
            sys.executable,
            '-P',
            '-c',
            f'import importlib.util; print(importlib.util.find_spec({module!r}).origin)',
        ],
        cwd=working_directory,
        env={**os.environ, **{str(key): str(value) for key, value in environment.items()}},
        capture_output=True,
        text=True,
        timeout=FIND_SPEC_TIMEOUT_S,
        check=False,
    )
    assert probe.returncode == 0, probe.stderr
    expected = Path(worker_drain.__file__).resolve().parent / 'worker_drain_app.py'
    assert Path(probe.stdout.strip()).resolve() == expected
