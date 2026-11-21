"""The private CPython steps that a stopped child runs before ``os._exit``,
and the fallback on a Python without one of them."""

from __future__ import annotations

import atexit
import platform
import subprocess
import sys
import threading
from pathlib import Path
from typing import Final, assert_never

import pytest

from horsies.core.worker.child_main import (
    ExitStep,
    ExitStepMissing,
    ExitStepsFound,
    look_up_exit_steps,
)

pytestmark = [pytest.mark.unit]

STEPS: Final = list(ExitStep)
STEP_IDS: Final = [step.dotted_name for step in STEPS]
STOP_CODE: Final = 37
"""An exit code that no exit step gives by itself."""
PROGRAM_TIMEOUT_S: Final = 60.0

# A child at its stop, on a Python without one exit step. Arguments: the
# module and the name of the step, the mark directory, the exit code. The step
# is deleted before ``child_main`` is imported.
STOP_PROGRAM: Final = '''
import atexit
import importlib
import os
import sys

module_name, attribute, directory, code = sys.argv[1:]
delattr(importlib.import_module(module_name), attribute)

from horsies.core.logging import configure_logging
from horsies.core.worker.child_main import end_without_teardown

FLAGS = os.O_WRONLY | os.O_CREAT | os.O_TRUNC


def write_mark(path):
    os.close(os.open(path, FLAGS, 0o600))


class MarkOnDelete:
    """Writes its mark from __del__: only the interpreter teardown frees it.
    Keeps its own references to os, which the teardown may clear first."""

    def __init__(self, path):
        self._path = path
        self._open = os.open
        self._close = os.close
        self._flags = FLAGS

    def __del__(self):
        self._close(self._open(self._path, self._flags, 0o600))


configure_logging()
atexit.register(write_mark, os.path.join(directory, 'atexit'))
KEPT_UNTIL_EXIT = MarkOnDelete(os.path.join(directory, 'teardown'))
end_without_teardown(int(code))
'''


def test_this_python_has_both_exit_steps() -> None:
    """A stop on a Python without a step still works, through the slow
    fallback, so only this test tells that the names need an update."""
    match look_up_exit_steps():
        case ExitStepsFound() as found:
            assert found.join_non_daemon_threads is getattr(threading, '_shutdown')
            assert found.run_atexit_handlers is getattr(atexit, '_run_exitfuncs')
        case ExitStepMissing(step=step):
            pytest.fail(
                f'Python {platform.python_version()} has no {step.dotted_name}. '
                'Each child stop on this Python takes the slow fallback: '
                'sys.exit with the interpreter teardown. Find the function that '
                'sys.exit runs for this step in this CPython, and update '
                'ExitStep in clients/python/horsies/core/worker/child_main.py.',
            )
        case _ as unreachable:
            assert_never(unreachable)


@pytest.mark.parametrize('step', STEPS, ids=STEP_IDS)
def test_the_lookup_names_the_missing_step(
    step: ExitStep,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With ``step`` gone from its module, the lookup names it."""
    monkeypatch.delattr(step.module, step.attribute)

    assert look_up_exit_steps() == ExitStepMissing(step)


@pytest.mark.parametrize('step', STEPS, ids=STEP_IDS)
def test_a_stop_without_a_step_takes_the_full_interpreter_exit(
    step: ExitStep,
    tmp_path: Path,
) -> None:
    """On a Python without ``step``, the child imports, and its stop logs the
    missing name and ends through ``sys.exit``: the exit code stays, the
    ``atexit`` handler runs, and the teardown frees the object that the child
    kept."""
    result = subprocess.run(
        [
            sys.executable,
            '-c',
            STOP_PROGRAM,
            step.module.__name__,
            step.attribute,
            str(tmp_path),
            str(STOP_CODE),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=PROGRAM_TIMEOUT_S,
    )

    assert result.returncode == STOP_CODE, result.stderr
    assert (
        f'This Python has no {step.dotted_name}; '
        f'the child ends through sys.exit({STOP_CODE})'
    ) in result.stderr
    assert (tmp_path / 'atexit').exists(), result.stderr
    assert (tmp_path / 'teardown').exists(), result.stderr
