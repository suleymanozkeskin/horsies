"""A worker process started with the public CLI, as an operator starts it.

``horsies worker <locator> --profile <name> --log-level <level>`` is the same
command in horsies 0.7.0 and in the current release. A telemetry level adds
``--metrics-listen HOST:PORT`` (``telemetry_levels.py``). The harness runs the
``horsies`` script of its own environment, with the working directory at the
root of the checkout: the CLI adds a working directory that holds
``pyproject.toml`` to ``sys.path``, so the worker imports the app module of
that checkout.

The worker writes its output to a file, never to a pipe: nothing reads a pipe
while the worker runs, and a full pipe blocks the worker. The worker starts in
a new session, so the harness can kill the worker and its children as one
process group when the worker does not stop.

This module does not import the library: it starts, watches and stops a
process.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sysconfig
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Final

WORKER_SCRIPT_NAME: Final = 'horsies'
"""The console script of the library (``[project.scripts]`` of both releases)."""

WORKER_LOG_PREFIX: Final = 'horsies-perf-worker-'

KILL_WAIT: Final = timedelta(seconds=10)
"""How long the harness waits for a process group that it killed to exit."""


@dataclass(frozen=True, slots=True)
class StartedWorker:
    """A worker process that runs. ``started_ns`` is the
    ``time.perf_counter_ns`` reading taken just before the process started.
    ``log_path`` holds the output of the worker and its children."""

    process: subprocess.Popen[bytes]
    started_ns: int
    log_path: Path


@dataclass(frozen=True, slots=True)
class WorkerStartFailed:
    """The process did not start: the script is missing or not executable,
    or the log file could not be made. ``cause`` names the OS error."""

    command: tuple[str, ...]
    cause: str


@dataclass(frozen=True, slots=True)
class WorkerRunning:
    """The worker process has not exited."""


@dataclass(frozen=True, slots=True)
class WorkerExited:
    """The worker process exited with ``exit_code``. A negative code is the
    number of the signal that ended it."""

    exit_code: int


type WorkerState = WorkerRunning | WorkerExited


@dataclass(frozen=True, slots=True)
class Drained:
    """Every task is terminal. ``elapsed_ns`` runs from the start of the
    worker to the start of the count that read every task terminal."""

    elapsed_ns: int


@dataclass(frozen=True, slots=True)
class DrainTimedOut:
    """``timeout`` passed after the start of the worker before every task was
    terminal. ``terminal`` is the result of the last count."""

    terminal: int
    expected: int
    timeout: timedelta


@dataclass(frozen=True, slots=True)
class WorkerExitedEarly:
    """The worker exited before every task was terminal."""

    exit_code: int
    terminal: int
    expected: int


type DrainOutcome = Drained | DrainTimedOut | WorkerExitedEarly


@dataclass(frozen=True, slots=True)
class WorkerStopped:
    """The worker exited with ``exit_code``, after SIGTERM or before it."""

    exit_code: int


@dataclass(frozen=True, slots=True)
class WorkerKilled:
    """The worker had not exited ``timeout`` after SIGTERM. The harness killed
    its process group with SIGKILL."""

    timeout: timedelta


type WorkerStop = WorkerStopped | WorkerKilled


def cli_script() -> Path:
    """The ``horsies`` script of the environment that runs the harness. Pure:
    it does not check that the file exists; ``start_worker`` reports that."""
    return Path(sysconfig.get_path('scripts')) / WORKER_SCRIPT_NAME


def start_worker(
    *,
    script: Path,
    app_locator: str,
    profile: str,
    log_level: str,
    extra_arguments: tuple[str, ...],
    environment: Mapping[str, str],
    working_directory: Path,
    clock_ns: Callable[[], int],
) -> StartedWorker | WorkerStartFailed:
    """Start ``script worker <app_locator> --profile <profile> --log-level
    <log_level> <extra_arguments>`` in a new session. Starts a process; writes
    a log file.

    The process gets the environment of the harness with ``environment``
    added. Its output goes to a new log file in the temporary directory; the
    caller deletes the file. It does not wait for the worker to be ready.
    """
    command = (
        str(script),
        'worker',
        app_locator,
        '--profile',
        profile,
        '--log-level',
        log_level,
        *extra_arguments,
    )
    try:
        log_descriptor, log_name = tempfile.mkstemp(
            prefix=WORKER_LOG_PREFIX,
            suffix='.log',
        )
    except OSError as error:
        return WorkerStartFailed(command=command, cause=_os_error(error))
    started_ns = clock_ns()
    try:
        process = subprocess.Popen(
            command,
            cwd=working_directory,
            env={**os.environ, **environment},
            stdin=subprocess.DEVNULL,
            stdout=log_descriptor,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as error:
        _remove_log(Path(log_name))
        return WorkerStartFailed(command=command, cause=_os_error(error))
    finally:
        os.close(log_descriptor)
    return StartedWorker(
        process=process,
        started_ns=started_ns,
        log_path=Path(log_name),
    )


def worker_state(process: subprocess.Popen[bytes]) -> WorkerState:
    """Whether ``process`` has exited. Reaps it when it has."""
    match process.poll():
        case None:
            return WorkerRunning()
        case int() as exit_code:
            return WorkerExited(exit_code=exit_code)


def wait_for_drain(
    *,
    count_terminal: Callable[[], int],
    state_of_worker: Callable[[], WorkerState],
    expected: int,
    started_ns: int,
    timeout: timedelta,
    interval: timedelta,
    clock_ns: Callable[[], int],
    sleep: Callable[[float], None],
) -> DrainOutcome:
    """Count the terminal tasks every ``interval`` until all ``expected`` are
    terminal, the worker exits, or ``timeout`` passes after ``started_ns``.

    It reads the clock before each count, so ``Drained.elapsed_ns`` is late
    by at most one interval and one count. It makes at most ``timeout //
    interval + 1`` counts, so it ends also with a clock that does not move.
    It does not stop the worker.

    Raises:
        AssertionError: a count read more than ``expected`` terminal tasks.
            The cleanup before a drain deletes the rows of the task, so more
            rows mean that another run sends the task to this database.
    """
    timeout_ns = timeout // timedelta(microseconds=1) * 1_000
    terminal = 0
    for _ in range(timeout // interval + 1):
        read_at_ns = clock_ns()
        if read_at_ns - started_ns > timeout_ns:
            break
        terminal = count_terminal()
        if terminal > expected:
            raise AssertionError(
                f'the drain counted {terminal} terminal tasks for {expected} '
                f'sent: another run sends the same task to this database'
            )
        if terminal == expected:
            return Drained(elapsed_ns=read_at_ns - started_ns)
        match state_of_worker():
            case WorkerExited(exit_code=exit_code):
                return WorkerExitedEarly(
                    exit_code=exit_code,
                    terminal=terminal,
                    expected=expected,
                )
            case WorkerRunning():
                sleep(interval.total_seconds())
    return DrainTimedOut(terminal=terminal, expected=expected, timeout=timeout)


def stop_worker(
    process: subprocess.Popen[bytes],
    timeout: timedelta,
) -> WorkerStop:
    """Send SIGTERM to the worker and wait ``timeout`` for it to exit; then
    kill its process group with SIGKILL. A worker that already exited gets
    no signal. The worker must be the leader of its process group
    (``start_worker`` starts it so).

    Raises:
        RuntimeError: the process group had not exited ``KILL_WAIT`` after
            SIGKILL. The harness cannot free the database or the host for the
            next drain, so the run stops.
    """
    match worker_state(process):
        case WorkerExited(exit_code=exit_code):
            return WorkerStopped(exit_code=exit_code)
        case WorkerRunning():
            process.send_signal(signal.SIGTERM)
    try:
        return WorkerStopped(exit_code=process.wait(timeout.total_seconds()))
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        _ = process.wait(KILL_WAIT.total_seconds())
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(
            f'worker process {process.pid} had not exited {KILL_WAIT} after '
            f'SIGKILL of its process group'
        ) from error
    return WorkerKilled(timeout=timeout)


def remove_log(started: StartedWorker) -> None:
    """Delete the log file of a worker that stopped clean."""
    _remove_log(started.log_path)


def _remove_log(path: Path) -> None:
    # Explicit discard: a log left in the temporary directory changes no
    # measurement, and the run already has its result.
    try:
        path.unlink()
    except OSError:
        pass


def _os_error(error: OSError) -> str:
    return f'{type(error).__name__}: {error}'
