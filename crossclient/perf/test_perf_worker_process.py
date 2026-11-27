"""The worker process of the ``claim-finish`` scenario: start, wait, stop.

The harness starts ``horsies worker`` with the CLI, counts the terminal tasks
until every sent task is terminal, and then stops the worker with SIGTERM. A
drain that does not end in its timeout, or a worker that exits early or does
not stop clean, must stop the run instead of giving a number. These tests
hold that logic with a fake clock and with plain processes; they need no
database and do not import the library.
"""

from __future__ import annotations

import os
import signal
import stat
import subprocess
import sys
import sysconfig
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path

import pytest

from crossclient.perf.worker_process import (
    WORKER_SCRIPT_NAME,
    Drained,
    DrainOutcome,
    DrainTimedOut,
    StartedWorker,
    WorkerExited,
    WorkerExitedEarly,
    WorkerKilled,
    WorkerRunning,
    WorkerStartFailed,
    WorkerState,
    WorkerStopped,
    cli_script,
    remove_log,
    start_worker,
    stop_worker,
    wait_for_drain,
    worker_state,
)

pytestmark = [
    pytest.mark.unit,
    pytest.mark.skipif(
        sys.platform == 'win32',
        reason='the harness starts the worker in a POSIX session',
    ),
]

_MS = 1_000_000
_INTERVAL = timedelta(milliseconds=100)
_TIMEOUT = timedelta(seconds=1)
_STARTED_NS = 7 * _MS


class _FakeClock:
    """A clock that moves only when the wait sleeps."""

    def __init__(self, now_ns: int) -> None:
        self.now_ns = now_ns
        self.sleeps: list[float] = []

    def read(self) -> int:
        return self.now_ns

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now_ns += round(seconds * 1_000_000_000)


class _StoppedClock(_FakeClock):
    """A clock that never moves."""

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)


class _Counts:
    """The results of the counts, in order; the last one repeats."""

    def __init__(self, *results: int) -> None:
        self._results = list(results)
        self.calls = 0

    def read(self) -> int:
        self.calls += 1
        return self._results[min(self.calls, len(self._results)) - 1]


def _wait(
    counts: _Counts,
    clock: _FakeClock,
    *,
    expected: int = 5,
    state: WorkerState = WorkerRunning(),
) -> DrainOutcome:
    return wait_for_drain(
        count_terminal=counts.read,
        state_of_worker=lambda: state,
        expected=expected,
        started_ns=_STARTED_NS,
        timeout=_TIMEOUT,
        interval=_INTERVAL,
        clock_ns=clock.read,
        sleep=clock.sleep,
    )


class TestWaitForDrain:
    def test_the_drain_ends_at_the_count_that_reads_every_task(self) -> None:
        """The time runs from the start of the worker to the start of the
        count that read every task terminal."""
        clock = _FakeClock(now_ns=_STARTED_NS + 5 * _MS)
        outcome = _wait(_Counts(0, 3, 5), clock)
        assert outcome == Drained(elapsed_ns=5 * _MS + 200 * _MS)
        assert clock.sleeps == [0.1, 0.1]

    def test_the_timeout_ends_the_wait_with_the_last_count(self) -> None:
        """A drain that does not end in its timeout gives no number."""
        counts = _Counts(2)
        outcome = _wait(counts, _FakeClock(now_ns=_STARTED_NS))
        assert outcome == DrainTimedOut(terminal=2, expected=5, timeout=_TIMEOUT)
        assert counts.calls == _TIMEOUT // _INTERVAL + 1

    def test_a_clock_past_the_timeout_ends_the_wait_before_a_count(self) -> None:
        counts = _Counts(5)
        clock = _FakeClock(now_ns=_STARTED_NS + 1_001 * _MS)
        outcome = _wait(counts, clock)
        assert outcome == DrainTimedOut(terminal=0, expected=5, timeout=_TIMEOUT)
        assert counts.calls == 0

    def test_a_clock_that_does_not_move_still_ends_the_wait(self) -> None:
        """The count bound ends the loop when the deadline cannot."""
        counts = _Counts(1)
        outcome = _wait(counts, _StoppedClock(now_ns=_STARTED_NS))
        assert outcome == DrainTimedOut(terminal=1, expected=5, timeout=_TIMEOUT)
        assert counts.calls == _TIMEOUT // _INTERVAL + 1

    def test_a_worker_that_exits_before_the_drain_ends_the_wait(self) -> None:
        outcome = _wait(
            _Counts(3),
            _FakeClock(now_ns=_STARTED_NS),
            state=WorkerExited(exit_code=1),
        )
        assert outcome == WorkerExitedEarly(exit_code=1, terminal=3, expected=5)

    def test_a_full_count_is_a_drain_whatever_the_worker_state(self) -> None:
        outcome = _wait(
            _Counts(5),
            _FakeClock(now_ns=_STARTED_NS),
            state=WorkerExited(exit_code=1),
        )
        assert outcome == Drained(elapsed_ns=0)

    def test_a_count_above_the_sent_tasks_is_a_defect(self) -> None:
        with pytest.raises(AssertionError, match='6 terminal tasks for 5 sent'):
            _ = _wait(_Counts(6), _FakeClock(now_ns=_STARTED_NS))


def _python(code: str) -> subprocess.Popen[bytes]:
    """A Python process in a new session that prints ``ready`` once its
    signal handler is set, as ``start_worker`` starts the worker."""
    process = subprocess.Popen(
        [sys.executable, '-c', code],
        stdout=subprocess.PIPE,
        start_new_session=True,
    )
    assert process.stdout is not None
    assert process.stdout.readline() == b'ready\n'
    return process


def _exits_on_sigterm(exit_code: int) -> str:
    return (
        'import signal, sys, time\n'
        f'signal.signal(signal.SIGTERM, lambda *_: sys.exit({exit_code}))\n'
        "print('ready', flush=True)\n"
        'time.sleep(30)\n'
    )


_IGNORES_SIGTERM = (
    'import signal, time\n'
    'signal.signal(signal.SIGTERM, signal.SIG_IGN)\n'
    "print('ready', flush=True)\n"
    'time.sleep(30)\n'
)


@pytest.fixture
def processes() -> Iterator[list[subprocess.Popen[bytes]]]:
    started: list[subprocess.Popen[bytes]] = []
    yield started
    for process in started:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            _ = process.wait(10)
        if process.stdout is not None:
            process.stdout.close()


class TestStopWorker:
    def test_a_worker_that_exits_0_on_sigterm_stops_clean(
        self,
        processes: list[subprocess.Popen[bytes]],
    ) -> None:
        processes.append(_python(_exits_on_sigterm(0)))
        assert stop_worker(processes[0], timedelta(seconds=10)) == WorkerStopped(
            exit_code=0,
        )

    def test_another_exit_code_after_sigterm_is_reported(
        self,
        processes: list[subprocess.Popen[bytes]],
    ) -> None:
        processes.append(_python(_exits_on_sigterm(3)))
        assert stop_worker(processes[0], timedelta(seconds=10)) == WorkerStopped(
            exit_code=3,
        )

    def test_a_worker_that_does_not_exit_after_sigterm_is_killed(
        self,
        processes: list[subprocess.Popen[bytes]],
    ) -> None:
        processes.append(_python(_IGNORES_SIGTERM))
        timeout = timedelta(milliseconds=300)
        assert stop_worker(processes[0], timeout) == WorkerKilled(timeout=timeout)
        assert processes[0].returncode == -signal.SIGKILL

    def test_a_worker_that_already_exited_gets_no_signal(
        self,
        processes: list[subprocess.Popen[bytes]],
    ) -> None:
        processes.append(_python("print('ready', flush=True)\nraise SystemExit(5)\n"))
        _ = processes[0].wait(10)
        assert stop_worker(processes[0], timedelta(seconds=10)) == WorkerStopped(
            exit_code=5,
        )


class TestWorkerState:
    def test_a_running_process_is_running(
        self,
        processes: list[subprocess.Popen[bytes]],
    ) -> None:
        processes.append(_python(_exits_on_sigterm(0)))
        assert worker_state(processes[0]) == WorkerRunning()

    def test_an_exited_process_gives_its_exit_code(
        self,
        processes: list[subprocess.Popen[bytes]],
    ) -> None:
        processes.append(_python("print('ready', flush=True)\nraise SystemExit(4)\n"))
        _ = processes[0].wait(10)
        assert worker_state(processes[0]) == WorkerExited(exit_code=4)


def _echo_script(directory: Path) -> Path:
    """A stand-in for the ``horsies`` script: it prints its arguments, one
    variable of its environment and its working directory."""
    script = directory / WORKER_SCRIPT_NAME
    script.write_text(
        '#!/bin/sh\necho "$@"\necho "$HORSIES_PERF_PROBE"\npwd -P\n',
        encoding='utf-8',
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return script


class TestStartWorker:
    def test_the_worker_runs_the_cli_command_in_the_checkout(
        self,
        tmp_path: Path,
    ) -> None:
        """``horsies worker <locator> --profile <name> --log-level <level>``
        and the extra arguments, with the added variables, in the working
        directory, to the log."""
        working_directory = tmp_path / 'checkout'
        working_directory.mkdir()
        started = start_worker(
            script=_echo_script(tmp_path),
            app_locator='crossclient.perf.worker_drain_app:app',
            profile='perf-drain',
            log_level='WARNING',
            extra_arguments=('--metrics-listen=127.0.0.1:19464',),
            environment={'HORSIES_PERF_PROBE': 'probe-value'},
            working_directory=working_directory,
            clock_ns=lambda: 42,
        )
        assert isinstance(started, StartedWorker)
        try:
            assert started.process.wait(10) == 0
            assert started.started_ns == 42
            assert started.log_path.read_text(encoding='utf-8').splitlines() == [
                'worker crossclient.perf.worker_drain_app:app --profile perf-drain '
                '--log-level WARNING --metrics-listen=127.0.0.1:19464',
                'probe-value',
                str(working_directory.resolve()),
            ]
        finally:
            remove_log(started)
        assert not started.log_path.exists()

    def test_a_missing_script_is_a_start_failure(self, tmp_path: Path) -> None:
        missing = tmp_path / WORKER_SCRIPT_NAME
        started = start_worker(
            script=missing,
            app_locator='crossclient.perf.worker_drain_app:app',
            profile='perf-drain',
            log_level='WARNING',
            extra_arguments=(),
            environment={},
            working_directory=tmp_path,
            clock_ns=lambda: 0,
        )
        assert isinstance(started, WorkerStartFailed)
        assert started.command[0] == str(missing)
        assert 'FileNotFoundError' in started.cause

    def test_the_script_is_the_one_of_the_running_environment(self) -> None:
        """The worker runs the library of the checkout that runs the harness."""
        assert cli_script() == Path(sysconfig.get_path('scripts')) / 'horsies'
