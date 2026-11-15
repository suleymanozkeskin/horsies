"""Worker process lifecycle helpers for e2e tests."""

from __future__ import annotations

import os
import signal
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Callable, Final, Generator, Sequence

import psutil


ReadyCheck = Callable[[], bool]

REPO_ROOT: Final = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
"""The root of the checkout that these helpers belong to."""

WORKER_OWNER_ENV: Final = 'HORSIES_E2E_WORKER_OWNER'
"""Set on each worker that these helpers start, to ``REPO_ROOT``. The cleanup
of stale workers kills only workers that carry this checkout's value, so the
e2e runs of another checkout on the same machine keep their workers."""

STALE_WORKER_TERM_WAIT_S: Final = 3
"""Seconds that stale workers get to stop after SIGTERM, before SIGKILL."""


def worker_env() -> dict[str, str]:
    """The environment of a worker that these helpers start: the current
    environment, the repo root on ``PYTHONPATH`` so absolute imports work,
    and the owner marker of this checkout."""
    env = os.environ.copy()
    env['PYTHONPATH'] = REPO_ROOT
    env[WORKER_OWNER_ENV] = REPO_ROOT
    return env


def _is_worker_command(cmdline: Sequence[str]) -> bool:
    return 'horsies worker' in ' '.join(cmdline)


def own_stale_worker_pids(owner: str = REPO_ROOT) -> list[int]:
    """The pids of ``horsies worker`` processes whose environment holds
    ``WORKER_OWNER_ENV`` equal to ``owner``.

    A process whose environment cannot be read (another user's, or one that
    exited) is not this checkout's worker and is left alone.
    """
    pids: list[int] = []
    for process in psutil.process_iter():
        try:
            cmdline = process.cmdline()
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
        if not _is_worker_command(cmdline):
            continue
        try:
            environment = process.environ()
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
        if environment.get(WORKER_OWNER_ENV) == owner:
            pids.append(process.pid)
    return pids


def _signal_pids(pids: Sequence[int], signal_number: int) -> None:
    for pid in pids:
        try:
            os.kill(pid, signal_number)
        except (ProcessLookupError, PermissionError):
            continue


# Number of active run_worker/run_workers contexts created by this module.
# Prevents stale-worker cleanup from killing intentionally running workers in
# nested contexts (e.g., tests that start worker A then worker B).
_ACTIVE_WORKER_CONTEXTS = 0


# Worker output goes to a FILE, never a pipe. Nothing reads a worker's
# pipes between readiness and teardown, so a worker that logs enough to
# fill the pipe buffer blocks on write — the process under test wedged
# by the harness observing it, and the explanation trapped in the buffer
# nobody drains. A file has no such bound, and the tail is printed when
# a worker exits badly so pytest surfaces it on the failing test.
_WORKER_LOGS: dict[int, str] = {}
_LOG_TAIL_BYTES = 8_192


def _worker_log() -> tuple[int, str]:
    handle, path = tempfile.mkstemp(prefix='horsies-e2e-worker-', suffix='.log')
    return handle, path


def worker_log_tail(proc: subprocess.Popen[str]) -> str:
    """The tail of one worker's log, or '' if it has none."""
    path = _WORKER_LOGS.get(proc.pid)
    if path is None:
        return ''
    try:
        with open(path, 'rb') as log:
            log.seek(0, os.SEEK_END)
            log.seek(max(0, log.tell() - _LOG_TAIL_BYTES))
            return log.read().decode('utf-8', 'replace')
    except OSError:
        return ''


def _report_worker_log(proc: subprocess.Popen[str]) -> None:
    """Print the tail so a failing test shows why its worker misbehaved."""
    tail = worker_log_tail(proc)
    if tail.strip():
        print(f'--- worker {proc.pid} log tail ---\n{tail}')
    path = _WORKER_LOGS.pop(proc.pid, None)
    if path is not None:
        try:
            os.unlink(path)
        except OSError:
            pass


def kill_stale_workers() -> None:
    """Kill leftover horsies workers that this checkout's helpers started.

    Workers started with start_new_session=True survive parent death and share
    queues, so a stale worker from a different app instance would pick up tasks
    it cannot resolve (WORKER_RESOLUTION_ERROR).
    Called automatically by run_worker / run_workers before starting new ones.

    Only workers that carry this checkout's ``WORKER_OWNER_ENV`` are touched;
    a worker of another checkout or another program keeps running.

    Strategy: graceful SIGTERM first (lets workers close DB connections),
    then SIGKILL after ``STALE_WORKER_TERM_WAIT_S`` if needed.
    """
    stale = own_stale_worker_pids()
    if not stale:
        return
    _signal_pids(stale, signal.SIGTERM)
    for _ in range(STALE_WORKER_TERM_WAIT_S):
        time.sleep(1)
        if not own_stale_worker_pids():
            return
    _signal_pids(own_stale_worker_pids(), signal.SIGKILL)
    time.sleep(1)


def _enter_worker_context() -> None:
    """Enter a managed worker context and reap stale workers if outermost."""
    global _ACTIVE_WORKER_CONTEXTS
    if _ACTIVE_WORKER_CONTEXTS == 0:
        kill_stale_workers()
    _ACTIVE_WORKER_CONTEXTS += 1


def _exit_worker_context() -> None:
    """Exit a managed worker context and opportunistically reap stale workers."""
    global _ACTIVE_WORKER_CONTEXTS
    _ACTIVE_WORKER_CONTEXTS = max(0, _ACTIVE_WORKER_CONTEXTS - 1)
    if _ACTIVE_WORKER_CONTEXTS == 0:
        kill_stale_workers()


def _wait_for_ready(
    proc: subprocess.Popen[str],
    timeout: float,
    ready_check: ReadyCheck | None,
) -> None:
    if ready_check is None:
        _poll_ready(proc, timeout, None)
        return
    # Ready checks send tasks with the sync API. run_worker() is entered from
    # async tests, so this poll runs on the event-loop thread, where sync send
    # fails closed with ASYNC_CONTEXT. Execute each check on a dedicated
    # thread: no running loop there, so the sync send API behaves as before.
    with ThreadPoolExecutor(max_workers=1) as checker:
        def _off_loop_check() -> bool:
            return checker.submit(ready_check).result()

        _poll_ready(proc, timeout, _off_loop_check)


def _drain_output(proc: subprocess.Popen[str]) -> tuple[str, str]:
    """Kill the process group and read what the worker logged.

    The log is a file, so this never waits on a descriptor the worker's
    executor children still hold open. The group dies first so the tail
    is final.
    """
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    return worker_log_tail(proc), ''


def _poll_ready(
    proc: subprocess.Popen[str],
    timeout: float,
    ready_check: ReadyCheck | None,
) -> None:
    deadline = time.time() + timeout
    # A probe that raises is retried, because early failures are the
    # normal shape of "not ready yet" -- the database is not accepting
    # connections, the schema is mid-install. But the exception is KEPT.
    # Swallowed outright, a probe that can never succeed -- a typo in its
    # SQL, a relation it names that does not exist -- is indistinguishable
    # from a worker that is merely slow, and the suite reports a timeout
    # for the whole startup while the real fault sat in the probe.
    last_failure: BaseException | None = None
    while time.time() < deadline:
        if proc.poll() is not None:
            returncode = proc.returncode
            stdout, stderr = _drain_output(proc)
            raise RuntimeError(
                f'Worker process exited before becoming ready (code={returncode})\n'
                f'stdout: {stdout}\nstderr: {stderr}'
            )
        if ready_check is None:
            time.sleep(0.1)
            return
        try:
            if ready_check():
                return
        except Exception as probe_error:
            last_failure = probe_error
        time.sleep(0.2)

    stdout, stderr = _drain_output(proc)
    probe_report = (
        f'\nready check last raised {type(last_failure).__name__}: '
        f'{last_failure}'
        if last_failure is not None
        else '\nready check returned false every attempt and never raised'
    )
    raise RuntimeError(
        f'Worker did not become ready before timeout{probe_report}\n'
        f'stdout: {stdout}\nstderr: {stderr}'
    )


def _kill_worker(proc: subprocess.Popen[str]) -> None:
    """Terminate worker process group, with fallback to SIGKILL."""
    if proc.poll() is not None:
        # Already exited
        return

    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    try:
        proc.wait(timeout=10.0)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        # Wait with timeout after SIGKILL to avoid blocking forever
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            # Force poll to reap zombie if possible
            proc.poll()

    # Final verification: ensure process is terminated
    if proc.poll() is None:
        raise RuntimeError(f'Failed to terminate worker process (pid={proc.pid})')


@contextmanager
def run_worker(
    instance_path: str,
    *,
    profile: str,
    timeout: float = 10.0,
    ready_check: ReadyCheck | None = None,
) -> Generator[subprocess.Popen[str], None, None]:
    """Start a worker with the named profile of the instance, yield, then
    terminate it. The profile holds every worker setting."""
    _enter_worker_context()

    cmd = [
        'uv',
        'run',
        'horsies',
        'worker',
        instance_path,
        f'--profile={profile}',
        '--loglevel=warning',
    ]

    env = worker_env()

    log_handle, log_path = _worker_log()
    proc: subprocess.Popen[str] = subprocess.Popen(
        cmd,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
        env=env,
    )
    os.close(log_handle)
    _WORKER_LOGS[proc.pid] = log_path

    try:
        _wait_for_ready(proc, timeout=timeout, ready_check=ready_check)
        yield proc
    finally:
        try:
            _kill_worker(proc)
        finally:
            _report_worker_log(proc)
            _exit_worker_context()


@contextmanager
def run_workers(
    instance_path: str,
    count: int,
    *,
    profile: str,
    timeout: float = 10.0,
    ready_check: ReadyCheck | None = None,
) -> Generator[Sequence[subprocess.Popen[str]], None, None]:
    """Start ``count`` workers with the named profile of the instance, yield,
    then terminate all."""
    _enter_worker_context()

    workers: list[subprocess.Popen[str]] = []

    env = worker_env()

    cmd_base = [
        'uv',
        'run',
        'horsies',
        'worker',
        instance_path,
        f'--profile={profile}',
        '--loglevel=warning',
    ]

    try:
        # Start all workers
        for _ in range(count):
            log_handle, log_path = _worker_log()
            proc: subprocess.Popen[str] = subprocess.Popen(
                cmd_base,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
                env=env,
            )
            os.close(log_handle)
            _WORKER_LOGS[proc.pid] = log_path
            workers.append(proc)

        # Wait for ALL workers to be ready before yielding.
        if workers and ready_check:
            for proc in workers:
                _wait_for_ready(proc, timeout=timeout, ready_check=ready_check)
        elif workers:
            time.sleep(0.1)

        yield workers
    finally:
        # Kill all workers
        try:
            for proc in workers:
                _kill_worker(proc)
                _report_worker_log(proc)
        finally:
            _exit_worker_context()


@contextmanager
def run_scheduler(
    instance_path: str,
    timeout: float = 10.0,
    ready_check: ReadyCheck | None = None,
) -> Generator[subprocess.Popen[str], None, None]:
    """Start a scheduler process, yield, then terminate it."""
    cmd = [
        'uv',
        'run',
        'horsies',
        'scheduler',
        instance_path,
        '--loglevel=warning',
    ]

    # Set PYTHONPATH to repo root so absolute imports work
    env = os.environ.copy()
    repo_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    )
    env['PYTHONPATH'] = repo_root

    log_handle, log_path = _worker_log()
    proc: subprocess.Popen[str] = subprocess.Popen(
        cmd,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
        env=env,
    )
    os.close(log_handle)
    _WORKER_LOGS[proc.pid] = log_path

    try:
        _wait_for_ready(proc, timeout=timeout, ready_check=ready_check)
        yield proc
    finally:
        _kill_worker(proc)
        _report_worker_log(proc)
