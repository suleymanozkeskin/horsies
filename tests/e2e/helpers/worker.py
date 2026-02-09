"""Worker process lifecycle helpers for e2e tests."""

from __future__ import annotations

import os
import signal
import subprocess
import time
from contextlib import contextmanager
from typing import Callable, Generator, Sequence


ReadyCheck = Callable[[], bool]

# Number of active run_worker/run_workers contexts created by this module.
# Prevents stale-worker cleanup from killing intentionally running workers in
# nested contexts (e.g., tests that start worker A then worker B).
_ACTIVE_WORKER_CONTEXTS = 0


def kill_stale_workers() -> None:
    """Kill any leftover horsies worker processes from previous tests.

    Workers started with start_new_session=True survive parent death and share
    queues, so a stale worker from a different app instance would pick up tasks
    it cannot resolve (WORKER_RESOLUTION_ERROR).
    Called automatically by run_worker / run_workers before starting new ones.

    Strategy: graceful SIGTERM first (lets workers close DB connections),
    then SIGKILL after 3s if needed.
    """
    result = subprocess.run(
        ['pgrep', '-f', 'horsies worker'],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return  # no stale workers

    # Graceful SIGTERM first
    subprocess.run(['pkill', '-TERM', '-f', 'horsies worker'], capture_output=True)
    for _ in range(3):
        time.sleep(1)
        check = subprocess.run(
            ['pgrep', '-f', 'horsies worker'],
            capture_output=True,
        )
        if check.returncode != 0:
            return  # all gone

    # Force SIGKILL if SIGTERM didn't work
    subprocess.run(['pkill', '-9', '-f', 'horsies worker'], capture_output=True)
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
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            stdout = proc.stdout.read() if proc.stdout else ''
            stderr = proc.stderr.read() if proc.stderr else ''
            raise RuntimeError(
                f'Worker process exited before becoming ready (code={proc.returncode})\n'
                f'stdout: {stdout}\nstderr: {stderr}'
            )
        if ready_check is None:
            time.sleep(0.1)
            return
        try:
            if ready_check():
                return
        except Exception:
            pass
        time.sleep(0.2)

    # Timeout - capture output for debugging
    stdout = ''
    stderr = ''
    if proc.stdout:
        import select

        if select.select([proc.stdout], [], [], 0)[0]:
            stdout = proc.stdout.read()
    if proc.stderr:
        import select

        if select.select([proc.stderr], [], [], 0)[0]:
            stderr = proc.stderr.read()
    raise RuntimeError(
        f'Worker did not become ready before timeout\n'
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
    processes: int = 1,
    timeout: float = 10.0,
    extra_args: list[str] | None = None,
    ready_check: ReadyCheck | None = None,
) -> Generator[subprocess.Popen[str], None, None]:
    """Start a worker process, yield, then terminate it."""
    _enter_worker_context()

    cmd = [
        'uv',
        'run',
        'horsies',
        'worker',
        instance_path,
        f'--processes={processes}',
        '--loglevel=warning',
    ]
    if extra_args:
        cmd.extend(extra_args)

    # Set PYTHONPATH to repo root so absolute imports work
    env = os.environ.copy()
    repo_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    )
    env['PYTHONPATH'] = repo_root

    proc: subprocess.Popen[str] = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
        env=env,
    )

    try:
        _wait_for_ready(proc, timeout=timeout, ready_check=ready_check)
        yield proc
    finally:
        try:
            _kill_worker(proc)
        finally:
            _exit_worker_context()


@contextmanager
def run_workers(
    instance_path: str,
    count: int,
    processes: int = 1,
    timeout: float = 10.0,
    extra_args: list[str] | None = None,
    ready_check: ReadyCheck | None = None,
) -> Generator[Sequence[subprocess.Popen[str]], None, None]:
    """Start multiple worker processes, yield, then terminate all."""
    _enter_worker_context()

    workers: list[subprocess.Popen[str]] = []

    # Set PYTHONPATH to repo root so absolute imports work
    env = os.environ.copy()
    repo_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    )
    env['PYTHONPATH'] = repo_root

    cmd_base = [
        'uv',
        'run',
        'horsies',
        'worker',
        instance_path,
        f'--processes={processes}',
        '--loglevel=warning',
    ]
    if extra_args:
        cmd_base.extend(extra_args)

    try:
        # Start all workers
        for _ in range(count):
            proc: subprocess.Popen[str] = subprocess.Popen(
                cmd_base,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
                env=env,
            )
            workers.append(proc)

        # Wait for first worker to be ready (others follow similar startup)
        if workers and ready_check:
            _wait_for_ready(workers[0], timeout=timeout, ready_check=ready_check)
        elif workers:
            time.sleep(0.1)

        yield workers
    finally:
        # Kill all workers
        try:
            for proc in workers:
                _kill_worker(proc)
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

    proc: subprocess.Popen[str] = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
        env=env,
    )

    try:
        _wait_for_ready(proc, timeout=timeout, ready_check=ready_check)
        yield proc
    finally:
        _kill_worker(proc)
