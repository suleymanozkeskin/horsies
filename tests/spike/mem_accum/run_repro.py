"""Driver: compare child recycling by count vs by memory under a leaky workload.

Runs the same retained-allocation burst under three worker configs and reports
distinct child PIDs (rotation), peak process-tree RSS, and throughput:

  - none:   no recycling
  - count:  --max-tasks-per-child=N
  - memory: --max-memory-per-child-mb=N

The leaky task retains memory per child, so child RSS ratchets up. Count
recycling rotates on a fixed task count regardless of bytes; memory recycling
rotates when a child crosses the RSS threshold — the point of the comparison.

Run from repo root with env loaded (and a reachable DB):
    set -a; . ./.env; set +a
    PYTHONPATH=. uv run python -m tests.spike.mem_accum.run_repro
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass

import psutil

from horsies.core.types.result import is_err
from tests.spike.mem_accum.repro_app import app  # noqa: F401  (registers tasks)
from tests.spike.mem_accum.repro_tasks import healthcheck, leaky_task

LOCATOR = 'tests.spike.mem_accum.repro_app:app'
PROCESSES = 2
BURST = 40
ALLOC_MB = 20
COUNT_N = 8
MEMORY_THRESHOLD_MB = 250
READY_TIMEOUT_S = 30.0
GET_TIMEOUT_MS = 30_000


@dataclass
class Result:
    mode: str
    distinct_pids: int
    completed: int
    peak_tree_rss_mb: float
    elapsed_s: float


def _wait_ready(deadline: float) -> bool:
    while time.monotonic() < deadline:
        r = healthcheck.send()
        if not is_err(r) and r.ok_value.get(timeout_ms=2000).is_ok():
            return True
        time.sleep(0.5)
    return False


def _sample_tree_rss_mb(root_pid: int) -> float:
    try:
        root = psutil.Process(root_pid)
        procs = [root, *root.children(recursive=True)]
    except psutil.Error:
        return 0.0
    total = 0
    for proc in procs:
        try:
            total += proc.memory_info().rss
        except psutil.Error:
            continue
    return total / 1024 / 1024


def _run_mode(mode: str, extra_args: list[str]) -> Result:
    worker_cmd = os.environ.get('HORSIES_WORKER_CMD', 'uv run horsies').split()
    worker = subprocess.Popen(
        [
            *worker_cmd, 'worker', LOCATOR,
            f'--processes={PROCESSES}', '--loglevel=warning', *extra_args,
        ],
        start_new_session=True,
    )
    peak_rss = 0.0
    stop = threading.Event()

    def _sampler() -> None:
        nonlocal peak_rss
        while not stop.is_set():
            peak_rss = max(peak_rss, _sample_tree_rss_mb(worker.pid))
            time.sleep(0.25)

    sampler = threading.Thread(target=_sampler, daemon=True)
    try:
        if not _wait_ready(time.monotonic() + READY_TIMEOUT_S):
            raise RuntimeError(
                f'[{mode}] worker not ready in {READY_TIMEOUT_S}s '
                '(baseline guard may have rejected the threshold)'
            )
        sampler.start()
        started = time.monotonic()
        handles = []
        for _ in range(BURST):
            send = leaky_task.send(mb=ALLOC_MB)
            if is_err(send):
                raise RuntimeError(f'[{mode}] send failed: {send.err_value}')
            handles.append(send.ok_value)

        pids: set[int] = set()
        completed = 0
        for handle in handles:
            result = handle.get(timeout_ms=GET_TIMEOUT_MS)
            if result.is_ok():
                pids.add(result.unwrap())
                completed += 1
        elapsed = time.monotonic() - started
    finally:
        stop.set()
        try:
            os.killpg(os.getpgid(worker.pid), signal.SIGTERM)
            worker.wait(timeout=10)
        except Exception:
            try:
                os.killpg(os.getpgid(worker.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass

    return Result(mode, len(pids), completed, round(peak_rss, 1), round(elapsed, 2))


def main() -> int:
    modes = [
        ('none', []),
        ('count', [f'--max-tasks-per-child={COUNT_N}']),
        ('memory', [f'--max-memory-per-child-mb={MEMORY_THRESHOLD_MB}']),
    ]
    print(
        f'workload: leaky, {BURST} tasks x {ALLOC_MB}MB retained, '
        f'{PROCESSES} processes\n'
        f'count N={COUNT_N}, memory threshold={MEMORY_THRESHOLD_MB}MB\n'
    )
    header = f'{"mode":<8} {"distinct_pids":>13} {"completed":>10} {"peak_tree_rss_mb":>17} {"elapsed_s":>10}'
    print(header)
    print('-' * len(header))
    for mode, extra_args in modes:
        try:
            r = _run_mode(mode, extra_args)
            print(
                f'{r.mode:<8} {r.distinct_pids:>13} {r.completed:>10} '
                f'{r.peak_tree_rss_mb:>17} {r.elapsed_s:>10}'
            )
        except Exception as exc:
            print(f'{mode:<8} ERROR: {exc}')
    print(
        '\nReading: with the leaky workload, "memory" should bound '
        'peak_tree_rss_mb regardless of task count, while "count" rotates on a '
        'fixed task budget that may under- or over-shoot the memory ceiling.'
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
