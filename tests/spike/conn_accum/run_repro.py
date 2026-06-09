"""Driver: measure app-owned connection accumulation across worker children.

Procedure:
  1. Sample baseline app-engine connections (should be ~0; no worker yet).
  2. Start a horsies worker with N children against repro_app.
  3. Enqueue a burst of slow DB tasks.
  4. Sample app-engine connections every 0.5s through the run and a drain window.
  5. Kill the worker; sample again to prove the worker children were holding them.

Attribution is by ``application_name='repro_app_engine'`` (set on the app-owned
engine in repro_db), which isolates app-owned connections from everything
horsies opens.

Run from repo root with env loaded:
    set -a; . ./.env; set +a
    PYTHONPATH=. uv run python -m tests.spike.conn_accum.run_repro
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from typing import Any

import psycopg

from tests.spike.conn_accum.repro_db import APP_ENGINE_APPLICATION_NAME
from tests.spike.conn_accum.repro_tasks import app_db_task

PROCESSES = 4
BURST = 40
SLEEP_MS = 500
WARMUP_S = 5.0
DRAIN_S = 12.0
LOCATOR = 'tests.spike.conn_accum.repro_app:app'


def _probe_conninfo() -> str:
    pw = os.environ.get('DB_PASSWORD', '')
    return f'host=localhost port=5432 dbname=horsies user=postgres password={pw} application_name=repro_probe'


def sample(conn: psycopg.Connection[Any]) -> dict[str, int]:
    """Return app-engine connection counts keyed by backend state."""
    rows = conn.execute(
        'SELECT state, count(*) FROM pg_stat_activity '
        'WHERE application_name = %s GROUP BY state',
        (APP_ENGINE_APPLICATION_NAME,),
    ).fetchall()
    counts: dict[str, int] = {}
    total = 0
    for state, n in rows:
        counts[state or 'unknown'] = int(n)
        total += int(n)
    counts['TOTAL'] = total
    return counts


def fmt(counts: dict[str, int]) -> str:
    total = counts.get('TOTAL', 0)
    parts = [f'{k}={v}' for k, v in sorted(counts.items()) if k != 'TOTAL']
    return f'total={total:<3} ({", ".join(parts) or "none"})'


def main() -> int:
    probe = psycopg.connect(_probe_conninfo(), autocommit=True)
    try:
        print(f'platform={sys.platform}  processes={PROCESSES}  burst={BURST}  sleep_ms={SLEEP_MS}')
        print(f'baseline (no worker):           {fmt(sample(probe))}')

        env = os.environ.copy()
        # Prepend cwd to the INHERITED PYTHONPATH (do not clobber it): in a
        # container the deps + repo are already on PYTHONPATH and must survive.
        env['PYTHONPATH'] = os.pathsep.join(
            p for p in (os.getcwd(), env.get('PYTHONPATH', '')) if p
        )
        # Worker launch command is configurable so the same driver runs locally
        # (`uv run horsies`) and inside a slim Linux container (`horsies`).
        worker_cmd = os.environ.get('HORSIES_WORKER_CMD', 'uv run horsies').split()
        worker = subprocess.Popen(
            [*worker_cmd, 'worker', LOCATOR,
             f'--processes={PROCESSES}', '--loglevel=warning'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
            start_new_session=True,
            env=env,
        )
        try:
            time.sleep(WARMUP_S)
            print(f'after worker warmup ({WARMUP_S}s):       {fmt(sample(probe))}')

            sent = 0
            for _ in range(BURST):
                res = app_db_task.send(sleep_ms=SLEEP_MS)
                if not getattr(res, 'is_err', lambda: False)():
                    sent += 1
            print(f'enqueued {sent}/{BURST} tasks; sampling through run + drain...')

            peak = 0
            start = time.time()
            run_window = (BURST / PROCESSES) * (SLEEP_MS / 1000.0) + DRAIN_S
            while time.time() - start < run_window:
                c = sample(probe)
                peak = max(peak, c.get('TOTAL', 0))
                print(f'  t={time.time() - start:5.1f}s  {fmt(c)}')
                time.sleep(0.5)

            post = sample(probe)
            print(f'post-run / drained:             {fmt(post)}   (peak during run={peak})')
        finally:
            try:
                os.killpg(worker.pid, signal.SIGTERM)
                worker.wait(timeout=10)
            except Exception:
                try:
                    os.killpg(worker.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

        time.sleep(2.0)
        print(f'after worker killed:            {fmt(sample(probe))}')

        print()
        print('VERDICT:')
        print(f'  peak app-engine conns during run : {peak}')
        print(f'  app-engine conns after drain     : {post.get("TOTAL", 0)} '
              f'(idle={post.get("idle", 0)}, iitx={post.get("idle in transaction", 0)})')
        print('  interpretation: a non-zero post-drain TOTAL that only falls to ~0 after the')
        print('  worker is killed = app-owned pool retention across children, not released by')
        print('  horsies (no per-child hook to dispose/reconfigure app engines).')
        return 0
    finally:
        probe.close()


if __name__ == '__main__':
    raise SystemExit(main())
