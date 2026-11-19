"""The child program (`horsies.core.worker.child_main`) as a shared async
child, run as a real process: it waits for its frames without spinning.

The test is the worker: it gives the child its socket, sends the init frame
and the stop frame, and reads the child's frames.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Final

import psutil
import pytest

from horsies.core.worker_manifest import CHILD_MAIN_MODULE
from tests.fixtures.core_frames import framed, init_meta, shared_async_kind
from tests.fixtures.preload_worker import read_frame

pytestmark = [pytest.mark.unit]

REPOSITORY_ROOT: Final = Path(__file__).resolve().parents[2]
STEP_TIMEOUT_S: Final = 30.0
IDLE_CPU_PROBE_S: Final = 1.0
IDLE_CPU_LIMIT_S: Final = 0.3
"""The most CPU time (user and system) that an idle child may use in
``IDLE_CPU_PROBE_S``: a loop that spins uses the whole probe."""

IDLE_APP: Final = """
from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode

app = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(database_url='postgresql://u:p@localhost/none'),
    ),
)
"""


def test_a_shared_child_waits_for_frames_without_spinning(tmp_path: Path) -> None:
    """`tests/unit/test_shared_slot_pool.py::test_a_production_shared_child_waits_for_frames_without_spinning`
    at `v0.7.0`:
    the real child program runs as ``__main__``. A frame type that it builds
    there must match the one that its seat loop checks (``child_inbox``), or
    the loop spins and never takes a frame. The pool of the child opens
    without a connection, so no database is needed."""
    app = tmp_path / 'idle_app.py'
    app.write_text(IDLE_APP, encoding='utf-8')
    worker_end, child_end = socket.socketpair()
    worker_end.settimeout(STEP_TIMEOUT_S)
    process = subprocess.Popen(
        [
            sys.executable,
            '-m',
            CHILD_MAIN_MODULE,
            str(child_end.fileno()),
            str(os.getpid()),
        ],
        pass_fds=(child_end.fileno(),),
        stdin=subprocess.DEVNULL,
        cwd=REPOSITORY_ROOT,
    )
    child_end.close()
    try:
        worker_end.sendall(
            framed(
                init_meta(
                    child_kind=shared_async_kind(seats=2),
                    app_locator=f'{app}:app',
                    parent_sys_path=(str(REPOSITORY_ROOT), *sys.path),
                    database_url='postgresql://unused@127.0.0.1:1/unused',
                ),
            ),
        )
        ready = read_frame(worker_end)
        assert ready['kind'] == 'ready', ready
        child = psutil.Process(process.pid)
        before = sum(child.cpu_times()[:2])
        time.sleep(IDLE_CPU_PROBE_S)
        used = sum(child.cpu_times()[:2]) - before
        worker_end.sendall(framed({'kind': 'stop'}))
        _ = process.wait(timeout=STEP_TIMEOUT_S)
    finally:
        worker_end.close()
        if process.poll() is None:
            process.kill()
            _ = process.wait(timeout=STEP_TIMEOUT_S)

    assert used < IDLE_CPU_LIMIT_S
