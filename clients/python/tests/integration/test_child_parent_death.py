"""A worker child exits at once when its parent dies while a task runs.

The child program (``horsies.core.worker.child_main``) runs as a real
process under a stand-in parent: a process that starts the child (``exec``:
``python -m horsies.core.worker.child_main``; ``preload``: forked by the real
preload program, Linux only), gives it its init frame and a run frame of a
task that sleeps, and then waits. The test kills the stand-in parent while
the task runs. The child's control thread reads the end of its socket and
ends the process; on Linux the parent-death signal also ends a child whose
task holds the GIL in C code.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Final

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tests.fixtures.core_frames import CLAIMED_AT, WORKER_ID
from tests.integration.conftest import DB_URL, compute_test_command_fingerprint

pytestmark = [pytest.mark.integration]

CHILD_EXIT_BOUND_S: Final = 3.0
"""The longest time from the death of the parent to the end of the child."""
RUNNING_WAIT_S: Final = 30.0
"""The longest wait for the child to set the row RUNNING."""
POLL_S: Final = 0.01
REPOSITORY_ROOT: Final = Path(__file__).resolve().parents[2]

TASK_APP: Final = """
import ctypes
import time

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Ok, Result

app = Horsies(AppConfig(broker=PostgresConfig(database_url='postgresql://u:p@localhost/none')))


@app.task(task_name='hang')
def hang() -> Result[None, TaskError]:
    time.sleep(60)
    return Ok(None)


@app.task(task_name='gil_hang')
def gil_hang() -> Result[None, TaskError]:
    # PyDLL keeps the GIL during the foreign call.
    ctypes.PyDLL(None).sleep(60)
    return Ok(None)
"""

PARENT_PROGRAM: Final = """
import ctypes
import os
import socket
import subprocess
import sys
import time
import uuid

sys.path[:0] = {paths!r}
from horsies.core.worker.preload_main import SPAWN_REQUEST
from tests.fixtures.core_frames import framed, init_meta, isolated_kind, run_meta
from tests.fixtures.preload_worker import read_frame

init = framed(
    init_meta(
        child_kind=isolated_kind(),
        app_locator={app_locator!r},
        parent_sys_path=tuple(sys.path),
        database_url={database_url!r},
    ),
)
run = framed(run_meta(task_id=uuid.UUID({task_id!r}), task_name={task_name!r}), b'{{}}')
if {mode!r} == 'exec':
    worker_end, child_end = socket.socketpair()
    child = subprocess.Popen(
        [sys.executable, '-m', 'horsies.core.worker.child_main',
         str(child_end.fileno()), str(os.getpid())],
        pass_fds=(child_end.fileno(),),
        stdin=subprocess.DEVNULL,
    )
    child_end.close()
    pid = child.pid
else:
    # The forked child reparents to this process, as to the core's worker.
    libc = ctypes.CDLL(None, use_errno=True)
    assert libc.prctl(36, 1, 0, 0, 0) == 0
    control, preload_end = socket.socketpair()
    control.settimeout(30.0)
    preload = subprocess.Popen(
        [sys.executable, '-m', 'horsies.core.worker.preload_main',
         str(preload_end.fileno()), str(os.getpid()), 'library'],
        pass_fds=(preload_end.fileno(),),
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    preload_end.close()
    control.sendall(init)
    assert read_frame(control)['kind'] == 'preload_ready'
    worker_end, child_end = socket.socketpair()
    socket.send_fds(control, [SPAWN_REQUEST], [child_end.fileno()])
    child_end.close()
    forked = read_frame(control)
    assert forked['kind'] == 'child_forked', forked
    pid = forked['pid']
worker_end.settimeout(30.0)
worker_end.sendall(init)
ready = read_frame(worker_end)
assert ready['kind'] == 'ready', ready
worker_end.sendall(run)
print(pid, flush=True)
time.sleep(60)
"""


@pytest.fixture(params=['exec', 'preload'])
def start_path(request: pytest.FixtureRequest) -> Iterator[str]:
    """Both start paths of a child. ``preload`` needs Linux."""
    if request.param == 'preload' and sys.platform != 'linux':
        pytest.skip('preload is Linux only')
    yield request.param


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    state = subprocess.run(
        ['ps', '-o', 'stat=', '-p', str(pid)],
        capture_output=True,
        text=True,
        check=False,
    )
    return bool(state.stdout.strip()) and not state.stdout.strip().startswith('Z')


async def _seed_claimed_task(session: AsyncSession, task_name: str) -> str:
    """A CLAIMED isolated row that ``WORKER_ID`` holds since ``CLAIMED_AT``:
    the claim that the run frame names."""
    task_id = str(uuid.uuid4())
    sent_at, fingerprint = compute_test_command_fingerprint(task_name=task_name)
    _ = await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    enqueued_at, created_at, updated_at, retry_count, max_retries,
                    claimed_at, claimed_by_worker_id, claim_expires_at,
                    is_workflow_task, retention_class_key, command_fingerprint_version,
                    command_fingerprint, retain_rerun_input, execution
                ) VALUES (
                    :id, :task_name, 'default', 100, '{}', 'CLAIMED', :sent_at,
                    NOW(), NOW(), NOW(), 0, 0, :claimed_at, :worker_id,
                    NOW() + interval '10 minutes', FALSE, 'standard_30d', 1,
                    :command_fingerprint, FALSE, 'ISOLATED'
                )
        """),
        {
            'id': task_id,
            'task_name': task_name,
            'sent_at': sent_at,
            'claimed_at': CLAIMED_AT,
            'worker_id': WORKER_ID,
            'command_fingerprint': fingerprint,
        },
    )
    await session.commit()
    return task_id


async def _wait_running(session: AsyncSession, task_id: str) -> None:
    deadline = time.monotonic() + RUNNING_WAIT_S
    while time.monotonic() < deadline:
        status = (
            await session.execute(
                text('SELECT status FROM horsies_tasks WHERE id = CAST(:id AS uuid)'),
                {'id': task_id},
            )
        ).scalar_one_or_none()
        await session.commit()
        if status == 'RUNNING':
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f'task {task_id} did not start within {RUNNING_WAIT_S} s')


async def _seconds_until_the_child_ends(
    session: AsyncSession,
    tmp_path: Path,
    mode: str,
    task_name: str,
) -> float:
    """Run a task in a child under a stand-in parent, kill the parent while
    the task runs, and return the time until the child is gone."""
    app = tmp_path / 'parent_death_app.py'
    app.write_text(TASK_APP, encoding='utf-8')
    task_id = await _seed_claimed_task(session, task_name)
    program = PARENT_PROGRAM.format(
        paths=[str(REPOSITORY_ROOT), *sys.path],
        app_locator=f'{app}:app',
        database_url=DB_URL,
        task_id=task_id,
        task_name=task_name,
        mode=mode,
    )
    parent = subprocess.Popen(
        [sys.executable, '-c', program],
        stdout=subprocess.PIPE,
        text=True,
        cwd=REPOSITORY_ROOT,
    )
    child_pid = 0
    try:
        assert parent.stdout is not None
        child_pid = int(parent.stdout.readline())
        await _wait_running(session, task_id)
        assert _alive(child_pid)
        started = time.monotonic()
        parent.kill()
        _ = parent.wait()
        while _alive(child_pid) and time.monotonic() - started < 10 * CHILD_EXIT_BOUND_S:
            time.sleep(POLL_S)
        return time.monotonic() - started
    finally:
        if parent.poll() is None:
            parent.kill()
            _ = parent.wait()
        if child_pid and _alive(child_pid):
            os.kill(child_pid, signal.SIGKILL)


@pytest.mark.asyncio(loop_scope='function')
async def test_child_exits_at_once_while_the_task_runs(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    start_path: str,
    tmp_path: Path,
) -> None:
    assert await _seconds_until_the_child_ends(session, tmp_path, start_path, 'hang') < (
        CHILD_EXIT_BOUND_S
    )


@pytest.mark.skipif(sys.platform != 'linux', reason='needs PR_SET_PDEATHSIG')
@pytest.mark.asyncio(loop_scope='function')
async def test_child_exits_while_c_code_holds_the_gil(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    start_path: str,
    tmp_path: Path,
) -> None:
    assert await _seconds_until_the_child_ends(session, tmp_path, start_path, 'gil_hang') < (
        CHILD_EXIT_BOUND_S
    )
