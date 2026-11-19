"""E2E: a worker child whose core stopped ends itself.

The core of a child stops after a panic in one of its background tasks
(``tests/e2e/tasks/instance_core_stopped_child.py`` makes its log forwarder
panic). The child never stays alive with a stopped core:

- At the start of a task, before user code ran: the child reports the start
  as ``OWNERSHIP_UNCONFIRMED``, so the worker releases the claim with no
  attempt charged; then it ends itself with ``CORE_STOPPED_EXIT_CODE``, and
  the worker starts a new child, which runs the task.
- After a body that finished: the finalizing marker meets the stopped core;
  the child reports the result, the worker writes it, and the child ends
  itself. The task keeps its result, with one run and one attempt.
- A body that the refusal ended (a send of the body meets the stopped
  core): the child ends itself with no report, the worker takes the
  crashed-child path (``WORKER_CRASHED``), and the retry policy of the task
  applies.
"""

from __future__ import annotations

import asyncio
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Final

import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import is_err
from horsies.core.worker.child_main import CORE_STOPPED_EXIT_CODE

from tests.broker_resources_support import current_resources
from tests.e2e.helpers.assertions import unwrap_send
from tests.e2e.helpers.db import wait_for_all_terminal
from tests.e2e.helpers.worker import run_worker, worker_log_tail
from tests.e2e.tasks import instance_core_stopped_child as instance
from tests.integration.history_seeding import read_attempts

INSTANCE: Final = 'tests.e2e.tasks.instance_core_stopped_child:app'
TERMINAL_TIMEOUT_S: Final = 60.0
RESULT_TIMEOUT_MS: Final = 10_000
MARKER_TIMEOUT_S: Final = 30.0
POLL_INTERVAL_S: Final = 0.1
AFTER_MARKER_S: Final = 1.0
"""The wait after the handler wrote its marker file and before the next
send: the panic follows the write at once, so the core is stopped by then."""
LOG_TIMEOUT_S: Final = 30.0
"""The longest wait for a line in the log of the worker."""


def _make_ready_check() -> Callable[[], bool]:
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            sent = instance.healthcheck.send()
            if is_err(sent):
                return False
            handle = sent.ok_value
        return handle.get(timeout_ms=2000).is_ok()

    return _check


async def _wait_for_file(path: Path) -> None:
    deadline = time.monotonic() + MARKER_TIMEOUT_S
    while not path.exists():
        assert time.monotonic() < deadline, f'no file at {path}'
        await asyncio.sleep(POLL_INTERVAL_S)


async def _ok_pid(handle: TaskHandle[int]) -> int:
    result = await handle.get_async(timeout_ms=RESULT_TIMEOUT_MS)
    assert result.is_ok(), result
    return result.ok_value


async def _wait_for_log_line(worker: subprocess.Popen[str], line: str) -> str:
    """The log tail of ``worker`` once it holds ``line``."""
    deadline = time.monotonic() + LOG_TIMEOUT_S
    while line not in (log := worker_log_tail(worker)):
        assert time.monotonic() < deadline, f'no log line {line!r}'
        await asyncio.sleep(POLL_INTERVAL_S)
    return log


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_start_refused_by_a_stopped_core_is_released_and_a_new_child_runs_it(
    core_stopped_broker: PostgresBroker,
    tmp_path: Path,
) -> None:
    """The first task arms the panic and ends; the core of its child stops
    while the child holds no task. The second task reaches that child: its
    start is released with no attempt charged, the child ends with the
    core-stopped code, and a new child runs the task once."""
    session_factory = current_resources(core_stopped_broker).session_factory
    marker = tmp_path / 'panicked'

    with run_worker(
        INSTANCE,
        profile='isolated-1',
        ready_check=_make_ready_check(),
    ) as worker:
        first = unwrap_send(
            await instance.stops_after_return.send_async(marker_path=str(marker)),
        )
        await wait_for_all_terminal(
            session_factory, [first.task_id], timeout_s=TERMINAL_TIMEOUT_S
        )
        await _wait_for_file(marker)
        await asyncio.sleep(AFTER_MARKER_S)
        second = unwrap_send(await instance.child_pid.send_async())
        await wait_for_all_terminal(
            session_factory, [second.task_id], timeout_s=TERMINAL_TIMEOUT_S
        )
        log = worker_log_tail(worker)

    stopped_child = await _ok_pid(first)
    new_child = await _ok_pid(second)
    attempts = await read_attempts(core_stopped_broker, str(second.task_id))

    assert int(marker.read_text(encoding='ascii')) == stopped_child
    assert new_child != stopped_child
    assert [(attempt.outcome, attempt.worker_pid) for attempt in attempts] == [
        ('COMPLETED', new_child),
    ]
    assert (
        f'At the start of task {second.task_id}: the core of this child stopped '
        'after a panic in a core background task'
    ) in log
    assert instance.PANIC_TEXT in log
    assert f'child {stopped_child} ended itself with exit code {CORE_STOPPED_EXIT_CODE}' in log


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_body_that_finished_keeps_its_result_and_a_new_child_serves_next(
    core_stopped_broker: PostgresBroker,
    tmp_path: Path,
) -> None:
    """The core of the child stops while the body runs, and the body then
    finishes. The finalizing marker after the body meets the stopped core:
    the child reports the result, the worker writes it, and the child ends
    with the core-stopped code. The task keeps the result of its one run,
    with one attempt; the next task runs in a new child."""
    session_factory = current_resources(core_stopped_broker).session_factory
    runs = tmp_path / 'runs'
    marker = tmp_path / 'panicked'

    with run_worker(
        INSTANCE,
        profile='isolated-1',
        ready_check=_make_ready_check(),
    ) as worker:
        handle = unwrap_send(
            await instance.stops_during_body.send_async(
                runs_path=str(runs),
                marker_path=str(marker),
            ),
        )
        await wait_for_all_terminal(
            session_factory, [handle.task_id], timeout_s=TERMINAL_TIMEOUT_S
        )
        stopped_child = await _ok_pid(handle)
        await _wait_for_log_line(
            worker,
            f'child {stopped_child} ended itself with exit code {CORE_STOPPED_EXIT_CODE}',
        )
        after = unwrap_send(await instance.child_pid.send_async())
        await wait_for_all_terminal(
            session_factory, [after.task_id], timeout_s=TERMINAL_TIMEOUT_S
        )
        log = worker_log_tail(worker)

    new_child = await _ok_pid(after)
    attempts = await read_attempts(core_stopped_broker, str(handle.task_id))

    assert runs.read_text(encoding='ascii').splitlines() == [str(stopped_child)]
    assert int(marker.read_text(encoding='ascii')) == stopped_child
    assert [
        (attempt.outcome, attempt.error_code, attempt.will_retry, attempt.worker_pid)
        for attempt in attempts
    ] == [
        ('COMPLETED', None, False, stopped_child),
    ]
    assert new_child != stopped_child
    assert (
        f'After the body of task {handle.task_id}: the core of this child stopped'
    ) in log


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_body_that_the_stopped_core_interrupted_takes_the_crash_path_and_the_retry(
    core_stopped_broker: PostgresBroker,
    tmp_path: Path,
) -> None:
    """The first run stops the core of its child; a send of its body then
    raises ``CoreStopped``, which ends the body. The child ends with no
    report, the attempt fails with ``WORKER_CRASHED``, and the retry policy
    runs the task again in a new child."""
    session_factory = current_resources(core_stopped_broker).session_factory
    first_run = tmp_path / 'first_run'
    marker = tmp_path / 'panicked'

    with run_worker(
        INSTANCE,
        profile='isolated-1',
        ready_check=_make_ready_check(),
    ) as worker:
        handle = unwrap_send(
            await instance.interrupted_in_body.send_async(
                first_run_path=str(first_run),
                marker_path=str(marker),
            ),
        )
        await wait_for_all_terminal(
            session_factory, [handle.task_id], timeout_s=TERMINAL_TIMEOUT_S
        )
        log = worker_log_tail(worker)

    retried_in = await _ok_pid(handle)
    stopped_child = int(first_run.read_text(encoding='ascii'))
    attempts = await read_attempts(core_stopped_broker, str(handle.task_id))

    assert retried_in != stopped_child
    assert [
        (attempt.outcome, attempt.error_code, attempt.will_retry, attempt.worker_pid)
        for attempt in attempts
    ] == [
        ('FAILED', 'WORKER_CRASHED', True, stopped_child),
        ('COMPLETED', None, False, retried_in),
    ]
    assert (
        f'During the body of task {handle.task_id}: the core of this child stopped'
    ) in log
