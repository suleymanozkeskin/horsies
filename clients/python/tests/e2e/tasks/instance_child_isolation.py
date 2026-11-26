"""App instance for child isolation e2e tests.

Short heartbeat and stale settings, so recovery paths finish inside a test.
"""

from __future__ import annotations

import ctypes
import multiprocessing
import os
import subprocess
import time
from typing import Final

from horsies._core import ForkedAfterCoreStart
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.outcome import WaitInsideTask
from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Err, Ok, Result, is_err

from tests.e2e.helpers.env import e2e_database_url
from tests.e2e.tasks.profiles import E2E_WORKER_PROFILES, preload_profiles

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(database_url=DB_URL, pool_size=5, max_overflow=5),
    recovery=RecoveryConfig(
        runner_heartbeat_interval_ms=1_000,
        claimer_heartbeat_interval_ms=1_000,
        # The smallest threshold with fail_stale_running on:
        # 2 x (runner_heartbeat_interval_ms + 2 s).
        running_stale_threshold_ms=6_000,
        finalizing_stale_threshold_ms=5_000,
        check_interval_ms=1_000,
    ),
    worker_profiles={**E2E_WORKER_PROFILES, **preload_profiles()},
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker
broker.app = app


@app.task(task_name='e2e_isolation_healthcheck')
def healthcheck() -> Result[str, TaskError]:
    return Ok('ready')


@app.task(task_name='e2e_isolation_wait_refused')
def wait_refused_in_child() -> Result[bool, TaskError]:
    """Report whether a result wait in this worker child is refused at once."""
    sent = healthcheck.send()
    if is_err(sent):
        return Err(
            TaskError(error_code='E2E_SEND_FAILED', message=str(sent.err_value)),
        )
    match sent.ok_value.get(timeout_ms=1000):
        case Err(err_value=WaitInsideTask()):
            return Ok(True)
        case _:
            return Ok(False)


@app.task(task_name='e2e_isolation_sleep')
def sleep_task(*, seconds: float) -> Result[str, TaskError]:
    time.sleep(seconds)
    return Ok(str(os.getpid()))


@app.task(task_name='e2e_isolation_gil_hold')
def gil_hold(*, seconds: int) -> Result[str, TaskError]:
    """Hold this child's GIL in C code: no other thread of the child runs."""
    ctypes.PyDLL(None).sleep(seconds)
    return Ok(str(os.getpid()))


@app.task(task_name='e2e_isolation_timeout', timeout_ms=1_000)
def timeout_task(*, seconds: float) -> Result[str, TaskError]:
    time.sleep(seconds)
    return Ok(str(os.getpid()))


@app.task(task_name='e2e_isolation_spawn_timeout', timeout_ms=1_000)
def spawn_then_hang(*, marker: str) -> Result[str, TaskError]:
    process = subprocess.Popen(['sleep', '120'])
    with open(marker, 'w', encoding='ascii') as target:
        target.write(str(process.pid))
    time.sleep(120)
    return Ok('unreachable')


@app.task(task_name='e2e_isolation_fork_helper')
def fork_helper_then_hang(*, helper_seconds: float) -> Result[str, TaskError]:
    if os.fork() == 0:
        time.sleep(helper_seconds)
        os._exit(0)
    time.sleep(120)
    return Ok('unreachable')


def _exit_with_three() -> None:
    os._exit(3)


@app.task(task_name='e2e_isolation_multiprocessing')
def run_multiprocessing(*, start_method: str) -> Result[str, TaskError]:
    context = multiprocessing.get_context(start_method)
    process = context.Process(target=_exit_with_three)
    process.start()
    process.join(30)
    return Ok(str(process.exitcode))


FORK_POOL_BOUND_S: Final = 10.0
"""The longest wait for the report of the fork pool worker; a worker that
hangs fails the task with the pool's ``TimeoutError``."""


def _send_in_pool_worker() -> dict[str, str]:
    """How a send ends in a worker of a fork-start ``multiprocessing.Pool``.
    Module level, so the pool can name it."""
    pool_worker_pid = str(os.getpid())
    try:
        sent = healthcheck.send()
    except ForkedAfterCoreStart as refused:
        core_pid, this_pid, advice = (str(argument) for argument in refused.args)
        return {
            'ended': 'raised',
            'class': type(refused).__name__,
            'core_pid': core_pid,
            'this_pid': this_pid,
            'advice': advice,
            'pool_worker_pid': pool_worker_pid,
        }
    match sent:
        case Ok():
            return {'ended': 'sent', 'pool_worker_pid': pool_worker_pid}
        case Err(err_value=error):
            return {
                'ended': 'returned',
                'class': type(error).__name__,
                'message': error.message,
                'pool_worker_pid': pool_worker_pid,
            }


@app.task(task_name='e2e_isolation_fork_pool_send')
def send_from_fork_pool() -> Result[dict[str, str], TaskError]:
    """Open a fork-start ``multiprocessing.Pool`` in this task body and send
    from its worker. The run started on the core of this child, so the pool
    worker is a fork after the core started."""
    context = multiprocessing.get_context('fork')
    with context.Pool(processes=1) as pool:
        report = pool.apply_async(_send_in_pool_worker).get(timeout=FORK_POOL_BOUND_S)
    return Ok({**report, 'child_pid': str(os.getpid())})
