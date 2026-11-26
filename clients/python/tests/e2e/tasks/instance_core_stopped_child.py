"""App instance whose tasks stop the core of their isolated child.

The core of a child stops after a panic in one of its background tasks. A
task here makes the log forwarder of its child panic: it puts a logging
handler on ``horsies.sql`` that raises ``PanicException`` on the record of
the ping of a core handle of its own (the case of
``tests/extension/programs/panics.py``). The child sets no panic rule, so
its core stops and every later core call raises ``CoreStopped``.

The core logs the record of a statement before the statement runs, and a
stop ends every core call in flight with ``CoreStopped``. So the handler
panics only after that ping returned to the task: a panic on the record
alone would stop the core while the ping runs, and the ping would raise
``CoreStopped`` in the body of the task.

- ``stops_after_return``: the handler waits ``STOP_DELAY_S`` after the
  ping before the panic, so the run ends first (its marker and its result
  frame) and the core stops while the child holds no task. The next start
  in that child meets the stopped core.
- ``stops_during_body``: the core stops while the body runs, and the body
  then finishes; the finalizing marker after the body meets the stopped
  core. Each run appends its process id to a file.
- ``interrupted_in_body``: on its first run the core stops while the body
  runs, and a send of the body then meets the stopped core: the refusal
  ends the body. Later runs return their process id.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Final

from pydantic import SecretStr

from horsies import _core as core
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import RetryPolicy, TaskError
from horsies.core.types.result import Ok, Result

from tests.e2e.helpers.env import e2e_database_url
from tests.e2e.tasks.profiles import E2E_WORKER_PROFILES
from tests.extension.programs.support import manifest_json

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')

PANIC_STATEMENT: Final = 'health_check'
"""The statement whose record makes the handler panic: the ping's."""

PANIC_TEXT: Final = 'planned panic in a log handler of an e2e task'

STOP_DELAY_S: Final = 2.0
"""How long the handler of ``stops_after_return`` waits before the panic:
longer than the end of its run (the marker and the result frame)."""

STOP_WAIT_S: Final = 10.0
"""The longest wait of ``stops_in_body`` for the stop of its core."""

STOP_POLL_S: Final = 0.05

PING_RETURN_WAIT_S: Final = 30.0
"""The longest wait of the handler for the ping that it panics on to
return to the task."""

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=SecretStr(DB_URL),
        pool_size=5,
        max_overflow=5,
    ),
    worker_profiles=E2E_WORKER_PROFILES,
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker
broker.app = app


class _PanicOnStatement(logging.Handler):
    """On the first record of ``statement``: wait until ``ping_returned`` is
    set (at most ``PING_RETURN_WAIT_S``), then ``delay_s``, write the process
    id to ``marker_path``, then raise ``PanicException``. The log forwarder
    of the core runs the handler, so the forwarder panics and the core of
    this process stops."""

    def __init__(
        self,
        statement: str,
        delay_s: float,
        marker_path: str,
        ping_returned: threading.Event,
    ) -> None:
        super().__init__(level=logging.NOTSET)
        self._statement = statement
        self._delay_s = delay_s
        self._marker_path = marker_path
        self._ping_returned = ping_returned
        self._fired = False

    def emit(self, record: logging.LogRecord) -> None:
        statement: object = getattr(record, 'statement', None)
        if self._fired or statement != self._statement:
            return
        self._fired = True
        # A ping that does not return within the wait still gets the panic:
        # the test then fails on what the task returned.
        _ = self._ping_returned.wait(PING_RETURN_WAIT_S)
        time.sleep(self._delay_s)
        with open(self._marker_path, 'w', encoding='ascii') as marker:
            marker.write(str(os.getpid()))
        raise core.PanicException(PANIC_TEXT)


def _stop_the_core(delay_s: float, marker_path: str) -> None:
    """Make the log forwarder of this process panic ``delay_s`` after a ping
    of a core handle of its own returned. The ping returns before the core
    stops, so no core call of the task is in flight at the stop."""
    ping_returned = threading.Event()
    sql_logger = logging.getLogger('horsies.sql')
    sql_logger.setLevel(logging.DEBUG)
    sql_logger.addHandler(
        _PanicOnStatement(PANIC_STATEMENT, delay_s, marker_path, ping_returned),
    )
    core.refresh_log_levels()
    handle = core.CoreHandle(
        manifest_json(DB_URL, schema='ASSUME_INITIALIZED', echo=True),
        pool_profile='PRODUCER',
    )
    _ = handle.ping_database()
    ping_returned.set()


@app.task(name='e2e_core_stopped_healthcheck')
def healthcheck() -> Result[str, TaskError]:
    return Ok('ready')


@app.task(name='e2e_core_stops_after_return')
def stops_after_return(*, marker_path: str) -> Result[int, TaskError]:
    """Arm the panic, which comes ``STOP_DELAY_S`` later, and return the
    process id of the child."""
    _stop_the_core(STOP_DELAY_S, marker_path)
    return Ok(os.getpid())


@app.task(name='e2e_core_stopped_child_pid')
def child_pid() -> Result[int, TaskError]:
    """The process id of the child that runs this task."""
    return Ok(os.getpid())


def _wait_for_the_stop() -> None:
    """Wait for the core of this process to stop, at most ``STOP_WAIT_S``."""
    deadline = time.monotonic() + STOP_WAIT_S
    while core.core_state() != 'stopped' and time.monotonic() < deadline:
        time.sleep(STOP_POLL_S)


@app.task(
    name='e2e_core_stops_during_body',
    retry_policy=RetryPolicy.fixed([1], retry_for=['WORKER_CRASHED']),
)
def stops_during_body(*, runs_path: str, marker_path: str) -> Result[int, TaskError]:
    """Append the process id of the child to ``runs_path``, stop the core of
    the child, wait for the stop, and return the process id. The retry
    policy runs the task again if the run takes the crashed-child path, so
    a second line in ``runs_path`` shows a second run."""
    with open(runs_path, 'a', encoding='ascii') as runs:
        runs.write(f'{os.getpid()}\n')
    _stop_the_core(0.0, marker_path)
    _wait_for_the_stop()
    return Ok(os.getpid())


@app.task(
    name='e2e_core_stops_and_interrupts_body',
    retry_policy=RetryPolicy.fixed([1], retry_for=['WORKER_CRASHED']),
)
def interrupted_in_body(*, first_run_path: str, marker_path: str) -> Result[int, TaskError]:
    """The first run (no file at ``first_run_path``) writes that file, stops
    the core of its child, waits for the stop, then sends ``child_pid``: the
    send raises ``CoreStopped``, which ends the body. Every later run
    returns the process id of its child."""
    if os.path.exists(first_run_path):
        return Ok(os.getpid())
    with open(first_run_path, 'w', encoding='ascii') as first_run:
        first_run.write(str(os.getpid()))
    _stop_the_core(0.0, marker_path)
    _wait_for_the_stop()
    _ = child_pid.send()
    return Ok(os.getpid())
