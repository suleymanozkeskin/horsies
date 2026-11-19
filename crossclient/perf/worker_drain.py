"""The app, the sends and the reads of the ``claim-finish`` scenario.

One drain sends ``DRAIN_TASK_COUNT`` tasks of a no-op task with ``send``, then
starts one worker with ``horsies worker`` (``worker_process.py``) and counts the
tasks that are COMPLETED or FAILED until all are. The worker claims, runs and
finishes them, so a drain measures the three together as throughput.

Every call here is public API of horsies 0.7.0 and of the current release,
with the same signature in both: ``Horsies``, ``AppConfig``,
``PostgresConfig``, ``WorkerProfile``, ``IsolatedChildren``,
``IsolatedRecycle``, ``NoTaskLimit``, ``app.task``, ``send``,
``PostgresBroker.close``. The count is one SQL statement on columns that both
schemas have. The harness of each release runs this file unchanged
(``tests/perf/releases/README.md``).
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Final

from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import Connection

from horsies import (
    AppConfig,
    Err,
    Horsies,
    IsolatedChildren,
    IsolatedRecycle,
    NoTaskLimit,
    Ok,
    PostgresConfig,
    TaskError,
    TaskHandle,
    TaskResult,
    TaskSendResult,
    WorkerProfile,
)
from tests.perf.worker_process import (
    StartedWorker,
    WorkerStartFailed,
    cli_script,
    start_worker,
)

DRAIN_TASK_NAME: Final = 'perf.drain.noop'
"""The task of the drain. No other code sends it."""

DRAIN_TASK_COUNT: Final = 10_000
"""The tasks that one drain sends and the worker drains."""

DRAIN_PROFILE_NAME: Final = 'perf-drain'
"""The name of ``DRAIN_WORKER_PROFILE`` in the app, given to ``--profile``."""

DRAIN_ISOLATED_CHILDREN: Final = 4

DRAIN_WORKER_PROFILE: Final = WorkerProfile(
    isolated=IsolatedChildren(
        count=DRAIN_ISOLATED_CHILDREN,
        recycle=IsolatedRecycle(after=NoTaskLimit()),
    ),
)
"""``DRAIN_ISOLATED_CHILDREN`` isolated children, the default execution of a
task, and the defaults of the other settings. A child is not replaced after a
count of tasks (the default replaces it after 100), so a drain does not
measure child starts."""

DRAIN_TIMEOUT: Final = timedelta(minutes=10)
"""The longest time from the start of the worker until every task of a drain
is terminal. A drain that takes longer stops the run."""

DRAIN_COUNT_INTERVAL: Final = timedelta(milliseconds=100)
"""The time between two counts of the terminal tasks."""

WORKER_STOP_TIMEOUT: Final = timedelta(seconds=60)
"""How long a drained worker gets to exit after SIGTERM."""

WORKER_LOGLEVEL: Final = 'WARNING'
"""The ``--loglevel`` of the worker."""

WORKER_APP_LOCATOR: Final = 'tests.perf.worker_drain_app:app'
"""The app that ``horsies worker`` loads: ``worker_drain_app.py``."""

CHECKOUT_ROOT: Final = Path(__file__).resolve().parents[2]
"""The repository root: the working directory of the worker and its
``PYTHONPATH``. ``WORKER_APP_LOCATOR`` names a module of the top-level
package ``tests``. The ``horsies`` CLI adds its working directory to
``sys.path`` only when that folder holds a ``pyproject.toml``, and the
repository root holds none (the Python project is ``clients/python/``)."""

IMPORT_PATH_VARIABLE: Final = 'PYTHONPATH'
"""The variable that puts ``CHECKOUT_ROOT`` first on the worker's ``sys.path``."""

DRAIN_DSN_VARIABLE: Final = 'HORSIES_PERF_DRAIN_DSN'
"""The environment variable that gives the worker the database URL."""

# The alias marks the read as harness bookkeeping: the server counts leave
# out each statement that holds it (tests/perf/counters.py).
_COUNT_TERMINAL_SQL = text("""
    SELECT count(*) AS perf_harness_read
    FROM horsies_task_history
    WHERE task_name = :task_name AND status IN ('COMPLETED', 'FAILED')
""")

# The attempt rows of a live task go first: the current schema refuses the
# delete of a live task that has attempt rows; 0.7.0 cascades it.
_DELETE_ATTEMPTS_SQL = text("""
    DELETE FROM horsies_task_attempts
    WHERE task_id IN (SELECT id FROM horsies_tasks WHERE task_name = :task_name)
""")

_DELETE_LIVE_TASKS_SQL = text("""
    DELETE FROM horsies_tasks WHERE task_name = :task_name
""")

_DELETE_HISTORY_TASKS_SQL = text("""
    DELETE FROM horsies_task_history WHERE task_name = :task_name
""")


@dataclass(frozen=True, slots=True)
class DrainApp:
    """The app of the drain, and the send of its one task."""

    app: Horsies
    send: Callable[[], TaskSendResult[TaskHandle[None]]]


def drain_app(dsn: str) -> DrainApp:
    """The app of ``dsn`` with the no-op task and ``DRAIN_WORKER_PROFILE``.
    No I/O: the broker connects at the first send.

    The harness builds it to send the tasks; ``worker_drain_app.py`` builds it
    in the worker and in each child to run them.
    """
    app = Horsies(
        AppConfig(
            broker=PostgresConfig(database_url=SecretStr(dsn)),
            worker_profiles={DRAIN_PROFILE_NAME: DRAIN_WORKER_PROFILE},
        ),
    )

    @app.task(DRAIN_TASK_NAME)
    def perf_drain_noop() -> TaskResult[None, TaskError]:
        return TaskResult(ok=None)

    return DrainApp(app=app, send=perf_drain_noop.send)


def drain_dsn_from_environment() -> str:
    """The database URL that the harness gives the worker it starts.

    Raises:
        RuntimeError: ``DRAIN_DSN_VARIABLE`` is not set: a process that the
            harness did not start loads the app module of the worker.
    """
    match os.environ.get(DRAIN_DSN_VARIABLE):
        case None:
            raise RuntimeError(
                f'{DRAIN_DSN_VARIABLE} is not set: the measurement harness sets '
                f'it for the worker that it starts'
            )
        case dsn:
            return dsn


class DrainSender:
    """Sends the tasks of a drain with ``send``, as a producer sends them. No
    worker runs in the harness process."""

    def __init__(self, dsn: str) -> None:
        """Build the app of ``dsn``. No I/O: the broker connects at the first
        send."""
        drain = drain_app(dsn)
        self._broker = drain.app.get_broker()
        self._send = drain.send

    def send(self, count: int) -> None:
        """Send the task ``count`` times. Writes ``count`` PENDING task rows.

        Raises:
            RuntimeError: a send returned an error; it names the error and the
                number of the send. The tasks sent before it stay.
        """
        for number in range(1, count + 1):
            match self._send():
                case Ok():
                    continue
                case Err(err_value=error):
                    raise RuntimeError(
                        f'send {number} of {count} of {DRAIN_TASK_NAME} '
                        f'returned {error!r}'
                    )

    def close(self) -> None:
        """Close the broker. A failed close is discarded: the process of the
        run ends after it, and the result of the run does not depend on it."""
        _ = self._broker.close()


def start_drain_worker(dsn: str) -> StartedWorker | WorkerStartFailed:
    """Start ``horsies worker`` with ``DRAIN_PROFILE_NAME`` on the app of
    ``dsn``, from the ``horsies`` script of the environment of the harness.
    Starts a process; it does not wait for the worker to be ready."""
    return start_worker(
        script=cli_script(),
        app_locator=WORKER_APP_LOCATOR,
        profile=DRAIN_PROFILE_NAME,
        loglevel=WORKER_LOGLEVEL,
        environment={
            DRAIN_DSN_VARIABLE: dsn,
            IMPORT_PATH_VARIABLE: str(CHECKOUT_ROOT),
        },
        working_directory=CHECKOUT_ROOT,
        clock_ns=time.perf_counter_ns,
    )


def count_terminal_tasks(connection: Connection) -> int:
    """The tasks of ``DRAIN_TASK_NAME`` that are COMPLETED or FAILED. Reads,
    then ends its transaction, so the session holds none while the worker
    writes."""
    terminal = int(
        connection.execute(
            _COUNT_TERMINAL_SQL,
            {'task_name': DRAIN_TASK_NAME},
        ).scalar_one()
    )
    connection.commit()
    return terminal


def delete_drained_tasks(connection: Connection) -> None:
    """Delete every task row of ``DRAIN_TASK_NAME``, live and in history,
    with the attempt rows of the live ones, and commit."""
    parameters = {'task_name': DRAIN_TASK_NAME}
    connection.execute(_DELETE_ATTEMPTS_SQL, parameters)
    connection.execute(_DELETE_LIVE_TASKS_SQL, parameters)
    connection.execute(_DELETE_HISTORY_TASKS_SQL, parameters)
    connection.commit()
