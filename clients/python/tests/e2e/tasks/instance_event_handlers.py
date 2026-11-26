"""App instance with an event handler and a handler child start hook.

The worker of this app runs a handler child. The handler of `AttemptEnded`
and the start hook write one line each to the file that the environment
variable `HANDLER_OUT_ENV` names: the hook `started <pid>`, the handler
`<task_id> <attempt> <end>` for each attempt that ended.
"""

from __future__ import annotations

import os
from typing import Final

from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Ok, Result
from horsies.telemetry import AttemptEnded

from tests.e2e.helpers.env import e2e_database_url
from tests.e2e.tasks.profiles import E2E_WORKER_PROFILES

HANDLER_OUT_ENV: Final = 'HORSIES_E2E_HANDLER_OUT'
"""The environment variable that names the file of the handler lines."""

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')

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
app._broker = broker  # pyright: ignore[reportPrivateUsage]
broker.app = app


def _write_line(line: str) -> None:
    with open(os.environ[HANDLER_OUT_ENV], 'a', encoding='utf-8') as out:
        _ = out.write(f'{line}\n')


@app.on_handler_child_start
def mark_handler_child() -> None:
    _write_line(f'started {os.getpid()}')


@app.on_event(AttemptEnded)
def record_attempt_end(event: AttemptEnded) -> None:
    _write_line(f'{event.task_id} {event.attempt} {type(event.end).__name__}')


@app.task(task_name='e2e_event_handlers_healthcheck')
def healthcheck() -> Result[str, TaskError]:
    return Ok('ready')


@app.task(task_name='e2e_event_handlers_add')
def add(*, left: int, right: int) -> Result[int, TaskError]:
    return Ok(left + right)


app.discover_tasks(['tests.e2e.tasks.instance_event_handlers'])
