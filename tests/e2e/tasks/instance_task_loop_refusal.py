"""App instance whose worker children refuse the task loop on demand.

The ``on_child_process_start`` hook wraps ``asyncio.new_event_loop`` in each
child. While the budget file named by ``HORSIES_E2E_LOOP_REFUSALS`` holds a
count above 0, a loop creation lowers the count by one and raises
``OSError(EMFILE)``, as a process at its file descriptor limit does. With no
such variable the hook changes nothing.
"""

from __future__ import annotations

import asyncio
import errno
import os
from typing import Final

from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode

from tests.e2e.helpers.env import e2e_database_url

REFUSALS_ENV: Final = 'HORSIES_E2E_LOOP_REFUSALS'

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=SecretStr(DB_URL),
        pool_size=5,
        max_overflow=5,
    ),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker  # pyright: ignore[reportPrivateUsage]
broker.app = app


def _take_one_refusal(budget_path: str) -> bool:
    """Lower the budget by one when it is above 0. Return whether it was."""
    with open(budget_path, 'r+', encoding='ascii') as budget:
        left = int(budget.read() or '0')
        if left <= 0:
            return False
        budget.seek(0)
        budget.truncate()
        budget.write(str(left - 1))
        return True


@app.on_child_process_start
def refuse_loops_on_demand() -> None:
    budget_path = os.environ.get(REFUSALS_ENV)
    if budget_path is None:
        return
    real_new_event_loop = asyncio.new_event_loop

    def new_event_loop() -> asyncio.AbstractEventLoop:
        if _take_one_refusal(budget_path):
            raise OSError(errno.EMFILE, 'Too many open files')
        return real_new_event_loop()

    asyncio.new_event_loop = new_event_loop


@app.task(task_name='e2e_loop_refusal_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


@app.task(task_name='e2e_loop_refusal_sync_pid')
def sync_pid() -> TaskResult[int, TaskError]:
    """A ``def`` task: it never creates the task loop."""
    return TaskResult(ok=os.getpid())


@app.task(task_name='e2e_loop_refusal_async_pid')
async def async_pid() -> TaskResult[int, TaskError]:
    """An ``async def`` task: its child creates the task loop first."""
    await asyncio.sleep(0)
    return TaskResult(ok=os.getpid())


node_async_pid = TaskNode(fn=async_pid)

spec_async_node = app.workflow(
    name='e2e_loop_refusal_async_node',
    tasks=[node_async_pid],
    output=node_async_pid,
    definition_key='tests.e2e.tasks.instance_task_loop_refusal.async_node.v1',
)


app.discover_tasks(['tests.e2e.tasks.instance_task_loop_refusal'])
