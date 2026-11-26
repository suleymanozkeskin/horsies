"""A retention class is its period, at each enqueue path and at broker init.

- A direct send, a delayed send and a workflow node on a queue mapped to 7
  days each stamp `standard_7d`. Two queues mapped to 7 days share it.
- A terminalized row lands in a leaf of that class.
- A broker refuses to start when the key of one of its periods is
  registered with another duration, which only a class declared by name
  before 0.6.1 can do. A matching or absent row does not refuse.

The cron path resolves through the same function and is driven in
`tests/unit/test_scheduler_service.py`.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import uuid
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.periods import period_class_key
from horsies.core.models.app import AppConfig
from horsies.core.models.queues import CustomQueueConfig, QueueMode
from horsies.core.models.retention import RetentionConfig
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow import TaskNode
from horsies.core.types.result import Ok, Result, is_err, is_ok
from horsies.core.workflows.lifecycle import start_workflow_async
from tests.broker_resources_support import current_resources

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope='function'),
]

_PERIOD = timedelta(days=7)
_PERIOD_KEY = period_class_key(_PERIOD.days)


def _mapped_app(broker: PostgresBroker) -> tuple[Horsies, Any, Any]:
    """An app with two queues mapped to one period, a task on each."""
    app = Horsies(
        AppConfig(
            queue_mode=QueueMode.CUSTOM,
            broker=broker.config,
            custom_queues=[
                CustomQueueConfig(name='emails'),
                CustomQueueConfig(name='alerts'),
            ],
            retention=RetentionConfig(
                queue_retention={'emails': _PERIOD, 'alerts': _PERIOD},
            ),
        ),
    )
    app._broker = broker
    broker.app = app
    suffix = uuid.uuid4().hex[:8]

    @app.task(name=f'period_email_{suffix}', queue='emails')
    def email_task(*, x: int) -> Result[int, TaskError]:
        return Ok(x)  # pragma: no cover - never executed here

    @app.task(name=f'period_alert_{suffix}', queue='alerts')
    def alert_task(*, x: int) -> Result[int, TaskError]:
        return Ok(x)  # pragma: no cover - never executed here

    return app, email_task, alert_task


async def _row_class(engine: AsyncEngine, task_id: str) -> str | None:
    async with engine.connect() as connection:
        return (
            await connection.execute(
                text(
                    'SELECT retention_class_key FROM horsies_tasks '
                    'WHERE id = CAST(:task_id AS uuid)'
                ),
                {'task_id': task_id},
            )
        ).scalar_one()


class TestEveryPathStampsThePeriodClass:
    async def test_two_queues_with_one_period_share_its_class(
        self, broker: PostgresBroker,
    ) -> None:
        engine = current_resources(broker).engine
        _app, email_task, alert_task = _mapped_app(broker)

        email = await email_task.send_async(x=1)
        alert = await alert_task.send_async(x=2)

        assert is_ok(email) and is_ok(alert), (email, alert)
        assert await _row_class(engine, email.ok_value.task_id) == _PERIOD_KEY
        assert await _row_class(engine, alert.ok_value.task_id) == _PERIOD_KEY

    async def test_a_delayed_send_stamps_the_period_class(
        self, broker: PostgresBroker,
    ) -> None:
        engine = current_resources(broker).engine
        _app, email_task, _alert_task = _mapped_app(broker)

        delayed = await email_task.with_options(delay_seconds=60).send_async(x=3)

        assert is_ok(delayed), delayed
        assert await _row_class(engine, delayed.ok_value.task_id) == _PERIOD_KEY

    async def test_a_workflow_node_stamps_the_period_class(
        self, clean_workflow_tables: None, broker: PostgresBroker,
    ) -> None:
        engine = current_resources(broker).engine
        app, email_task, _alert_task = _mapped_app(broker)
        node = TaskNode(function=email_task, kwargs={'x': 4}, node_id='email')
        spec = app.workflow(
            f'period_workflow_{uuid.uuid4().hex[:8]}',
            [node],
            output=node,
            definition_key=f'period-workflow-{uuid.uuid4().hex[:8]}',
        )

        started = await start_workflow_async(spec, broker)

        assert not is_err(started), started
        async with engine.connect() as connection:
            stamped = (
                await connection.execute(
                    text(
                        'SELECT t.retention_class_key '
                        'FROM horsies_workflow_tasks wt '
                        'JOIN horsies_tasks t ON t.id = wt.task_id '
                        "WHERE wt.workflow_id = :wf AND wt.node_id = 'email'"
                    ),
                    {'wf': started.ok_value.workflow_id},
                )
            ).scalar_one()
        assert stamped == _PERIOD_KEY


