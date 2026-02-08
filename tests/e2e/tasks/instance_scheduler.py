"""Scheduler-enabled app instance for e2e tests (Layer 3)."""

from __future__ import annotations

import os

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.schedule import (
    ScheduleConfig,
    TaskSchedule,
    IntervalSchedule,
)

DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ["DB_PASSWORD"]}@localhost:5432/horsies',
)

# Schedule config with a short interval for testing
# Note: Schedules are defined dynamically in tests, but we need at least
# one valid schedule for the scheduler to start properly.
schedule_config = ScheduleConfig(
    enabled=True,
    schedules=[
        TaskSchedule(
            name='e2e_scheduled_simple_interval',
            task_name='e2e_scheduled_simple',
            pattern=IntervalSchedule(seconds=2),
            timezone='UTC',
            catch_up_missed=False,
        ),
        TaskSchedule(
            name='e2e_disabled_schedule',
            task_name='e2e_scheduled_simple',
            pattern=IntervalSchedule(seconds=1),
            enabled=False,
        ),
        TaskSchedule(
            name='e2e_schedule_with_args',
            task_name='e2e_scheduled_with_args',
            pattern=IntervalSchedule(seconds=2),
            args=(42,),
        ),
        TaskSchedule(
            name='e2e_schedule_catch_up',
            task_name='e2e_catch_up_task',
            pattern=IntervalSchedule(seconds=1),
            catch_up_missed=True,
        ),
    ],
    check_interval_seconds=1,
)

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=DB_URL,
        pool_size=5,
        max_overflow=5,
    ),
    schedule=schedule_config,
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker

# Import tasks to register them in current process
from tests.e2e.tasks import scheduler as scheduler_tasks  # noqa: F401, E402

# Also register for worker subprocess discovery
app.discover_tasks(['tests.e2e.tasks.scheduler'])
