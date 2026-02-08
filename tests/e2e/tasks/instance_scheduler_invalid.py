"""Scheduler instance with invalid schedule (nonexistent task) for e2e tests.

This instance is intentionally misconfigured to test scheduler fail-fast behavior.
The scheduler should fail to start because the scheduled task doesn't exist.
"""

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

# Schedule config referencing a task that does NOT exist
schedule_config = ScheduleConfig(
    enabled=True,
    schedules=[
        TaskSchedule(
            name='invalid_schedule_nonexistent_task',
            task_name='this_task_does_not_exist_anywhere',  # <-- Nonexistent!
            pattern=IntervalSchedule(seconds=5),
            timezone='UTC',
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

# NOTE: We intentionally do NOT register any tasks here.
# The scheduler should fail at startup because 'this_task_does_not_exist_anywhere'
# is not in the task registry.
