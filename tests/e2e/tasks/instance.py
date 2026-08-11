"""Default-mode app instance for e2e tests."""

from __future__ import annotations

import os

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode

from tests.e2e.helpers.env import e2e_database_url

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=DB_URL,
        pool_size=5,
        max_overflow=5,
    ),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker
broker.app = app

# Import tasks to register them in current process
from tests.e2e.tasks import basic  # noqa: F401
from tests.e2e.tasks import retry  # noqa: F401
from tests.e2e.tasks import workflows  # noqa: F401

# Also register for worker subprocess discovery
app.discover_tasks(
    [
        'tests.e2e.tasks.basic',
        'tests.e2e.tasks.retry',
        'tests.e2e.tasks.workflows',
    ]
)
