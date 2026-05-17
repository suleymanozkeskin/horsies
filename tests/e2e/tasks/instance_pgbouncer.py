'''PgBouncer-mode app instance for e2e smoke tests.'''

from __future__ import annotations

import os

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode


DB_URL = os.environ["HORSIES_TEST_DATABASE_URL_TRANSACTION"]
SESSION_DB_URL = os.environ["HORSIES_TEST_DATABASE_URL_DIRECT"]

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=DB_URL,
        session_database_url=SESSION_DB_URL,
        pgbouncer_transaction_mode=True,
        pool_size=5,
        max_overflow=5,
    ),
)

app = Horsies(config)
broker = app.get_broker()

app.discover_tasks(
    [
        "tests.e2e.tasks.pgbouncer_tasks",
        "tests.e2e.tasks.pgbouncer_workflows",
    ]
)
