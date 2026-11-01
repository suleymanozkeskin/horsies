"""App instance for the dynamic fan-out subworkflow e2e test.

The fan-out tasks and workflow definitions live on their own app, so the
shared e2e instance (``tests.e2e.tasks.instance``) does not import them.
"""

from __future__ import annotations

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

# Register tasks/definitions in this process and for worker subprocess discovery.
from tests.e2e.tasks import dynamic_fanout  # noqa: E402, F401

app.discover_tasks(['tests.e2e.tasks.dynamic_fanout'])
