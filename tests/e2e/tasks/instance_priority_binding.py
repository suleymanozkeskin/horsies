"""App instance for the subworkflow priority-binding claim-order e2e.

A CUSTOM-mode app with a single-concurrency 'serial' queue at priority 30 so a
subworkflow child and a direct-send task contend for one slot in claim order,
plus a 'ctrl' queue for the worker readiness probe (kept off the serial slot).
"""

from __future__ import annotations

import os

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode, CustomQueueConfig

DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ["DB_PASSWORD"]}@localhost:5432/horsies',
)

config = AppConfig(
    queue_mode=QueueMode.CUSTOM,
    custom_queues=[
        # Single slot forces serialized claim order on this queue.
        CustomQueueConfig(name='serial', priority=30, max_concurrency=1),
        # Readiness/control queue, kept separate so the probe never holds the
        # serial slot.
        CustomQueueConfig(name='ctrl', priority=1, max_concurrency=4),
    ],
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
from tests.e2e.tasks import priority_binding  # noqa: E402, F401

app.discover_tasks(['tests.e2e.tasks.priority_binding'])
