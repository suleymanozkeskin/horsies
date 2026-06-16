"""Repro app instance for the per-child memory-recycle spike.

A horsies worker started against this instance runs the retained-allocation
tasks in ``repro_tasks``. The driver (``run_repro``) compares child rotation
and process-tree RSS under no recycling, count recycling
(``--max-tasks-per-child``), and memory recycling
(``--max-memory-per-child-mb``).

Mirrors tests/e2e/tasks/instance.py: define the app + broker, then import the
task module and register it for child-process discovery.
"""

from __future__ import annotations

import os

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode

_DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ.get("DB_PASSWORD", "")}@localhost:5432/horsies',
)

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=_DB_URL,
        pool_size=5,
        max_overflow=5,
    ),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker
broker.app = app

# Register tasks in the client process and for worker-subprocess discovery.
from tests.spike.mem_accum import repro_tasks  # noqa: E402,F401

app.discover_tasks(['tests.spike.mem_accum.repro_tasks'])
