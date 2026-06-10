"""Repro app instance for the app-owned-connection accumulation finding.

A horsies worker started against this instance runs task code that checks out
an app-owned engine connection (see ``repro_db``). The repro driver
(``run_repro``) measures whether those connections accumulate across worker
children and fail to shrink after the task burst completes.

Mirrors the structure of tests/e2e/tasks/instance.py: define the app + broker,
then import the task module and register it for child-process discovery.
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
from tests.spike.conn_accum import repro_tasks  # noqa: E402,F401

app.discover_tasks(['tests.spike.conn_accum.repro_tasks'])
