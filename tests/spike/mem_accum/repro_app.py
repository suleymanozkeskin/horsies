"""Repro app instance for the per-child memory-recycle spike.

A horsies worker started against this instance runs the retained-allocation
tasks in ``repro_tasks``. The driver (``run_repro``) compares child rotation
and process-tree RSS under no recycling, count recycling (``recycle.after``),
and memory recycling (``recycle.above``): one worker profile for each.

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
from horsies.core.models.worker_profile import (
    IsolatedChildren,
    IsolatedRecycle,
    MemoryLimit,
    NoTaskLimit,
    TaskLimit,
    WorkerProfile,
)

_DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ.get("DB_PASSWORD", "")}@localhost:5432/horsies',
)

PROCESSES = 2
COUNT_N = 8
MEMORY_THRESHOLD_MB = 250

# One profile per driver mode; the driver passes the mode name as --profile.
REPRO_PROFILES = {
    'none': WorkerProfile(
        isolated=IsolatedChildren(
            count=PROCESSES,
            recycle=IsolatedRecycle(after=NoTaskLimit()),
        ),
    ),
    'count': WorkerProfile(
        isolated=IsolatedChildren(
            count=PROCESSES,
            recycle=IsolatedRecycle(after=TaskLimit(tasks=COUNT_N)),
        ),
    ),
    'memory': WorkerProfile(
        isolated=IsolatedChildren(
            count=PROCESSES,
            recycle=IsolatedRecycle(above=MemoryLimit(mb=MEMORY_THRESHOLD_MB)),
        ),
    ),
}

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=_DB_URL,
        pool_size=5,
        max_overflow=5,
    ),
    worker_profiles=REPRO_PROFILES,
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker
broker.app = app

# Register tasks in the client process and for worker-subprocess discovery.
from tests.spike.mem_accum import repro_tasks  # noqa: E402,F401

app.discover_tasks(['tests.spike.mem_accum.repro_tasks'])
