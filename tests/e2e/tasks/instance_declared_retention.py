"""App instance declaring a retention class, for the assembly e2e.

The declaration is the whole point: a real worker booted from this module
must register `E2E_CLASS_KEY` before anything else can be asserted, and
it can only do that if every link from `AppConfig.retention` through the
CLI, `WorkerConfig`, the reaper loop and its pass reaches the registrar.
"""

from __future__ import annotations

import os
from datetime import timedelta

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.retention import RetentionClassConfig, RetentionConfig

from tests.e2e.helpers.env import e2e_database_url

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')

E2E_CLASS_KEY = 'e2e_declared_5d'
E2E_CLASS_DURATION = timedelta(days=5)

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=DB_URL,
        pool_size=5,
        max_overflow=5,
    ),
    recovery=RecoveryConfig(check_interval_ms=1_000),
    retention=RetentionConfig(
        # The floor, so the first maintenance tick lands promptly and the
        # test needs no clock manipulation.
        partition_maintenance_interval_s=60,
        retention_classes=(
            RetentionClassConfig(
                key=E2E_CLASS_KEY, duration=E2E_CLASS_DURATION
            ),
        ),
    ),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker
broker.app = app

from tests.e2e.tasks import basic  # noqa: E402, F401

app.discover_tasks(
    [
        'tests.e2e.tasks.basic',
    ]
)
