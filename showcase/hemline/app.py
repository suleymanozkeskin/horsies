# showcase/hemline/app.py
"""The Hemline horsies application: queues, recovery tuning, and schedules.

Run every process from the repository root:

    uv run horsies worker showcase.hemline.app:app --processes 12
    uv run horsies scheduler showcase.hemline.app:app
    uv run horsies web showcase.hemline.app:app --enable-actions
    uv run horsies check showcase.hemline.app:app
"""

from __future__ import annotations

from typing import Final

from pydantic import SecretStr

from horsies import (
    AppConfig,
    CustomQueueConfig,
    Horsies,
    IntervalSchedule,
    PostgresConfig,
    QueueMode,
    RecoveryConfig,
    ScheduleConfig,
    TaskSchedule,
)

from . import domain, tuning
from .settings import DATABASE

QUEUE_PAYMENTS: Final[str] = 'payments'
QUEUE_FULFILLMENT: Final[str] = 'fulfillment'
QUEUE_NOTIFICATIONS: Final[str] = 'notifications'
QUEUE_ANALYTICS: Final[str] = 'analytics'

# Four queues, four priorities, four caps. `payments` is claimed first and
# stays responsive; `notifications` is capped at 3 so a marketing blast
# builds a visible PENDING backlog on one queue while the rest run clear.
QUEUES: Final[list[CustomQueueConfig]] = [
    CustomQueueConfig(name=QUEUE_PAYMENTS, priority=1, max_concurrency=4),
    CustomQueueConfig(name=QUEUE_FULFILLMENT, priority=10, max_concurrency=8),
    CustomQueueConfig(name=QUEUE_NOTIFICATIONS, priority=50, max_concurrency=3),
    CustomQueueConfig(name=QUEUE_ANALYTICS, priority=90, max_concurrency=2),
]

# Demo-tuned recovery: snapshots every 10 s keep the worker charts moving,
# the reaper polls at the same cadence, and terminal rows are kept for a day
# so yesterday's run is still readable. Everything else is the default.
RECOVERY: Final[RecoveryConfig] = RecoveryConfig(
    worker_state_snapshot_interval_ms=10_000,
    check_interval_ms=10_000,
    terminal_record_retention_hours=24,
)

SCHEDULES: Final[ScheduleConfig] = ScheduleConfig(
    schedules=[
        TaskSchedule(
            name='supplier-feed-atlas',
            task_name='sync_supplier_feed',
            pattern=IntervalSchedule(seconds=tuning.SUPPLIER_FEED_INTERVAL_SECONDS),
            kwargs={'supplier': tuning.SUPPLIERS[0]},
            catch_up_missed=False,
        ),
    ],
)

app = Horsies(
    config=AppConfig(
        queue_mode=QueueMode.CUSTOM,
        custom_queues=QUEUES,
        broker=PostgresConfig(database_url=SecretStr(DATABASE.url)),
        recovery=RECOVERY,
        schedule=SCHEDULES,
        # One global mapping: any task that lets a KeyError escape reports
        # DATA_CORRUPTION. `apply_promotions` shows it next to an unmapped
        # exception, so the two interception paths sit side by side.
        exception_mapper={KeyError: domain.DATA_CORRUPTION},
        # Infra retry for the send/start path only — execution retries are a
        # per-task RetryPolicy concern.
        resend_on_transient_err=True,
    ),
)

app.discover_tasks([
    'showcase.hemline.tasks.payments',
    'showcase.hemline.tasks.inventory',
    'showcase.hemline.tasks.orders',
    'showcase.hemline.tasks.promotions',
    'showcase.hemline.tasks.shipping',
    'showcase.hemline.tasks.notify',
    'showcase.hemline.workflows.order_fulfillment',
    'showcase.hemline.workflows.shipping',
])
