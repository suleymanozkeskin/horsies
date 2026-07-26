# showcase/acme/app.py
"""The Acme Clothing horsies application: queues, recovery tuning, and schedules.

Run every process from the repository root:

    uv run horsies worker showcase.acme.app:app --processes 12
    uv run horsies scheduler showcase.acme.app:app
    uv run horsies web showcase.acme.app:app --enable-actions
    uv run horsies check showcase.acme.app:app
"""

from __future__ import annotations

from datetime import time
from typing import Final

from pydantic import SecretStr

from horsies import (
    AppConfig,
    ByMonthDay,
    BothDays,
    CronEnumValues,
    CronEvery,
    CronRange,
    CronSchedule,
    CronStep,
    CronValues,
    CustomQueueConfig,
    DailySchedule,
    EveryDay,
    Horsies,
    HourlySchedule,
    IntervalSchedule,
    Month,
    MonthlySchedule,
    PostgresConfig,
    QueueMode,
    RecoveryConfig,
    ScheduleConfig,
    TaskSchedule,
    Weekday,
    WeeklySchedule,
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

def _supplier_feeds() -> list[TaskSchedule]:
    """One interval schedule per supplier, deliberately out of phase."""
    return [
        TaskSchedule(
            name=f'supplier-feed-{supplier}',
            task_name='sync_supplier_feed',
            pattern=IntervalSchedule(
                seconds=tuning.SUPPLIER_FEED_INTERVAL_SECONDS + index * 30,
            ),
            kwargs={'supplier': supplier},
            catch_up_missed=False,
        )
        for index, supplier in enumerate(tuning.SUPPLIERS)
    ]


def _regional_rollups() -> list[TaskSchedule]:
    """One hourly rollup per region, staggered five minutes apart."""
    return [
        TaskSchedule(
            name=f'rollup-{region}',
            task_name='regional_rollup',
            pattern=HourlySchedule(minute=10 + index * 5, second=0),
            kwargs={'region': region},
            catch_up_missed=False,
        )
        for index, region in enumerate(tuning.REGIONS)
    ]


def _cache_warms() -> list[TaskSchedule]:
    """Edge cache warms, one per region, on staggered intervals."""
    return [
        TaskSchedule(
            name=f'cache-warm-{region}',
            task_name='warm_cache_edge',
            pattern=IntervalSchedule(
                minutes=tuning.CACHE_WARM_INTERVAL_MINUTES + index,
            ),
            kwargs={'campaign_id': f'steady-{region}'},
            catch_up_missed=False,
        )
        for index, region in enumerate(tuning.REGIONS)
    ]


# Every pattern type, on jobs that would really run on that shape. Three are
# disabled on purpose — a schedules tab where everything is enabled does not
# show you what a disabled schedule looks like. `catch_up_missed` is False
# throughout: restarting a demo should not replay a backlog of missed runs.
SCHEDULES: Final[ScheduleConfig] = ScheduleConfig(
    schedules=[
        *_supplier_feeds(),
        *_regional_rollups(),
        *_cache_warms(),
        TaskSchedule(
            name='search-prewarm',
            task_name='prewarm_search',
            pattern=IntervalSchedule(
                minutes=tuning.SEARCH_PREWARM_INTERVAL_MINUTES,
            ),
            kwargs={'campaign_id': 'steady-state'},
            catch_up_missed=False,
        ),
        TaskSchedule(
            name='abandoned-cart-sweep',
            task_name='abandoned_cart_sweep',
            pattern=HourlySchedule(minute=tuning.ABANDONED_CART_MINUTE, second=0),
            kwargs={'older_than_minutes': tuning.ABANDONED_CART_AGE_MINUTES},
            catch_up_missed=False,
        ),
        TaskSchedule(
            name='retention-audit-hourly',
            task_name='retention_audit',
            pattern=HourlySchedule(minute=50, second=0),
            kwargs={'older_than_days': tuning.RETENTION_AUDIT_DAYS},
            catch_up_missed=False,
        ),
        TaskSchedule(
            name='sales-rollup-daily',
            task_name='sales_rollup',
            pattern=DailySchedule(time=time(tuning.SALES_ROLLUP_HOUR, 0, 0)),
            kwargs={'window': 'daily'},
            catch_up_missed=False,
        ),
        TaskSchedule(
            name='retention-audit-daily',
            task_name='retention_audit',
            pattern=DailySchedule(time=time(3, 30, 0)),
            kwargs={'older_than_days': 90},
            catch_up_missed=False,
        ),
        TaskSchedule(
            name='reconcile-daily',
            task_name='reconcile_payments',
            pattern=DailySchedule(time=time(4, 0, 0)),
            kwargs={'window': 'daily'},
            catch_up_missed=False,
        ),
        TaskSchedule(
            name='winback-blast',
            task_name='marketing_blast',
            pattern=DailySchedule(time=time(9, 0, 0)),
            kwargs={'segment': 'winback'},
            catch_up_missed=False,
        ),
        # Disabled: the newsletter is drafted by hand, so the schedule exists
        # but must not fire on its own.
        TaskSchedule(
            name='newsletter-blast',
            task_name='marketing_blast',
            pattern=DailySchedule(time=time(10, 0, 0)),
            kwargs={'segment': 'newsletter'},
            enabled=False,
            catch_up_missed=False,
        ),
        TaskSchedule(
            name='weekly-sales-review',
            task_name='sales_rollup',
            pattern=WeeklySchedule(days=[Weekday.MONDAY], time=time(6, 0)),
            kwargs={'window': 'weekly'},
            catch_up_missed=False,
        ),
        TaskSchedule(
            name='weekly-supplier-audit',
            task_name='sync_supplier_feed',
            pattern=WeeklySchedule(
                days=[Weekday.WEDNESDAY], time=time(7, 0),
            ),
            kwargs={'supplier': tuning.SUPPLIERS[0]},
            catch_up_missed=False,
        ),
        TaskSchedule(
            name='weekend-flash-prep',
            task_name='prewarm_search',
            pattern=WeeklySchedule(
                days=[Weekday.FRIDAY, Weekday.SATURDAY], time=time(12, 0),
            ),
            kwargs={'campaign_id': 'weekend'},
            catch_up_missed=False,
        ),
        # Disabled: superseded by the hourly audit, kept for the quarter-end.
        TaskSchedule(
            name='weekly-retention-audit',
            task_name='retention_audit',
            pattern=WeeklySchedule(days=[Weekday.SUNDAY], time=time(2, 0)),
            kwargs={'older_than_days': 180},
            enabled=False,
            catch_up_missed=False,
        ),
        TaskSchedule(
            name='monthly-close',
            task_name='reconcile_payments',
            pattern=MonthlySchedule(day=1, time=time(5, 0)),
            kwargs={'window': 'monthly'},
            catch_up_missed=False,
        ),
        TaskSchedule(
            name='monthly-catalog-audit',
            task_name='retention_audit',
            pattern=MonthlySchedule(day=15, time=time(8, 0)),
            kwargs={'older_than_days': 365},
            catch_up_missed=False,
        ),
        TaskSchedule(
            name='monthly-markdown-review',
            task_name='sales_rollup',
            pattern=MonthlySchedule(day=28, time=time(11, 0)),
            kwargs={'window': 'monthly'},
            catch_up_missed=False,
        ),
        # "Every 4 hours at minute 15" as typed cron terms — no cron string,
        # and `day=EveryDay()` makes the day-of-month / day-of-week choice
        # explicit rather than inheriting cron's ambiguous default.
        TaskSchedule(
            name='payment-reconciliation',
            task_name='reconcile_payments',
            pattern=CronSchedule(
                minute=[CronValues(values=[tuning.RECONCILE_MINUTE])],
                hour=[CronStep(step=tuning.RECONCILE_HOUR_STEP)],
                month=[CronEvery()],
                day=EveryDay(),
            ),
            kwargs={'window': '4h'},
            catch_up_missed=False,
        ),
        TaskSchedule(
            name='price-sync-quarter-hour',
            task_name='warm_cache_edge',
            pattern=CronSchedule(
                minute=[CronStep(step=tuning.PRICE_SYNC_MINUTE_STEP)],
                hour=[CronEvery()],
                month=[CronEvery()],
                day=EveryDay(),
            ),
            kwargs={'campaign_id': 'price-sync'},
            catch_up_missed=False,
        ),
        # Friday the 13th, not "the 13th or a Friday" — `BothDays` is how that
        # distinction is stated, and cron cannot state it at all.
        TaskSchedule(
            name='fraud-review-friday-13th',
            task_name='reconcile_payments',
            pattern=CronSchedule(
                minute=[CronValues(values=[0])],
                hour=[CronValues(values=[9])],
                month=[CronEvery()],
                day=BothDays(
                    day_of_month=[CronValues(values=[13])],
                    day_of_week=[CronEnumValues[Weekday](values=[Weekday.FRIDAY])],
                ),
            ),
            kwargs={'window': 'fraud-review'},
            catch_up_missed=False,
        ),
        # Disabled: this export kills its own process by design. The chaos
        # scenario drives it deliberately; a schedule must not.
        TaskSchedule(
            name='nightly-export',
            task_name='flaky_export',
            pattern=CronSchedule(
                minute=[CronValues(values=[40])],
                hour=[CronRange(start=1, end=5, step=2)],
                month=[CronEvery()],
                day=EveryDay(),
            ),
            kwargs={'export_id': 'nightly'},
            enabled=False,
            catch_up_missed=False,
        ),
        TaskSchedule(
            name='quarterly-supplier-review',
            task_name='sync_supplier_feed',
            pattern=CronSchedule(
                minute=[CronValues(values=[30])],
                hour=[CronValues(values=[6])],
                month=[
                    CronEnumValues[Month](
                        values=[Month.JANUARY, Month.APRIL, Month.JULY, Month.OCTOBER],
                    ),
                ],
                day=ByMonthDay(day_of_month=[CronValues(values=[1])]),
            ),
            kwargs={'supplier': tuning.SUPPLIERS[2]},
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
    'showcase.acme.tasks.payments',
    'showcase.acme.tasks.inventory',
    'showcase.acme.tasks.orders',
    'showcase.acme.tasks.promotions',
    'showcase.acme.tasks.shipping',
    'showcase.acme.tasks.returns',
    'showcase.acme.tasks.notify',
    'showcase.acme.tasks.analytics',
    'showcase.acme.workflows.order_fulfillment',
    'showcase.acme.workflows.shipping',
    'showcase.acme.workflows.returns_review',
    'showcase.acme.workflows.restock',
    'showcase.acme.workflows.flash_sale',
    'showcase.acme.workflows.catalog_import',
    'showcase.acme.workflows.daily_report',
    'showcase.acme.workflows.price_sync',
    'showcase.acme.workflows.customer_winback',
    'showcase.acme.workflows.warehouse_transfer',
    'showcase.acme.workflows.seasonal_markdown',
    'showcase.acme.workflows.fraud_review',
])
