# showcase/acme/tasks/analytics.py
"""Reporting, imports, and the deliberately crash-prone export.

`sales_rollup` and `abandoned_cart_sweep` are the scheduled reporting jobs.
`catalog_import_chunk` is the long unit of work the cancellable import fans out
over. `flaky_export` is the chaos scenario's tool: it kills its own process, so
the worker never receives a result and reports `WORKER_CRASHED`.
"""

from __future__ import annotations

import hashlib
import os

from horsies import (
    Err,
    Ok,
    OperationalErrorCode,
    RetryPolicy,
    TaskError,
    TaskResult,
)

from .. import simulate, store, tuning
from ..app import QUEUE_ANALYTICS, app
from ..domain import (
    AbandonedCartSweep,
    CatalogChunk,
    ExportManifest,
    RegionalRollup,
    RetentionAudit,
    SalesRollup,
)
from . import store_failure


@app.task(
    'sales_rollup',
    queue_name=QUEUE_ANALYTICS,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def sales_rollup(*, window: str) -> TaskResult[SalesRollup, TaskError]:
    """Aggregate the day's orders. Runs on a DailySchedule at 03:00."""
    simulate.perform(tuning.SALES_ROLLUP_WORK, window, 'rollup')

    match store.sales_totals():
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(totals):
            orders, gross_cents, captured_cents = totals
            return TaskResult(
                ok=SalesRollup(
                    orders_counted=orders,
                    gross_cents=gross_cents,
                    captured_cents=captured_cents,
                ),
            )


@app.task(
    'abandoned_cart_sweep',
    queue_name=QUEUE_ANALYTICS,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def abandoned_cart_sweep(
    *,
    older_than_minutes: int,
) -> TaskResult[AbandonedCartSweep, TaskError]:
    """Count orders that never reached a capture. Runs hourly at :05."""
    simulate.perform(tuning.ABANDONED_CART_WORK, str(older_than_minutes), 'sweep')

    match store.abandoned_orders(older_than_minutes):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(result):
            stranded, oldest = result
            return TaskResult(
                ok=AbandonedCartSweep(swept=stranded, oldest_order_id=oldest),
            )


@app.task(
    'regional_rollup',
    queue_name=QUEUE_ANALYTICS,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def regional_rollup(*, region: str) -> TaskResult[RegionalRollup, TaskError]:
    """Roll up sales for one region. One schedule per region."""
    simulate.perform(tuning.SALES_ROLLUP_WORK, region, 'regional')

    match store.sales_totals():
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(totals):
            orders, gross_cents, _captured = totals
            share = simulate.integer(15, 55, region, 'share')
            return TaskResult(
                ok=RegionalRollup(
                    region=region,
                    orders_counted=orders * share // 100,
                    gross_cents=gross_cents * share // 100,
                ),
            )


@app.task(
    'retention_audit',
    queue_name=QUEUE_ANALYTICS,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def retention_audit(*, older_than_days: int) -> TaskResult[RetentionAudit, TaskError]:
    """Report what a retention policy would prune.

    Reports only — it never deletes. horsies prunes its own terminal rows on
    the schedule `RecoveryConfig.terminal_record_retention_hours` sets; this is
    the application's own data, and deleting a demo's history mid-demo would be
    a poor showcase.
    """
    simulate.perform(tuning.ABANDONED_CART_WORK, str(older_than_days), 'retention')

    match store.sales_totals():
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(totals):
            orders, _gross, _captured = totals
            return TaskResult(
                ok=RetentionAudit(
                    older_than_days=older_than_days,
                    orders_examined=orders,
                    rows_prunable=0,
                ),
            )


@app.task(
    'catalog_import_chunk',
    queue_name=QUEUE_ANALYTICS,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def catalog_import_chunk(
    *,
    import_id: str,
    chunk_index: int,
) -> TaskResult[CatalogChunk, TaskError]:
    """Import one chunk of a supplier catalog.

    Slow on purpose. The import fans out 40 of these onto a queue capped at 2,
    which is what makes the run long enough to cancel and watch drain.
    """
    simulate.perform(tuning.CATALOG_IMPORT_CHUNK_WORK, import_id, str(chunk_index))

    digest = hashlib.sha256(f'{import_id}:{chunk_index}'.encode('utf-8')).hexdigest()
    return TaskResult(
        ok=CatalogChunk(
            chunk_index=chunk_index,
            rows=tuning.CATALOG_IMPORT_ROWS_PER_CHUNK,
            checksum=digest[:16],
        ),
    )


@app.task(
    'flaky_export',
    queue_name=QUEUE_ANALYTICS,
    # A longer backoff than every other task here, and deliberately so: this
    # task breaks the executor pool on purpose, and the pool needs time to warm
    # a full set of children before the retry lands. See
    # `tuning.CHAOS_EXPORT_RETRY_INTERVALS_SECONDS`.
    retry_policy=RetryPolicy.fixed(
        tuning.CHAOS_EXPORT_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def flaky_export(*, export_id: str) -> TaskResult[ExportManifest, TaskError]:
    """Export a data extract, badly.

    Half of all export ids kill the child process outright with `os._exit(1)`.
    No exception is raised and nothing is returned — the worker simply loses the
    child, reports `WORKER_CRASHED`, and the retry policy above brings the task
    back. The crash draw hashes the export id and the retry keeps that id, so a
    crashing export crashes every attempt until its retries are spent: this is
    the one place in the showcase where recovery is demonstrated rather than
    survived.
    """
    simulate.perform(tuning.FLAKY_EXPORT_WORK, export_id, 'export')

    if simulate.draw(tuning.CHAOS_EXPORT_CRASH_RATE, export_id, 'crash'):
        os._exit(1)

    return TaskResult(
        ok=ExportManifest(
            export_id=export_id,
            rows=simulate.integer(1_000, 50_000, export_id, 'rows'),
        ),
    )
