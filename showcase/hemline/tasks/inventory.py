# showcase/hemline/tasks/inventory.py
"""Stock reservation, release, and the scheduled supplier feed pull.

`reserve_stock` is the honest-failure showpiece: it reserves against the real
`hemline_stock` row with a conditional UPDATE, so an order that includes a
discontinued SKU fails because the database refused it.

`sync_supplier_feed` is the exception-mapper showpiece: it raises a plain
`TimeoutError`, and the per-task `exception_mapper` reports it as the domain
code `SUPPLIER_TIMEOUT`, which its fixed retry schedule then retries.
"""

from __future__ import annotations

import time

from horsies import (
    Err,
    Ok,
    OperationalErrorCode,
    RetryPolicy,
    TaskError,
    TaskResult,
)

from .. import simulate, store, tuning
from ..app import QUEUE_ANALYTICS, QUEUE_FULFILLMENT, app
from ..domain import (
    INSUFFICIENT_STOCK,
    SUPPLIER_TIMEOUT,
    UNKNOWN_SKU,
    StockRelease,
    StockReservation,
    SupplierFeed,
)
from . import store_failure


@app.task(
    'reserve_stock',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def reserve_stock(
    *,
    order_id: str,
    line_no: int,
    sku: str,
    quantity: int,
) -> TaskResult[StockReservation, TaskError]:
    """Hold stock for one order line. One node per line runs in parallel."""
    simulate.perform(tuning.RESERVE_STOCK_WORK, order_id, sku, 'reserve')

    match store.reserve_line(order_id, line_no, sku, quantity):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(outcome):
            if not outcome.known_sku:
                return TaskResult(
                    err=TaskError(
                        error_code=UNKNOWN_SKU,
                        message=f'{sku} is not in the catalog',
                        data={'order_id': order_id, 'sku': sku},
                    ),
                )
            if not outcome.reserved:
                return TaskResult(
                    err=TaskError(
                        error_code=INSUFFICIENT_STOCK,
                        message=(
                            f'{sku} has {outcome.available} available, '
                            f'order {order_id} needs {quantity}'
                        ),
                        data={
                            'order_id': order_id,
                            'sku': sku,
                            'requested': quantity,
                            'available': outcome.available,
                        },
                    ),
                )
            return TaskResult(
                ok=StockReservation(
                    order_id=order_id,
                    sku=sku,
                    quantity=quantity,
                    available_after=outcome.available,
                    replayed=outcome.replayed,
                ),
            )


@app.task(
    'release_stock',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def release_stock(
    *,
    sku: str,
    quantity: int,
) -> TaskResult[StockRelease, TaskError]:
    """Hand reserved units back to available stock."""
    simulate.perform(tuning.RELEASE_STOCK_WORK, sku, 'release')

    match store.release_line(sku, quantity):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(available):
            if available is None:
                return TaskResult(
                    err=TaskError(
                        error_code=UNKNOWN_SKU,
                        message=f'{sku} is not in the catalog',
                        data={'sku': sku},
                    ),
                )
            return TaskResult(
                ok=StockRelease(sku=sku, quantity=quantity, available_after=available),
            )


def _feed_window() -> str:
    """The pull window a feed request falls into.

    Feeds run on a schedule, so there is no order id to hash. The window
    keeps the outcome stable across the retries of one pull while letting the
    next scheduled pull draw again.
    """
    return str(int(time.time()) // tuning.SUPPLIER_FEED_INTERVAL_SECONDS)


@app.task(
    'sync_supplier_feed',
    queue_name=QUEUE_ANALYTICS,
    retry_policy=RetryPolicy.fixed(
        tuning.SUPPLIER_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[SUPPLIER_TIMEOUT, OperationalErrorCode.WORKER_CRASHED],
    ),
    exception_mapper={TimeoutError: SUPPLIER_TIMEOUT},
)
def sync_supplier_feed(*, supplier: str) -> TaskResult[SupplierFeed, TaskError]:
    """Pull a supplier's catalog feed.

    Raises `TimeoutError` when the supplier stalls. The task never converts it:
    the `exception_mapper` above is what turns the exception into
    `SUPPLIER_TIMEOUT`, and the fixed retry schedule takes it from there.
    """
    window = _feed_window()
    simulate.perform(tuning.SUPPLIER_FEED_WORK, supplier, window)

    if simulate.draw(tuning.SUPPLIER_TIMEOUT_RATE, supplier, window, 'timeout'):
        raise TimeoutError(f'{supplier} did not answer within the pull window')

    match store.count_products():
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(sku_count):
            return TaskResult(
                ok=SupplierFeed(
                    supplier=supplier,
                    sku_count=sku_count,
                    changed_count=simulate.integer(0, 12, supplier, window, 'changed'),
                ),
            )
