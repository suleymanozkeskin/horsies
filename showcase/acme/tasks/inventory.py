# showcase/acme/tasks/inventory.py
"""Stock reservation, release, and the scheduled supplier feed pull.

`reserve_stock` is the honest-failure showpiece: it reserves against the real
`acme_stock` row with a conditional UPDATE, so an order that includes a
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
    TaskNode,
    TaskResult,
    WorkflowContext,
)

from .. import simulate, store, tuning
from ..app import QUEUE_ANALYTICS, QUEUE_FULFILLMENT, app
from ..domain import (
    INSUFFICIENT_STOCK,
    NO_WORKFLOW_CONTEXT,
    QUORUM_NOT_MET,
    SUPPLIER_TIMEOUT,
    UNKNOWN_SKU,
    RestockPlan,
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


def _feed_node(node_id: str) -> TaskNode[SupplierFeed]:
    """A lookup handle for one feed node.

    `WorkflowContext.has_result` keys on `node_id` alone, and it is the only
    way to ask whether a result exists without `result_for` raising. A task
    receives node ids as data, not node objects, so it rebuilds the handle.
    """
    return TaskNode(fn=sync_supplier_feed, node_id=node_id)


@app.task(
    'update_stock_levels',
    queue_name=QUEUE_ANALYTICS,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def update_stock_levels(
    *,
    feed_node_ids: list[str],
    workflow_ctx: WorkflowContext | None = None,
) -> TaskResult[RestockPlan, TaskError]:
    """Apply whatever supplier feeds reported — the quorum aggregate.

    Its node declares `join='quorum'` with `min_success=2`, so it fires as soon
    as two of the three feeds land and does not wait for the third. Whichever
    feed is missing or failed is simply absent from the context, which is why
    every lookup is guarded rather than assumed.
    """
    if workflow_ctx is None:
        return TaskResult(
            err=TaskError(
                error_code=NO_WORKFLOW_CONTEXT,
                message='update_stock_levels runs only as a workflow node',
                data={'feed_node_ids': feed_node_ids},
            ),
        )

    match store.list_catalog():
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(catalog):
            skus = [entry.product.sku for entry in catalog]

    reporting: list[str] = []
    missing: list[str] = []
    adjusted = 0

    for node_id in feed_node_ids:
        node = _feed_node(node_id)
        if not workflow_ctx.has_result(node):
            missing.append(node_id)
            continue
        feed = workflow_ctx.result_for(node)
        if feed.is_err():
            missing.append(node_id)
            continue
        supplier = feed.ok_value.supplier
        reporting.append(supplier)
        for sku in simulate.sample(
            skus, min(tuning.RESTOCK_SKUS_PER_SUPPLIER, len(skus)), supplier, 'restock',
        ):
            match store.adjust_stock(sku, tuning.RESTOCK_UNITS_PER_SUPPLIER):
                case Err(error):
                    return TaskResult(err=store_failure(error))
                case Ok(applied):
                    adjusted += 1 if applied else 0

    if not reporting:
        return TaskResult(
            err=TaskError(
                error_code=QUORUM_NOT_MET,
                message='no supplier feed reached the aggregate',
                data={'missing': missing},
            ),
        )

    return TaskResult(
        ok=RestockPlan(
            suppliers_reporting=reporting,
            suppliers_missing=missing,
            skus_adjusted=adjusted,
            units_added=adjusted * tuning.RESTOCK_UNITS_PER_SUPPLIER,
        ),
    )
