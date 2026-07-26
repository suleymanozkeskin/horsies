# showcase/acme/tasks/orders.py
"""Order validation, warehouse work, and invoice rendering.

`generate_invoice` carries `timeout_ms`. A stuck render is SIGKILLed at the
deadline and fails with `TASK_TIMEOUT`; that kill breaks the worker's executor
pool, so sibling tasks in flight report `WORKER_CRASHED` and come back through
their own retry policies. Every task that writes is idempotent on replay, which
is what makes that ripple heal instead of corrupting an order.
"""

from __future__ import annotations

from horsies import (
    Err,
    Ok,
    OperationalErrorCode,
    RetryPolicy,
    TaskError,
    TaskResult,
    WorkflowMeta,
)

from .. import simulate, store, tuning
from ..app import QUEUE_FULFILLMENT, app
from ..domain import (
    ORDER_NOT_FOUND,
    UNKNOWN_SKU,
    Invoice,
    OrderValidation,
    PickPack,
    WarehouseAllocation,
)
from . import store_failure


@app.task(
    'validate_order',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def validate_order(*, order_id: str) -> TaskResult[OrderValidation, TaskError]:
    """Confirm the stored order is complete before anything is reserved."""
    simulate.perform(tuning.VALIDATE_ORDER_WORK, order_id, 'validate')

    match store.get_order(order_id):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(order):
            if order is None or not order.lines:
                return TaskResult(
                    err=TaskError(
                        error_code=ORDER_NOT_FOUND,
                        message=f'no order {order_id} with lines to fulfill',
                        data={'order_id': order_id},
                    ),
                )
            validated = order

    match store.set_order_status(order_id, 'validated'):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(_):
            return TaskResult(
                ok=OrderValidation(
                    order_id=order_id,
                    line_count=len(validated.lines),
                    total_cents=validated.total_cents,
                ),
            )


@app.task(
    'pick_pack',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def pick_pack(
    *,
    order_id: str,
    workflow_meta: WorkflowMeta | None = None,
) -> TaskResult[PickPack, TaskError]:
    """Pick and pack the order.

    Declaring `workflow_meta` is all it takes to receive the workflow id and
    node index; they land in the result so the node detail shows which run
    produced this pack.
    """
    simulate.perform(tuning.PICK_PACK_WORK, order_id, 'pick')

    match store.get_order(order_id):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(order):
            if order is None:
                return TaskResult(
                    err=TaskError(
                        error_code=ORDER_NOT_FOUND,
                        message=f'no order {order_id} to pick',
                        data={'order_id': order_id},
                    ),
                )
            units = sum(line.quantity for line in order.lines)

    match store.set_order_status(order_id, 'packed'):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(_):
            return TaskResult(
                ok=PickPack(
                    order_id=order_id,
                    station=simulate.choice(tuning.PICK_STATIONS, order_id, 'station'),
                    units_picked=units,
                    workflow_id=None if workflow_meta is None else workflow_meta.workflow_id,
                    task_index=None if workflow_meta is None else workflow_meta.task_index,
                ),
            )


@app.task(
    'allocate_warehouse',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def allocate_warehouse(
    *,
    sku: str,
    quantity: int,
) -> TaskResult[WarehouseAllocation, TaskError]:
    """Pick the warehouse a transfer should draw from.

    Used by `warehouse_transfer`, which moves stock between sites rather than
    fulfilling an order — the flagship order DAG allocates implicitly when it
    reserves.
    """
    simulate.perform(tuning.ALLOCATE_WAREHOUSE_WORK, sku, 'allocate')

    match store.list_catalog():
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(catalog):
            known = any(entry.product.sku == sku for entry in catalog)

    if not known:
        return TaskResult(
            err=TaskError(
                error_code=UNKNOWN_SKU,
                message=f'{sku} is not in the catalog',
                data={'sku': sku},
            ),
        )
    return TaskResult(
        ok=WarehouseAllocation(
            order_id=f'transfer:{sku}',
            warehouse_code=simulate.choice(tuning.WAREHOUSES, sku, 'warehouse'),
            distance_km=simulate.integer(20, 1_400, sku, 'distance'),
        ),
    )


@app.task(
    'generate_invoice',
    queue_name=QUEUE_FULFILLMENT,
    timeout_ms=tuning.INVOICE_TIMEOUT_MS,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def generate_invoice(*, order_id: str) -> TaskResult[Invoice, TaskError]:
    """Render the invoice PDF.

    A small share of orders trip a render that never finishes. `TASK_TIMEOUT`
    is deliberately not in `auto_retry_for`: the stall is a stable property of
    the order, so retrying it would only stall again.
    """
    if simulate.draw(tuning.INVOICE_HANG_RATE, order_id, 'invoice'):
        simulate.stall(tuning.INVOICE_HANG_MS)

    render_ms = simulate.perform(tuning.GENERATE_INVOICE_WORK, order_id, 'render')

    match store.get_order(order_id):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(order):
            if order is None:
                return TaskResult(
                    err=TaskError(
                        error_code=ORDER_NOT_FOUND,
                        message=f'no order {order_id} to invoice',
                        data={'order_id': order_id},
                    ),
                )
            return TaskResult(
                ok=Invoice(
                    order_id=order_id,
                    invoice_number=f'INV-{order_id}',
                    total_cents=order.total_cents,
                    render_ms=render_ms,
                ),
            )
