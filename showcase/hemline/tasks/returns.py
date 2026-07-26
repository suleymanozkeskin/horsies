# showcase/hemline/tasks/returns.py
"""The returns desk: receive, inspect, and either restock or write off.

`inspect_item` is where the pause story starts. When it finds damage it
returns a `DAMAGED_ITEM` error, and because `returns_review` is declared with
`OnError.PAUSE`, the whole workflow stops there and waits for a person — no new
nodes start, and the run sits PAUSED with the reason readable in the node.

`restock_or_writeoff` sets `allow_failed_deps=True`, so once the run is resumed
it executes on either branch: it reads the inspection's `TaskResult` and writes
the item off when it is damaged, or returns it to stock when it is not.
"""

from __future__ import annotations

from datetime import datetime, timezone

from horsies import (
    Err,
    Ok,
    OperationalErrorCode,
    RetryPolicy,
    TaskError,
    TaskResult,
)

from .. import simulate, store, tuning
from ..app import QUEUE_FULFILLMENT, app
from ..domain import (
    DAMAGED_ITEM,
    RETURN_NOT_FOUND,
    Inspection,
    RestockDecision,
    ReturnCase,
    ReturnReceipt,
)
from . import store_failure


@app.task(
    'receive_return',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def receive_return(
    *,
    return_id: str,
    order_id: str,
    sku: str,
    quantity: int,
) -> TaskResult[ReturnReceipt, TaskError]:
    """Book a returned item in. Idempotent — a replay finds its own row."""
    simulate.perform(tuning.RECEIVE_RETURN_WORK, return_id, 'receive')

    case = ReturnCase(
        return_id=return_id,
        order_id=order_id,
        sku=sku,
        quantity=quantity,
        status='received',
        condition=None,
        created_at=datetime.now(timezone.utc),
    )
    match store.open_return(case):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(_):
            return TaskResult(
                ok=ReturnReceipt(
                    return_id=return_id,
                    order_id=order_id,
                    sku=sku,
                    quantity=quantity,
                ),
            )


@app.task(
    'inspect_item',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def inspect_item(*, return_id: str) -> TaskResult[Inspection, TaskError]:
    """Inspect the returned item.

    Damage is recorded either way — the inspector saw what they saw — but a
    damaged item is reported as an error, which is what pauses the workflow.
    `DAMAGED_ITEM` is deliberately absent from `auto_retry_for`: re-inspecting
    would find the same damage.
    """
    simulate.perform(tuning.INSPECT_ITEM_WORK, return_id, 'inspect')

    match store.get_return(return_id):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(case):
            if case is None:
                return TaskResult(
                    err=TaskError(
                        error_code=RETURN_NOT_FOUND,
                        message=f'no return {return_id} to inspect',
                        data={'return_id': return_id},
                    ),
                )
            inspected = case

    damaged = simulate.draw(tuning.RETURN_DAMAGE_RATE, return_id, 'damage')
    match store.record_inspection(return_id, 'damaged' if damaged else 'resellable'):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(_):
            pass

    if damaged:
        return TaskResult(
            err=TaskError(
                error_code=DAMAGED_ITEM,
                message=f'{inspected.sku} came back damaged and cannot be resold',
                data={
                    'return_id': return_id,
                    'sku': inspected.sku,
                    'quantity': inspected.quantity,
                },
            ),
        )
    return TaskResult(
        ok=Inspection(
            return_id=return_id,
            sku=inspected.sku,
            condition='resellable',
            notes='no visible wear, tags intact',
        ),
    )


@app.task(
    'restock_or_writeoff',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def restock_or_writeoff(
    *,
    return_id: str,
    inspection: TaskResult[Inspection, TaskError],
) -> TaskResult[RestockDecision, TaskError]:
    """Handle both inspection outcomes.

    Reached on the damaged branch only because its node sets
    `allow_failed_deps=True` — without it the node would be SKIPPED and the
    damaged item would sit in the returns desk forever.
    """
    match store.get_return(return_id):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(case):
            if case is None:
                return TaskResult(
                    err=TaskError(
                        error_code=RETURN_NOT_FOUND,
                        message=f'no return {return_id} to settle',
                        data={'return_id': return_id},
                    ),
                )
            settling = case

    simulate.perform(tuning.RESTOCK_OR_WRITEOFF_WORK, return_id, 'settle')

    if inspection.is_err():
        return _write_off(settling)
    return _restock(settling)


def _write_off(case: ReturnCase) -> TaskResult[RestockDecision, TaskError]:
    """Take a damaged item out of circulation."""
    match store.close_return(case.return_id, 'written_off'):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(_):
            return TaskResult(
                ok=RestockDecision(
                    return_id=case.return_id,
                    sku=case.sku,
                    quantity=case.quantity,
                    outcome='written_off',
                    available_after=None,
                ),
            )


def _restock(case: ReturnCase) -> TaskResult[RestockDecision, TaskError]:
    """Put a resellable item back on the shelf."""
    match store.adjust_stock(case.sku, case.quantity):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(_):
            pass

    match store.close_return(case.return_id, 'restocked'):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(_):
            return TaskResult(
                ok=RestockDecision(
                    return_id=case.return_id,
                    sku=case.sku,
                    quantity=case.quantity,
                    outcome='restocked',
                    available_after=None,
                ),
            )
