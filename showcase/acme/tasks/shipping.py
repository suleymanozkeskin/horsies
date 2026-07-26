# showcase/acme/tasks/shipping.py
"""Courier booking, label printing, and tracking — the child workflow's tasks.

The three run in sequence inside the `shipping` sub-workflow, each consuming
the previous node's `TaskResult` through `args_from`. `book_courier` is the
only one that can fail on its own: a share of orders hit a flaking carrier
API, which is retryable and clears on the next attempt.
"""

from __future__ import annotations

import uuid

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
    COURIER_UNAVAILABLE,
    SHIPMENT_NOT_FOUND,
    CourierBooking,
    ShippingLabel,
    TrackingSeed,
)
from . import store_failure


@app.task(
    'book_courier',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.exponential(
        base_seconds=tuning.COURIER_RETRY_BASE_SECONDS,
        max_retries=tuning.COURIER_MAX_RETRIES,
        auto_retry_for=[COURIER_UNAVAILABLE, OperationalErrorCode.WORKER_CRASHED],
    ),
)
def book_courier(
    *,
    order_id: str,
    courier: str,
    express: bool,
) -> TaskResult[CourierBooking, TaskError]:
    """Reserve a carrier slot. The shipment row counts the attempts."""
    match store.count_courier_attempt(order_id, courier, express):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(shipment):
            if shipment.booking_reference is not None:
                return TaskResult(
                    ok=CourierBooking(
                        order_id=order_id,
                        courier=courier,
                        express=express,
                        booking_reference=shipment.booking_reference,
                        attempt=shipment.attempt,
                        replayed=True,
                    ),
                )
            attempt = shipment.attempt

    simulate.perform(tuning.BOOK_COURIER_WORK, order_id, courier, 'book')

    flaking = simulate.draw(tuning.COURIER_FLAKE_RATE, order_id, 'courier')
    if flaking and attempt <= tuning.COURIER_FAILING_ATTEMPTS:
        return TaskResult(
            err=TaskError(
                error_code=COURIER_UNAVAILABLE,
                message=f'{courier} refused the booking on attempt {attempt}',
                data={'order_id': order_id, 'courier': courier, 'attempt': attempt},
            ),
        )

    booking_reference = f'{courier[:3].upper()}-{uuid.uuid4().hex[:10]}'
    match store.set_booking_reference(order_id, booking_reference):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(stored):
            if not stored:
                return TaskResult(
                    err=TaskError(
                        error_code=SHIPMENT_NOT_FOUND,
                        message=f'no shipment row for order {order_id}',
                        data={'order_id': order_id},
                    ),
                )
            return TaskResult(
                ok=CourierBooking(
                    order_id=order_id,
                    courier=courier,
                    express=express,
                    booking_reference=booking_reference,
                    attempt=attempt,
                    replayed=False,
                ),
            )


@app.task(
    'print_label',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def print_label(
    *,
    order_id: str,
    booking: TaskResult[CourierBooking, TaskError],
) -> TaskResult[ShippingLabel, TaskError]:
    """Print the carrier label for a booked shipment."""
    if booking.is_err():
        return TaskResult(err=booking.err_value)
    booked = booking.ok_value

    simulate.perform(tuning.PRINT_LABEL_WORK, order_id, 'label')

    label_url = f'https://labels.acme.invalid/{booked.booking_reference}.pdf'
    match store.set_label_url(order_id, label_url):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(stored):
            if not stored:
                return TaskResult(
                    err=TaskError(
                        error_code=SHIPMENT_NOT_FOUND,
                        message=f'no shipment row for order {order_id}',
                        data={'order_id': order_id},
                    ),
                )
            return TaskResult(
                ok=ShippingLabel(
                    order_id=order_id,
                    label_url=label_url,
                    label_format='A6' if booked.express else 'A5',
                ),
            )


@app.task(
    'tracking_seed',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def tracking_seed(
    *,
    order_id: str,
    label: TaskResult[ShippingLabel, TaskError],
) -> TaskResult[TrackingSeed, TaskError]:
    """Hand the customer a tracking code — the child workflow's output."""
    if label.is_err():
        return TaskResult(err=label.err_value)

    simulate.perform(tuning.TRACKING_SEED_WORK, order_id, 'tracking')

    match store.get_shipment(order_id):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(shipment):
            if shipment is None:
                return TaskResult(
                    err=TaskError(
                        error_code=SHIPMENT_NOT_FOUND,
                        message=f'no shipment row for order {order_id}',
                        data={'order_id': order_id},
                    ),
                )
            booked = shipment

    tracking_code = f'ACME{simulate.integer(10_000_000, 99_999_999, order_id, "track")}'
    match store.set_tracking_code(order_id, tracking_code):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(_):
            pass

    match store.set_order_status(order_id, 'shipped'):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(_):
            return TaskResult(
                ok=TrackingSeed(
                    order_id=order_id,
                    courier=booked.courier,
                    tracking_code=tracking_code,
                    tracking_url=(
                        f'https://track.{booked.courier}.invalid/{tracking_code}'
                    ),
                ),
            )
