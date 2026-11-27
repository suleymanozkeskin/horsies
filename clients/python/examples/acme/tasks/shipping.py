# clients/python/examples/acme/tasks/shipping.py
"""Courier booking, label printing, and tracking — the child workflow's tasks.

The three run in sequence inside the `shipping` sub-workflow, each consuming
the previous node's `TaskOutcome` through `args_from`. `book_courier` is the
only one that can fail on its own: a share of orders hit a flaking carrier
API, which is retryable and clears on the next attempt.
"""

from __future__ import annotations

import uuid

from horsies import (
    Err,
    Failed,
    Ok,
    OperationalCode,
    Result,
    RetryPolicy,
    Succeeded,
    TaskError,
    TaskOutcome,
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
    queue=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.exponential(
        base_seconds=tuning.COURIER_RETRY_BASE_SECONDS,
        max_retries=tuning.COURIER_MAX_RETRIES,
        retry_for=[COURIER_UNAVAILABLE, OperationalCode.WORKER_CRASHED],
    ),
)
def book_courier(
    *,
    order_id: str,
    courier: str,
    express: bool,
) -> Result[CourierBooking, TaskError]:
    """Reserve a carrier slot. The shipment row counts the attempts."""
    match store.count_courier_attempt(order_id, courier, express):
        case Err(error):
            return Err(store_failure(error))
        case Ok(shipment):
            if shipment.booking_reference is not None:
                return Ok(
                    CourierBooking(
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
        return Err(
            TaskError(
                code=COURIER_UNAVAILABLE,
                message=f'{courier} refused the booking on attempt {attempt}',
                data={'order_id': order_id, 'courier': courier, 'attempt': attempt},
            ),
        )

    booking_reference = f'{courier[:3].upper()}-{uuid.uuid4().hex[:10]}'
    match store.set_booking_reference(order_id, booking_reference):
        case Err(error):
            return Err(store_failure(error))
        case Ok(stored):
            if not stored:
                return Err(
                    TaskError(
                        code=SHIPMENT_NOT_FOUND,
                        message=f'no shipment row for order {order_id}',
                        data={'order_id': order_id},
                    ),
                )
            return Ok(
                CourierBooking(
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
    queue=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        retry_for=[OperationalCode.WORKER_CRASHED],
    ),
)
def print_label(
    *,
    order_id: str,
    booking: TaskOutcome[CourierBooking, TaskError],
) -> Result[ShippingLabel, TaskError]:
    """Print the carrier label for a booked shipment."""
    match booking:
        case Failed(upstream_error):
            return Err(upstream_error)
        case Succeeded(courier_booking):
            booked = courier_booking

    simulate.perform(tuning.PRINT_LABEL_WORK, order_id, 'label')

    label_url = f'https://labels.acme.invalid/{booked.booking_reference}.pdf'
    match store.set_label_url(order_id, label_url):
        case Err(error):
            return Err(store_failure(error))
        case Ok(stored):
            if not stored:
                return Err(
                    TaskError(
                        code=SHIPMENT_NOT_FOUND,
                        message=f'no shipment row for order {order_id}',
                        data={'order_id': order_id},
                    ),
                )
            return Ok(
                ShippingLabel(
                    order_id=order_id,
                    label_url=label_url,
                    label_format='A6' if booked.express else 'A5',
                ),
            )


@app.task(
    'tracking_seed',
    queue=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        retry_for=[OperationalCode.WORKER_CRASHED],
    ),
)
def tracking_seed(
    *,
    order_id: str,
    label: TaskOutcome[ShippingLabel, TaskError],
) -> Result[TrackingSeed, TaskError]:
    """Hand the customer a tracking code — the child workflow's output."""
    match label:
        case Failed(upstream_error):
            return Err(upstream_error)
        case Succeeded(_):
            pass

    simulate.perform(tuning.TRACKING_SEED_WORK, order_id, 'tracking')

    match store.get_shipment(order_id):
        case Err(error):
            return Err(store_failure(error))
        case Ok(shipment):
            if shipment is None:
                return Err(
                    TaskError(
                        code=SHIPMENT_NOT_FOUND,
                        message=f'no shipment row for order {order_id}',
                        data={'order_id': order_id},
                    ),
                )
            booked = shipment

    tracking_code = f'ACME{simulate.integer(10_000_000, 99_999_999, order_id, "track")}'
    match store.set_tracking_code(order_id, tracking_code):
        case Err(error):
            return Err(store_failure(error))
        case Ok(_):
            pass

    match store.set_order_status(order_id, 'shipped'):
        case Err(error):
            return Err(store_failure(error))
        case Ok(_):
            return Ok(
                TrackingSeed(
                    order_id=order_id,
                    courier=booked.courier,
                    tracking_code=tracking_code,
                    tracking_url=(
                        f'https://track.{booked.courier}.invalid/{tracking_code}'
                    ),
                ),
            )
