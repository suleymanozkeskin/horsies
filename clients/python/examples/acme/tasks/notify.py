# clients/python/examples/acme/tasks/notify.py
"""Customer notifications.

`send_order_email` is the workflow's recovery handler. Its node sets
`allow_failed_deps=True`, so it runs whether the capture completed, failed, or
was skipped — it reads the upstream `TaskOutcome` and picks the template. The
workflow still fails by policy; the customer still hears from Acme Clothing.
"""

from __future__ import annotations

from horsies import (
    Err,
    Failed,
    Ok,
    OperationalErrorCode,
    Result,
    RetryPolicy,
    Succeeded,
    TaskError,
    TaskOutcome,
)

from .. import simulate, store, tuning
from ..app import QUEUE_NOTIFICATIONS, app
from ..domain import (
    ORDER_NOT_FOUND,
    AbandonedCartSweep,
    EmailReceipt,
    MarketingBlast,
    PaymentCapture,
    ShippingNotice,
)
from . import store_failure

CONFIRMATION_TEMPLATE = 'order-confirmed'
APOLOGY_TEMPLATE = 'order-problem'


@app.task(
    'send_order_email',
    queue_name=QUEUE_NOTIFICATIONS,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def send_order_email(
    *,
    order_id: str,
    capture: TaskOutcome[PaymentCapture, TaskError],
) -> Result[EmailReceipt, TaskError]:
    """Tell the customer how their order went — either way."""
    match store.get_order(order_id):
        case Err(error):
            return Err(store_failure(error))
        case Ok(order):
            if order is None:
                return Err(
                    TaskError(
                        error_code=ORDER_NOT_FOUND,
                        message=f'no order {order_id} to write about',
                        data={'order_id': order_id},
                    ),
                )
            recipient = f'{order.customer_id}@customers.acme.invalid'

    simulate.perform(tuning.SEND_ORDER_EMAIL_WORK, order_id, 'email')

    # The upstream outcome decides the template, and an upstream failure is not
    # this task's failure: the apology went out, so the handler completed.
    match capture:
        case Failed(_):
            template = APOLOGY_TEMPLATE
        case Succeeded(_):
            template = CONFIRMATION_TEMPLATE
    return Ok(
        EmailReceipt(order_id=order_id, template=template, recipient=recipient),
    )


@app.task(
    'send_shipping_sms',
    queue_name=QUEUE_NOTIFICATIONS,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def send_shipping_sms(
    *,
    order_id: str,
    tracking_code: str,
) -> Result[ShippingNotice, TaskError]:
    """Text the customer their tracking code."""
    match store.get_order(order_id):
        case Err(error):
            return Err(store_failure(error))
        case Ok(order):
            if order is None:
                return Err(
                    TaskError(
                        error_code=ORDER_NOT_FOUND,
                        message=f'no order {order_id} to notify',
                        data={'order_id': order_id},
                    ),
                )
            recipient = f'+00{simulate.integer(1_000_000, 9_999_999, order.customer_id, "phone")}'

    simulate.perform(tuning.SEND_SHIPPING_SMS_WORK, order_id, 'sms')

    return Ok(
        ShippingNotice(
            order_id=order_id,
            recipient=recipient,
            tracking_code=tracking_code,
        ),
    )


@app.task(
    'marketing_blast',
    queue_name=QUEUE_NOTIFICATIONS,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def marketing_blast(*, segment: str) -> Result[MarketingBlast, TaskError]:
    """Send one segment of a campaign.

    Slow, and sent in bulk onto a queue capped at 3. The backlog that builds is
    the point: it is what the queue pivot and the cancel-a-PENDING-task action
    are demonstrated on. Takes no upstream result, so it is equally usable from
    a schedule and from a scenario burst.
    """
    simulate.perform(tuning.MARKETING_BLAST_WORK, segment, 'blast')

    return Ok(
        MarketingBlast(
            segment=segment,
            recipients=tuning.MARKETING_SEGMENT_SIZE,
        ),
    )


@app.task(
    'winback_blast',
    queue_name=QUEUE_NOTIFICATIONS,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def winback_blast(
    *,
    segment: str,
    sweep: TaskOutcome[AbandonedCartSweep, TaskError],
) -> Result[MarketingBlast, TaskError]:
    """Mail the carts a sweep actually found.

    The `sweep` parameter is a required `TaskOutcome`. The workflow fills it
    through `from_node`. A failed sweep fails the blast with the error of the
    sweep. A send that has no upstream sweep uses `marketing_blast` above.
    """
    simulate.perform(tuning.MARKETING_BLAST_WORK, segment, 'winback')

    match sweep:
        case Failed(upstream_error):
            return Err(upstream_error)
        case Succeeded(found):
            return Ok(
                MarketingBlast(segment=segment, recipients=found.swept),
            )
