# showcase/hemline/tasks/notify.py
"""Customer notifications.

`send_order_email` is the workflow's recovery handler. Its node sets
`allow_failed_deps=True`, so it runs whether the capture completed, failed, or
was skipped — it reads the upstream `TaskResult` and picks the template. The
workflow still fails by policy; the customer still hears from Hemline.
"""

from __future__ import annotations

from horsies import (
    Err,
    Ok,
    OperationalErrorCode,
    RetryPolicy,
    TaskError,
    TaskResult,
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
    capture: TaskResult[PaymentCapture, TaskError],
) -> TaskResult[EmailReceipt, TaskError]:
    """Tell the customer how their order went — either way."""
    match store.get_order(order_id):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(order):
            if order is None:
                return TaskResult(
                    err=TaskError(
                        error_code=ORDER_NOT_FOUND,
                        message=f'no order {order_id} to write about',
                        data={'order_id': order_id},
                    ),
                )
            recipient = f'{order.customer_id}@customers.hemline.invalid'

    simulate.perform(tuning.SEND_ORDER_EMAIL_WORK, order_id, 'email')

    # The upstream result decides the template, and an upstream failure is not
    # this task's failure: the apology went out, so the handler completed.
    template = APOLOGY_TEMPLATE if capture.is_err() else CONFIRMATION_TEMPLATE
    return TaskResult(
        ok=EmailReceipt(order_id=order_id, template=template, recipient=recipient),
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
) -> TaskResult[ShippingNotice, TaskError]:
    """Text the customer their tracking code."""
    match store.get_order(order_id):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(order):
            if order is None:
                return TaskResult(
                    err=TaskError(
                        error_code=ORDER_NOT_FOUND,
                        message=f'no order {order_id} to notify',
                        data={'order_id': order_id},
                    ),
                )
            recipient = f'+00{simulate.integer(1_000_000, 9_999_999, order.customer_id, "phone")}'

    simulate.perform(tuning.SEND_SHIPPING_SMS_WORK, order_id, 'sms')

    return TaskResult(
        ok=ShippingNotice(
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
def marketing_blast(
    *,
    segment: str,
    sweep: TaskResult[AbandonedCartSweep, TaskError] | None = None,
) -> TaskResult[MarketingBlast, TaskError]:
    """Send one segment of a campaign.

    Slow, and sent in bulk onto a queue capped at 3. The backlog that builds is
    the point: it is what the queue pivot and the cancel-a-PENDING-task action
    are demonstrated on.

    `sweep` defaults to `None` so the daily schedule can send a fixed segment.
    When a workflow injects a sweep result, the recipient count follows the
    number of carts actually found instead of the nominal segment size.
    """
    simulate.perform(tuning.MARKETING_BLAST_WORK, segment, 'blast')

    recipients = tuning.MARKETING_SEGMENT_SIZE
    if sweep is not None and sweep.is_ok():
        recipients = sweep.ok_value.swept

    return TaskResult(
        ok=MarketingBlast(segment=segment, recipients=recipients),
    )
