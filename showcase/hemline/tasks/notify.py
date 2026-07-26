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
    EmailReceipt,
    PaymentCapture,
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
