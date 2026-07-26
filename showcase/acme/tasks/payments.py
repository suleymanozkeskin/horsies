# showcase/acme/tasks/payments.py
"""Card authorization, capture, and refund against the simulated provider.

Two very different failures live here. `PSP_UNAVAILABLE` is operational: the
provider is unreachable, the retry policy backs off exponentially, and the
attempt history in the dashboard shows the outage clearing. `CARD_DECLINED`
is domain: no retry, and because the draw is a stable hash of the order id, a
dashboard retry of a declined payment declines again.
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
from ..app import QUEUE_PAYMENTS, app
from ..domain import (
    CARD_DECLINED,
    ORDER_NOT_FOUND,
    PAYMENT_ALREADY_CAPTURED,
    PSP_UNAVAILABLE,
    STORE_UNAVAILABLE,
    PaymentAuthorization,
    PaymentCapture,
    PaymentIntent,
    PaymentRefund,
    ReconciliationReport,
)
from . import store_failure


def _stored_authorization(
    order_id: str,
    attempt: int,
) -> TaskResult[PaymentAuthorization, TaskError] | None:
    """The hold this order already carries, as a replay result. None when absent.

    A task can run twice: the executor pool breaks when a sibling task is
    killed on timeout, and every task in flight comes back through its retry
    policy. Reporting the recorded hold keeps that replay from charging the
    card a second time.
    """
    match store.find_payment(order_id, 'authorization'):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(payment):
            if payment is None:
                return None
            return TaskResult(
                ok=PaymentAuthorization(
                    order_id=payment.order_id,
                    authorization_id=payment.payment_id,
                    amount_cents=payment.amount_cents,
                    psp_reference=payment.psp_reference,
                    attempt=attempt,
                    replayed=True,
                ),
            )


@app.task(
    'authorize_payment',
    queue_name=QUEUE_PAYMENTS,
    retry_policy=RetryPolicy.exponential(
        base_seconds=tuning.PSP_RETRY_BASE_SECONDS,
        max_retries=tuning.PSP_MAX_RETRIES,
        auto_retry_for=[PSP_UNAVAILABLE, OperationalErrorCode.WORKER_CRASHED],
    ),
)
def authorize_payment(
    *,
    order_id: str,
    amount_cents: int,
) -> TaskResult[PaymentAuthorization, TaskError]:
    """Place a hold on the customer's card."""
    match store.count_authorization_attempt(order_id):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(counted):
            if counted is None:
                return TaskResult(
                    err=TaskError(
                        error_code=ORDER_NOT_FOUND,
                        message=f'no order {order_id} to authorize',
                        data={'order_id': order_id},
                    ),
                )
            attempt = counted

    replay = _stored_authorization(order_id, attempt)
    if replay is not None:
        return replay

    simulate.perform(tuning.AUTHORIZE_PAYMENT_WORK, order_id, 'authorize')

    if simulate.draw(tuning.CARD_DECLINE_RATE, order_id, 'card'):
        return TaskResult(
            err=TaskError(
                error_code=CARD_DECLINED,
                message=f'issuer declined the card for order {order_id}',
                data={
                    'order_id': order_id,
                    'amount_cents': amount_cents,
                    'attempt': attempt,
                },
            ),
        )

    provider_down = simulate.draw(tuning.PSP_UNAVAILABLE_RATE, order_id, 'psp')
    if provider_down and attempt <= tuning.PSP_FAILING_ATTEMPTS:
        return TaskResult(
            err=TaskError(
                error_code=PSP_UNAVAILABLE,
                message=f'payment provider unreachable on attempt {attempt}',
                data={
                    'order_id': order_id,
                    'attempt': attempt,
                    'clears_after_attempts': tuning.PSP_FAILING_ATTEMPTS,
                },
            ),
        )

    psp_reference = f'psp_{uuid.uuid4().hex[:12]}'
    match store.record_payment(order_id, 'authorization', amount_cents, psp_reference):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(payment):
            if payment is not None:
                return TaskResult(
                    ok=PaymentAuthorization(
                        order_id=order_id,
                        authorization_id=payment.payment_id,
                        amount_cents=amount_cents,
                        psp_reference=psp_reference,
                        attempt=attempt,
                        replayed=False,
                    ),
                )

    concurrent = _stored_authorization(order_id, attempt)
    if concurrent is not None:
        return concurrent
    return TaskResult(
        err=TaskError(
            error_code=STORE_UNAVAILABLE,
            message='authorization conflicted on insert but no row was found',
            data={'order_id': order_id},
        ),
    )


@app.task(
    'capture_payment',
    queue_name=QUEUE_PAYMENTS,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def capture_payment(
    *,
    order_id: str,
    authorization: TaskResult[PaymentAuthorization, TaskError],
) -> TaskResult[PaymentCapture, TaskError]:
    """Settle the held funds, using the hold the authorization node produced."""
    if authorization.is_err():
        return TaskResult(err=authorization.err_value)
    hold = authorization.ok_value

    match store.find_payment(order_id, 'capture'):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(existing):
            if existing is not None:
                return _capture_of(existing, hold)

    simulate.perform(tuning.CAPTURE_PAYMENT_WORK, order_id, 'capture')

    match store.record_payment(order_id, 'capture', hold.amount_cents, hold.psp_reference):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(payment):
            if payment is None:
                return TaskResult(
                    err=TaskError(
                        error_code=PAYMENT_ALREADY_CAPTURED,
                        message=f'order {order_id} was captured by another attempt',
                        data={'order_id': order_id},
                    ),
                )
            capture = payment

    match store.set_order_status(order_id, 'captured'):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(_):
            return TaskResult(
                ok=PaymentCapture(
                    order_id=order_id,
                    capture_id=capture.payment_id,
                    authorization_id=hold.authorization_id,
                    amount_cents=capture.amount_cents,
                    replayed=False,
                ),
            )


def _capture_of(
    existing: PaymentIntent,
    hold: PaymentAuthorization,
) -> TaskResult[PaymentCapture, TaskError]:
    """Read a recorded capture as a replay, or refuse a second, different one.

    The unique constraint on (order_id, kind) is what makes a double capture
    impossible; this decides whether the row that blocked the insert is this
    task's own earlier run or a genuinely different settlement.
    """
    if existing.psp_reference != hold.psp_reference:
        return TaskResult(
            err=TaskError(
                error_code=PAYMENT_ALREADY_CAPTURED,
                message=(
                    f'order {existing.order_id} is already captured under a '
                    f'different authorization'
                ),
                data={
                    'order_id': existing.order_id,
                    'captured_reference': existing.psp_reference,
                    'presented_reference': hold.psp_reference,
                },
            ),
        )
    return TaskResult(
        ok=PaymentCapture(
            order_id=existing.order_id,
            capture_id=existing.payment_id,
            authorization_id=hold.authorization_id,
            amount_cents=existing.amount_cents,
            replayed=True,
        ),
    )


@app.task(
    'refund_payment',
    queue_name=QUEUE_PAYMENTS,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def refund_payment(
    *,
    order_id: str,
    amount_cents: int,
) -> TaskResult[PaymentRefund, TaskError]:
    """Return money for an accepted return."""
    match store.find_payment(order_id, 'capture'):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(capture):
            if capture is None:
                return TaskResult(
                    err=TaskError(
                        error_code=ORDER_NOT_FOUND,
                        message=f'order {order_id} has no capture to refund',
                        data={'order_id': order_id},
                    ),
                )

    simulate.perform(tuning.REFUND_PAYMENT_WORK, order_id, 'refund')

    match store.record_payment(order_id, 'refund', amount_cents, capture.psp_reference):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(refund):
            if refund is None:
                return _stored_refund(order_id)
            return TaskResult(
                ok=PaymentRefund(
                    order_id=order_id,
                    refund_id=refund.payment_id,
                    amount_cents=refund.amount_cents,
                ),
            )


@app.task(
    'reconcile_payments',
    queue_name=QUEUE_PAYMENTS,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def reconcile_payments(*, window: str) -> TaskResult[ReconciliationReport, TaskError]:
    """Check authorizations against captures.

    Runs on a `CronSchedule` — every 4 hours at :15, written as typed cron
    terms rather than a cron string.
    """
    simulate.perform(tuning.RECONCILE_PAYMENTS_WORK, window, 'reconcile')

    match store.payment_reconciliation():
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(counts):
            authorizations, captures, unmatched = counts
            return TaskResult(
                ok=ReconciliationReport(
                    authorizations=authorizations,
                    captures=captures,
                    unmatched=unmatched,
                ),
            )


def _stored_refund(order_id: str) -> TaskResult[PaymentRefund, TaskError]:
    """Report the refund an earlier run of this task already recorded."""
    match store.find_payment(order_id, 'refund'):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(refund):
            if refund is None:
                return TaskResult(
                    err=TaskError(
                        error_code=STORE_UNAVAILABLE,
                        message='refund conflicted on insert but no row was found',
                        data={'order_id': order_id},
                    ),
                )
            return TaskResult(
                ok=PaymentRefund(
                    order_id=order_id,
                    refund_id=refund.payment_id,
                    amount_cents=refund.amount_cents,
                ),
            )
