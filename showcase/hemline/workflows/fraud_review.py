# showcase/hemline/workflows/fraud_review.py
"""Reconcile first, refund only if a person says so.

    reconcile_payments -> refund_payment          (OnError.PAUSE)

The second `OnError.PAUSE` workflow, and deliberately a different reason from
`returns_review`. There the pause is a judgement about a physical item; here it
is a guard on an irreversible money movement. If reconciliation cannot be
completed, the run must not proceed to refund on its own — it stops, and
someone decides.

That is the useful property of PAUSE over FAIL: a failed run is over, while a
paused run is still holding its place in the graph, with everything upstream
intact and one instruction away from continuing.
"""

from __future__ import annotations

from typing import Any, Final

from horsies import (
    Horsies,
    OnError,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)

from ..app import app
from ..domain import PaymentRefund
from ..tasks.payments import reconcile_payments, refund_payment


class FraudReview(WorkflowDefinition[PaymentRefund]):
    """Check the books, then refund a disputed order — with a human gate."""

    name = 'fraud_review'
    definition_key = 'hemline.fraud_review.v1'

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        order_id: str,
        amount_cents: int,
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec[PaymentRefund]:
        """Build a fresh spec for one disputed order."""
        reconcile = TaskNode(
            fn=reconcile_payments,
            kwargs={'window': f'dispute-{order_id}'},
            node_id='reconcile_payments',
        )
        refund = TaskNode(
            fn=refund_payment,
            kwargs={'order_id': order_id, 'amount_cents': amount_cents},
            waits_for=[reconcile],
            node_id='refund_payment',
        )
        return app.workflow(
            name=cls.name,
            tasks=[reconcile, refund],
            on_error=OnError.PAUSE,
            output=refund,
        )


_CHECK_ORDER: Final[str] = 'HEM-CHECK-0001'


@app.workflow_builder(cases=[{'order_id': _CHECK_ORDER, 'amount_cents': 4_900}])
def build_fraud_review(
    *,
    order_id: str,
    amount_cents: int,
) -> WorkflowSpec[PaymentRefund]:
    """Build the fraud-review DAG."""
    return FraudReview.build_with(app, order_id=order_id, amount_cents=amount_cents)
