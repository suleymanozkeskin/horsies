# showcase/hemline/workflows/returns_review.py
"""A returns workflow that pauses itself when it needs a person.

    receive_return -> inspect_item -> restock_or_writeoff

`Meta.on_error = OnError.PAUSE` is the whole story. When `inspect_item` reports
`DAMAGED_ITEM`, the workflow does not fail and does not carry on: it goes
PAUSED, no further node is enqueued, and it stays that way until someone
resumes or cancels it from the dashboard.

Resuming runs `restock_or_writeoff`, which is reachable on the damaged branch
only because its node declares `allow_failed_deps=True`. It receives the
inspection's `TaskResult`, sees the error, and writes the item off. Cancelling
instead leaves the return open — which is also a legitimate answer, and the
reason the decision is a person's.
"""

from __future__ import annotations

from typing import Any

from horsies import (
    Horsies,
    OnError,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)

from ..app import app
from ..domain import RestockDecision
from ..tasks.returns import inspect_item, receive_return, restock_or_writeoff


class ReturnsReview(WorkflowDefinition[RestockDecision]):
    """One return, inspected and settled — with a pause in the middle."""

    name = 'returns_review'
    definition_key = 'hemline.returns_review.v1'

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        return_id: str,
        order_id: str,
        sku: str,
        quantity: int,
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec[RestockDecision]:
        """Build a fresh spec for one return."""
        receive = TaskNode(
            fn=receive_return,
            kwargs={
                'return_id': return_id,
                'order_id': order_id,
                'sku': sku,
                'quantity': quantity,
            },
            node_id='receive_return',
        )
        inspect = TaskNode(
            fn=inspect_item,
            kwargs={'return_id': return_id},
            waits_for=[receive],
            node_id='inspect_item',
        )
        settle = TaskNode(
            fn=restock_or_writeoff,
            kwargs={'return_id': return_id},
            waits_for=[inspect],
            args_from={'inspection': inspect},
            allow_failed_deps=True,
            node_id='restock_or_writeoff',
        )
        return app.workflow(
            name=cls.name,
            tasks=[receive, inspect, settle],
            on_error=OnError.PAUSE,
            output=settle,
        )


@app.workflow_builder(
    cases=[
        {
            'return_id': 'RET-CHECK',
            'order_id': 'HEM-CHECK-0001',
            'sku': 'HEM-SKU-0001',
            'quantity': 1,
        },
    ],
)
def build_returns_review(
    *,
    return_id: str,
    order_id: str,
    sku: str,
    quantity: int,
) -> WorkflowSpec[RestockDecision]:
    """Build the review DAG for one return."""
    return ReturnsReview.build_with(
        app,
        return_id=return_id,
        order_id=order_id,
        sku=sku,
        quantity=quantity,
    )
