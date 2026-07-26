# showcase/hemline/workflows/shipping.py
"""The `shipping` child workflow: book a courier, print the label, seed tracking.

Started as a `SubWorkflowNode` inside `order_fulfillment`, which forwards the
courier and express flag into `build_with`. The child is a workflow in its own
right — its own run, its own graph, its own node results — and its output
becomes the parent node's result.
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

from ..domain import TrackingSeed
from ..tasks.shipping import book_courier, print_label, tracking_seed


class ShippingWorkflow(WorkflowDefinition[TrackingSeed]):
    """Three sequential nodes, each consuming the previous node's TaskResult."""

    name = 'shipping'
    definition_key = 'hemline.shipping.v1'

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        order_id: str,
        courier: str,
        express: bool,
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec[TrackingSeed]:
        """Build a fresh spec for one order's shipment."""
        book = TaskNode(
            fn=book_courier,
            kwargs={'order_id': order_id, 'courier': courier, 'express': express},
            node_id='book_courier',
        )
        label = TaskNode(
            fn=print_label,
            kwargs={'order_id': order_id},
            waits_for=[book],
            args_from={'booking': book},
            node_id='print_label',
        )
        seed = TaskNode(
            fn=tracking_seed,
            kwargs={'order_id': order_id},
            waits_for=[label],
            args_from={'label': label},
            node_id='tracking_seed',
        )
        return app.workflow(
            name=cls.name,
            tasks=[book, label, seed],
            on_error=OnError.FAIL,
            output=seed,
        )
