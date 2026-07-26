# showcase/acme/workflows/order_fulfillment.py
"""The flagship DAG: one order, validated, reserved, paid, packed, and shipped.

    validate_order
      |- reserve_stock[line 1..n]        (one node per line, parallel)
      \\-> authorize_payment              (waits for every reservation)
             |- pick_pack
             |- generate_invoice
             \\-> SubWorkflow: shipping(courier, express)
                    \\-> capture_payment  (args_from = authorize_payment)
                          \\-> send_order_email  (allow_failed_deps=True)

The shape changes per order: an order with three lines fans out three
reservations, and the courier and express flag come from the order id. That is
why the DAG is built in `build_with` rather than declared as class attributes —
every call returns a fresh spec.

Failure semantics, all visible in the graph view:

  * a line that cannot be reserved fails under `join='all'`, so everything
    downstream is SKIPPED;
  * `generate_invoice` running past its deadline fails the shipping join the
    same way;
  * `send_order_email` sets `allow_failed_deps=True`, so it runs anyway,
    receives the upstream error, and completes — a COMPLETED node underneath a
    FAILED one. The workflow still fails: `OnError.FAIL` accounts for the
    failure regardless of the handler.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Final

from horsies import (
    Horsies,
    OnError,
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)

from .. import simulate, tuning
from ..app import app
from ..domain import Order, OrderLine, PaymentCapture
from ..tasks.inventory import reserve_stock
from ..tasks.notify import send_order_email
from ..tasks.orders import generate_invoice, pick_pack, validate_order
from ..tasks.payments import authorize_payment, capture_payment
from .shipping import ShippingWorkflow


class OrderFulfillment(WorkflowDefinition[PaymentCapture]):
    """Fulfillment for one order. Output is the settled capture."""

    name = 'order_fulfillment'
    definition_key = 'acme.order_fulfillment.v1'

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        order: Order,
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec[PaymentCapture]:
        """Build a fresh spec shaped by this order's lines."""
        order_id = order.order_id
        validate = TaskNode(
            fn=validate_order,
            kwargs={'order_id': order_id},
            node_id='validate_order',
        )
        reservations = [
            TaskNode(
                fn=reserve_stock,
                kwargs={
                    'order_id': order_id,
                    'line_no': line.line_no,
                    'sku': line.sku,
                    'quantity': line.quantity,
                },
                waits_for=[validate],
                node_id=f'reserve_stock_{line.line_no}',
            )
            for line in order.lines
        ]
        authorize = TaskNode(
            fn=authorize_payment,
            kwargs={'order_id': order_id, 'amount_cents': order.total_cents},
            waits_for=reservations,
            node_id='authorize_payment',
        )
        pick = TaskNode(
            fn=pick_pack,
            kwargs={'order_id': order_id},
            waits_for=[authorize],
            node_id='pick_pack',
        )
        invoice = TaskNode(
            fn=generate_invoice,
            kwargs={'order_id': order_id},
            waits_for=[authorize],
            node_id='generate_invoice',
        )
        shipping = SubWorkflowNode(
            workflow_def=ShippingWorkflow,
            kwargs={
                'order_id': order_id,
                'courier': simulate.choice(tuning.COURIERS, order_id, 'courier'),
                'express': simulate.draw(tuning.EXPRESS_RATE, order_id, 'express'),
            },
            waits_for=[pick, invoice],
            node_id='shipping',
        )
        capture = TaskNode(
            fn=capture_payment,
            kwargs={'order_id': order_id},
            waits_for=[shipping, authorize],
            args_from={'authorization': authorize},
            node_id='capture_payment',
        )
        email = TaskNode(
            fn=send_order_email,
            kwargs={'order_id': order_id},
            waits_for=[capture],
            args_from={'capture': capture},
            allow_failed_deps=True,
            node_id='send_order_email',
        )
        return app.workflow(
            name=cls.name,
            tasks=[validate, *reservations, authorize, pick, invoice, shipping, capture, email],
            on_error=OnError.FAIL,
            output=capture,
        )


_CHECK_ORDER: Final[Order] = Order(
    order_id='ACME-CHECK-0001',
    customer_id='CUS-CHECK',
    status='placed',
    total_cents=9_900,
    lines=[
        OrderLine(
            line_no=line_no,
            sku=f'ACME-SKU-{line_no:04d}',
            size_code=tuning.SIZE_CODES[0],
            quantity=1,
            unit_price_cents=3_300,
        )
        for line_no in range(1, tuning.MAX_LINES_PER_ORDER + 1)
    ],
    created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
)
"""Widest shape the builder can produce, used by `horsies check`."""


@app.workflow_builder(cases=[{'order': _CHECK_ORDER}])
def build_order_fulfillment(*, order: Order) -> WorkflowSpec[PaymentCapture]:
    """Build the fulfillment DAG for one order.

    Registered as a workflow builder so `horsies check` executes it and
    validates the whole graph — including the shipping child — without a
    database or a worker.
    """
    return OrderFulfillment.build_with(app, order=order)
