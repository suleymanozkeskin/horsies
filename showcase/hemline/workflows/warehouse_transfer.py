# showcase/hemline/workflows/warehouse_transfer.py
"""Move stock between sites.

    allocate_warehouse -> release_stock

Short on purpose. It exists because stock movement that is not an order needs
somewhere to live, and because `allocate_warehouse` is a real decision — which
site to draw from — that the order path makes implicitly when it reserves.

The release is the second half of a transfer: units held at the source site are
handed back to available stock once the destination has been chosen.
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
from ..domain import StockRelease
from ..tasks.inventory import release_stock
from ..tasks.orders import allocate_warehouse


class WarehouseTransfer(WorkflowDefinition[StockRelease]):
    """Allocate a destination, then free the source's hold."""

    name = 'warehouse_transfer'
    definition_key = 'hemline.warehouse_transfer.v1'

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        sku: str,
        quantity: int,
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec[StockRelease]:
        """Build a fresh spec for one transfer."""
        allocate = TaskNode(
            fn=allocate_warehouse,
            kwargs={'sku': sku, 'quantity': quantity},
            node_id='allocate_warehouse',
        )
        release = TaskNode(
            fn=release_stock,
            kwargs={'sku': sku, 'quantity': quantity},
            waits_for=[allocate],
            node_id='release_stock',
        )
        return app.workflow(
            name=cls.name,
            tasks=[allocate, release],
            on_error=OnError.FAIL,
            output=release,
        )


_CHECK_SKU: Final[str] = 'HEM-SKU-0001'


@app.workflow_builder(cases=[{'sku': _CHECK_SKU, 'quantity': 5}])
def build_warehouse_transfer(
    *,
    sku: str,
    quantity: int,
) -> WorkflowSpec[StockRelease]:
    """Build the transfer DAG."""
    return WarehouseTransfer.build_with(app, sku=sku, quantity=quantity)
