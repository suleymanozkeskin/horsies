# showcase/hemline/workflows/restock.py
"""Three supplier feeds, two of which are enough.

    sync_supplier_feed[atlas]     \\
    sync_supplier_feed[brera]      >-- update_stock_levels   (quorum, min_success=2)
    sync_supplier_feed[coastline] /

The aggregate declares `join='quorum'` with `min_success=2`, so it fires as
soon as two feeds land rather than waiting for all three, and the workflow
COMPLETES with a failed branch still showing in the graph.

It reads the feeds through `workflow_ctx_from` rather than `args_from`. Under a
quorum join an `args_from` source may not be terminal when the node fires, and
its kwarg is then simply not injected; `WorkflowContext` makes the same
uncertainty explicit — the aggregate asks `has_result` before reading, and
reports which suppliers were missing.
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

from .. import tuning
from ..app import app
from ..domain import RestockPlan
from ..tasks.inventory import sync_supplier_feed, update_stock_levels


def _feed_node_id(supplier: str) -> str:
    """Stable node id for one supplier's feed node."""
    return f'feed_{supplier.replace("-", "_")}'


class Restock(WorkflowDefinition[RestockPlan]):
    """Pull every supplier feed and apply whichever ones answered."""

    name = 'restock'
    definition_key = 'hemline.restock.v1'

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        suppliers: list[str],
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec[RestockPlan]:
        """Build a fresh spec with one feed node per supplier."""
        feeds = [
            TaskNode(
                fn=sync_supplier_feed,
                kwargs={'supplier': supplier},
                node_id=_feed_node_id(supplier),
            )
            for supplier in suppliers
        ]
        aggregate = TaskNode(
            fn=update_stock_levels,
            kwargs={'feed_node_ids': [_feed_node_id(s) for s in suppliers]},
            waits_for=feeds,
            workflow_ctx_from=feeds,
            join='quorum',
            min_success=min(tuning.RESTOCK_MIN_SUCCESSFUL_FEEDS, len(feeds)),
            node_id='update_stock_levels',
        )
        return app.workflow(
            name=cls.name,
            tasks=[*feeds, aggregate],
            on_error=OnError.FAIL,
            output=aggregate,
        )


_CHECK_SUPPLIERS: Final[list[str]] = list(tuning.SUPPLIERS)


@app.workflow_builder(cases=[{'suppliers': _CHECK_SUPPLIERS}])
def build_restock(*, suppliers: list[str]) -> WorkflowSpec[RestockPlan]:
    """Build the restock DAG for a set of suppliers."""
    return Restock.build_with(app, suppliers=suppliers)
