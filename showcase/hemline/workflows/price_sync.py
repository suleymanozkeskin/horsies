# showcase/hemline/workflows/price_sync.py
"""The routine price sync — the same three tasks as `flash_sale`, opposite policy.

    publish_cdn     \\
                     >-- warm_cache_edge     (join='all', no SuccessPolicy)
    publish_origin  /

Worth reading next to `flash_sale.py`, because the tasks are identical and the
meaning is not. A flash sale is a marketing event: getting the price out to one
target is a win, so it declares a `SuccessPolicy` with a case per target and
joins on `any`. A routine sync is a consistency operation: a price that reached
the CDN but not origin is a bug waiting to be reported, so it joins on `all`
and has no success policy — any failure fails the run.

Same tasks, same graph shape, different answer to "what does done mean".
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
from ..domain import CacheWarm
from ..tasks.promotions import publish_cdn, publish_origin, warm_cache_edge


class PriceSync(WorkflowDefinition[CacheWarm]):
    """Publish one SKU's price to every target, then warm the edge."""

    name = 'price_sync'
    definition_key = 'hemline.price_sync.v1'

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        campaign_id: str,
        sku: str,
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec[CacheWarm]:
        """Build a fresh spec for one price sync."""
        cdn = TaskNode(
            fn=publish_cdn,
            kwargs={'campaign_id': campaign_id, 'sku': sku},
            node_id='publish_cdn',
        )
        origin = TaskNode(
            fn=publish_origin,
            kwargs={'campaign_id': campaign_id, 'sku': sku},
            node_id='publish_origin',
        )
        warm = TaskNode(
            fn=warm_cache_edge,
            kwargs={'campaign_id': campaign_id},
            waits_for=[cdn, origin],
            node_id='warm_cache_edge',
        )
        return app.workflow(
            name=cls.name,
            tasks=[cdn, origin, warm],
            on_error=OnError.FAIL,
            output=warm,
        )


_CHECK_CAMPAIGN: Final[str] = 'SYNC-CHECK'


@app.workflow_builder(
    cases=[{'campaign_id': _CHECK_CAMPAIGN, 'sku': 'HEM-SKU-0001'}],
)
def build_price_sync(*, campaign_id: str, sku: str) -> WorkflowSpec[CacheWarm]:
    """Build the routine price-sync DAG."""
    return PriceSync.build_with(app, campaign_id=campaign_id, sku=sku)
