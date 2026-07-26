# showcase/hemline/workflows/flash_sale.py
"""A sale that succeeds if *either* publish target accepts it.

    publish_cdn     ---\\
                        >-- warm_cache_edge      (join='any')
    publish_origin  ---/
    prewarm_search  (optional, excluded from failure accounting)

Two mechanisms are on show.

`SuccessPolicy` decides what "succeeded" means. Two `SuccessCase`s, one per
publish target: the sale is COMPLETED when either lands, even though the other
failed. `prewarm_search` is listed as `optional`, so its failure never counts —
it fails about half the time and nothing happens. When *both* publishes fail,
no case is satisfied and the workflow fails with
`WORKFLOW_SUCCESS_CASE_NOT_MET`, which is a different and more informative
outcome than "a task failed".

`join='any'` decides when to move. `warm_cache_edge` fires on the first publish
to complete rather than waiting for both, because there is no reason to hold a
cache warm-up for a target that may never answer.
"""

from __future__ import annotations

from typing import Any, Final

from horsies import (
    Horsies,
    OnError,
    SuccessCase,
    SuccessPolicy,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)

from ..app import app
from ..domain import CacheWarm
from ..tasks.promotions import prewarm_search, publish_cdn, publish_origin, warm_cache_edge


class FlashSale(WorkflowDefinition[CacheWarm]):
    """Publish a flash sale to two targets and warm the edge behind it."""

    name = 'flash_sale'
    definition_key = 'hemline.flash_sale.v1'

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        campaign_id: str,
        sku: str,
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec[CacheWarm]:
        """Build a fresh spec for one campaign."""
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
        search = TaskNode(
            fn=prewarm_search,
            kwargs={'campaign_id': campaign_id},
            node_id='prewarm_search',
        )
        warm = TaskNode(
            fn=warm_cache_edge,
            kwargs={'campaign_id': campaign_id},
            waits_for=[cdn, origin],
            join='any',
            node_id='warm_cache_edge',
        )
        return app.workflow(
            name=cls.name,
            tasks=[cdn, origin, search, warm],
            on_error=OnError.FAIL,
            output=warm,
            success_policy=SuccessPolicy(
                cases=[
                    SuccessCase(required=[cdn]),
                    SuccessCase(required=[origin]),
                ],
                optional=[search],
            ),
        )


_CHECK_CAMPAIGN: Final[str] = 'FLASH-CHECK'


@app.workflow_builder(
    cases=[{'campaign_id': _CHECK_CAMPAIGN, 'sku': 'HEM-SKU-0001'}],
)
def build_flash_sale(*, campaign_id: str, sku: str) -> WorkflowSpec[CacheWarm]:
    """Build the flash-sale DAG for one campaign."""
    return FlashSale.build_with(app, campaign_id=campaign_id, sku=sku)
