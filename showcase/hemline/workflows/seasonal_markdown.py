# showcase/hemline/workflows/seasonal_markdown.py
"""Mark a whole range down, then measure what it did.

    update_price[sku 1..n]  -->  sales_rollup

A fan-out with a real join at the end: every price change is an independent
node, and the rollup waits for all of them under the default `join='all'`. One
SKU that will not reprice stops the measurement, which is correct — a rollup
taken halfway through a markdown is worse than no rollup.

Contrast with `catalog_import`, the other fan-out here: that one has no join
node at all, because its chunks genuinely do not need each other. The shape you
choose is a claim about whether the work is related.
"""

from __future__ import annotations

from typing import Any, Final

from horsies import (
    Horsies,
    OnError,
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)

from ..app import app
from ..domain import SalesRollup
from ..tasks.analytics import sales_rollup
from ..tasks.promotions import update_price


class SeasonalMarkdown(WorkflowDefinition[SalesRollup]):
    """Reprice a range in parallel, then roll up the result."""

    name = 'seasonal_markdown'
    definition_key = 'hemline.seasonal_markdown.v1'

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        campaign_id: str,
        skus: list[str],
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec[SalesRollup]:
        """Build a fresh spec with one repricing node per SKU."""
        repricings: list[TaskNode[Any] | SubWorkflowNode[Any]] = [
            TaskNode(
                fn=update_price,
                kwargs={'sku': sku, 'campaign_id': campaign_id},
                node_id=f'update_price_{index:02d}',
            )
            for index, sku in enumerate(skus)
        ]
        rollup = TaskNode(
            fn=sales_rollup,
            kwargs={'window': f'markdown-{campaign_id}'},
            waits_for=repricings,
            node_id='sales_rollup',
        )
        return app.workflow(
            name=cls.name,
            tasks=[*repricings, rollup],
            on_error=OnError.FAIL,
            output=rollup,
        )


_CHECK_SKUS: Final[list[str]] = [f'HEM-SKU-{index:04d}' for index in range(1, 7)]


@app.workflow_builder(
    cases=[{'campaign_id': 'MARKDOWN-CHECK', 'skus': _CHECK_SKUS}],
)
def build_seasonal_markdown(
    *,
    campaign_id: str,
    skus: list[str],
) -> WorkflowSpec[SalesRollup]:
    """Build the markdown DAG."""
    return SeasonalMarkdown.build_with(app, campaign_id=campaign_id, skus=skus)
