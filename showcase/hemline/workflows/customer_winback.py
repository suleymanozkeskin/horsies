# showcase/hemline/workflows/customer_winback.py
"""Find the abandoned carts, then mail the people who left them.

    abandoned_cart_sweep -> marketing_blast

Two nodes, and the reason it exists as its own definition rather than a branch
of `daily_report` is the `args_from` edge: the blast's recipient count comes
from what the sweep actually found, not from a fixed segment size. The same
`marketing_blast` task runs unparameterised on its daily schedule, which is why
its `sweep` argument carries a default.
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
from ..domain import MarketingBlast
from ..tasks.analytics import abandoned_cart_sweep
from ..tasks.notify import marketing_blast


class CustomerWinback(WorkflowDefinition[MarketingBlast]):
    """Sweep abandoned carts and mail the segment they imply."""

    name = 'customer_winback'
    definition_key = 'hemline.customer_winback.v1'

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        segment: str,
        older_than_minutes: int,
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec[MarketingBlast]:
        """Build a fresh spec for one winback campaign."""
        sweep = TaskNode(
            fn=abandoned_cart_sweep,
            kwargs={'older_than_minutes': older_than_minutes},
            node_id='abandoned_cart_sweep',
        )
        blast = TaskNode(
            fn=marketing_blast,
            kwargs={'segment': segment},
            waits_for=[sweep],
            args_from={'sweep': sweep},
            node_id='marketing_blast',
        )
        return app.workflow(
            name=cls.name,
            tasks=[sweep, blast],
            on_error=OnError.FAIL,
            output=blast,
        )


_CHECK_SEGMENT: Final[str] = 'winback-check'


@app.workflow_builder(
    cases=[
        {
            'segment': _CHECK_SEGMENT,
            'older_than_minutes': tuning.ABANDONED_CART_AGE_MINUTES,
        },
    ],
)
def build_customer_winback(
    *,
    segment: str,
    older_than_minutes: int,
) -> WorkflowSpec[MarketingBlast]:
    """Build the winback DAG."""
    return CustomerWinback.build_with(
        app,
        segment=segment,
        older_than_minutes=older_than_minutes,
    )
