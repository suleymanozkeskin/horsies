# showcase/acme/workflows/daily_report.py
"""The same DAG idea in the other style: `app.workflow()` with `.node()()`.

Every other workflow here is class-based. This one is functional, because the
two styles are worth seeing side by side:

  * `.node(...)` takes the *workflow* options and returns a factory; calling
    the factory takes the *task* kwargs, type-checked against the task
    signature. Two calls, two different kinds of argument.
  * `from_node(upstream)` used as a kwarg value does two things at once — it
    registers the upstream node in `args_from` under that kwarg, and adds it to
    `waits_for` if it is not already there. It is the ergonomic form of what
    the class-based workflows spell out as `args_from={'rollup': rollup}`.

Functional construction is the better fit when node kwargs depend on runtime
values, which is the case here: the report's window and the sweep's age
threshold are parameters of the report being built.

`abandoned_cart_sweep` is the same task the HourlySchedule runs. Its `rollup`
parameter defaults to `None`, so it works standalone; inside this workflow
`from_node` injects the rollup's `TaskResult` and the sweep reports against it.
"""

from __future__ import annotations

from typing import Final

from horsies import OnError, WorkflowSpec, from_node

from .. import tuning
from ..app import app
from ..domain import AbandonedCartSweep
from ..tasks.analytics import abandoned_cart_sweep, sales_rollup
from ..tasks.notify import marketing_blast

DEFINITION_KEY: Final[str] = 'acme.daily_report.v1'


@app.workflow_builder(
    cases=[{'window': 'CHECK', 'older_than_minutes': tuning.ABANDONED_CART_AGE_MINUTES}],
)
def build_daily_report(
    *,
    window: str,
    older_than_minutes: int,
) -> WorkflowSpec[AbandonedCartSweep]:
    """Roll up the day, sweep abandoned carts against it, then mail the segment."""
    rollup = sales_rollup.node()(window=window)
    sweep = abandoned_cart_sweep.node()(
        older_than_minutes=older_than_minutes,
        rollup=from_node(rollup),
    )
    blast = marketing_blast.node(waits_for=[sweep])(segment=f'winback-{window}')

    return app.workflow(
        name='daily_report',
        tasks=[rollup, sweep, blast],
        on_error=OnError.FAIL,
        output=sweep,
        definition_key=DEFINITION_KEY,
    )
