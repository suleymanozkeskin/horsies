# pyright: reportPrivateUsage=false
"""The claim-time settle of the worker, before it called the core.

``ClaimMixin._filter_nonrunnable_workflow_tasks``
(``python/horsies/core/worker/claiming.py``) sent
``horsies_workflow_claimed_tasks_settle`` in autocommit on the worker's
session factory, one statement per ``SETTLE_CLAIMED_WORKFLOW_TASKS_BOUND``
workflow tasks. ``filter_nonrunnable_workflow_tasks`` below is that method
body, with the same signature, so the dual-run rebinding can put it in the
place of the method.
"""

from __future__ import annotations

from typing import Any

from horsies.core.utils.db import (
    SingleStatement,
    rows_or_raise,
    run_in_autocommit_result,
)
from horsies.core.worker.claiming import ClaimMixin, blocked_claims_of, claim_time_of
from horsies.core.worker.sql import (
    SETTLE_CLAIMED_WORKFLOW_TASKS_BOUND,
    SETTLE_CLAIMED_WORKFLOW_TASKS_SQL,
)


async def filter_nonrunnable_workflow_tasks(
    self: ClaimMixin, rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Settle the claimed tasks of PAUSED and CANCELLED workflows, and
    return the rows to dispatch, in their order.

    One ``horsies_workflow_claimed_tasks_settle`` per
    ``SETTLE_CLAIMED_WORKFLOW_TASKS_BOUND`` workflow tasks, each in
    autocommit. Under the lock of each workflow row the function reads
    the workflow status again:
    - PAUSED: this worker's claim at its claim generation is abandoned,
      and the node of an abandoned task goes back to READY with no task;
    - CANCELLED: the claim is cancelled, and the node of a cancelled
      task is SKIPPED.
    A task of a PAUSED or CANCELLED workflow is never dispatched, also
    when its move found the row moved on (that row belongs to its
    current owner). A resume that committed before the lock makes the
    workflow RUNNING, and its tasks are dispatched. Plain tasks send
    nothing.

    Raises:
        OutcomeDecodeError: rows outside the function's contract.
        Exception: what the pool and the driver raise; it propagates to
            the main loop. The statements before a failed one stay
            committed; the claims of the failed one are recovered by
            lease expiry (a lost reply may have committed its moves).
    """
    if not rows:
        return rows

    workflow_rows = [
        row for row in rows if bool(row.get('is_workflow_task', True))
    ]
    if not workflow_rows:
        return rows

    blocked_task_ids: set[str] = set()
    for start in range(
        0, len(workflow_rows), SETTLE_CLAIMED_WORKFLOW_TASKS_BOUND,
    ):
        chunk = workflow_rows[start:start + SETTLE_CLAIMED_WORKFLOW_TASKS_BOUND]
        task_ids = [str(row['id']) for row in chunk]
        # The claim generation each row was dispatched from: the moves
        # are fenced to it, so a lapsed-and-re-claimed row of this
        # worker is not ended under its new claim.
        settled = rows_or_raise(
            await run_in_autocommit_result(
                self.sf,
                SingleStatement(
                    statement=SETTLE_CLAIMED_WORKFLOW_TASKS_SQL,
                    parameters={
                        'ids': task_ids,
                        'claimed_ats': [claim_time_of(row) for row in chunk],
                        'worker_id': self.worker_instance_id,
                    },
                ),
            ),
        )
        blocked_task_ids |= blocked_claims_of(task_ids, settled.mappings())

    return [row for row in rows if str(row['id']) not in blocked_task_ids]
