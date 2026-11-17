# pyright: reportPrivateUsage=false
"""The phase 2 and the parent advance of the worker, before they called the core.

``FinalizeMixin._finalize_workflow_phase`` and
``FinalizeMixin._drive_parent_propagations``
(``python/horsies/core/worker/finalize.py``) ran ``finish_node`` and
``advance_parents`` on the worker's SQLAlchemy session factory, with the
children of the owed child starts built in Python. The functions below are
those method bodies, with the same signatures, so the dual-run rebinding can
put them in the place of the production methods.
"""

from __future__ import annotations

from typing import assert_never

from horsies.core.logging import get_logger
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.finalize import (
    _FINALIZE_STAGE_PARENT,
    FinalizeMixin,
    _FinalizeError,
    _NoBrokerError,
    _NodeFinishOwed,
)
from horsies.core.workflows.node_finish import CapacityWake, finish_node
from horsies.core.workflows.steps import (
    NodeFinishDisposition,
    ParentAdvanceFailure,
    advance_parents,
)

logger = get_logger('worker')


async def finalize_workflow_phase(
    self: FinalizeMixin,
    task_id: str,
    owed: _NodeFinishOwed,
) -> Result[None, _FinalizeError]:
    """Phase 2 of a workflow task: ``horsies_workflow_node_finish``.

    ``finish_node`` runs the step in the form of ``owed.facts``: one
    statement in autocommit, or a transaction with the child starts
    that the step owes. The step writes the node from the phase-2
    record, sends the capacity wake of the node's queue (delivered at
    the commit), and runs the failure policy, promotion and the
    completion check. Only a workflow task reaches phase 2: the
    statement that ends a plain task sends its capacity wake.

    A raise, and a disposition that keeps the record (``SOURCE_*``: the
    stored result does not pass its check, nothing written), are a
    phase-2 error; phase-2 recovery applies or quarantines a kept
    record. The parent advances that the step owes run after the
    commit (``_drive_parent_propagations``), outside the phase-2 error:
    a phase-2 retry would find the record gone and could not reach
    them. A worker with no broker (a running worker always has one)
    cannot build child starts: a phase-2 error that is not retryable.
    """
    match self.broker:
        case None:
            return Err(
                self._phase2_error(task_id, owed.queue_name, exc=_NoBrokerError())
            )
        case broker:
            pass
    try:
        committed = await finish_node(
            self.sf, broker, task_id, owed.facts, CapacityWake.SEND,
        )
    except Exception as exc:
        return Err(self._phase2_error(task_id, owed.queue_name, exc=exc))
    match committed.outcome.disposition:
        case NodeFinishDisposition.APPLIED:
            pass
        case (
            NodeFinishDisposition.APPLIED_WORKFLOW_ENDED
            | NodeFinishDisposition.ALREADY_APPLIED
            | NodeFinishDisposition.PENDING_ABSENT
        ):
            logger.debug(
                'Phase 2 of workflow task %s: %s (workflow %s)',
                task_id,
                committed.outcome.disposition.value,
                committed.outcome.workflow_status,
            )
        case (
            NodeFinishDisposition.SOURCE_ABSENT
            | NodeFinishDisposition.SOURCE_STATE_CONFLICT
            | NodeFinishDisposition.SOURCE_VERSION_CONFLICT
            | NodeFinishDisposition.SOURCE_DIGEST_MISMATCH
        ):
            return Err(
                self._kept_record_error(
                    task_id, owed.queue_name, committed.outcome.disposition,
                )
            )
        case _ as unreachable:
            assert_never(unreachable)
    if committed.parent_advances:
        await self._drive_parent_propagations(list(committed.parent_advances))
    return Ok(None)


async def drive_parent_propagations(
    self: FinalizeMixin, child_workflow_ids: list[str],
) -> None:
    """Advance the parent nodes of ended child workflows, one transaction
    per ancestor level.

    ``advance_parents`` runs ``horsies_workflow_child_finish`` with the
    child starts that each level owes, in its own transaction; a level
    that ends its workflow names the next level. Iterative, at most
    ``steps.MAX_PARENT_ADVANCES`` levels, no nested FOR UPDATE locks.

    Total: a level's failure folds into a parent_propagation-stage
    finalize error carrying the advances still owed, the failed level
    first; the bounded retry re-enters the chain from there (a phase-2
    retry could not: the phase-2 record is gone). Workflow recovery
    (a terminal child under a live parent node) is the backstop when
    retries exhaust or the worker dies.
    """
    match self.broker:
        case None:
            await self._handle_finalize_error(
                self._parent_error(child_workflow_ids, _NoBrokerError())
            )
            return
        case broker:
            pass
    match await advance_parents(broker, self.sf, child_workflow_ids):
        case None:
            for child_id in child_workflow_ids:
                self._clear_finalize_retry_attempts(child_id, _FINALIZE_STAGE_PARENT)
        case ParentAdvanceFailure(still_owed=still_owed, cause=cause):
            await self._handle_finalize_error(
                self._parent_error(list(still_owed), cause)
            )
        case _ as unreachable:
            assert_never(unreachable)
