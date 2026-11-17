# pyright: reportPrivateUsage=false
"""The worker caller of ``horsies_finish_workflow_success``, before it called the core.

``FinalizeMixin._finish_workflow_success`` (``python/horsies/core/worker/finalize.py``)
ran ``finish_workflow_success`` (``python/horsies/core/worker/workflow_success.py``)
on the worker's SQLAlchemy session factory, one statement in autocommit.
``finish_workflow_success`` below is that method's body. It takes the worker
as its first parameter, so the dual-run rebinding can put it in the place of
the method.
"""

from __future__ import annotations

from typing import assert_never

from horsies.core.lifecycle.commands import CompleteLockedTask
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    LostClaim,
    PhaseTwoOwed,
    SourceStateConflict,
    SuccessApplied,
    TaskAbsent,
)
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.finalize import (
    FinalizeMixin,
    _NodeFinishOwed,
    _Phase1Input,
    _phase2_of_a_replay,
)
from horsies.core.worker.runtime import _FinalizeError
from horsies.core.worker.workflow_success import finish_workflow_success as finish_on_engine
from horsies.core.worker.workflow_success import success_for


async def finish_workflow_success(
    self: FinalizeMixin,
    completion: CompleteLockedTask,
    *,
    child: _Phase1Input,
) -> Result[_NodeFinishOwed | None, _FinalizeError]:
    """Phase 1 of a workflow task's success: one statement in autocommit on
    the worker's session factory. Database errors become a phase-1 error."""
    try:
        outcome = await finish_on_engine(
            self.sf,
            success_for(completion),
        )
    except Exception as exc:
        return Err(
            self._phase1_error(
                completion.task_id,
                'Failed to persist terminal task state',
                exc,
                child,
            )
        )
    match outcome:
        case SuccessApplied(
            phase_two=PhaseTwoOwed(queue_name=queue_name, step=step),
        ):
            return Ok(_NodeFinishOwed(queue_name=queue_name, facts=step))
        case AlreadyApplied(task_id=task_id):
            return _phase2_of_a_replay(
                await self._load_persisted_terminal(task_id),
            )
        case LostClaim() | SourceStateConflict() | TaskAbsent():
            return Ok(None)
        case _ as unreachable:
            assert_never(unreachable)
