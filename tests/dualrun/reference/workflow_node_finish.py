"""The worker's phase 2 of a workflow task, before ``horsies_workflow_node_finish``.

``FinalizeMixin._finalize_workflow_phase`` (``horsies/core/worker/finalize.py``)
ran one session transaction: the engine's ``on_workflow_task_complete`` with
the capacity wake of the task's queue (``COMPLETE_WORKFLOW_TASK_SQL``: the
wake, the workflow row lock, the node write with the result that the worker
encoded, the delete of the phase-2 record; then the failure policy, the
paused guard, promotion and the completion check), then ``COMMIT``. The
parent advances that the completion check queued ran after the commit, one
transaction per level (``_drive_parent_propagations`` with the engine's
``on_subworkflow_complete``).

The engine functions stay in the package: workflow recovery calls
``on_subworkflow_complete``, and ``on_workflow_task_complete`` keeps its
other callers.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskError, TaskResult


async def finish_node(
    session_factory: async_sessionmaker[AsyncSession],
    broker: PostgresBroker,
    *,
    task_id: str,
    task_name: str,
    queue_name: str,
    result: TaskResult[Any, TaskError],
) -> None:
    """The phase-2 transaction of one task, then its parent advances."""
    from horsies.core.workflows.engine import (
        QueueWake,
        on_workflow_task_complete,
        pop_pending_parent_propagations,
    )

    async with session_factory() as session:
        await on_workflow_task_complete(
            session,
            task_id,
            result,
            broker,
            task_name=task_name,
            wake=QueueWake(channel=f'task_queue_{queue_name}', payload='capacity'),
        )
        await session.commit()
        pending = pop_pending_parent_propagations(session)
    await _advance_parents(session_factory, broker, pending)


async def _advance_parents(
    session_factory: async_sessionmaker[AsyncSession],
    broker: PostgresBroker,
    child_workflow_ids: list[str],
) -> None:
    """One transaction per level, while a level queues the next."""
    from horsies.core.workflows.engine import (
        on_subworkflow_complete,
        pop_pending_parent_propagations,
    )

    worklist = list(child_workflow_ids)
    while worklist:
        child_id = worklist.pop(0)
        async with session_factory() as session:
            await on_subworkflow_complete(session, child_id, broker)
            await session.commit()
            worklist.extend(pop_pending_parent_propagations(session))
