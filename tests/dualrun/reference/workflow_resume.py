"""Workflow resume, before ``horsies_workflow_resume``.

``resume_workflow`` (``python/horsies/core/workflows/lifecycle.py``) sent, in one
session transaction: the root PAUSED to RUNNING; on no row, the existence
read; else, for the root, the re-evaluation of each PENDING node
(``try_make_ready_and_enqueue``) and the enqueue of each READY node
(``enqueue_workflow_task`` or ``enqueue_subworkflow_task``, with the
dependency results); then, breadth first, the same for each PAUSED child
(``cascade_resume_to_children``) with its completion check; the completion
check of the root; the drain of the parent advances. After the commit, the
recovery pass of the tree before its redesign
(``tests/dualrun/reference/workflow_recovery_pass.py``).

The engine functions that the resume called stay in the package (workflow
recovery calls them); this module keeps the resume's own statements and its
order.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Final, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from horsies.core.brokers.postgres import PostgresBroker

RESUME_WORKFLOW_SQL: Final = text("""
    UPDATE horsies_workflows
    SET status = 'RUNNING', updated_at = NOW()
    WHERE id = :wf_id AND status = 'PAUSED'
    RETURNING id, depth, root_workflow_id
""")

CHECK_WORKFLOW_EXISTS_SQL: Final = text(
    """SELECT id FROM horsies_workflows WHERE id = :wf_id"""
)

GET_PENDING_WORKFLOW_TASKS_SQL: Final = text("""
    SELECT task_index FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id AND status = 'PENDING'
""")

GET_READY_WORKFLOW_TASKS_SQL: Final = text("""
    SELECT task_index, dependencies, is_subworkflow FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id AND status = 'READY'
""")

GET_PAUSED_CHILD_WORKFLOWS_SQL: Final = text("""
    SELECT id, depth, root_workflow_id FROM horsies_workflows
    WHERE parent_workflow_id = :wf_id AND status = 'PAUSED'
""")

RESUME_CHILD_WORKFLOW_SQL: Final = text("""
    UPDATE horsies_workflows
    SET status = 'RUNNING', updated_at = NOW()
    WHERE id = :wf_id AND status = 'PAUSED'
""")


@dataclass(frozen=True, slots=True)
class Resumed:
    """The workflow was PAUSED and is RUNNING now."""


@dataclass(frozen=True, slots=True)
class NotPaused:
    """The workflow exists and was not PAUSED. Nothing was written."""


@dataclass(frozen=True, slots=True)
class NotFound:
    """No workflow has the id. Nothing was written."""


type ResumeReport = Resumed | NotPaused | NotFound


async def _resume_nodes(
    session: AsyncSession,
    broker: PostgresBroker,
    workflow_id: str,
    depth: int,
    root_workflow_id: str,
) -> None:
    """The PENDING re-evaluation and the READY enqueue of one resumed workflow."""
    from horsies.core.workflows.engine import (
        enqueue_subworkflow_task,
        enqueue_workflow_task,
        get_dependency_results,
        try_make_ready_and_enqueue,
    )

    pending = (await session.execute(GET_PENDING_WORKFLOW_TASKS_SQL, {'wf_id': workflow_id})).fetchall()
    for row in pending:
        await try_make_ready_and_enqueue(
            session, broker, workflow_id, row.task_index, depth, root_workflow_id,
        )
    ready = (await session.execute(GET_READY_WORKFLOW_TASKS_SQL, {'wf_id': workflow_id})).fetchall()
    for task_index, dependencies, is_subworkflow in ready:
        indexes: list[int] = (
            cast(list[int], dependencies) if isinstance(dependencies, list) else []
        )
        results, names, keys = await get_dependency_results(
            session, workflow_id, indexes, app=broker.app,
        )
        if is_subworkflow:
            await enqueue_subworkflow_task(
                session,
                broker,
                workflow_id,
                task_index,
                all_dep_results=results,
                all_dep_task_names=names,
                parent_depth=depth,
                root_workflow_id=root_workflow_id,
            )
        else:
            await enqueue_workflow_task(
                session,
                workflow_id,
                task_index,
                all_dep_results=results,
                all_dep_task_names=names,
                broker=broker,
                all_dep_definition_keys=keys,
            )


async def _cascade(session: AsyncSession, broker: PostgresBroker, workflow_id: str) -> None:
    from horsies.core.workflows.engine import check_workflow_completion

    queue: deque[str] = deque([workflow_id])
    while queue:
        current = queue.popleft()
        children = (
            await session.execute(GET_PAUSED_CHILD_WORKFLOWS_SQL, {'wf_id': current})
        ).fetchall()
        for child in children:
            child_id = str(child.id)
            await session.execute(RESUME_CHILD_WORKFLOW_SQL, {'wf_id': child_id})
            await _resume_nodes(
                session,
                broker,
                child_id,
                child.depth or 0,
                str(child.root_workflow_id or child_id),
            )
            await check_workflow_completion(session, child_id, broker)
            queue.append(child_id)


async def resume(
    session_factory: async_sessionmaker[AsyncSession],
    broker: PostgresBroker,
    workflow_id: str,
) -> ResumeReport:
    """The resume of ``workflow_id`` and its tree, in one transaction, then the recovery pass."""
    from horsies.core.workflows.engine import (
        check_workflow_completion,
        drain_parent_propagations_in_session,
    )
    from tests.dualrun.reference.workflow_recovery_pass import (
        GET_WORKFLOW_TREE_IDS_SQL,
        recover_stuck_workflow_tree,
    )

    async with session_factory() as session:
        row = (await session.execute(RESUME_WORKFLOW_SQL, {'wf_id': workflow_id})).fetchone()
        if row is None:
            exists = (
                await session.execute(CHECK_WORKFLOW_EXISTS_SQL, {'wf_id': workflow_id})
            ).fetchone()
            match exists:
                case None:
                    return NotFound()
                case _:
                    return NotPaused()
        await _resume_nodes(
            session,
            broker,
            workflow_id,
            row.depth or 0,
            str(row.root_workflow_id or workflow_id),
        )
        await _cascade(session, broker, workflow_id)
        await check_workflow_completion(session, workflow_id, broker)
        await drain_parent_propagations_in_session(session, broker)
        await session.commit()
    async with session_factory() as recovery_session:
        tree = (
            await recovery_session.execute(GET_WORKFLOW_TREE_IDS_SQL, {'wf_id': workflow_id})
        ).fetchall()
        tree_ids: list[Any] = [tree_row.id for tree_row in tree]
        await recover_stuck_workflow_tree(recovery_session, broker, tree_ids)
        await recovery_session.commit()
    return Resumed()
