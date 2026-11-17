"""Phase 2 of a finished workflow task: ``horsies_workflow_node_finish``.

The function writes the node from the phase-2 record of its task (the result
bytes of the history row), deletes the record, sends the capacity wake of
the node's queue, runs the failure policy, promotes the dependents (task
rows minted; a promoted sub-workflow node owes its child start) and runs the
completion check.

The form of the call follows the node facts that phase 1 read after its
move (``PhaseTwoStep``):

- ``NO_DEPENDENTS``: one statement in autocommit, dependents ``'NONE'``
  (the function reads no dependents).
- ``DEPENDENTS_WITHOUT_SUBWORKFLOW``: one statement in autocommit,
  dependents ``'READ'``. No sub-workflow node is below the node, so the step
  owes no child start.
- ``SUBWORKFLOW_REACHABLE``: ``BEGIN``, the call with ``'READ'``, one
  ``horsies_workflow_child_start`` per ``CHILD_START`` row, ``COMMIT``
  (``steps.run_in_transaction``).
- A replay of a committed terminal row and a phase-2 retry do not have the
  facts (``FactsUnknown``): the transaction form.

The step can end a child workflow; the parent node of that child advances
after the commit, one transaction per level (``steps.advance_parents``),
which the caller drives.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Final, assert_never

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from horsies.core.lifecycle.outcomes import PhaseTwoStep
from horsies.core.utils.db import autocommit_connection
from horsies.core.workflows.steps import (
    NODE_FINISH_FUNCTION_SQL,
    CommittedStep,
    NodeFinishDisposition,
    add_parent_advance,
    decode_step_rows,
    require_no_child_starts,
    run_in_transaction,
)

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker

NODE_FINISH_FUNCTION: Final = 'horsies_workflow_node_finish'


@dataclass(frozen=True, slots=True)
class FactsUnknown:
    """A replay of a committed terminal row, or a phase-2 retry: the node
    facts that phase 1 read are not at hand."""


type NodeFinishFacts = PhaseTwoStep | FactsUnknown
"""What the caller knows of the node: the phase-1 facts, or nothing."""


class CapacityWake(Enum):
    """Whether the step sends the capacity wake of the node's queue."""

    SEND = 'SEND'
    """``pg_notify('task_queue_<queue>', 'capacity')``, delivered at the
    commit. A step that finds no phase-2 record sends none."""

    NONE = 'NONE'
    """No wake."""


class _Dependents(Enum):
    """The ``p_dependents`` argument of the function."""

    NONE = 'NONE'
    """No node depends on this node: the function reads no dependents."""

    READ = 'READ'
    """The function reads the dependents."""


def _wake_flag(wake: CapacityWake) -> bool:
    match wake:
        case CapacityWake.SEND:
            return True
        case CapacityWake.NONE:
            return False
        case _ as unreachable:
            assert_never(unreachable)


def _parameters(
    task_id: str, dependents: _Dependents, wake: CapacityWake,
) -> dict[str, object]:
    """The parameters of ``NODE_FINISH_FUNCTION_SQL``. Pure."""
    return {'task_id': task_id, 'wake': _wake_flag(wake), 'dependents': dependents.value}


async def _one_statement(
    session: AsyncSession, task_id: str, dependents: _Dependents, wake: CapacityWake,
) -> CommittedStep[NodeFinishDisposition]:
    """The step alone in autocommit: it commits when the statement ends.

    Raises:
        OutcomeDecodeError: a row outside the contract, or an owed child
            start; the step committed.
        Exception: what the driver raises; nothing committed, or the outcome
            is unknown when the connection broke.
    """
    connection = await autocommit_connection(session)
    rows = (
        await connection.execute(
            NODE_FINISH_FUNCTION_SQL, _parameters(task_id, dependents, wake),
        )
    ).fetchall()
    outcome = require_no_child_starts(
        decode_step_rows(rows, NODE_FINISH_FUNCTION, NodeFinishDisposition),
        NODE_FINISH_FUNCTION,
    )
    advances: list[str] = []
    add_parent_advance(advances, outcome.parent_advance)
    return CommittedStep(outcome=outcome, parent_advances=tuple(advances))


async def finish_node(
    session_factory: async_sessionmaker[AsyncSession],
    broker: PostgresBroker,
    task_id: str,
    facts: NodeFinishFacts,
    wake: CapacityWake,
) -> CommittedStep[NodeFinishDisposition]:
    """Run the phase 2 of ``task_id`` in the form that ``facts`` gives, and
    return the committed outcome with the parent advances it owes.

    Does not run the parent advances. A retaining disposition
    (``SOURCE_*``) wrote nothing and keeps the record for phase-2 recovery.

    Raises:
        OutcomeDecodeError: a row outside the contract. In the autocommit
            form the step committed; in the transaction form nothing commits.
        ChildStartLimitExceeded, SerializationError: as
            ``steps.run_in_transaction``; nothing commits.
        Exception: what the pool or the driver raises. Nothing committed,
            or the outcome is unknown: the connection broke after the
            statement went out (autocommit form), or the COMMIT reply was
            lost (transaction form).
    """
    async with session_factory() as session:
        match facts:
            case PhaseTwoStep.NO_DEPENDENTS:
                return await _one_statement(session, task_id, _Dependents.NONE, wake)
            case PhaseTwoStep.DEPENDENTS_WITHOUT_SUBWORKFLOW:
                return await _one_statement(session, task_id, _Dependents.READ, wake)
            case PhaseTwoStep.SUBWORKFLOW_REACHABLE | FactsUnknown():
                return await run_in_transaction(
                    session,
                    broker,
                    NODE_FINISH_FUNCTION_SQL,
                    _parameters(task_id, _Dependents.READ, wake),
                    NODE_FINISH_FUNCTION,
                    NodeFinishDisposition,
                )
            case _ as unreachable:
                assert_never(unreachable)
