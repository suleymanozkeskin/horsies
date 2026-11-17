"""The workflow step functions that owe child starts, and the typed read of their rows.

A step function of ``0005_workflow_functions.sql`` returns
``SETOF horsies_workflow_step_row``: exactly one ``OUTCOME`` row, one
``CHILD_START`` row per sub-workflow node whose child start the step owes,
and one ``SOURCE`` row per terminal dependency of such a node.

- A step that owes no child start is one statement in autocommit.
- A step that can owe child starts runs in a transaction: ``BEGIN``, the
  step, then for each ``CHILD_START`` row the child build
  (``child_build.build_child``) and ``horsies_workflow_child_start``, then
  ``COMMIT``. The parent node, the child rows and the link commit together.
  A child start can owe further child starts; they run in the same
  transaction, at most ``MAX_WORKFLOW_NODES`` per step. The phase 2 of a
  workflow task chooses its form from the node facts of phase 1
  (``node_finish.finish_node``).
- ``advance_child_workflow_id`` of an ``OUTCOME`` row names a child workflow
  that the step ended: after the commit, ``horsies_workflow_child_finish``
  advances its parent node, one transaction per level
  (``advance_parents``).

A row outside a function's contract raises ``OutcomeDecodeError``.
"""

from __future__ import annotations

import uuid
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Final, LiteralString, assert_never

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from horsies.core.lifecycle.outcomes import OutcomeDecodeError
from horsies.core.logging import get_logger
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow.limits import MAX_SUBWORKFLOW_DEPTH, MAX_WORKFLOW_NODES
from horsies.core.workflows.child_build import (
    ChildBuildFailed,
    ChildBuilt,
    ChildOfNode,
    build_child,
    node_failure_result,
)
from horsies.core.workflows.sql import SubWorkflowParams
from horsies.core.workflows.start_tree import (
    StartTree,
    child_tree,
    no_tree_parameters,
    tree_parameters,
)

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker

logger = get_logger('workflow.engine')

_STEP_ROW_COLUMNS: Final[LiteralString] = (
    'r.row_kind, r.disposition, r.workflow_id, r.workflow_status, '
    'r.advance_child_workflow_id, r.detail, r.task_index, r.node_input, r.args_from, '
    'r.sub_workflow_name, r.sub_definition_key, r.sub_workflow_params, r.parent_depth, '
    'r.root_workflow_id, r.source_task_index, r.source_node_id, r.source_status, '
    'r.source_task_name, r.source_definition_key, r.source_result, r.source_summary, '
    'w.name AS workflow_name'
)
"""The columns of a step row, and the name of the parent workflow of a
``CHILD_START`` row. The node error of a child that cannot start names the
parent workflow by its name. The row type has no such column yet (#1065), so
the statement joins the workflow row, which the step holds and whose name
does not change. The join reads the statement's snapshot: a workflow that the
same statement inserted (a start, a child start) is not visible to it, and
its name comes from the tree that the caller sent (``decode_step_rows``)."""


def _step_statement(call: LiteralString) -> Any:
    return text(
        f'SELECT {_STEP_ROW_COLUMNS} FROM {call} AS r '
        "LEFT JOIN horsies_workflows w ON r.row_kind = 'CHILD_START' AND w.id = r.workflow_id",
    )


_TREE_ARGUMENTS: Final[LiteralString] = """
    CAST(:workflow_ids AS uuid[]), CAST(:names AS text[]), CAST(:definition_keys AS text[]),
    CAST(:on_errors AS text[]), CAST(:output_task_indexes AS integer[]),
    CAST(:success_policies AS text[]), CAST(:parent_workflow_ids AS uuid[]),
    CAST(:parent_task_indexes AS integer[]), CAST(:depths AS integer[])"""

_NODE_ARGUMENTS: Final[LiteralString] = """
    CAST(:node_workflow_ids AS uuid[]), CAST(:node_task_indexes AS integer[]),
    CAST(:node_ids AS text[]), CAST(:node_task_names AS text[]), CAST(:node_inputs AS text[]),
    CAST(:node_queue_names AS text[]), CAST(:node_priorities AS integer[]),
    CAST(:node_dependencies AS text[]), CAST(:node_args_from AS text[]),
    CAST(:node_workflow_ctx_from AS text[]), CAST(:node_allow_failed_deps AS boolean[]),
    CAST(:node_join_types AS text[]), CAST(:node_min_successes AS integer[]),
    CAST(:node_task_options AS text[]), CAST(:node_max_retries AS integer[]),
    CAST(:node_executions AS text[]), CAST(:node_good_untils AS timestamptz[]),
    CAST(:node_retention_class_keys AS text[]), CAST(:node_is_subworkflow AS boolean[]),
    CAST(:node_sub_workflow_names AS text[]), CAST(:node_sub_definition_keys AS text[]),
    CAST(:node_sub_workflow_params AS text[]),
    CAST(:failed_workflow_ids AS uuid[]), CAST(:failed_task_indexes AS integer[]),
    CAST(:failed_results AS text[])"""

START_WORKFLOW_FUNCTION_SQL: Final = _step_statement(
    'horsies_workflow_start(' + _TREE_ARGUMENTS + ', CAST(:sent_at AS timestamptz),'
    + _NODE_ARGUMENTS + ')'
)
"""A workflow tree in one call: the workflow rows, the start call time, the
node rows and the failed-node records."""

CHILD_START_FUNCTION_SQL: Final = _step_statement(
    'horsies_workflow_child_start(CAST(:parent_workflow_id AS uuid), '
    'CAST(:parent_task_index AS integer), CAST(:outcome_kind AS text), '
    'CAST(:failure_result AS text), CAST(:sent_at AS timestamptz),'
    + _TREE_ARGUMENTS + ',' + _NODE_ARGUMENTS + ')'
)
"""The owed child start of one sub-workflow node: its child tree (TREE) or
the error envelope of a child that cannot be built (FAILURE)."""

CHILD_FINISH_FUNCTION_SQL: Final = _step_statement(
    'horsies_workflow_child_finish(CAST(:child_workflow_id AS uuid))'
)
"""The advance of the parent node of a terminal child workflow, one level."""

RESUME_WORKFLOW_FUNCTION_SQL: Final = _step_statement(
    'horsies_workflow_resume(CAST(:wf_id AS uuid))'
)
"""The resume of a PAUSED workflow tree, in the caller's transaction."""

NODE_FINISH_FUNCTION_SQL: Final = _step_statement(
    'horsies_workflow_node_finish(CAST(:task_id AS uuid), CAST(:wake AS boolean), '
    'CAST(:dependents AS text))'
)
"""Phase 2 of a finished workflow task: the node write from its phase-2
record, the capacity wake, the failure policy, promotion and the completion
check."""

PHASE2_RECOVER_FUNCTION_SQL: Final = _step_statement(
    'horsies_workflow_phase2_recover(CAST(:task_id AS uuid), '
    'CAST(:attempt_bound AS integer))'
)
"""Phase-2 recovery of one record, in the caller's transaction: the body of
the node finish with no wake, and one attempt with the quarantine at the
bound for a record that it keeps."""

NODE_REEVALUATE_FUNCTION_SQL: Final = _step_statement(
    'horsies_workflow_node_reevaluate(CAST(:wf_id AS uuid), CAST(:task_index AS integer))'
)
"""Recovery case 0, in the caller's transaction: the join verdict of one
PENDING node of a RUNNING workflow, then the completion check."""

NODES_ENQUEUE_READY_FUNCTION_SQL: Final = _step_statement(
    'horsies_workflow_nodes_enqueue_ready(CAST(:wf_id AS uuid), '
    'CAST(:task_indexes AS integer[]))'
)
"""Recovery cases 1 and 1.5, in the caller's transaction: READY nodes with
no task (task node) or no child (sub-workflow node) move to ENQUEUED, then
the completion check."""

MAX_PARENT_ADVANCES: Final = MAX_SUBWORKFLOW_DEPTH + 1
"""The most parent advance levels of one drive: the levels of a tree, depth
0 to ``MAX_SUBWORKFLOW_DEPTH``."""


class StartDisposition(Enum):
    """The disposition of ``horsies_workflow_start``."""

    STARTED = 'STARTED'
    """The tree was written."""

    EXISTS = 'EXISTS'
    """A workflow has the top id; nothing was written. ``detail``: its stored name."""


class ChildStartDisposition(Enum):
    """The disposition of ``horsies_workflow_child_start``."""

    STARTED = 'STARTED'
    """The child rows, its node rows and the link are written; the child's
    start progression ran."""

    NODE_FAILED = 'NODE_FAILED'
    """The node is FAILED with the error envelope; its progression ran."""

    STALE = 'STALE'
    """The node is not an ENQUEUED sub-workflow node with no child; nothing written."""

    WORKFLOW_NOT_RUNNING = 'WORKFLOW_NOT_RUNNING'
    """The parent workflow is not RUNNING: the node is READY again."""


class ChildFinishDisposition(Enum):
    """The disposition of ``horsies_workflow_child_finish``."""

    APPLIED = 'APPLIED'
    """The parent node was written, then the parent's progression ran."""

    ALREADY_APPLIED = 'ALREADY_APPLIED'
    """The parent node was terminal already; nothing written."""

    CHILD_ABSENT = 'CHILD_ABSENT'
    """No workflow has the child id; nothing written."""

    NOT_A_CHILD = 'NOT_A_CHILD'
    """The workflow has no parent; nothing written."""

    CHILD_NOT_TERMINAL = 'CHILD_NOT_TERMINAL'
    """The child is not terminal; nothing written."""

    PARENT_ABSENT = 'PARENT_ABSENT'
    """The parent workflow row is gone; nothing written."""


class ResumeDisposition(Enum):
    """The disposition of ``horsies_workflow_resume``."""

    RESUMED = 'RESUMED'
    """The workflow and its PAUSED descendants are RUNNING."""

    NOT_PAUSED = 'NOT_PAUSED'
    """The workflow exists and is not PAUSED (``detail``: its status); nothing written."""

    NOT_FOUND = 'NOT_FOUND'
    """No workflow has this id; nothing written."""


class NodeFinishDisposition(Enum):
    """The disposition of ``horsies_workflow_node_finish`` and
    ``horsies_workflow_phase2_recover`` (those of ``horsies_workflow_node_apply``)."""

    APPLIED = 'APPLIED'
    """The node was written and the workflow is live: the failure policy,
    promotion and completion ran."""

    APPLIED_WORKFLOW_ENDED = 'APPLIED_WORKFLOW_ENDED'
    """The node was written; the workflow had ended, so nothing more ran."""

    ALREADY_APPLIED = 'ALREADY_APPLIED'
    """The node was terminal already: nothing written."""

    PENDING_ABSENT = 'PENDING_ABSENT'
    """No phase-2 record, and the node is not terminal: nothing written."""

    SOURCE_ABSENT = 'SOURCE_ABSENT'
    """No history row at the locator and no quarantine copy: the record is kept."""

    SOURCE_STATE_CONFLICT = 'SOURCE_STATE_CONFLICT'
    """The source row belongs to another task: the record is kept."""

    SOURCE_VERSION_CONFLICT = 'SOURCE_VERSION_CONFLICT'
    """The history schema version differs: the record is kept."""

    SOURCE_DIGEST_MISMATCH = 'SOURCE_DIGEST_MISMATCH'
    """The digest of the stored bytes differs: the record is kept."""


class ReevaluateDisposition(Enum):
    """The disposition of ``horsies_workflow_node_reevaluate``."""

    APPLIED = 'APPLIED'
    """The node left PENDING (``detail``: its status)."""

    WAITING = 'WAITING'
    """The join is not met; nothing written."""

    STALE = 'STALE'
    """The node is absent or not PENDING (``detail``: its status); nothing written."""

    NOT_RUNNING = 'NOT_RUNNING'
    """The workflow is not RUNNING (``detail``: its status); nothing written."""

    NOT_FOUND = 'NOT_FOUND'
    """No workflow has this id; nothing written."""


class EnqueueReadyDisposition(Enum):
    """The disposition of ``horsies_workflow_nodes_enqueue_ready``."""

    APPLIED = 'APPLIED'
    """The nodes left READY (``detail``: their indexes, a JSON array): a task
    node with its task row or failed by its build, a sub-workflow node with
    its child start owed or failed."""

    STALE = 'STALE'
    """None of the nodes is READY with no work; nothing written."""

    NOT_RUNNING = 'NOT_RUNNING'
    """The workflow is not RUNNING (``detail``: its status); nothing written."""

    NOT_FOUND = 'NOT_FOUND'
    """No workflow has this id; nothing written."""


@dataclass(frozen=True, slots=True)
class NoParentAdvance:
    """The step ended no child workflow that has a parent."""


@dataclass(frozen=True, slots=True)
class ParentAdvanceOwed:
    """The step ended ``child_workflow_id``; its parent node advances after the commit."""

    child_workflow_id: str


type ParentAdvance = NoParentAdvance | ParentAdvanceOwed


@dataclass(frozen=True, slots=True)
class StepOutcome[D: Enum]:
    """The ``OUTCOME`` row of a step call."""

    disposition: D
    workflow_status: str | None
    """The status of the step's workflow after the step; ``None`` when it is gone."""
    parent_advance: ParentAdvance
    detail: str | None
    """An observed status, a stored name or a quarantine verdict, by function."""


class SourceStatus(Enum):
    """The status of a terminal dependency of an owed child start."""

    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    SKIPPED = 'SKIPPED'


@dataclass(frozen=True, slots=True)
class ChildStartSource:
    """A terminal dependency of an owed child start (a ``SOURCE`` row)."""

    task_index: int
    node_id: str | None
    status: SourceStatus
    task_name: str
    definition_key: str | None
    result: str | None
    """The stored result text, copied; ``None`` when the node stores none."""


@dataclass(frozen=True, slots=True)
class OwedChildStart:
    """A promoted sub-workflow node whose child start the step owes: a
    ``CHILD_START`` row and its ``SOURCE`` rows. The node is ENQUEUED with
    no child."""

    workflow_id: str
    workflow_name: str
    task_index: int
    node_input: str
    args_from: str | None
    sub_definition_key: str | None
    sub_workflow_params: SubWorkflowParams
    parent_depth: int
    sources: tuple[ChildStartSource, ...]


@dataclass(frozen=True, slots=True)
class StepRows[D: Enum]:
    """The rows of one step call."""

    outcome: StepOutcome[D]
    owed_child_starts: tuple[OwedChildStart, ...]


@dataclass(frozen=True, slots=True)
class CommittedStep[D: Enum]:
    """A step that committed with every child start it owed."""

    outcome: StepOutcome[D]
    parent_advances: tuple[str, ...]
    """The child workflows whose parent nodes advance after the commit, each once."""


class ChildStartLimitExceeded(Exception):
    """A step owed more than ``MAX_WORKFLOW_NODES`` child starts, counting
    those that its child starts owed in turn. A defect: a tree has fewer
    nodes. The step's transaction rolls back."""


class ParentChainTooLong(Exception):
    """A drive of parent advances did not end within ``MAX_PARENT_ADVANCES``
    levels. The levels it ran committed; workflow recovery advances the rest."""


def _decode_error(function: str, reason: str) -> OutcomeDecodeError:
    return OutcomeDecodeError(f'{function} returned a row outside its contract: {reason}')


def _uuid_text(value: object, function: str, column: str) -> str:
    """A uuid column as text. The driver gives ``uuid.UUID``, or the text of
    one, by the connection's type map."""
    match value:
        case uuid.UUID():
            return str(value)
        case str():
            try:
                return str(uuid.UUID(value))
            except ValueError:
                raise _decode_error(function, f'{column} is {value!r}; expected a uuid') from None
        case _:
            raise _decode_error(function, f'{column} is {value!r}; expected a uuid')


def _optional_uuid_text(value: object, function: str, column: str) -> str | None:
    match value:
        case None:
            return None
        case _:
            return _uuid_text(value, function, column)


def _text(value: object, function: str, column: str) -> str:
    match value:
        case str():
            return value
        case _:
            raise _decode_error(function, f'{column} is {value!r}; expected text')


def _optional_text(value: object, function: str, column: str) -> str | None:
    match value:
        case None:
            return None
        case _:
            return _text(value, function, column)


def _integer(value: object, function: str, column: str) -> int:
    match value:
        case bool():
            raise _decode_error(function, f'{column} is {value!r}; expected an integer')
        case int():
            return value
        case _:
            raise _decode_error(function, f'{column} is {value!r}; expected an integer')


def _outcome[D: Enum](row: Any, function: str, dispositions: type[D]) -> StepOutcome[D]:
    disposition_text = _text(row.disposition, function, 'disposition')
    try:
        disposition = dispositions(disposition_text)
    except ValueError:
        raise _decode_error(
            function, f'disposition {disposition_text!r} is not one of {dispositions.__name__}',
        ) from None
    advance = _optional_uuid_text(
        row.advance_child_workflow_id, function, 'advance_child_workflow_id',
    )
    return StepOutcome(
        disposition=disposition,
        workflow_status=_optional_text(row.workflow_status, function, 'workflow_status'),
        parent_advance=(
            NoParentAdvance() if advance is None else ParentAdvanceOwed(child_workflow_id=advance)
        ),
        detail=_optional_text(row.detail, function, 'detail'),
    )


def _params_choice(value: object, function: str) -> SubWorkflowParams:
    text_value = _text(value, function, 'sub_workflow_params')
    try:
        return SubWorkflowParams(text_value)
    except ValueError:
        raise _decode_error(
            function, f'sub_workflow_params is {text_value!r}; expected GIVEN or NOT_GIVEN',
        ) from None


def _parent_name(
    row: Any, workflow_id: str, function: str, inserted_names: Mapping[str, str],
) -> str:
    """The name of the parent workflow of a CHILD_START row: the joined name, or
    the name in the tree of a workflow that the statement inserted."""
    match row.workflow_name:
        case None:
            name = inserted_names.get(workflow_id)
            if name is None:
                raise _decode_error(
                    function,
                    f'workflow_name is None for workflow {workflow_id}, which the call '
                    'did not insert',
                )
            return name
        case joined:
            return _text(joined, function, 'workflow_name')


def _child_start(
    row: Any, function: str, inserted_names: Mapping[str, str],
) -> OwedChildStart:
    workflow_id = _uuid_text(row.workflow_id, function, 'workflow_id')
    return OwedChildStart(
        workflow_id=workflow_id,
        workflow_name=_parent_name(row, workflow_id, function, inserted_names),
        task_index=_integer(row.task_index, function, 'task_index'),
        node_input=_text(row.node_input, function, 'node_input'),
        args_from=_optional_text(row.args_from, function, 'args_from'),
        sub_definition_key=_optional_text(row.sub_definition_key, function, 'sub_definition_key'),
        sub_workflow_params=_params_choice(row.sub_workflow_params, function),
        parent_depth=_integer(row.parent_depth, function, 'parent_depth'),
        sources=(),
    )


def _source(row: Any, function: str) -> tuple[str, int, ChildStartSource]:
    status_text = _text(row.source_status, function, 'source_status')
    try:
        status = SourceStatus(status_text)
    except ValueError:
        raise _decode_error(
            function, f'source_status is {status_text!r}; expected a terminal node status',
        ) from None
    return (
        _uuid_text(row.workflow_id, function, 'workflow_id'),
        _integer(row.task_index, function, 'task_index'),
        ChildStartSource(
            task_index=_integer(row.source_task_index, function, 'source_task_index'),
            node_id=_optional_text(row.source_node_id, function, 'source_node_id'),
            status=status,
            task_name=_text(row.source_task_name, function, 'source_task_name'),
            definition_key=_optional_text(
                row.source_definition_key, function, 'source_definition_key',
            ),
            result=_optional_text(row.source_result, function, 'source_result'),
        ),
    )


def decode_step_rows[D: Enum](
    rows: Sequence[Any],
    function: str,
    dispositions: type[D],
    inserted_names: Mapping[str, str] | None = None,
) -> StepRows[D]:
    """The rows of a step function call. Pure. Row order does not matter.

    ``inserted_names`` maps the id of each workflow that the call inserted (the
    tree of a start or of a child start) to its name; ``None`` for a call that
    inserts no workflow.

    Raises:
        OutcomeDecodeError: a row kind outside OUTCOME, CHILD_START and SOURCE;
            not exactly one OUTCOME row; a column that does not have its
            type; a disposition outside ``dispositions``; a SOURCE row whose
            node has no CHILD_START row.
    """
    outcomes: list[StepOutcome[D]] = []
    starts: list[OwedChildStart] = []
    sources: list[tuple[str, int, ChildStartSource]] = []
    for row in rows:
        match row.row_kind:
            case 'OUTCOME':
                outcomes.append(_outcome(row, function, dispositions))
            case 'CHILD_START':
                starts.append(_child_start(row, function, inserted_names or {}))
            case 'SOURCE':
                sources.append(_source(row, function))
            case other:
                raise _decode_error(function, f'row_kind {other!r}')
    by_node: dict[tuple[str, int], list[ChildStartSource]] = {
        (start.workflow_id, start.task_index): [] for start in starts
    }
    for workflow_id, task_index, source in sources:
        attached = by_node.get((workflow_id, task_index))
        if attached is None:
            raise _decode_error(
                function,
                f'a SOURCE row names node {task_index} of workflow {workflow_id}, '
                'which has no CHILD_START row',
            )
        attached.append(source)
    match outcomes:
        case [outcome]:
            return StepRows(
                outcome=outcome,
                owed_child_starts=tuple(
                    OwedChildStart(
                        workflow_id=start.workflow_id,
                        workflow_name=start.workflow_name,
                        task_index=start.task_index,
                        node_input=start.node_input,
                        args_from=start.args_from,
                        sub_definition_key=start.sub_definition_key,
                        sub_workflow_params=start.sub_workflow_params,
                        parent_depth=start.parent_depth,
                        sources=tuple(by_node[(start.workflow_id, start.task_index)]),
                    )
                    for start in starts
                ),
            )
        case _:
            raise _decode_error(function, f'{len(outcomes)} OUTCOME rows; expected one')


def require_no_child_starts[D: Enum](step: StepRows[D], function: str) -> StepOutcome[D]:
    """The outcome of a step that ran alone in autocommit.

    Raises:
        OutcomeDecodeError: the step returned owed child starts. Its form owes
            none, so the rows break its contract; the step committed.
    """
    match step.owed_child_starts:
        case ():
            return step.outcome
        case owed:
            raise _decode_error(
                function,
                f'it ran in autocommit and returned {len(owed)} owed child starts, '
                'which commit only inside a transaction',
            )


def add_parent_advance(advances: list[str], advance: ParentAdvance) -> None:
    """Add the advance that an OUTCOME row names to ``advances``, once."""
    match advance:
        case NoParentAdvance():
            return
        case ParentAdvanceOwed(child_workflow_id=child_workflow_id):
            if child_workflow_id not in advances:
                advances.append(child_workflow_id)


# ---------------------------------------------------------------------------
# Child starts
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TreeChildStart:
    """The child built: its tree."""

    tree: StartTree


@dataclass(frozen=True, slots=True)
class FailureChildStart:
    """The child cannot start: the node's error envelope."""

    failure_result: str


type ChildStartKind = TreeChildStart | FailureChildStart


def source_results(
    broker: PostgresBroker, owed: OwedChildStart,
) -> dict[int, TaskResult[Any, TaskError]]:
    """The result of each terminal dependency of ``owed``, by index. Does no I/O."""
    from horsies.core.workflows.engine import terminal_dependency_result

    return {
        source.task_index: terminal_dependency_result(
            broker.app,
            workflow_id=owed.workflow_id,
            task_index=source.task_index,
            node_id=source.node_id,
            status=source.status.value,
            stored_result=source.result,
            task_name=source.task_name,
            sub_definition_key=source.definition_key,
        )
        for source in owed.sources
    }


def child_start_kind(broker: PostgresBroker, owed: OwedChildStart) -> ChildStartKind:
    """What the child start of ``owed`` passes: the built child tree, or the failure.

    Does no I/O; it runs the child's ``build_with``.

    Raises:
        SerializationError: a value of the child tree does not serialize (a
            defect).
    """
    built = build_child(
        broker,
        ChildOfNode(
            workflow_id=owed.workflow_id,
            workflow_name=owed.workflow_name,
            task_index=owed.task_index,
            parent_depth=owed.parent_depth,
            sub_definition_key=owed.sub_definition_key,
            node_input=owed.node_input,
            args_from=owed.args_from,
        ),
        source_results(broker, owed),
    )
    match built:
        case ChildBuilt():
            return TreeChildStart(
                tree=child_tree(broker, built, owed.workflow_id, owed.task_index),
            )
        case ChildBuildFailed(error=error):
            return FailureChildStart(failure_result=node_failure_result(error))


def child_start_parameters(
    owed: OwedChildStart, kind: ChildStartKind, sent_at: datetime,
) -> dict[str, object]:
    """The parameters of ``horsies_workflow_child_start``. Pure apart from serialization."""
    node = {'parent_workflow_id': owed.workflow_id, 'parent_task_index': owed.task_index}
    match kind:
        case TreeChildStart(tree=tree):
            return {
                **node,
                'outcome_kind': 'TREE',
                'failure_result': None,
                'sent_at': sent_at,
                **tree_parameters(tree),
            }
        case FailureChildStart(failure_result=failure_result):
            return {
                **node,
                'outcome_kind': 'FAILURE',
                'failure_result': failure_result,
                'sent_at': None,
                **no_tree_parameters(),
            }


def _log_child_start(
    owed: OwedChildStart, kind: ChildStartKind, outcome: StepOutcome[ChildStartDisposition],
) -> None:
    match outcome.disposition:
        case ChildStartDisposition.STARTED:
            match kind:
                case TreeChildStart(tree=tree):
                    logger.info(
                        f'Started child workflow {tree.top_workflow_id} for '
                        f'{owed.workflow_name}:{owed.task_index}',
                    )
                case FailureChildStart():
                    raise _decode_error(
                        'horsies_workflow_child_start', 'STARTED for a FAILURE call',
                    )
        case ChildStartDisposition.NODE_FAILED:
            logger.warning(
                f'Sub-workflow node {owed.workflow_name}:{owed.task_index} failed: '
                'its child could not be started',
            )
        case ChildStartDisposition.STALE:
            logger.debug(
                f'Child start of {owed.workflow_id}:{owed.task_index} found the node '
                f'{outcome.detail}; nothing written',
            )
        case ChildStartDisposition.WORKFLOW_NOT_RUNNING:
            logger.info(
                f'Child start of {owed.workflow_id}:{owed.task_index} found the workflow '
                f'{outcome.detail}; the node is READY again',
            )
        case _ as unreachable:
            assert_never(unreachable)


def _inserted_names(kind: ChildStartKind) -> dict[str, str]:
    match kind:
        case TreeChildStart(tree=tree):
            return tree.workflow_names()
        case FailureChildStart():
            return {}


async def run_child_starts(
    session: AsyncSession, broker: PostgresBroker, owed: Sequence[OwedChildStart],
) -> tuple[str, ...]:
    """Run the owed child starts of a step in its open transaction, in row order,
    then the child starts that each owes in turn. Does not commit.

    Returns the child workflows whose parent nodes advance after the commit.

    Raises:
        ChildStartLimitExceeded: more than ``MAX_WORKFLOW_NODES`` child starts.
        OutcomeDecodeError: a row outside the contract of a child start.
        SerializationError: as ``child_start_kind``.
        Exception: what the driver raises for a statement. After a raise the
            child starts before it are written in the open transaction; the
            caller rolls it back.
    """
    pending: deque[OwedChildStart] = deque(owed)
    taken = 0
    advances: list[str] = []
    while pending:
        if taken >= MAX_WORKFLOW_NODES:
            raise ChildStartLimitExceeded(
                f'a workflow step owed more than {MAX_WORKFLOW_NODES} child starts '
                f'(next: workflow {pending[0].workflow_id} node {pending[0].task_index})',
            )
        taken += 1
        start = pending.popleft()
        kind = child_start_kind(broker, start)
        rows = (
            await session.execute(
                CHILD_START_FUNCTION_SQL,
                child_start_parameters(start, kind, datetime.now(timezone.utc)),
            )
        ).fetchall()
        step = decode_step_rows(
            rows, 'horsies_workflow_child_start', ChildStartDisposition, _inserted_names(kind),
        )
        _log_child_start(start, kind, step.outcome)
        add_parent_advance(advances, step.outcome.parent_advance)
        pending.extend(step.owed_child_starts)
    return tuple(advances)


async def run_in_transaction[D: Enum](
    session: AsyncSession,
    broker: PostgresBroker,
    statement: Any,
    parameters: dict[str, object],
    function: str,
    dispositions: type[D],
    inserted_names: Mapping[str, str] | None = None,
) -> CommittedStep[D]:
    """Run one step and its owed child starts in the session's transaction, then commit.

    ``inserted_names``: as ``decode_step_rows``.

    Raises:
        OutcomeDecodeError, ChildStartLimitExceeded, SerializationError: as
            ``run_child_starts``; nothing commits.
        Exception: what the driver raises; nothing commits, or the outcome of
            the COMMIT is unknown.
    """
    rows = (await session.execute(statement, parameters)).fetchall()
    step = decode_step_rows(rows, function, dispositions, inserted_names)
    advances: list[str] = []
    add_parent_advance(advances, step.outcome.parent_advance)
    for child_workflow_id in await run_child_starts(session, broker, step.owed_child_starts):
        add_parent_advance(advances, ParentAdvanceOwed(child_workflow_id=child_workflow_id))
    await session.commit()
    return CommittedStep(outcome=step.outcome, parent_advances=tuple(advances))


# ---------------------------------------------------------------------------
# Parent advances
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ParentAdvanceFailure:
    """A parent advance failed at ``failed_child_workflow_id``.

    The levels before it committed; that level wrote nothing, or its commit
    outcome is unknown. ``still_owed`` is every advance a retry runs, the
    failed level first.
    """

    failed_child_workflow_id: str
    still_owed: tuple[str, ...]
    cause: Exception


async def advance_parent(
    broker: PostgresBroker,
    session_factory: async_sessionmaker[AsyncSession],
    child_workflow_id: str,
) -> tuple[str, ...]:
    """One level: ``horsies_workflow_child_finish`` with its owed child starts, in
    one transaction. Returns the advances that the level owes in turn.

    Raises:
        Exception: as ``run_in_transaction``; the level wrote nothing, or its
            commit outcome is unknown.
    """
    async with session_factory() as session:
        committed = await run_in_transaction(
            session,
            broker,
            CHILD_FINISH_FUNCTION_SQL,
            {'child_workflow_id': child_workflow_id},
            'horsies_workflow_child_finish',
            ChildFinishDisposition,
        )
    match committed.outcome.disposition:
        case ChildFinishDisposition.APPLIED:
            pass
        case (
            ChildFinishDisposition.ALREADY_APPLIED
            | ChildFinishDisposition.CHILD_ABSENT
            | ChildFinishDisposition.NOT_A_CHILD
            | ChildFinishDisposition.CHILD_NOT_TERMINAL
            | ChildFinishDisposition.PARENT_ABSENT
        ):
            logger.debug(
                f'Parent node of child workflow {child_workflow_id} not advanced '
                f'({committed.outcome.disposition.value}); nothing written',
            )
        case _ as unreachable:
            assert_never(unreachable)
    return committed.parent_advances


async def advance_parents(
    broker: PostgresBroker,
    session_factory: async_sessionmaker[AsyncSession],
    owed: Sequence[str],
) -> ParentAdvanceFailure | None:
    """Advance the parent nodes above the child workflows in ``owed``, one
    transaction per level, while the levels name further ended children.

    The advances that a level owes run before the next advance of the list.
    At most ``MAX_PARENT_ADVANCES`` levels run; one more owed is a
    ``ParentChainTooLong`` failure at that level. Returns ``None`` when every
    level committed. Any ``Exception`` of a level is caught here and named in
    the failure; ``asyncio.CancelledError`` propagates.
    """
    stack: list[str] = list(reversed(owed))
    levels = 0
    while stack:
        child_workflow_id = stack.pop()
        if levels >= MAX_PARENT_ADVANCES:
            return ParentAdvanceFailure(
                failed_child_workflow_id=child_workflow_id,
                still_owed=(child_workflow_id, *reversed(stack)),
                cause=ParentChainTooLong(
                    f'a drive of parent advances did not end within '
                    f'{MAX_PARENT_ADVANCES} levels',
                ),
            )
        levels += 1
        try:
            next_advances = await advance_parent(broker, session_factory, child_workflow_id)
        except Exception as exc:
            return ParentAdvanceFailure(
                failed_child_workflow_id=child_workflow_id,
                still_owed=(child_workflow_id, *reversed(stack)),
                cause=exc,
            )
        stack.extend(reversed(next_advances))
    return None
